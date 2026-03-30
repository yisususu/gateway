use boom_core::provider::RateLimiter;
use boom_core::types::{RateLimitDecision, RateLimitKey};
use boom_core::GatewayError;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;

/// In-memory sliding window rate limiter.
///
/// Uses DashMap for lock-free concurrent access.
/// Each (key, model, window) combination tracks its own counter.
///
/// Supports:
/// - RPM (requests per minute) — standard per-minute sliding window.
/// - Custom time windows — e.g. 100 requests per 5 hours (18000 seconds).
pub struct SlidingWindowLimiter {
    /// Window counters: key → (count, window_start_instant).
    windows: Arc<DashMap<String, WindowCounter>>,
}

#[derive(Debug, Clone)]
struct WindowCounter {
    count: u64,
    window_start: Instant,
    window_secs: u64,
}

impl SlidingWindowLimiter {
    pub fn new() -> Self {
        Self {
            windows: Arc::new(DashMap::new()),
        }
    }

    /// Build the internal cache key from rate limit key and window duration.
    fn cache_key(key: &RateLimitKey, window_secs: u64) -> String {
        format!("{}:{}:{}", key.key_hash, key.model, window_secs)
    }

    /// Check a single window limit. Returns (allowed, current_count, limit, reset_at).
    fn check_window(
        &self,
        cache_key: &str,
        limit: u64,
        window_secs: u64,
    ) -> (bool, u64, u64, chrono::DateTime<chrono::Utc>) {
        let now = Instant::now();

        let allowed = match self.windows.get(cache_key) {
            Some(counter) => {
                let elapsed = now.duration_since(counter.window_start).as_secs();
                if elapsed >= counter.window_secs {
                    // Window expired — reset.
                    true
                } else {
                    counter.count < limit
                }
            }
            None => true,
        };

        // Get or create counter to read current count.
        let current_count = if allowed {
            // Atomically increment.
            let counter = self
                .windows
                .entry(cache_key.to_string())
                .and_modify(|c| {
                    let elapsed = Instant::now().duration_since(c.window_start).as_secs();
                    if elapsed >= c.window_secs {
                        // Reset window.
                        c.count = 1;
                        c.window_start = Instant::now();
                    } else {
                        c.count += 1;
                    }
                })
                .or_insert(WindowCounter {
                    count: 1,
                    window_start: Instant::now(),
                    window_secs,
                });
            counter.count
        } else {
            self.windows
                .get(cache_key)
                .map(|c| c.count)
                .unwrap_or(0)
        };

        // Calculate reset time.
        let reset_at = match self.windows.get(cache_key) {
            Some(counter) => {
                let elapsed = Instant::now().duration_since(counter.window_start);
                let remaining = counter.window_secs.saturating_sub(elapsed.as_secs());
                chrono::Utc::now() + chrono::Duration::seconds(remaining as i64)
            }
            None => chrono::Utc::now() + chrono::Duration::seconds(window_secs as i64),
        };

        (allowed, current_count, limit, reset_at)
    }
}

impl Default for SlidingWindowLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RateLimiter for SlidingWindowLimiter {
    async fn check_and_record(
        &self,
        key: &RateLimitKey,
        rpm_limit: Option<u64>,
        window_limits: &[(u64, u64)],
    ) -> Result<RateLimitDecision, GatewayError> {
        // 1. Check RPM (per-minute) if configured.
        if let Some(rpm) = rpm_limit {
            let rpm_key = Self::cache_key(key, 60);
            let (allowed, count, limit, reset_at) = self.check_window(&rpm_key, rpm, 60);

            if !allowed {
                let elapsed = self
                    .windows
                    .get(&rpm_key)
                    .map(|c| Instant::now().duration_since(c.window_start).as_secs())
                    .unwrap_or(0);
                let retry_after = 60u64.saturating_sub(elapsed);

                return Ok(RateLimitDecision {
                    allowed: false,
                    remaining: 0,
                    limit,
                    reset_at,
                    retry_after_secs: Some(retry_after),
                });
            }
        }

        // 2. Check custom time windows.
        for &(limit, window_secs) in window_limits {
            let win_key = Self::cache_key(key, window_secs);
            let (allowed, count, _, reset_at) = self.check_window(&win_key, limit, window_secs);

            if !allowed {
                let elapsed = self
                    .windows
                    .get(&win_key)
                    .map(|c| Instant::now().duration_since(c.window_start).as_secs())
                    .unwrap_or(0);
                let retry_after = window_secs.saturating_sub(elapsed);

                return Ok(RateLimitDecision {
                    allowed: false,
                    remaining: 0,
                    limit,
                    reset_at,
                    retry_after_secs: Some(retry_after),
                });
            }
        }

        // 3. All checks passed.
        let rpm_remaining = rpm_limit
            .map(|rpm| {
                let rpm_key = Self::cache_key(key, 60);
                self.windows
                    .get(&rpm_key)
                    .map(|c| rpm.saturating_sub(c.count))
                    .unwrap_or(rpm)
            })
            .unwrap_or(u64::MAX);

        Ok(RateLimitDecision {
            allowed: true,
            remaining: rpm_remaining,
            limit: rpm_limit.unwrap_or(0),
            reset_at: chrono::Utc::now() + chrono::Duration::seconds(60),
            retry_after_secs: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rpm_limit() {
        let limiter = SlidingWindowLimiter::new();
        let key = RateLimitKey {
            key_hash: "test_key".to_string(),
            model: "gpt-4".to_string(),
        };

        // Should allow up to 3 RPM.
        for _ in 0..3 {
            let decision = limiter.check_and_record(&key, Some(3), &[]).await.unwrap();
            assert!(decision.allowed);
        }

        // 4th request should be rejected.
        let decision = limiter.check_and_record(&key, Some(3), &[]).await.unwrap();
        assert!(!decision.allowed);
        assert!(decision.retry_after_secs.is_some());
    }

    #[tokio::test]
    async fn test_custom_window() {
        let limiter = SlidingWindowLimiter::new();
        let key = RateLimitKey {
            key_hash: "test_key2".to_string(),
            model: "gpt-4".to_string(),
        };

        // Allow 2 requests per 5 hours (18000 seconds).
        let windows = vec![(2u64, 18000u64)];

        let decision = limiter
            .check_and_record(&key, None, &windows)
            .await
            .unwrap();
        assert!(decision.allowed);

        let decision = limiter
            .check_and_record(&key, None, &windows)
            .await
            .unwrap();
        assert!(decision.allowed);

        let decision = limiter
            .check_and_record(&key, None, &windows)
            .await
            .unwrap();
        assert!(!decision.allowed);
    }
}
