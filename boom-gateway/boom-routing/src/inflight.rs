use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Per-model in-flight metrics.
#[derive(Debug)]
struct InFlightMetrics {
    request_count: AtomicU64,
    input_chars: AtomicU64,
}

/// Tracks in-flight requests per model.
///
/// Lives at `AppState` level — survives config reloads.
/// Used by scheduling policies for load-aware routing and exposed
/// via the dashboard for operational visibility.
#[derive(Debug)]
pub struct InFlightTracker {
    metrics: DashMap<String, InFlightMetrics>,
}

/// Snapshot of in-flight stats for one model.
#[derive(Debug)]
pub struct InFlightStat {
    pub model: String,
    pub inflight_requests: u64,
    pub inflight_input_chars: u64,
}

/// RAII guard that decrements in-flight metrics on Drop.
///
/// Create via [`InFlightGuard::new`] before forwarding a request;
/// the counters are released automatically whether the request
/// succeeds, fails, or the stream is dropped mid-way.
pub struct InFlightGuard {
    tracker: Arc<InFlightTracker>,
    model: String,
    input_chars: u64,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self {
            metrics: DashMap::new(),
        }
    }

    /// Current in-flight stats for all models with active requests.
    pub fn get_stats(&self) -> Vec<InFlightStat> {
        self.metrics
            .iter()
            .filter(|r| r.value().request_count.load(Ordering::Relaxed) > 0)
            .map(|r| InFlightStat {
                model: r.key().clone(),
                inflight_requests: r.value().request_count.load(Ordering::Relaxed),
                inflight_input_chars: r.value().input_chars.load(Ordering::Relaxed),
            })
            .collect()
    }
}

impl Default for InFlightTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl InFlightGuard {
    /// Acquire an in-flight slot. The returned guard decrements the
    /// counters when dropped, so it **must** live until the request completes.
    ///
    /// For streaming responses, move the guard into a wrapper stream so it
    /// is dropped when the stream finishes (or the connection is torn down).
    pub fn new(tracker: Arc<InFlightTracker>, model: &str, input_chars: u64) -> Self {
        {
            let metrics = tracker
                .metrics
                .entry(model.to_string())
                .or_insert_with(|| InFlightMetrics {
                    request_count: AtomicU64::new(0),
                    input_chars: AtomicU64::new(0),
                });
            metrics.request_count.fetch_add(1, Ordering::Relaxed);
            metrics.input_chars.fetch_add(input_chars, Ordering::Relaxed);
        }
        // metrics ref dropped here — safe to move tracker.

        Self {
            tracker,
            model: model.to_string(),
            input_chars,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(metrics) = self.tracker.metrics.get(&self.model) {
            metrics.request_count.fetch_sub(1, Ordering::Relaxed);
            metrics.input_chars.fetch_sub(self.input_chars, Ordering::Relaxed);
        }
    }
}
