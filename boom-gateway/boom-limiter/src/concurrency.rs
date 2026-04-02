use chrono::Timelike;
use dashmap::DashMap;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

/// A rate limit plan definition.
///
/// Plans define concurrency and sliding-window limits that can be
/// assigned to API keys via the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitPlan {
    pub name: String,
    #[serde(default)]
    pub concurrency_limit: Option<u32>,
    #[serde(default)]
    pub rpm_limit: Option<u64>,
    #[serde(default)]
    pub window_limits: Vec<(u64, u64)>,
    /// Optional time-based schedule overrides.
    #[serde(default)]
    pub schedule: Vec<ScheduleSlot>,
}

impl RateLimitPlan {
    /// Return the effective limits for the current time.
    ///
    /// If a schedule slot matches the current local time, its limits are used;
    /// otherwise, the plan's base limits are returned.
    pub fn effective_limits(&self) -> (Option<u32>, Option<u64>, Vec<(u64, u64)>) {
        for slot in &self.schedule {
            if slot.is_active_now() {
                return (
                    slot.concurrency_limit,
                    slot.rpm_limit,
                    slot.window_limits.clone(),
                );
            }
        }
        (
            self.concurrency_limit,
            self.rpm_limit,
            self.window_limits.clone(),
        )
    }
}

/// A time-based schedule slot within a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleSlot {
    /// Time range, e.g. "9:00-21:00" or "21:00-9:00" (cross-midnight).
    pub hours: String,
    #[serde(default)]
    pub concurrency_limit: Option<u32>,
    #[serde(default)]
    pub rpm_limit: Option<u64>,
    #[serde(default)]
    pub window_limits: Vec<(u64, u64)>,
}

impl ScheduleSlot {
    /// Check whether this slot is currently active (server local time).
    pub fn is_active_now(&self) -> bool {
        let (start_min, end_min) = match parse_hours(&self.hours) {
            Some(pair) => pair,
            None => return false,
        };
        let now = chrono::Local::now();
        let current_min = now.hour() * 60 + now.minute();
        if start_min <= end_min {
            // Same day: e.g. 9:00-21:00
            current_min >= start_min && current_min < end_min
        } else {
            // Cross-midnight: e.g. 21:00-9:00 → [21:00, 24:00) ∪ [0:00, 9:00)
            current_min >= start_min || current_min < end_min
        }
    }
}

/// Parse a time range string like "9:00-21:00" into (start_minutes, end_minutes).
fn parse_hours(s: &str) -> Option<(u32, u32)> {
    let (start, end) = s.split_once('-')?;
    Some((parse_hm(start.trim())?, parse_hm(end.trim())?))
}

/// Parse "H:MM" or "HH:MM" into minutes since midnight.
fn parse_hm(s: &str) -> Option<u32> {
    let (h, m) = s.split_once(':')?;
    Some(h.parse::<u32>().ok()? * 60 + m.parse::<u32>().ok()?)
}

/// In-memory store for rate limit plans and key assignments.
/// Survives config reloads.
#[derive(Debug)]
pub struct PlanStore {
    plans: DashMap<String, RateLimitPlan>,
    key_assignments: DashMap<String, String>,
    concurrency_counters: DashMap<String, Arc<AtomicU32>>,
    default_plan_name: std::sync::Mutex<Option<String>>,
}

impl PlanStore {
    pub fn new() -> Self {
        Self {
            plans: DashMap::new(),
            key_assignments: DashMap::new(),
            concurrency_counters: DashMap::new(),
            default_plan_name: std::sync::Mutex::new(None),
        }
    }

    /// Set the default plan name (called during config load / reload).
    pub fn set_default_plan(&self, name: Option<String>) {
        let mut guard = self.default_plan_name.lock().unwrap();
        *guard = name;
    }

    /// Get the default plan (if configured and the plan actually exists).
    pub fn get_default_plan(&self) -> Option<RateLimitPlan> {
        let name = self.default_plan_name.lock().unwrap().clone()?;
        self.plans.get(&name).map(|r| r.value().clone())
    }

    /// Resolve the plan assigned to a key.
    pub fn resolve_plan(&self, key_hash: &str) -> Option<RateLimitPlan> {
        let plan_name = self.key_assignments.get(key_hash)?;
        let plan = self.plans.get(plan_name.value())?;
        Some(plan.value().clone())
    }

    /// Try to acquire a concurrency slot for a key.
    /// Returns a guard that decrements on drop, or None if limit exceeded.
    pub fn try_acquire(&self, key_hash: &str, limit: u32) -> Option<ConcurrencyGuard> {
        let counter_ref = self
            .concurrency_counters
            .entry(key_hash.to_string())
            .or_insert_with(|| Arc::new(AtomicU32::new(0)));

        let counter = counter_ref.value().clone();
        let prev = counter.fetch_add(1, Ordering::Relaxed);

        if prev >= limit {
            // Over limit — roll back.
            counter.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(ConcurrencyGuard { counter })
        }
    }

    // ── Plan CRUD ──────────────────────────────────────────────

    pub fn upsert_plan(&self, plan: RateLimitPlan) {
        let name = plan.name.clone();
        self.plans.insert(name, plan);
    }

    pub fn get_plan(&self, name: &str) -> Option<RateLimitPlan> {
        self.plans.get(name).map(|r| r.value().clone())
    }

    pub fn list_plans(&self) -> Vec<RateLimitPlan> {
        self.plans.iter().map(|r| r.value().clone()).collect()
    }

    /// Delete a plan and remove all key assignments referencing it.
    /// In-flight concurrency counters are untouched — they drain naturally
    /// as guards are dropped.
    pub fn delete_plan(&self, name: &str) -> bool {
        self.key_assignments
            .retain(|_, plan_name| plan_name != name);
        self.plans.remove(name).is_some()
    }

    // ── Key assignment CRUD ────────────────────────────────────

    pub fn assign_key(&self, key_hash: &str, plan_name: &str) -> Result<(), String> {
        if !self.plans.contains_key(plan_name) {
            return Err(format!("Plan '{}' not found", plan_name));
        }
        self.key_assignments
            .insert(key_hash.to_string(), plan_name.to_string());
        Ok(())
    }

    pub fn unassign_key(&self, key_hash: &str) -> bool {
        self.key_assignments.remove(key_hash).is_some()
    }

    pub fn list_assignments(&self) -> Vec<(String, String)> {
        self.key_assignments
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Read the current concurrency count for a key.
    pub fn get_concurrency(&self, key_hash: &str) -> u32 {
        self.concurrency_counters
            .get(key_hash)
            .map(|c| c.value().load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    // ── Persistence helpers ───────────────────────────────────

    /// Snapshot all key→plan assignments for DB persistence.
    pub fn snapshot_assignments(&self) -> Vec<(String, String)> {
        self.key_assignments
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Restore a single key→plan assignment from DB into memory.
    /// Called at startup. Does NOT validate plan existence (plan may be loaded later).
    pub fn restore_assignment(&self, key_hash: &str, plan_name: &str) {
        self.key_assignments
            .insert(key_hash.to_string(), plan_name.to_string());
    }

    /// Remove an assignment from memory only (DB deletion handled separately).
    /// Returns true if the assignment existed.
    pub fn remove_assignment_persisted(&self, key_hash: &str) -> bool {
        self.key_assignments.remove(key_hash).is_some()
    }

    /// Clear all plan definitions and default_plan, but keep key assignments
    /// and concurrency counters intact. Used during hot-reload.
    pub fn clear_plans(&self) {
        self.plans.clear();
        let mut guard = self.default_plan_name.lock().unwrap();
        *guard = None;
    }

    /// Remove assignments pointing to plans that no longer exist.
    /// Call after reloading plans to clean up orphaned entries.
    pub fn cleanup_assignments(&self) {
        self.key_assignments
            .retain(|_, plan_name| self.plans.contains_key(plan_name));
    }

    /// Remove concurrency entries with count==0 to free memory.
    /// Returns the count of removed entries.
    pub fn cleanup_concurrency(&self) -> usize {
        let before = self.concurrency_counters.len();
        self.concurrency_counters.retain(|_, counter| {
            counter.load(Ordering::Relaxed) > 0
        });
        before - self.concurrency_counters.len()
    }
}

impl Default for PlanStore {
    fn default() -> Self {
        Self::new()
    }
}

// ────────────────────────────────────────────────────────────
// ConcurrencyGuard — RAII auto-decrement
// ────────────────────────────────────────────────────────────

/// RAII guard that decrements the concurrency counter on drop.
pub struct ConcurrencyGuard {
    counter: Arc<AtomicU32>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

// ────────────────────────────────────────────────────────────
// GuardedStream — drops guard when stream ends
// ────────────────────────────────────────────────────────────

/// Stream wrapper that holds a concurrency guard.
/// When the stream ends (returns `None`) or is dropped (client disconnect),
/// the guard is released automatically.
pub struct GuardedStream<S> {
    inner: S,
    guard: Option<ConcurrencyGuard>,
}

impl<S> GuardedStream<S> {
    pub fn new(inner: S, guard: Option<ConcurrencyGuard>) -> Self {
        Self {
            inner,
            guard,
        }
    }
}

impl<S: Stream + Unpin> Stream for GuardedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: GuardedStream<S> is Unpin when S: Unpin (all fields are Unpin).
        let this = self.get_mut();

        // Safety: S: Unpin.
        let result = Pin::new(&mut this.inner).poll_next(cx);

        if matches!(result, Poll::Ready(None)) {
            // Stream finished — release the guard.
            this.guard.take();
        }
        result
    }
}
