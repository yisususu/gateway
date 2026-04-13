use boom_core::DeploymentQueueInfo;
use dashmap::DashMap;
use futures::Stream;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

// ═══════════════════════════════════════════════════════════
// Public types
// ═══════════════════════════════════════════════════════════

/// Per-deployment flow control configuration.
#[derive(Debug, Clone)]
pub struct FlowControlConfig {
    /// Max concurrent in-flight requests. 0 = unlimited.
    pub max_inflight: u32,
    /// Max total input context chars across all in-flight requests. 0 = unlimited.
    pub max_context: u64,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_inflight: 0,
            max_context: 0,
        }
    }
}

/// Snapshot of a single deployment's flow control state.
#[derive(Debug, Clone)]
pub struct FlowControlStat {
    pub deployment_id: String,
    pub current_inflight: u32,
    pub current_context: u64,
    pub waiters: usize,
    pub vip_waiters: usize,
    pub max_inflight: u32,
    pub max_context: u64,
}

/// Error returned when flow control acquire fails.
#[derive(Debug)]
pub enum FlowControlError {
    /// Timed out waiting in the queue.
    Timeout {
        deployment_id: String,
        waiters: usize,
    },
    /// No slot configured for this deployment (pass-through).
    NoSlot,
}

/// Per-deployment queued waiter info for dashboard visibility.
#[derive(Debug, Clone)]
pub struct QueuedWaiterStat {
    pub deployment_id: String,
    pub waiters: Vec<QueuedWaiterEntry>,
}

/// A single queued waiter's info.
#[derive(Debug, Clone)]
pub struct QueuedWaiterEntry {
    pub key_alias: Option<String>,
    pub is_vip: bool,
}

// ═══════════════════════════════════════════════════════════
// Internal types
// ═══════════════════════════════════════════════════════════

/// Tracks individual waiters for dashboard visibility.
struct QueuedWaiter {
    key_alias: Option<String>,
    is_vip: bool,
}

// ═══════════════════════════════════════════════════════════
// FlowController
// ═══════════════════════════════════════════════════════════

/// Per-deployment flow controller. Survives config reloads.
///
/// Each deployment has a `FlowControlSlot` that tracks in-flight
/// request count and total input context. When limits are exceeded,
/// new requests wait asynchronously until a slot opens or timeout.
/// VIP keys are always woken before non-VIP keys.
pub struct FlowController {
    slots: Arc<DashMap<String, FlowControlSlot>>,
}

struct FlowControlSlot {
    max_inflight: AtomicU32,
    max_context: AtomicU64,
    current_inflight: AtomicU32,
    current_context: AtomicU64,
    /// Non-VIP waiter count.
    waiters: AtomicU32,
    /// VIP waiter count.
    vip_waiters: AtomicU32,
    /// Non-VIP wake-up channel.
    notify: tokio::sync::Notify,
    /// VIP wake-up channel.
    vip_notify: tokio::sync::Notify,
    /// Individual waiter tracking for dashboard visibility.
    queued_waiters: std::sync::Mutex<Vec<QueuedWaiter>>,
}

impl FlowController {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(DashMap::new()),
        }
    }

    /// Ensure a slot exists for the given deployment_id with the given config.
    /// If the slot already exists, only updates the config limits (preserves runtime state).
    /// If config is all zeros (unlimited), removes the slot.
    pub fn ensure_slot(&self, deployment_id: &str, config: &FlowControlConfig) {
        if config.max_inflight == 0 && config.max_context == 0 {
            // No limits configured — remove if exists.
            self.slots.remove(deployment_id);
            return;
        }

        let mut created = false;
        let slot = self
            .slots
            .entry(deployment_id.to_string())
            .or_insert_with(|| {
                created = true;
                FlowControlSlot {
                    max_inflight: AtomicU32::new(config.max_inflight),
                    max_context: AtomicU64::new(config.max_context),
                    current_inflight: AtomicU32::new(0),
                    current_context: AtomicU64::new(0),
                    waiters: AtomicU32::new(0),
                    vip_waiters: AtomicU32::new(0),
                    notify: tokio::sync::Notify::new(),
                    vip_notify: tokio::sync::Notify::new(),
                    queued_waiters: std::sync::Mutex::new(Vec::new()),
                }
            });

        if !created {
            // Update config only (runtime state preserved).
            slot.max_inflight.store(config.max_inflight, Ordering::Relaxed);
            slot.max_context.store(config.max_context, Ordering::Relaxed);
        }
    }

    /// Remove a slot (called when a deployment is deleted).
    /// In-flight requests will drain naturally via Drop.
    pub fn remove_slot(&self, deployment_id: &str) {
        self.slots.remove(deployment_id);
    }

    /// Remove slots that are no longer in the provided list.
    pub fn retain_slots(&self, active_ids: &[String]) {
        self.slots.retain(|id, _| active_ids.contains(id));
    }

    /// Try to acquire a flow control slot for a deployment.
    ///
    /// If the deployment has no slot configured, returns `Err(FlowControlError::NoSlot)`.
    /// If limits are exceeded, waits asynchronously up to `timeout` duration.
    /// VIP requests are always woken before non-VIP requests.
    /// On success, returns a `FlowControlGuard` that releases the slot on Drop.
    pub async fn acquire(
        &self,
        deployment_id: &str,
        context_chars: u64,
        timeout: Duration,
        is_vip: bool,
        key_alias: Option<String>,
    ) -> Result<FlowControlGuard, FlowControlError> {
        let slot = match self.slots.get(deployment_id) {
            Some(s) => s,
            None => return Err(FlowControlError::NoSlot),
        };

        // Fast path: try to acquire without waiting.
        if try_acquire_slot(&slot, context_chars) {
            return Ok(FlowControlGuard {
                slots: self.slots.clone(),
                deployment_id: deployment_id.to_string(),
                context_chars,
            });
        }

        // Slow path: wait for a slot to open.
        // Track this waiter for dashboard visibility.
        {
            let mut q = slot.queued_waiters.lock().unwrap();
            q.push(QueuedWaiter {
                key_alias: key_alias.clone(),
                is_vip,
            });
        }

        if is_vip {
            slot.vip_waiters.fetch_add(1, Ordering::Relaxed);
        } else {
            slot.waiters.fetch_add(1, Ordering::Relaxed);
        }

        // Retry acquire AFTER registering as waiter.
        // This closes the race window where a guard drops between the initial
        // try_acquire_slot failure and waiter registration — the guard would see
        // waiters=0 and skip notify_one(), leaving us stuck.
        if try_acquire_slot(&slot, context_chars) {
            // Got it — clean up waiter tracking and return.
            if is_vip {
                slot.vip_waiters.fetch_sub(1, Ordering::Relaxed);
            } else {
                slot.waiters.fetch_sub(1, Ordering::Relaxed);
            }
            {
                let mut q = slot.queued_waiters.lock().unwrap();
                q.retain(|w| !(w.key_alias == key_alias && w.is_vip == is_vip));
            }
            return Ok(FlowControlGuard {
                slots: self.slots.clone(),
                deployment_id: deployment_id.to_string(),
                context_chars,
            });
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let notify_ref = if is_vip {
            &slot.vip_notify
        } else {
            &slot.notify
        };

        let waiter_key_alias = key_alias.clone();
        let result = loop {
            tokio::select! {
                _ = notify_ref.notified() => {
                    if try_acquire_slot(&slot, context_chars) {
                        break Ok(FlowControlGuard {
                            slots: self.slots.clone(),
                            deployment_id: deployment_id.to_string(),
                            context_chars,
                        });
                    }
                    // Failed to acquire — let someone else try.
                    notify_ref.notify_one();
                    if tokio::time::Instant::now() >= deadline {
                        break Err(FlowControlError::Timeout {
                            deployment_id: deployment_id.to_string(),
                            waiters: total_waiters(&slot),
                        });
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break Err(FlowControlError::Timeout {
                        deployment_id: deployment_id.to_string(),
                        waiters: total_waiters(&slot),
                    });
                }
            }
        };

        // Clean up waiter tracking.
        if is_vip {
            slot.vip_waiters.fetch_sub(1, Ordering::Relaxed);
        } else {
            slot.waiters.fetch_sub(1, Ordering::Relaxed);
        }
        {
            let mut q = slot.queued_waiters.lock().unwrap();
            q.retain(|w| !(w.key_alias == waiter_key_alias && w.is_vip == is_vip));
        }

        result
    }

    /// Get stats for all deployments with flow control configured.
    pub fn get_stats(&self) -> Vec<FlowControlStat> {
        self.slots
            .iter()
            .map(|r| {
                let s = r.value();
                FlowControlStat {
                    deployment_id: r.key().clone(),
                    current_inflight: s.current_inflight.load(Ordering::Relaxed),
                    current_context: s.current_context.load(Ordering::Relaxed),
                    waiters: s.waiters.load(Ordering::Relaxed) as usize,
                    vip_waiters: s.vip_waiters.load(Ordering::Relaxed) as usize,
                    max_inflight: s.max_inflight.load(Ordering::Relaxed),
                    max_context: s.max_context.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    /// Get per-deployment queued waiter details for dashboard visibility.
    pub fn get_queued_waiters(&self) -> Vec<QueuedWaiterStat> {
        self.slots
            .iter()
            .map(|r| {
                let s = r.value();
                let q = s.queued_waiters.lock().unwrap();
                // VIP entries first.
                let mut entries: Vec<QueuedWaiterEntry> = q
                    .iter()
                    .filter(|w| w.is_vip)
                    .map(|w| QueuedWaiterEntry {
                        key_alias: w.key_alias.clone(),
                        is_vip: true,
                    })
                    .collect();
                entries.extend(
                    q.iter()
                        .filter(|w| !w.is_vip)
                        .map(|w| QueuedWaiterEntry {
                            key_alias: w.key_alias.clone(),
                            is_vip: false,
                        }),
                );
                QueuedWaiterStat {
                    deployment_id: r.key().clone(),
                    waiters: entries,
                }
            })
            .collect()
    }
}

impl Default for FlowController {
    fn default() -> Self {
        Self::new()
    }
}

impl DeploymentQueueInfo for FlowController {
    fn total_load(&self, deployment_id: &str) -> u64 {
        match self.slots.get(deployment_id) {
            Some(slot) => {
                let inflight = slot.current_inflight.load(Ordering::Relaxed) as u64;
                let waiters = slot.waiters.load(Ordering::Relaxed) as u64;
                let vip_waiters = slot.vip_waiters.load(Ordering::Relaxed) as u64;
                inflight + waiters + vip_waiters
            }
            None => 0,
        }
    }
}

/// Total waiters (VIP + normal).
fn total_waiters(slot: &FlowControlSlot) -> usize {
    (slot.waiters.load(Ordering::Relaxed) + slot.vip_waiters.load(Ordering::Relaxed)) as usize
}

/// Attempt to acquire a slot. Returns true if successful.
fn try_acquire_slot(slot: &FlowControlSlot, context_chars: u64) -> bool {
    let max_inflight = slot.max_inflight.load(Ordering::Relaxed);
    let max_context = slot.max_context.load(Ordering::Relaxed);

    // Check inflight limit.
    if max_inflight > 0 {
        let current = slot.current_inflight.load(Ordering::Relaxed);
        if current >= max_inflight {
            return false;
        }
    }

    // Check context limit.
    if max_context > 0 {
        let current_ctx = slot.current_context.load(Ordering::Relaxed);
        if current_ctx + context_chars > max_context {
            return false;
        }
    }

    // Both checks passed — increment counters.
    slot.current_inflight.fetch_add(1, Ordering::Relaxed);
    slot.current_context.fetch_add(context_chars, Ordering::Relaxed);
    true
}

// ═══════════════════════════════════════════════════════════
// FlowControlGuard — RAII
// ═══════════════════════════════════════════════════════════

/// RAII guard that releases the flow control slot on Drop.
pub struct FlowControlGuard {
    slots: Arc<DashMap<String, FlowControlSlot>>,
    deployment_id: String,
    context_chars: u64,
}

impl Drop for FlowControlGuard {
    fn drop(&mut self) {
        if let Some(slot) = self.slots.get(&self.deployment_id) {
            slot.current_inflight.fetch_sub(1, Ordering::Relaxed);
            slot.current_context.fetch_sub(self.context_chars, Ordering::Relaxed);

            // Wake one waiter — VIP first.
            let vip_count = slot.vip_waiters.load(Ordering::Relaxed);
            if vip_count > 0 {
                slot.vip_notify.notify_one();
            } else if slot.waiters.load(Ordering::Relaxed) > 0 {
                slot.notify.notify_one();
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowControlledStream — releases guard when stream ends
// ═══════════════════════════════════════════════════════════

/// Stream wrapper that holds an optional flow control guard.
/// When the stream ends (returns `None`) or is dropped, the guard is released.
pub struct FlowControlledStream<S> {
    inner: S,
    guard: Option<FlowControlGuard>,
}

impl<S> FlowControlledStream<S> {
    pub fn new(inner: S, guard: FlowControlGuard) -> Self {
        Self {
            inner,
            guard: Some(guard),
        }
    }

    /// Create a passthrough wrapper (no guard to release).
    pub fn passthrough(inner: S) -> Self {
        Self {
            inner,
            guard: None,
        }
    }
}

impl<S: Stream + Unpin> Stream for FlowControlledStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);

        if matches!(result, Poll::Ready(None)) {
            // Stream finished — release the guard.
            self.guard.take();
        }
        result
    }
}
