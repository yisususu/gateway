use boom_core::DeploymentQueueInfo;
use dashmap::DashMap;
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
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

/// All mutable state for a single deployment's flow control slot.
/// Protected by a std::sync::Mutex — critical sections are short
/// (no async, no I/O), so blocking is acceptable on a 32-thread runtime.
struct SlotInner {
    max_inflight: u32,
    max_context: u64,
    current_inflight: u32,
    current_context: u64,
    /// VIP FIFO queue — always drained before the normal queue.
    vip_queue: VecDeque<QueuedRequest>,
    /// Normal FIFO queue — drained only when VIP queue is empty or blocked.
    normal_queue: VecDeque<QueuedRequest>,
    /// Monotonic ID for correlating timeout cleanup with dispatch.
    next_id: u64,
}

/// A single queued request waiting to be dispatched.
struct QueuedRequest {
    /// Unique ID for timeout-vs-dispatch race detection.
    id: u64,
    /// Input context size in chars (reserved against max_context).
    context_chars: u64,
    /// Key alias for dashboard visibility.
    key_alias: Option<String>,
    /// Oneshot channel — dispatch sends () to grant, waiter receives it.
    grant: tokio::sync::oneshot::Sender<()>,
}

// ═══════════════════════════════════════════════════════════
// FlowControlSlot
// ═══════════════════════════════════════════════════════════

struct FlowControlSlot {
    inner: std::sync::Mutex<SlotInner>,
}

impl FlowControlSlot {
    /// Greedily dispatch queued requests to fill available capacity.
    ///
    /// Loops until: both queues empty, or inflight limit hit, or
    /// neither queue head fits within the remaining context budget.
    /// VIP queue is always tried first (strict priority).
    fn dispatch(inner: &mut SlotInner) {
        loop {
            // Inflight limit check.
            if inner.max_inflight > 0 && inner.current_inflight >= inner.max_inflight {
                break;
            }

            // Try VIP head first, then normal head.
            if Self::try_dispatch_one(inner, true) {
                continue;
            }
            if Self::try_dispatch_one(inner, false) {
                continue;
            }
            // Neither queue could dispatch — done.
            break;
        }
    }

    /// Try to dispatch the head of the specified queue.
    /// Returns true if a request was dispatched.
    fn try_dispatch_one(inner: &mut SlotInner, vip: bool) -> bool {
        let queue = if vip {
            &mut inner.vip_queue
        } else {
            &mut inner.normal_queue
        };
        match queue.front() {
            Some(req) => {
                // Context limit check.
                if inner.max_context > 0
                    && inner.current_context + req.context_chars > inner.max_context
                {
                    return false;
                }
                // Dispatch: reserve capacity, pop, notify.
                let req = queue.pop_front().unwrap();
                inner.current_inflight += 1;
                inner.current_context += req.context_chars;
                let _ = req.grant.send(());
                true
            }
            None => false,
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowController
// ═══════════════════════════════════════════════════════════

/// Per-deployment flow controller. Survives config reloads.
///
/// Each deployment has a `FlowControlSlot` backed by a single Mutex
/// protecting all mutable state (counters + two FIFO queues).
///
/// Dispatch triggers (all serialized by the per-slot Mutex):
///   1. `acquire()` — enqueue then dispatch
///   2. `FlowControlGuard::drop()` — decrement then dispatch
///   3. `periodic_dispatch()` — 1s timer, iterate all slots
pub struct FlowController {
    slots: Arc<DashMap<String, FlowControlSlot>>,
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
                    inner: std::sync::Mutex::new(SlotInner {
                        max_inflight: config.max_inflight,
                        max_context: config.max_context,
                        current_inflight: 0,
                        current_context: 0,
                        vip_queue: VecDeque::new(),
                        normal_queue: VecDeque::new(),
                        next_id: 0,
                    }),
                }
            });

        if !created {
            let mut inner = slot.inner.lock().unwrap();
            inner.max_inflight = config.max_inflight;
            inner.max_context = config.max_context;
        }
    }

    /// Remove a slot (called when a deployment is deleted).
    /// Queued waiters' oneshot senders are dropped, causing receivers to error.
    /// In-flight requests drain naturally via guard Drop (no-ops if slot gone).
    pub fn remove_slot(&self, deployment_id: &str) {
        self.slots.remove(deployment_id);
    }

    /// Remove slots that are no longer in the provided list.
    pub fn retain_slots(&self, active_ids: &[String]) {
        self.slots.retain(|id, _| active_ids.contains(id));
    }

    /// Acquire a flow control slot for a deployment.
    ///
    /// 1. Enqueue at the tail of the appropriate FIFO queue (VIP or normal).
    /// 2. Immediately trigger dispatch — may grant this request or others ahead.
    /// 3. Await grant signal (oneshot) or timeout.
    /// 4. On timeout, check if already dispatched (race with guard drop).
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

        let (grant_tx, grant_rx) = tokio::sync::oneshot::channel();
        let request_id: u64;

        // ── Enqueue + dispatch (single mutex hold) ──
        {
            let mut inner = slot.inner.lock().unwrap();
            request_id = inner.next_id;
            inner.next_id += 1;

            let req = QueuedRequest {
                id: request_id,
                context_chars,
                key_alias,
                grant: grant_tx,
            };

            if is_vip {
                inner.vip_queue.push_back(req);
            } else {
                inner.normal_queue.push_back(req);
            }

            FlowControlSlot::dispatch(&mut inner);
        }
        // Mutex released, DashMap ref dropped — safe to await.

        // ── Wait for grant or timeout ──
        match tokio::time::timeout(timeout, grant_rx).await {
            Ok(Ok(())) => {
                // Granted — create guard.
                Ok(FlowControlGuard {
                    slots: self.slots.clone(),
                    deployment_id: deployment_id.to_string(),
                    context_chars,
                })
            }
            Ok(Err(_)) => {
                // Slot removed while waiting (sender dropped).
                Err(FlowControlError::NoSlot)
            }
            Err(_) => {
                // Timeout — check if already dispatched (race with guard drop).
                let already_dispatched = {
                    let slot = self.slots.get(deployment_id);
                    match slot {
                        Some(slot) => {
                            let mut inner = slot.inner.lock().unwrap();
                            let queue = if is_vip {
                                &mut inner.vip_queue
                            } else {
                                &mut inner.normal_queue
                            };
                            match queue.iter().position(|r| r.id == request_id) {
                                Some(idx) => {
                                    queue.remove(idx);
                                    false // Still in queue — removed.
                                }
                                None => true, // Already dispatched.
                            }
                        }
                        None => false, // Slot gone — treat as timeout.
                    }
                };

                if already_dispatched {
                    // Counters already incremented — must create guard for cleanup.
                    Ok(FlowControlGuard {
                        slots: self.slots.clone(),
                        deployment_id: deployment_id.to_string(),
                        context_chars,
                    })
                } else {
                    Err(FlowControlError::Timeout {
                        deployment_id: deployment_id.to_string(),
                        waiters: self.total_waiters_for(deployment_id),
                    })
                }
            }
        }
    }

    /// Total waiters for a specific deployment (both queues).
    fn total_waiters_for(&self, deployment_id: &str) -> usize {
        match self.slots.get(deployment_id) {
            Some(slot) => {
                let inner = slot.inner.lock().unwrap();
                inner.vip_queue.len() + inner.normal_queue.len()
            }
            None => 0,
        }
    }

    /// Periodic dispatch: iterate all slots and try to fill capacity.
    /// Called from a 1-second background timer to prevent idle capacity.
    pub fn periodic_dispatch(&self) {
        for r in self.slots.iter() {
            let mut inner = r.value().inner.lock().unwrap();
            FlowControlSlot::dispatch(&mut inner);
        }
    }

    /// Get stats for all deployments with flow control configured.
    pub fn get_stats(&self) -> Vec<FlowControlStat> {
        self.slots
            .iter()
            .map(|r| {
                let inner = r.value().inner.lock().unwrap();
                FlowControlStat {
                    deployment_id: r.key().clone(),
                    current_inflight: inner.current_inflight,
                    current_context: inner.current_context,
                    waiters: inner.normal_queue.len(),
                    vip_waiters: inner.vip_queue.len(),
                    max_inflight: inner.max_inflight,
                    max_context: inner.max_context,
                }
            })
            .collect()
    }

    /// Get per-deployment queued waiter details for dashboard visibility.
    pub fn get_queued_waiters(&self) -> Vec<QueuedWaiterStat> {
        self.slots
            .iter()
            .map(|r| {
                let inner = r.value().inner.lock().unwrap();
                let mut entries: Vec<QueuedWaiterEntry> = inner
                    .vip_queue
                    .iter()
                    .map(|r| QueuedWaiterEntry {
                        key_alias: r.key_alias.clone(),
                        is_vip: true,
                    })
                    .collect();
                entries.extend(
                    inner
                        .normal_queue
                        .iter()
                        .map(|r| QueuedWaiterEntry {
                            key_alias: r.key_alias.clone(),
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
                let inner = slot.inner.lock().unwrap();
                inner.current_inflight as u64
                    + inner.vip_queue.len() as u64
                    + inner.normal_queue.len() as u64
            }
            None => 0,
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowControlGuard — RAII
// ═══════════════════════════════════════════════════════════

/// RAII guard that releases the flow control slot on Drop.
/// Drop decrements counters and triggers dispatch for queued waiters.
pub struct FlowControlGuard {
    slots: Arc<DashMap<String, FlowControlSlot>>,
    deployment_id: String,
    context_chars: u64,
}

impl Drop for FlowControlGuard {
    fn drop(&mut self) {
        if let Some(slot) = self.slots.get(&self.deployment_id) {
            let mut inner = slot.inner.lock().unwrap();
            inner.current_inflight = inner.current_inflight.saturating_sub(1);
            inner.current_context = inner.current_context.saturating_sub(self.context_chars);
            FlowControlSlot::dispatch(&mut inner);
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
