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
    /// Request context alone exceeds max_context — can never be dispatched.
    ContextExceeded {
        deployment_id: String,
        context_chars: u64,
        max_context: u64,
    },
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

/// All state for a single deployment's flow control slot.
///
/// NO separate counters — the queue IS the source of truth.
/// `dispatched == true` means in-flight; `dispatched == false` means waiting.
/// This makes counter leaks structurally impossible.
struct SlotInner {
    max_inflight: u32,
    max_context: u64,
    vip_queue: VecDeque<QueuedRequest>,
    normal_queue: VecDeque<QueuedRequest>,
    next_id: u64,
}

/// A request in the flow control queue.
struct QueuedRequest {
    id: u64,
    context_chars: u64,
    key_alias: Option<String>,
    /// `false` = waiting in queue, `true` = dispatched (in-flight).
    dispatched: bool,
    /// Oneshot sender — taken and fired when dispatched.
    grant: Option<tokio::sync::oneshot::Sender<()>>,
}

// ═══════════════════════════════════════════════════════════
// AcquireCleanup — RAII guard for cancellation safety
// ═══════════════════════════════════════════════════════════

/// When the acquire future is cancelled (client disconnect), this Drop
/// removes the request from the queue. If it was already dispatched,
/// removal frees the slot — dispatch is re-triggered to fill it.
/// No counter to leak — removing from the queue IS the rollback.
struct AcquireCleanup {
    request_id: u64,
    deployment_id: String,
    is_vip: bool,
    slots: Arc<DashMap<String, FlowControlSlot>>,
    consumed: bool,
}

impl Drop for AcquireCleanup {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        let slot = match self.slots.get(&self.deployment_id) {
            Some(s) => s,
            None => return,
        };
        let mut inner = slot.inner.lock().unwrap();
        let queue = if self.is_vip {
            &mut inner.vip_queue
        } else {
            &mut inner.normal_queue
        };
        if let Some(idx) = queue.iter().position(|r| r.id == self.request_id) {
            queue.remove(idx);
            FlowControlSlot::dispatch(&mut inner);
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowControlSlot
// ═══════════════════════════════════════════════════════════

struct FlowControlSlot {
    inner: std::sync::Mutex<SlotInner>,
}

impl FlowControlSlot {
    /// Count in-flight requests and sum their context.
    fn inflight_stats(inner: &SlotInner) -> (u32, u64) {
        inner
            .vip_queue
            .iter()
            .chain(inner.normal_queue.iter())
            .filter(|r| r.dispatched)
            .fold((0u32, 0u64), |(cnt, ctx), r| {
                (cnt + 1, ctx + r.context_chars)
            })
    }

    /// Greedily dispatch waiting requests to fill available capacity.
    ///
    /// Strict FIFO within each queue: only dispatch the FIRST waiting request.
    /// If the head doesn't fit (context limit), stop — don't scan past it.
    /// This prevents starvation of large-context requests.
    /// VIP queue always tried first.
    fn dispatch(inner: &mut SlotInner) {
        loop {
            let (inflight, used_ctx) = Self::inflight_stats(inner);
            if inner.max_inflight > 0 && inflight >= inner.max_inflight {
                break;
            }

            // Clean up cancelled entries at queue heads, then try to dispatch.
            Self::drain_cancelled(&mut inner.vip_queue);
            if Self::try_dispatch_head(inner, used_ctx, true) {
                continue;
            }
            Self::drain_cancelled(&mut inner.normal_queue);
            if Self::try_dispatch_head(inner, used_ctx, false) {
                continue;
            }
            break;
        }
    }

    /// Remove cancelled requests (grant sender already dropped) from queue head.
    fn drain_cancelled(queue: &mut VecDeque<QueuedRequest>) {
        while let Some(front) = queue.front() {
            if front.dispatched {
                break;
            }
            if front.grant.is_none() {
                queue.pop_front();
                continue;
            }
            break;
        }
    }

    /// Try to dispatch the first waiting request in the queue (strict FIFO).
    /// Returns true if dispatched, false if queue empty or head can't fit.
    fn try_dispatch_head(
        inner: &mut SlotInner,
        used_ctx: u64,
        vip: bool,
    ) -> bool {
        let queue = if vip {
            &mut inner.vip_queue
        } else {
            &mut inner.normal_queue
        };

        // Find the first non-dispatched entry (should be near the front).
        let idx = match queue.iter().position(|r| !r.dispatched) {
            Some(i) => i,
            None => return false,
        };

        // Context check — strict FIFO: if head doesn't fit, don't skip.
        if inner.max_context > 0 && used_ctx + queue[idx].context_chars > inner.max_context {
            return false;
        }

        // Dispatch: mark in-flight and notify waiter.
        queue[idx].dispatched = true;
        let sender = queue[idx].grant.take().unwrap();
        if sender.send(()).is_err() {
            queue.remove(idx);
            return false;
        }
        true
    }
}

// ═══════════════════════════════════════════════════════════
// FlowController
// ═══════════════════════════════════════════════════════════

pub struct FlowController {
    slots: Arc<DashMap<String, FlowControlSlot>>,
}

impl FlowController {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(DashMap::new()),
        }
    }

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

    pub fn remove_slot(&self, deployment_id: &str) {
        self.slots.remove(deployment_id);
    }

    pub fn retain_slots(&self, active_ids: &[String]) {
        self.slots.retain(|id, _| active_ids.contains(id));
    }

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

        {
            let mut inner = slot.inner.lock().unwrap();

            // Reject immediately if request alone exceeds max_context (can never fit).
            if inner.max_context > 0 && context_chars > inner.max_context {
                return Err(FlowControlError::ContextExceeded {
                    deployment_id: deployment_id.to_string(),
                    context_chars,
                    max_context: inner.max_context,
                });
            }

            request_id = inner.next_id;
            inner.next_id += 1;

            let req = QueuedRequest {
                id: request_id,
                context_chars,
                key_alias,
                dispatched: false,
                grant: Some(grant_tx),
            };

            if is_vip {
                inner.vip_queue.push_back(req);
            } else {
                inner.normal_queue.push_back(req);
            }

            FlowControlSlot::dispatch(&mut inner);
        }
        drop(slot);

        let mut cleanup = AcquireCleanup {
            request_id,
            deployment_id: deployment_id.to_string(),
            is_vip,
            slots: self.slots.clone(),
            consumed: false,
        };

        match tokio::time::timeout(timeout, grant_rx).await {
            Ok(Ok(())) => {
                cleanup.consumed = true;
                Ok(FlowControlGuard {
                    slots: self.slots.clone(),
                    deployment_id: deployment_id.to_string(),
                    request_id,
                    is_vip,
                })
            }
            Ok(Err(_)) => {
                cleanup.consumed = true;
                Err(FlowControlError::NoSlot)
            }
            Err(_) => {
                // Timeout — check if already dispatched.
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
                                    if queue[idx].dispatched {
                                        true
                                    } else {
                                        queue.remove(idx);
                                        false
                                    }
                                }
                                None => true,
                            }
                        }
                        None => false,
                    }
                };

                if already_dispatched {
                    cleanup.consumed = true;
                    Ok(FlowControlGuard {
                        slots: self.slots.clone(),
                        deployment_id: deployment_id.to_string(),
                        request_id,
                        is_vip,
                    })
                } else {
                    cleanup.consumed = true;
                    Err(FlowControlError::Timeout {
                        deployment_id: deployment_id.to_string(),
                        waiters: self.total_waiters_for(deployment_id),
                    })
                }
            }
        }
    }

    fn total_waiters_for(&self, deployment_id: &str) -> usize {
        match self.slots.get(deployment_id) {
            Some(slot) => {
                let inner = slot.inner.lock().unwrap();
                inner
                    .vip_queue
                    .iter()
                    .chain(inner.normal_queue.iter())
                    .filter(|r| !r.dispatched)
                    .count()
            }
            None => 0,
        }
    }

    pub fn periodic_dispatch(&self) {
        for r in self.slots.iter() {
            let mut inner = r.value().inner.lock().unwrap();
            FlowControlSlot::dispatch(&mut inner);
        }
    }

    pub fn get_stats(&self) -> Vec<FlowControlStat> {
        self.slots
            .iter()
            .map(|r| {
                let inner = r.value().inner.lock().unwrap();
                let (inflight, context) = FlowControlSlot::inflight_stats(&inner);
                let vip_waiting = inner.vip_queue.iter().filter(|r| !r.dispatched).count();
                let normal_waiting = inner.normal_queue.iter().filter(|r| !r.dispatched).count();
                FlowControlStat {
                    deployment_id: r.key().clone(),
                    current_inflight: inflight,
                    current_context: context,
                    waiters: normal_waiting,
                    vip_waiters: vip_waiting,
                    max_inflight: inner.max_inflight,
                    max_context: inner.max_context,
                }
            })
            .collect()
    }

    pub fn get_queued_waiters(&self) -> Vec<QueuedWaiterStat> {
        self.slots
            .iter()
            .map(|r| {
                let inner = r.value().inner.lock().unwrap();
                let mut entries: Vec<QueuedWaiterEntry> = inner
                    .vip_queue
                    .iter()
                    .filter(|r| !r.dispatched)
                    .map(|r| QueuedWaiterEntry {
                        key_alias: r.key_alias.clone(),
                        is_vip: true,
                    })
                    .collect();
                entries.extend(
                    inner
                        .normal_queue
                        .iter()
                        .filter(|r| !r.dispatched)
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
                inner.vip_queue.len() as u64 + inner.normal_queue.len() as u64
            }
            None => 0,
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowControlGuard — RAII
// ═══════════════════════════════════════════════════════════

pub struct FlowControlGuard {
    slots: Arc<DashMap<String, FlowControlSlot>>,
    deployment_id: String,
    request_id: u64,
    is_vip: bool,
}

impl Drop for FlowControlGuard {
    fn drop(&mut self) {
        if let Some(slot) = self.slots.get(&self.deployment_id) {
            let mut inner = slot.inner.lock().unwrap();
            let queue = if self.is_vip {
                &mut inner.vip_queue
            } else {
                &mut inner.normal_queue
            };
            if let Some(idx) = queue.iter().position(|r| r.id == self.request_id) {
                queue.remove(idx);
                FlowControlSlot::dispatch(&mut inner);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════
// FlowControlledStream — releases guard when stream ends
// ═══════════════════════════════════════════════════════════

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
            self.guard.take();
        }
        result
    }
}
