# Rate Limit and Queue — 限速与队列设计文档

**所属项目：** BooMGateway  
**文档路径：** `SystemDesign/rate_limit_and_queue.md`  
**Last Updated:** May 2026

---

## 目录

1. [概述：两道关卡](#1-概述两道关卡)
2. [第一道关卡：Rate Limit Check](#2-第一道关卡rate-limit-check)
   - 2.1 Plan 解析与优先级
   - 2.2 并发限制（Concurrency Guard）
   - 2.3 滑动窗口算法
   - 2.4 调用栈
3. [第二道关卡：Flow Control Queue](#3-第二道关卡flow-control-queue)
   - 3.1 问题背景
   - 3.2 队列数据结构
   - 3.3 入队与等待机制
   - 3.4 dispatch 调度算法
   - 3.5 VIP 优先级
   - 3.6 调用栈
4. [请求等待的完整时序图](#4-请求等待的完整时序图)
5. [RAII Guard 链：资源生命周期管理](#5-raii-guard-链资源生命周期管理)
6. [两道关卡的本质区别](#6-两道关卡的本质区别)
7. [失败回滚与反 DDoS 设计](#7-失败回滚与反-ddos-设计)
8. [持久化与 Crash Recovery](#8-持久化与-crash-recovery)

---

## 1. 概述：两道关卡

完成 Authentication（鉴权）和 Model Access Check（模型访问检查）之后，一个请求在真正打到 upstream LLM 之前，还要经过**两道独立的关卡**：

```
Authentication ──► Model Access Check ──► [关卡 A] Rate Limit ──► Provider 选择 ──► [关卡 B] Flow Control Queue ──► Upstream LLM
                                                  ▲                                          ▲
                                           立即返回 429                              可能排队等待
                                           (从不等待)                               (最长 1200s)
```

| 关卡 | 名称 | 等待？ | 限制维度 | 拒绝方式 |
|------|------|--------|----------|----------|
| A | Rate Limit Check | **否**，立即通过或 429 | 每个 API Key / Plan | HTTP 429 + `Retry-After` |
| B | Flow Control Queue | **是**，可阻塞排队 | 每个 Deployment | 排队等待，超时后 503 |

这两道关卡的目的不同：
- **Rate Limit**：控制某个 key 的**请求频率**和**并发数**，防止单个用户耗尽资源。
- **Flow Control**：控制某个**后端 deployment 的负载**（inflight 数量和 context 总量），防止 LLM 服务过载。

---

## 2. 第一道关卡：Rate Limit Check

### 2.1 Plan 解析与优先级

每次请求到达时，系统按以下优先级查找适用的限速规则：

```
key_hash
   │
   ▼
PlanStore.resolve_plan(key_hash)
   │  key_assignments: DashMap<key_hash, plan_name>
   │  plans: DashMap<plan_name, RateLimitPlan>
   │
   ├── 有 assignment → 使用该 Plan
   │
   └── 无 assignment
           │
           ▼
       PlanStore.get_default_plan()
           │
           ├── 有 default_plan → 使用 default_plan
           │
           └── 无 default_plan
                       │
                       ▼
               使用 config.rate_limit.window_limits
               (全局 fallback，不走 Plan 体系)
```

`RateLimitPlan` 的结构：

```rust
pub struct RateLimitPlan {
    pub name: String,
    pub concurrency_limit: Option<u32>,   // 并发上限（同时进行中的请求数）
    pub rpm_limit: Option<u64>,           // 每分钟请求上限
    pub window_limits: Vec<(u64, u64)>,   // 自定义时间窗口 [(limit, window_secs), ...]
    pub schedule: Vec<ScheduleSlot>,      // 时段限速（按北京时间区分白天/夜间）
}
```

时段调度（`effective_limits()`）：按当前北京时间（UTC+8）遍历 `schedule` 列表，找到第一个 `is_active_now()` 为 true 的 slot，合并其字段（slot 优先，未设置的字段 fall back 到 Plan 基础值）。

### 2.2 并发限制（Concurrency Guard）

并发限制是最先检查的，因为并发超限的开销最小（一个原子操作）：

```rust
// PlanStore.try_acquire(key_hash, limit)
let counter = concurrency_counters
    .entry(key_hash)
    .or_insert(Arc::new(AtomicU32::new(0)));

let prev = counter.fetch_add(1, Ordering::Relaxed);   // 先加 1

if prev >= limit {
    counter.fetch_sub(1, Ordering::Relaxed);           // 超限：回滚
    return None;                                        // → 429 ConcurrencyExceeded
}
return Some(ConcurrencyGuard { counter });             // 成功：返回 RAII guard
```

`ConcurrencyGuard` 在 `drop()` 时自动执行 `counter.fetch_sub(1)`，这意味着：
- 非流式响应：请求函数返回时 guard 自动 drop。
- 流式响应：guard 被移入 `GuardedStream`，**在最后一个 SSE chunk 发送完毕时**才 drop。

这正确地统计了"真实并发"：从请求进入到**响应完全传输完成**算作一个并发单位。

### 2.3 滑动窗口算法

并发检查通过后，进行滑动窗口 RPM 和自定义窗口检查。

#### 数据结构

```
DashMap<cache_key, WindowCounter>

cache_key 格式: "{key_hash}:{model}:{window_secs}"
             例: "sha256abc:__plan__:60"     ← RPM 窗口
                 "sha256abc:__plan__:18000"  ← 5小时自定义窗口

WindowCounter {
    count: u64,           // 当前窗口已使用计数
    window_start: u64,    // 窗口起始时间（Unix epoch seconds）
    window_secs: u64,     // 窗口长度（秒）
}
```

注意：Plan 限速使用的 `model` 字段固定为 `"__plan__"`，而不是具体的 model 名。这意味着 Plan 下的计数**跨模型共享**——key 对所有模型的请求合并计数。

#### 两阶段 check-and-record

`check_and_record` 分两个阶段，**先全部 peek，全部通过后再全部 record**：

```
Phase 1: 只读检查（不修改任何计数器）
┌──────────────────────────────────────────────────────────┐
│  检查 RPM 窗口（60s）：                                    │
│    now - window_start >= 60 → 窗口已过期 → allowed=true   │
│    count + weight <= rpm_limit → allowed=true             │
│    count + weight > rpm_limit → allowed=false → 立即返回  │
│                                                           │
│  检查每个自定义窗口（依次）：                              │
│    同上逻辑                                               │
│    任一失败 → 立即返回 RateLimitDecision{allowed:false}    │
└──────────────────────────────────────────────────────────┘
              │ 全部通过
              ▼
Phase 2: 原子写入（全部窗口 +weight）
┌──────────────────────────────────────────────────────────┐
│  record_window(rpm_key, 60s, weight)                      │
│  record_window(win_key_1, w1_secs, weight)                │
│  record_window(win_key_2, w2_secs, weight)                │
│  ...                                                      │
└──────────────────────────────────────────────────────────┘
```

**重要属性**：若任何一个窗口在 Phase 1 拒绝，整个 check 返回 `allowed=false`，**一个计数器都不会被修改**。这保证了被拒绝的请求不消耗任何配额。

#### 窗口过期与重置

```
record_window(cache_key, window_secs, weight):
    entry.and_modify(|c| {
        elapsed = now - c.window_start
        if elapsed >= c.window_secs:
            c.count = weight           // 窗口到期：重置为本次请求的 weight
            c.window_start = now
        else:
            c.count += weight          // 窗口内：累加
    }).or_insert(WindowCounter { count: weight, window_start: now, window_secs })
```

这是一个**固定窗口**实现（不是真正的滑动窗口）。窗口在首次请求时锚定起始时间，到期后重置。对于 RPM 这类短窗口（60 秒），这足够精确；对于长窗口（如 5 小时），边界效应可以接受。

#### weight（配额权重）

不同的模型可能成本差异很大（如 GPT-4o vs GPT-4o-mini）。`quota_count_ratio` 允许配置一个整数倍数：

```
weight = DeploymentStore.get_quota_ratio(resolved_model_name)
         ↑ 默认为 1；配置为 2 表示每次请求扣 2 个配额单位
```

一次请求可能在滑动窗口中扣除多于 1 的计数，实现 token-weighted 的限速语义。

### 2.4 调用栈

```
routes::chat_completions_inner()
  │
  ├── [Auth] RequiredAuth extractor（Axum 自动调用）
  │
  ├── [Step 1] check_model_access()           ← 模型访问检查
  │
  ├── [Step 2] check_plan_or_default_limits() ← ★ Rate Limit 入口
  │     │
  │     ├── plan_store.resolve_plan(key_hash)
  │     │         └── key_assignments.get(key_hash) → plans.get(plan_name)
  │     │
  │     ├── [Plan found]
  │     │     │
  │     │     ├── plan.effective_limits()         ← 时段调度解析
  │     │     │
  │     │     ├── plan_store.try_acquire(key_hash, concurrency_limit)
  │     │     │         └── AtomicU32::fetch_add → ConcurrencyGuard 或 None→429
  │     │     │
  │     │     └── limiter.check_and_record(
  │     │               key={key_hash, "__plan__"},
  │     │               rpm_limit,
  │     │               window_limits,
  │     │               weight
  │     │             )
  │     │               ├── Phase 1: peek_window × N    ← 纯读，无副作用
  │     │               └── Phase 2: record_window × N  ← 全通过后写入
  │     │
  │     └── [No plan] 使用 config 全局 window_limits
  │           └── limiter.check_and_record(
  │                     key={key_hash, model_name},
  │                     key's own rpm_limit (from LiteLLM token),
  │                     global_window_limits,
  │                     weight
  │                   )
  │
  ├── [Step 3] router.select_provider(model, key_hash, input_chars)
  │
  └── [Step 4] acquire_fc_guard()             ← ★ Flow Control 入口（下一节）
```

---

## 3. 第二道关卡：Flow Control Queue

### 3.1 问题背景

Rate Limit 控制的是**用户侧**的请求频率，但无法防止大量高并发请求同时打到同一个 LLM deployment，导致：

1. **LLM 端超时**：deployment 同时处理太多请求，每个都很慢甚至超时。
2. **Context 碎片化**：许多超长上下文请求同时占用带宽，互相干扰。
3. **Key Affinity 失效**：某个 deployment 已经有大量 inflight，key-affinity 策略无法将相同 key 路由到它。

Flow Control 的目标是：**在 deployment 级别，平滑地控制 inflight 数量和 context 总量**，超限的请求**排队等待**而不是被立即拒绝。

### 3.2 队列数据结构

```
FlowController
  └── slots: DashMap<deployment_id, FlowControlSlot>
                                          │
                                          ▼
                              FlowControlSlot {
                                inner: Mutex<SlotInner>
                              }
                                          │
                                          ▼
                              SlotInner {
                                max_inflight: u32,      // 最大并发请求数
                                max_context: u64,       // 最大 context 字符总量
                                vip_queue: VecDeque<QueuedRequest>,
                                normal_queue: VecDeque<QueuedRequest>,
                                next_id: u64,
                              }

QueuedRequest {
    id: u64,
    context_chars: u64,
    key_alias: Option<String>,
    dispatched: bool,       // false=排队中, true=已分发(in-flight)
    grant: Option<oneshot::Sender<()>>,  // 分发时 fire 这个 channel
}
```

**核心设计哲学：队列即真相（Queue as Source of Truth）**

没有独立的 `inflight_count` 计数器。in-flight 的定义是：`dispatched == true` 的 `QueuedRequest` 数量。  
释放一个 in-flight slot 的唯一方式是：从队列中 `remove()` 对应的 `QueuedRequest`。  
这使得**计数器泄漏在结构上不可能发生**。

### 3.3 入队与等待机制

`FlowController::acquire()` 是请求等待发生的核心。下面是详细流程：

```
acquire(deployment_id, context_chars, timeout=1200s, is_vip, key_alias)
  │
  ├── slots.get(deployment_id)
  │     └── 不存在 → Err(NoSlot) → 跳过 FC（无限流）
  │
  ├── Lock SlotInner
  │     │
  │     ├── 如果 max_context > 0 且 context_chars > max_context
  │     │       → Err(ContextExceeded) → 立即 429（永远无法 fit）
  │     │
  │     ├── 构造 QueuedRequest {
  │     │       id = next_id++,
  │     │       context_chars,
  │     │       dispatched: false,
  │     │       grant: Some(oneshot_sender),   ← 创建 oneshot channel
  │     │   }
  │     │
  │     ├── push_back 到 vip_queue 或 normal_queue
  │     │
  │     └── dispatch(&mut inner)               ← 立即尝试分发
  │           └── 如果有容量，立即设 dispatched=true 并 fire oneshot
  │
  ├── Unlock SlotInner
  │
  ├── 注册 AcquireCleanup (RAII: 取消时从队列移除)
  │
  └── tokio::time::timeout(1200s, oneshot_receiver.await)
        │
        ├── Ok(Ok(())) → 分发成功，返回 FlowControlGuard
        │
        ├── Ok(Err(_)) → sender 被 drop（slot 被移除），返回 Err(NoSlot)
        │
        └── Err(_) → 超时
              │
              ├── 检查 dispatched 状态
              │     ├── 已分发 → 返回 FlowControlGuard（正常继续）
              │     └── 未分发 → 从队列移除，返回 Err(Timeout)→503
```

**请求等待的本质**：

```rust
// 这里是 Tokio 异步等待的核心：
tokio::time::timeout(timeout, grant_rx).await
//                             ↑
//              grant_rx 是 oneshot::Receiver<()>
//              请求在这里 .await，让出当前 Tokio worker thread
//              等到 dispatch() 调用 grant_tx.send(()) 时唤醒
```

等待期间，**请求不占用任何线程**。Tokio 的 async runtime 将其注册为一个等待中的 Future，当 `oneshot_sender.send(())` 被调用时，Future 被 wakeup 并重新调度到 worker thread。

### 3.4 dispatch 调度算法

`dispatch()` 在以下时机被调用：
1. 新请求入队时（immediately after enqueue）。
2. 一个 in-flight 请求完成时（`FlowControlGuard::drop()`）。
3. 一个等待中的请求被取消时（`AcquireCleanup::drop()`）。
4. 后台任务每 1 秒调用 `periodic_dispatch()`。

```
dispatch(inner):
  loop:
    (inflight, used_ctx) = count dispatched requests and sum their context_chars
    
    if max_inflight > 0 && inflight >= max_inflight:
      break  // 并发已满
    
    // 优先尝试 VIP 队列
    if try_dispatch_fitting(inner, used_ctx, vip=true):
      continue  // 成功分发一个，再尝试下一个
    
    // 再尝试 normal 队列
    if try_dispatch_fitting(inner, used_ctx, vip=false):
      continue
    
    break  // 没有可以 fit 的请求了

try_dispatch_fitting(inner, used_ctx, vip):
  queue = vip ? vip_queue : normal_queue
  
  for each request in queue:
    if request.dispatched: continue   // 已在飞，跳过
    
    if request.grant.is_none():
        queue.remove(idx)             // client 已断开，清理
        continue
    
    if max_context > 0 && used_ctx + request.context_chars > max_context:
        continue   // context 装不下，跳过（不是永久跳过，等 load 降低后会 fit）
    
    request.dispatched = true
    grant_tx = request.grant.take()
    grant_tx.send(())                 // 唤醒等待中的 Future
    return true
  
  return false
```

**关键保证**：`context_chars <= max_context` 在入队时已检查，所以每个已入队的请求**最终一定能 fit**，不存在永久饥饿（starvation-free）。

跳过某个请求不是拒绝它，而是说"当前 context budget 不够，等 load 降低后再来"。

### 3.5 VIP 优先级

`key_alias: Option<String>` 对应的 API key 如果在 LiteLLM token 的 `metadata` 字段中有 `{"vip": true}`，则视为 VIP：

```rust
fn is_vip_key(metadata: &serde_json::Value) -> bool {
    metadata.as_object()
        .and_then(|m| m.get("vip"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}
```

VIP 请求进入 `vip_queue`，`dispatch()` 总是先处理 VIP 队列。在高负载时，VIP 请求比普通请求更快被分发。

### 3.6 调用栈

```
routes::chat_completions_inner()
  │
  ├── ... Rate Limit 通过 ...
  │
  ├── [Step 3] router.select_provider()          ← 获取 provider Arc
  │               └── 得到 deployment_id
  │
  └── [Step 4] acquire_fc_guard(state, deployment_id, context_chars, ...)
        │
        ├── flow_controller.acquire(
        │       deployment_id,
        │       context_chars,     ← 由 input messages 字符数计算
        │       timeout=1200s,
        │       is_vip,            ← 从 key metadata 读取
        │       key_alias,
        │     ).await              ← ★ 可能在此处挂起等待
        │
        ├── Ok(Some(guard)) → 继续
        │
        ├── Err(Timeout{waiters}) → 503 FlowControlQueueTimeout
        │     └── log_error(...)
        │
        ├── Err(NoSlot) → Ok(None)（该 deployment 未配置 FC，直通）
        │
        └── Err(ContextExceeded) → 429（单次请求太大，永远无法 fit）
```

---

## 4. 请求等待的完整时序图

下面展示两个请求 R1（先到，context 大）和 R2（后到，context 小）竞争同一个 deployment 的完整时序。

**配置假设**：`max_inflight=2, max_context=100000 chars`

```
时间轴 →

R1 (context=80000)    R2 (context=30000)    R3 (context=60000, VIP)    Slot State
─────────────────────────────────────────────────────────────────────  ──────────

t=0:  R1 到达
      Rate Limit ✓
      enter acquire()
      加入 normal_queue
      dispatch():
        inflight=0 < max_inflight=2
        0 + 80000 <= 100000 ✓
        R1.dispatched=true
        send(R1.grant)
      R1 await 立即唤醒                                                 inflight=1
      R1 → provider.chat_stream().await ──────────────────────────────►

t=1:  R2 到达
      Rate Limit ✓
      enter acquire()
      加入 normal_queue
      dispatch():
        inflight=1 < max_inflight=2
        80000 + 30000 = 110000 > 100000 ✗  ← context 超限，跳过
      R2 await grant_rx ... (挂起等待)                                  inflight=1
                                                                        waiters=1

t=2:  R3 (VIP) 到达
      Rate Limit ✓
      enter acquire()
      加入 vip_queue                        ← VIP 进 vip_queue
      dispatch():
        inflight=1 < max_inflight=2
        先尝试 vip_queue:
          80000 + 60000 = 140000 > 100000 ✗ ← 也装不下，跳过
      R3 await grant_rx ... (挂起等待)                                  inflight=1
                                                                        vip_waiters=1
                                                                        waiters=1

t=10: R1 的 FlowControlGuard drop()
      (R1 SSE 流结束或客户端断开)
      queue.remove(R1)
      dispatch():
        inflight=0
        先尝试 vip_queue:
          R3: 0 + 60000 <= 100000 ✓
          R3.dispatched=true
          send(R3.grant)
        再尝试 normal_queue:
          R2: 60000 + 30000 = 90000 <= 100000 ✓
          R2.dispatched=true
          send(R2.grant)                                                inflight=2

t=10: R3 await 唤醒 → provider.chat_stream().await
      R2 await 唤醒 → provider.chat_stream().await

t=30: R3 完成                                                           inflight=1
t=40: R2 完成                                                           inflight=0
```

**关键观察**：
- R2 在 t=1 时 context budget 装不下，**没有被拒绝**，而是安静地等待。
- R3 是 VIP，比 R2 先到达，但也因 context budget 等待。t=10 时 R1 释放，R3 因 VIP 优先级先于 R2 被分发。
- R2 紧接着 R3 在同一次 `dispatch()` 循环内被分发（两个 inflight slot 都空闲）。

---

## 5. RAII Guard 链：资源生命周期管理

对于**流式响应**，多个资源的释放时机必须精确对齐到"最后一个 SSE chunk 发送完毕"。BooMGateway 通过嵌套的 stream wrapper 实现这一点：

```
provider.chat_stream(req).await
         │
         │ 返回 ChatStream（原始 SSE 字节流）
         ▼
InFlightStream::new(chat_stream, inflight_guard)
         │ InFlightGuard 在 stream 结束时 drop → InFlightTracker 计数 -1
         ▼
FlowControlledStream::new(inflight_stream, fc_guard)
         │ FlowControlGuard 在 stream 结束时 drop → dispatch() 被触发
         ▼
GuardedStream::new(fc_stream, concurrency_guard)
         │ ConcurrencyGuard 在 stream 结束时 drop → AtomicU32 -1
         ▼
LoggedStream::new(guarded_stream, db_pool, request_log, start_time, usage_tracker)
         │ 在 Drop 时计算 real_duration_ms，fire-and-forget 写 audit log
         ▼
PromptLogStream::new(logged_stream, sender, prompt_entry)   [可选]
         │ 在 stream 结束时序列化完整响应，send 到 prompt log writer
         ▼
Sse::new(outermost_stream).keep_alive(KeepAlive::default())
         │ axum 的 SSE 包装，处理 HTTP 协议层
         ▼
       网络 → 客户端
```

每个 wrapper 实现 `futures::Stream`，`poll_next` 委托给内层：

```rust
impl<S: Stream + Unpin> Stream for GuardedStream<S> {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            self.guard.take();   // stream 结束 → 释放 guard
        }
        result
    }
}
```

**Drop 链的触发顺序**（由内向外，当最外层 stream 被 drop 时）：

```
Sse dropped
  → PromptLogStream dropped → send prompt log entry
    → LoggedStream dropped → spawn audit log write
      → GuardedStream dropped → ConcurrencyGuard::drop() → AtomicU32 -= 1
        → FlowControlledStream dropped → FlowControlGuard::drop()
            → queue.remove(request) → dispatch() → 唤醒下一个等待请求
          → InFlightStream dropped → InFlightGuard::drop() → InFlightTracker 计数 -= 1
```

**非流式响应**：没有 stream wrapper，guard 是在 `chat_completions_inner` 函数作用域结束时 drop，这发生在 `provider.chat(req).await` 返回后立即执行。

---

## 6. 两道关卡的本质区别

| 维度 | Rate Limit (关卡 A) | Flow Control (关卡 B) |
|------|--------------------|-----------------------|
| **控制对象** | API Key / Plan | Backend Deployment |
| **等待行为** | 不等待，立即返回 429 | 排队等待，最长 1200s |
| **等待实现** | 无 | Tokio `oneshot::channel` |
| **并发安全** | `AtomicU32` (lock-free) | `Mutex<VecDeque>` |
| **限制维度** | RPM + 并发 + 自定义时间窗口 | inflight 数 + context chars |
| **配置粒度** | per key（通过 Plan 体系） | per deployment_id |
| **释放时机** | stream 结束时（RAII Guard） | stream 结束时（RAII Guard） |
| **拒绝后果** | 即时 HTTP 429，客户端重试 | 20 分钟后超时 503 |
| **热加载** | 计数器不受影响 | 队列不受影响 |

### 为什么 Rate Limit 不排队？

Rate Limit 是**用户级别的策略**。让用户排队会隐藏 rate limit 的事实，导致：
1. 客户端误以为请求成功，实际在隐形等待。
2. 攻击者可以通过大量请求消耗队列资源（DDoS）。

正确的做法是立即返回 429 + `Retry-After` header，让客户端知道何时重试。

### 为什么 Flow Control 要排队？

Flow Control 是**基础设施级别的保护**。LLM inference 耗时长（几秒到几分钟），排队是合理的：
1. 用户请求有价值，不应该因后端短暂满载而丢失。
2. LLM deployment 的 inflight 恢复速度可预测（一个请求完成就释放一个 slot）。
3. 1200s 超时匹配了最长 LLM inference 时间，实践中极少触发。

---

## 7. 失败回滚与反 DDoS 设计

### RPM 计数不回滚

当 upstream 返回错误时，`rollback_plan_quota()` 只回滚**自定义时间窗口**计数，**RPM 计数不回滚**：

```rust
pub fn rollback_plan_windows(
    &self,
    key: &RateLimitKey,
    window_limits: &[(u64, u64)],   // 只有自定义 window，不含 RPM(60s)
    weight: u64,
) {
    for &(_, window_secs) in window_limits {
        let win_key = Self::cache_key(key, window_secs);
        self.unrecord_window(&win_key, weight);   // 减回去
    }
    // RPM(60s) 窗口故意不在这里，不回滚
}
```

**原因**：RPM 是防频率攻击的第一道防线。如果 upstream 失败后 RPM 可以回滚，攻击者可以通过构造必然失败的请求（如无效参数）来绕过 RPM 限制。

自定义长周期窗口（如 5 小时内 100 次）代表真实的 quota 消耗，失败了应该归还。

### ConcurrencyGuard 的可靠性

即使请求因任何原因 panic 或提前 drop，Rust 的 `Drop` trait 保证 `ConcurrencyGuard::drop()` 一定被调用：

```rust
impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);   // 无条件执行
    }
}
```

这等价于 Go 的 `defer` 或 Java 的 `finally`，但在 Rust 中是语言级别的保证，无需开发者手动编写。

### AcquireCleanup：FC 队列的取消安全

Flow Control 的 `acquire()` 是一个 async future。如果 Tokio 在等待期间 cancel 这个 future（例如 Axum 的请求超时），会发生什么？

```rust
struct AcquireCleanup {
    request_id: u64,
    deployment_id: String,
    consumed: bool,    // true = 已正常完成，不需要清理
    ...
}

impl Drop for AcquireCleanup {
    fn drop(&mut self) {
        if self.consumed { return; }   // 正常路径，无需清理
        
        // 取消路径：从队列中移除此请求，并触发 dispatch
        let slot = self.slots.get(&self.deployment_id);
        let mut inner = slot.inner.lock().unwrap();
        let queue = if self.is_vip { &mut inner.vip_queue } else { &mut inner.normal_queue };
        if let Some(idx) = queue.iter().position(|r| r.id == self.request_id) {
            queue.remove(idx);
            FlowControlSlot::dispatch(&mut inner);   // 释放出来的 slot 让给下一个
        }
    }
}
```

当 future 被 cancel 时，`AcquireCleanup` 的 `Drop` 确保：
1. 请求从队列中被移除（不管 dispatched 是 true 还是 false）。
2. `dispatch()` 被重新触发，让后续等待者能填补空缺。

---

## 8. 持久化与 Crash Recovery

Rate Limit 状态（滑动窗口计数）在内存中维护，通过后台 sync task 定期持久化：

```
后台 sync task (每 10 分钟):
  ├── limiter.snapshot()
  │     └── 返回所有未过期的 (cache_key, count, window_start, window_secs)
  │
  └── limiter.sync_counters_to_db(&pool)
        └── INSERT INTO boom_rate_limit_state ... ON CONFLICT DO UPDATE

启动时恢复:
  └── limiter.restore_counters_from_db(&pool)
        SELECT cache_key, count, window_start, window_secs
        FROM boom_rate_limit_state
        WHERE window_start + window_secs > EXTRACT(EPOCH FROM NOW())::BIGINT
        └── 只恢复窗口尚未过期的记录
```

**热加载时计数器不受影响**：`limiter` 是 `AppState` 顶层字段（不在 `AppStateInner` 内），热加载只替换 `AppStateInner`（通过 `ArcSwap`），limiter 的内存数据完全保留。

Flow Control 队列状态**不持久化**：
- 队列中的请求都绑定了活跃的 TCP 连接。
- 进程重启后这些连接都已断开，等待中的请求已超时。
- 因此不需要也不应该持久化 FC 队列。

---

## 附录：关键数据结构速查

### Rate Limit 相关

```
RateLimitKey { key_hash: String, model: String }
  ↓ 用于 Plan 时，model = "__plan__"
  ↓ 无 Plan 时，model = 实际 model name

WindowCounter { count, window_start (unix epoch), window_secs }
  ↓ cache_key = "{key_hash}:{model}:{window_secs}"

RateLimitDecision {
    allowed: bool,
    remaining: u64,          // rpm 剩余
    limit: u64,
    reset_at: DateTime<Utc>,
    retry_after_secs: Option<u64>,
    rejected_window_secs: Option<u64>,   // 哪个窗口触发了拒绝
}

ConcurrencyGuard { counter: Arc<AtomicU32> }
  ↓ drop() → fetch_sub(1)
```

### Flow Control 相关

```
QueuedRequest {
    id: u64,
    context_chars: u64,
    key_alias: Option<String>,
    dispatched: bool,
    grant: Option<oneshot::Sender<()>>,
}

FlowControlGuard { slots, deployment_id, request_id, is_vip }
  ↓ drop() → queue.remove(request_id) → dispatch()

AcquireCleanup { request_id, deployment_id, is_vip, consumed: bool }
  ↓ drop() → if !consumed → queue.remove(request_id) → dispatch()

FlowControlError {
    NoSlot,                   // 该 deployment 未配置 FC
    ContextExceeded { ... },  // 单请求超 max_context → 429
    Timeout { waiters },      // 等待超时 → 503
}
```
