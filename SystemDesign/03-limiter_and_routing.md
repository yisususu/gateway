# Limiter and Routing 深入设计

**所属项目：** BooMGateway  
**文档路径：** `SystemDesign/limiter_and_routing.md`  
**Last Updated:** May 2026

---

## 1. 先回答你的核心问题

### 1.1 Rate limit 和 queue 是不是一个东西？

不是一个东西，是两层不同的控制面：

- `boom-limiter` 负责 **Rate Limit**（配额/并发规则判断），本质是 **admission control**：通过或拒绝。
- `queue` 实现不在 `boom-limiter`，而在 `boom-flowcontrol`，本质是 **backpressure control**：超载时排队等待。
- `boom-routing` 负责 **选 deployment/provider**，本身不存请求队列，不做等待。

一句话：

> **Limiter 决定你“有没有资格进”；FlowControl 决定你“什么时候能发”；Routing 决定你“发给谁”。**

### 1.2 “request 在 boomgateway 排队，然后再根据 routing 规则发给 instance”这句话准确吗？

这句话在当前实现里 **顺序不准确**。当前真实顺序是：

1. 先过 limiter（可能 429，绝不排队）
2. 先 routing 选出具体 provider/deployment
3. 再进入该 deployment 的 flow-control queue（可能等待）
4. 被唤醒后直接发送到 **已选定** 的 provider

也就是说，**排队发生在 routing 之后**，并且是“按已选 deployment 入队”，不是“先全局排队再选 instance”。

---

## 2. 三个模块的职责边界

## 2.1 `boom-limiter`：Rate Limit 与并发门禁

负责：
- `PlanStore`：解析 key 绑定的 plan、default plan、schedule 时段规则
- `ConcurrencyGuard`：每 key 并发占位（RAII 自动归还）
- `SlidingWindowLimiter`：RPM + 自定义窗口计数与检查

不负责：
- 不做 provider 选择
- 不维护请求等待队列

## 2.2 `boom-routing`：模型解析与 provider 选择

负责：
- `DeploymentStore`：`model_name -> Vec<Provider>`
- `AliasStore`：`alias -> target_model`
- `Router::select_provider()`：exact/alias/wildcard 解析 + policy 选择
- `SchedulePolicy`：`round_robin` 或 `key_affinity`

不负责：
- 不阻塞请求，不等待
- 不持有请求队列

## 2.3 `boom-flowcontrol`：deployment 级排队与唤醒

负责：
- `FlowController::acquire()`：入队 + await + 超时
- `FlowControlSlot::dispatch()`：调度哪些等待请求被放行
- VIP/普通双队列
- `FlowControlGuard`：请求结束时自动释放 slot，触发下一轮 dispatch

不负责：
- 不做模型 alias 解析
- 不做 plan/rpm 判定

---

## 3. 认证后到上游前的真实执行流程

以下以 `POST /v1/chat/completions` 为例，且已完成 authentication + model access check：

```text
chat_completions_inner()
  ├─ A. check_plan_or_default_limits()         // boom-limiter
  │    ├─ resolve_plan/default plan
  │    ├─ try_acquire() 并发占位
  │    └─ check_and_record() 窗口计数
  │       └─ fail -> 429 返回
  │
  ├─ B. router.select_provider()               // boom-routing
  │    ├─ resolve_candidates(exact/alias/*)
  │    └─ policy.select(round_robin/key_affinity)
  │       └─ fail -> model not found
  │
  ├─ C. acquire_fc_guard(deployment_id, ...)   // boom-flowcontrol
  │    ├─ enqueue -> await oneshot grant
  │    ├─ timeout -> queue timeout error
  │    └─ granted -> FlowControlGuard
  │
  └─ D. provider.chat()/chat_stream()
       └─ 通过选定 provider 直接发往上游
```

关键点：  
`B` 在 `C` 前面，因此 flow queue 是“每个 deployment 的局部队列”，不是 gateway 全局队列。

---

## 4. `boom-limiter` 深入设计

## 4.1 Plan 解析路径

`check_plan_or_default_limits()` 的决策顺序：

1. `plan_store.resolve_plan(key_hash)`：显式绑定 plan
2. `plan_store.get_default_plan()`：默认 plan
3. 都没有：使用全局配置 `config.rate_limit.window_limits`

当命中 plan 时，`RateLimitKey` 使用 `model="__plan__"`，即同一 key 跨模型共享同一个 plan 配额桶。

## 4.2 并发门禁（`ConcurrencyGuard`）

并发判定是原子计数：

- `fetch_add(1)` 先占位
- 如果超 limit，立刻 `fetch_sub(1)` 回滚并拒绝
- 否则返回 `ConcurrencyGuard`

`ConcurrencyGuard` 被移入 stream wrapper（流式）或函数作用域（非流式），drop 时自动归还。

## 4.3 窗口门禁（`SlidingWindowLimiter`）

`check_and_record()` 使用两阶段：

1. **peek phase**：检查 RPM 与所有 custom windows；任何一个拒绝则立即返回（不改计数）
2. **record phase**：全部通过后再统一 `record_window()`

这保证“拒绝请求不消耗 quota”。

## 4.4 limiter 调用栈

```text
routes::chat_completions_inner
  -> check_plan_or_default_limits
       -> PlanStore::resolve_plan / get_default_plan
       -> RateLimitPlan::effective_limits
       -> PlanStore::try_acquire
       -> SlidingWindowLimiter::check_and_record
            -> peek_window (N 次)
            -> record_window (N 次)
```

---

## 5. `boom-routing` 深入设计

## 5.1 候选集解析（不是直接选一个）

`Router::select_provider(model, key_hash, input_chars)` 先做 `resolve_candidates(model)`：

1. exact model（`deployment_store.get_providers(model)`）
2. alias 解析（`alias_store.resolve(model)` 再取 target providers）
3. wildcard `"*"`（仅当 model 完全未知）

重要语义：
- 如果 model 已配置但 deployment 全部 down（空列表），**不会 fallback 到 `"*"`**。
- wildcard 只兜底“未知模型名”，避免把显式模型 silently 路由到错误模型。

## 5.2 policy 选择

### round_robin
- `DashMap<String, AtomicUsize>` 每模型一个计数器
- `idx % candidates.len()`

### key_affinity
- affinity map：`{key_hash}:{model} -> deployment_id`
- warm-up：当模型总 inflight context 低于阈值时，优先 lowest-load 建立初始分布
- rebalance：preferred load 比最小 load 高出阈值则重新绑定

load 估算来自：
- `InFlightTracker`（在飞请求数）
- `DeploymentQueueInfo`（flow queue 总负载）

注意这里 routing“读取 queue 负载”仅用于更聪明地选 deployment，不等于 routing 持有 queue。

## 5.3 routing 调用栈

```text
routes::chat_completions_inner
  -> state.router.select_provider(model, Some(key_hash), input_chars)
       -> Router::resolve_candidates
            -> DeploymentStore::get_providers (exact)
            -> AliasStore::resolve + get_providers (alias)
            -> DeploymentStore::get_providers("*") (wildcard)
       -> policy.select(...)
            -> RoundRobinPolicy::select
               or
            -> KeyAffinityPolicy::select
                 -> InFlightTracker::get_model_input_chars
                 -> deployment_load(...)
```

---

## 6. “request 等待”的时序图（最关键）

下面是最接近你关注点的完整时序：请求在 gateway 内部到底何时等待、等谁、何时被唤醒。

```text
Client             boom-main/routes          boom-limiter          boom-routing         boom-flowcontrol        Provider
  |                       |                      |                     |                      |                    |
  | POST /chat            |                      |                     |                      |                    |
  |---------------------->|                      |                     |                      |                    |
  |                       | check limits         |                     |                      |                    |
  |                       |--------------------->|                     |                      |                    |
  |                       |<-- allowed / reject -|                     |                      |                    |
  |                       |   (reject=429)       |                     |                      |                    |
  |                       |                      |                     |                      |                    |
  |                       | select provider      |                     |                      |                    |
  |                       |------------------------------------------->|                      |                    |
  |                       |<------------- chosen deployment/provider ---|                      |                    |
  |                       |                      |                     |                      |                    |
  |                       | acquire_fc_guard     |                     |                      |                    |
  |                       |------------------------------------------------------------------>|                    |
  |                       |                      |                     |               enqueue + dispatch           |
  |                       |                      |                     |                      |                    |
  |                       |                      |                     |                      |-- no capacity --> |
  |                       |                      |                     |                      |   await oneshot   |
  |                       |      (request future suspended here; no worker thread occupied)  |                    |
  |                       |                      |                     |                      |                    |
  |                       |                      |                     |                      |<-- slot released --|
  |                       |                      |                     |                      | dispatch + grant() |
  |                       |<------------------------------------------------------------------|                    |
  |                       | provider.chat/stream |                     |                      |                    |
  |                       |--------------------------------------------------------------------------------------->|
  |                       |<---------------------------------------------------------------------------------------|
  |<----------------------| response                                                                              |
```

关键结论：
- **等待点唯一在 `acquire_fc_guard().await`**（flowcontrol）。
- routing 已经结束，provider 已经确定，不会“出队后再二次路由”。

---

## 7. 两种典型场景（帮助你向同事解释）

## 7.1 场景 A：limiter 拒绝（不会排队）

条件：
- key 的并发超限，或 RPM/window 超限

行为：
- 直接返回 `429`
- 不会进入 routing
- 不会进入 flow queue

## 7.2 场景 B：limiter 放行，但 deployment 满载（会排队）

条件：
- rate limit 都通过
- routing 选中 deployment X
- X 的 flow slot 满（inflight/context）

行为：
- 请求进入 X 的 queue，`await oneshot`
- 等到已有请求完成释放 `FlowControlGuard` 后被唤醒
- 唤醒后直接发送给 X（不会重选）

---

## 8. 为什么设计成“先 routing 后 queue”

这是一个工程上很关键的架构选择：

- queue 限制是 deployment-local（每个 deployment 的 `max_inflight/max_context`）
- 只有先完成 routing，才能知道应该入哪个 deployment 的队列
- 若先 queue 再 route，会引入全局队列 + 二次选择复杂度，且无法准确表达 deployment 局部负载

当前实现中，routing 与 queue 通过 `deployment_id` 解耦连接，结构清晰，热加载安全。

---

## 9. 设计上的边界与注意点

## 9.1 routing 不是“调度器 + 执行器”

`boom-routing` 只返回一个 `Arc<dyn Provider>` 句柄，不执行请求，也不保存请求状态。

## 9.2 queue 不做 reroute

如果某 deployment 的 queue 很长，当前实现不会把已入队请求迁移到别的 deployment。  
这是有意保持语义简单：一旦选定 deployment，请求生命周期固定在该 deployment 上。

## 9.3 key_affinity 读取 queue 负载 ≠ queue 在 routing

`KeyAffinityPolicy` 会读取 `DeploymentQueueInfo.total_load()` 作为负载信号，但 queue 仍由 `boom-flowcontrol` 持有；routing 只是消费者。

---

## 10. 你可以用的一句“对外讲解版本”

> BooMGateway 不是“全局排队后再路由”，而是“先按路由策略选 deployment，再在该 deployment 的 flow queue 中等待可用容量”；同时 limiter 在更前面做硬性准入，超限直接 429，不进入排队。

---

## 11. 附：关键调用栈总览（一步看全）

```text
routes::chat_completions_inner
  ├─ check_model_access(...)
  ├─ check_plan_or_default_limits(...)           // boom-limiter
  │    ├─ resolve_plan/get_default_plan
  │    ├─ try_acquire -> ConcurrencyGuard
  │    └─ check_and_record -> SlidingWindowLimiter
  ├─ router.select_provider(...)                 // boom-routing
  │    ├─ resolve_candidates(exact/alias/*)
  │    └─ policy.select(round_robin/key_affinity)
  ├─ acquire_fc_guard(...deployment_id...)       // boom-flowcontrol
  │    └─ flow_controller.acquire(...).await     // request 可能在这里等待
  ├─ provider.chat / provider.chat_stream
  └─ stream wrappers drop chain
       ├─ FlowControlGuard::drop -> dispatch next waiter
       └─ ConcurrencyGuard::drop
```

