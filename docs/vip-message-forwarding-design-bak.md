# VIP request 信息透传设计：`X-Gateway-Priority` Header

> **涉及模块**: `boom-provider`（BoomGateway 侧）、`LLM-LA`（新调度器侧）

## 1. Background

### 1.1 BoomGateway 的 VIP 队列机制

BoomGateway 在 `boom-flowcontrol` 模块中为每个 deployment 维护两条队列：`vip_queue` 和 `normal_queue`。调度时始终优先消化 `vip_queue` 中的请求。

**请求进入哪个队列的判定调用栈：**

```text
HTTP POST /v1/chat/completions
  └─ RequiredAuth::from_request_parts          [boom-main/src/extractor.rs]
       └─ DbAuthenticator::authenticate        [boom-auth/src/key_auth.rs]
            └─ token_to_identity()
                 └─ AuthIdentity { metadata: token.metadata }
                        ↑ 从 boom_verification_token 表读取 metadata JSONB

  └─ chat_completions_inner()                  [boom-main/src/routes.rs]
       └─ acquire_fc_guard(
              is_vip_key(&identity.metadata),  ← 读 API key 的 metadata.vip 字段
          )
            └─ FlowController::acquire(is_vip) [boom-flowcontrol/src/lib.rs]
                 ├─ is_vip=true  → push_back(vip_queue)
                 └─ is_vip=false → push_back(normal_queue)
```

**`is_vip_key` 函数**（`boom-main/src/routes.rs`）：

```rust
fn is_vip_key(metadata: &serde_json::Value) -> bool {
    metadata
        .as_object()
        .and_then(|m| m.get("vip"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}
```

VIP 状态存储在数据库 `boom_verification_token.metadata` 字段（JSONB），由 Dashboard 的 `update_key()` 接口写入 `{ "vip": true }`。**主密钥的 metadata 为 null，永远不是 VIP。**

### 1.2 VIP 信息在哪里丢失

Request 在 BoomGateway 内部的三种形态：

```
客户端 JSON bytes
    ↓  axum::Json::from_request()  [反序列化]
ChatCompletionRequest (Rust struct)   ← is_vip 只用于 FlowController::acquire，不写回 struct
    ↓  serde_json::to_value(&req)  [重新序列化]
serde_json::Value (JSON)              ← extra 字段因 skip_serializing 被丢弃
    ↓  reqwest .json(&body).send()
LLM-LA / vLLM 收到的 JSON bytes      ← 无任何 VIP 信息
```

`ChatCompletionRequest` 的 `extra` 字段带有 `#[serde(default, flatten, skip_serializing)]`，即使客户端在 body 里发 `"vip": true`，也不会被转发给上游。

**VIP 信息丢失的位置**：`OpenAIProvider::build_request()`（`boom-provider/src/openai.rs`）执行 `serde_json::to_value(&req)` 时，`is_vip` 这个 Rust 局部变量已不在 `req` 内，无法被序列化。

### 1.3 完整调用栈总结

```text
1. 认证阶段（VIP 信息产生）
   boom-main/src/extractor.rs: RequiredAuth::from_request_parts
     → boom-auth/src/key_auth.rs: DbAuthenticator::authenticate
          → token_to_identity() → AuthIdentity { metadata.vip }

2. Flow Control 阶段（VIP 信息被使用，仅用于网关内部队列）
   boom-main/src/routes.rs: chat_completions_inner
     → is_vip_key(&identity.metadata) → bool
     → acquire_fc_guard → FlowController::acquire(is_vip)
          → is_vip ? vip_queue : normal_queue

3. 转发阶段（VIP 信息丢失）
   boom-main/src/routes.rs: provider.chat_stream(req)  ← req 里无 vip
     → boom-provider/src/openai.rs: OpenAIProvider::build_request
          → serde_json::to_value(&req)  ← extra.skip_serializing
     → POST to LLM-LA / vLLM           ← 标准 OpenAI body，无 VIP
```

### 1.4 问题陈述

在 BoomGateway 与 vLLM 之间插入 **LLM-LA**（LLM Load-Aware 调度器）后，LLM-LA 需要感知每个请求的 VIP 状态以实现优先调度。然而请求离开 BoomGateway 后，VIP 信息已经丢失，LLM-LA 无从获取。

### 1.5 为什么只有 OpenAI Provider 注入 header？

不同 provider 的部署拓扑不同：

```
                                    ┌─ Anthropic API（云端，直连）
                                    ├─ Azure OpenAI（云端，直连）
BoomGateway ── 选择 Provider ──────├─ Gemini API（云端，直连）
                                    ├─ Bedrock（云端，直连）
                                    │
                                    └─ OpenAI Provider ──→ LLM-LA ──→ vLLM（自建集群）
                                       hosted_vllm / vllm / ollama 都走 OpenAIProvider
```

- **Anthropic / Azure / Gemini / Bedrock**：请求直连云端 API，中间没有 LLM-LA，加 header 无人读取。
- **OpenAI Provider**（含 `hosted_vllm`、`vllm`、`ollama`）：请求发到自建 vLLM 集群，中间经过 LLM-LA，需要 header 传递优先级。

`hosted_vllm`、`vllm`、`ollama` 等 provider 类型都映射到同一个 `OpenAIProvider`（`boom-provider/src/lib.rs`），因此只改这一个 provider 即可覆盖所有自建集群场景。

---

## 2. High-Level Design

### 2.1 设计决策：写入 HTTP Header

在 BoomGateway 转发请求给 LLM-LA 时，将优先级注入自定义 HTTP Header：

```
X-Gateway-Priority: 100    （VIP 请求）
X-Gateway-Priority: 0      （普通请求）
```

LLM-LA 读取该 header 进行优先调度，转发给 vLLM 前将其 strip 掉。

**为什么用数值而不是字符串？** 数值（`0`~`100`）比 `"vip"` / `"normal"` 更具扩展性，LLM-LA 可以直接按数值大小排优先队列，未来新增中间层级无需修改协议。

**选型理由（对比其他方案）：**

| 方案 | 说明 | 结论 |
|------|------|------|
| **HTTP Header（本方案）** | BoomGateway 注入 `X-Gateway-Priority`，LLM-LA 读取后 strip | ✅ 推荐 |
| 双 Deployment URL | 配置两套 deployment 指向不同路径，LLM-LA 按 URL 区分 | 可行但配置复杂，路由层需感知 VIP |
| LLM-LA 自查 DB | LLM-LA 查 `boom_verification_token` | ✗ 不可行：BoomGateway 转发时用 provider key，原始用户 key 已被消费 |
| 在 JSON body 里加字段 | 改 `ChatCompletionRequest` struct | ✗ 会暴露给 vLLM，污染 OpenAI 接口语义 |

### 2.2 对现有 vLLM 服务的影响

**HTTP 协议规范允许自定义 header（`X-` 前缀），收到未知 header 应当忽略。** vLLM 基于 FastAPI 实现，对 `X-Gateway-Priority` 的处理方式：

- **不会拒绝请求**
- **不会返回错误**
- **不会影响推理结果**
- 仅在 server 日志中可能记录该 header（无害）

因此，即使 `X-Gateway-Priority` 意外透传给 vLLM，**功能完全不受影响**。

### 2.3 灰度上线策略

```
                        ┌─── 10% ───→ LLM-LA ──(strip header)──→ vLLM 集群
BoomGateway ────────────┤             识别 X-Gateway-Priority，VIP 优先调度
(所有请求均携带           │
 X-Gateway-Priority header) └─── 90% ───→ 普通 vLLM
                                       收到 X-Gateway-Priority，忽略，正常服务
```

**灰度期各阶段状态：**

| 阶段 | 流量占比 | VIP 优先调度 | 服务影响 |
|------|---------|------------|---------|
| 灰度期 → LLM-LA | 10% | **生效** | 无 |
| 灰度期 → 普通 vLLM | 90% | 不生效（header 被忽略） | **无** |
| 全量切换后 | 100% → LLM-LA | 全部生效 | 无 |

**关键优势**：BoomGateway 只需修改一次，无需根据目标下游（LLM-LA vs vLLM）区分是否加 header。统一注入，各自处理，灰度切换对 BoomGateway 完全透明。

### 2.4 请求流转 curl 示例

请求经过两段链路，header 完全不同：

**第一段：客户端 → 网关**（用户手动发起，携带用户自己的 API key）

```bash
curl -X POST http://gateway.example.com/v1/chat/completions \
  -H "Authorization: Bearer sk-your-user-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3",
    "messages": [{"role": "user", "content": "give me 10 emoji"}],
    "stream": true
  }'
```

> `-H` 设置 HTTP Header，`-d` 设置请求 body（JSON 内容）。  
> 这一段 **没有** `X-Gateway-Priority`，网关尚未介入。

**第二段：网关 → LLM-LA / vLLM**（网关内部自动构造并发出，对用户不可见）

```http
POST /v1/chat/completions HTTP/1.1
Host: llm-la.internal:8000
Authorization: Bearer sk-provider-key
Content-Type: application/json
X-Gateway-Priority: 100

{"model":"actual-model-id","messages":[{"role":"user","content":"give me 10 emoji"}],"stream":true}
```

> - `Authorization` 变成了 **provider 侧的 key**（YAML 配置里的 `api_key`），用户的 key 在认证层已被消费。
> - `X-Gateway-Priority: 100` 是 **本次新增的**，只出现在这段链路。
> - `model` 被替换为 provider 侧的实际模型 ID。

**直连测试**：对网关发送 curl，如果你的 key 在 DB 中标记为 VIP（`metadata.vip = true`），网关转发时就会带 `X-Gateway-Priority: 100`。可在 LLM-LA 或 vLLM 的访问日志中确认。

### 2.5 HTTP Header 容量与格式说明

HTTP Header 是纯文本的 key-value 对，每行一个，用 `\r\n` 分隔。value 是 ASCII 字符串（数字、字母、标点均可），**不是 JSON**。一个实际发出的请求示例：

```http
POST /v1/chat/completions HTTP/1.1
Host: llm-la.example.com
Authorization: Bearer sk-provider-xxx
Content-Type: application/json
X-Gateway-Priority: 100

{"model":"llama3","messages":[...]}
```

**容量限制：**

| 维度 | 典型上限 |
|------|---------|
| 单个 header value | 4KB ~ 8KB（nginx 默认 4KB/行） |
| 所有 header 总和 | 8KB ~ 16KB（取决于 server 配置） |
| vLLM (uvicorn) | 约 8KB 总 header |

`X-Gateway-Priority: 100` 仅约 **25 字节**，即使未来扩展多个自定义 header（如 `X-Gateway-Team-Id`、`X-Gateway-Request-Id` 等）也远不会触及限制。

### 2.6 Extensibility（可扩展性）

当前实现使用数值优先级（`0` = 普通，`100` = VIP），预留了细粒度扩展空间：

**优先级层级扩展：**

| 级别 | 数值 | 场景 |
|------|------|------|
| 普通 | 0 | 默认请求 |
| 高优 | 50 | 付费用户（未来） |
| 紧急 | 70 | 内部服务调用（未来） |
| VIP | 100 | VIP API key |

扩展时只需在 `RequestContext` 中新增常量，LLM-LA 按数值排序调度即可，**无需修改 header 协议**。

**Header 命名空间扩展：**

`X-Gateway-*` 作为 BoomGateway 的 header 前缀，未来可透传更多网关内部元数据给中间调度层：

```
X-Gateway-Priority: 100          ← 优先级（已实现）
X-Gateway-Team-Id: team_abc      ← 团队标识（未来，用于团队级调度）
X-Gateway-Request-Id: req_xxx    ← 请求追踪（未来，用于端到端链路追踪）
X-Gateway-Model-Hint: llama3-70b ← 模型提示（未来，用于调度器感知模型）
```

这些 header 对不认识它们的 vLLM 实例**完全无影响**（HTTP 协议规定未知 header 应忽略），因此可以随需增加，无兼容性风险。

---

## 3. Low-Level Design

### 3.1 最小改动实现路径

需修改的文件共 **7 处**，新增文件 **0 处**：

```
boom-gateway/
  boom-core/src/provider.rs       ← [1] 新增 RequestContext 结构体，扩展 Provider trait
  boom-provider/src/openai.rs     ← [2] 注入 X-Gateway-Priority header
  boom-provider/src/anthropic.rs  ← [2b] 适配 trait 签名（_ctx unused）
  boom-provider/src/azure.rs      ← [2c] 适配 trait 签名（_ctx unused）
  boom-provider/src/bedrock.rs    ← [2d] 适配 trait 签名（_ctx unused）
  boom-provider/src/gemini.rs     ← [2e] 适配 trait 签名（_ctx unused）
  boom-main/src/routes.rs         ← [3] 构造 RequestContext 并传入 provider 调用（4 处调用点）
```

### 3.2 改动详情

#### [1] `boom-core/src/provider.rs` — 新增 `RequestContext`

```rust
pub struct RequestContext {
    /// Numeric priority (0 = normal, 100 = VIP).
    /// Injected as `X-Gateway-Priority: <value>` header.
    pub priority: u32,
}

impl RequestContext {
    pub const PRIORITY_NORMAL: u32 = 0;
    pub const PRIORITY_VIP: u32 = 100;
}
```

同时修改 `Provider` trait 签名：

```rust
#[async_trait]
pub trait Provider: Send + Sync {
    async fn chat(
        &self,
        req: ChatCompletionRequest,
        ctx: &RequestContext,        // 新增
    ) -> Result<ChatCompletionResponse, GatewayError>;

    async fn chat_stream(
        &self,
        req: ChatCompletionRequest,
        ctx: &RequestContext,        // 新增
    ) -> Result<ChatStream, GatewayError>;

    // ... 其余方法不变
}
```

#### [2] `boom-provider/src/openai.rs` — 注入 Header

在 `chat()` 和 `chat_stream()` 的 `RequestBuilder` 构造处加入：

```rust
builder = builder.header("X-Gateway-Priority", ctx.priority.to_string());
```

#### [3] `boom-main/src/routes.rs` — 构造 `RequestContext`

在 `chat_completions_inner` / `messages` 等 handler 中，`acquire_fc_guard` 调用之后，构造 ctx 并传入 provider：

```rust
let is_vip = is_vip_key(&identity.metadata);

// Flow control（已有逻辑，不变）
let fc_guard = acquire_fc_guard(&state, ..., is_vip, ...).await?;

// 新增：构造请求上下文，将 VIP 状态映射为数值优先级
let req_ctx = RequestContext {
    priority: if is_vip { RequestContext::PRIORITY_VIP } else { RequestContext::PRIORITY_NORMAL },
};

// 原来：provider.chat_stream(req)
// 改为：
let stream = provider.chat_stream(req, &req_ctx).await?;
```

### 3.3 LLM-LA 侧实现（参考）

```python
# LLM-LA 接收请求，读取数值优先级
priority = int(request.headers.get("X-Gateway-Priority", "0"))

# 按数值大小调度（数值越大优先级越高）
target = scheduler.select(priority=priority)

# 转发前 strip gateway-internal headers
forward_headers = {
    k: v for k, v in request.headers.items()
    if not k.lower().startswith("x-gateway-")
}
# POST to target vLLM with forward_headers
```

### 3.4 其他 Provider 的处理

`OpenAIProvider` 之外的 provider（`AzureProvider`、`AnthropicProvider`、`BedrockProvider`、`GeminiProvider`）不经过 LLM-LA，**无需注入该 header**。各自 impl 中让 `ctx` 参数 unused（`_ctx`），仅在 `OpenAIProvider` 中注入 header。

### 3.5 改动影响范围

| 模块 | 改动类型 | 风险 |
|------|---------|------|
| `boom-core` | 新增 `RequestContext` struct，Provider trait 加参数 | 编译时强制所有 impl 适配，无遗漏风险 |
| `boom-provider` | `openai.rs` 加 header 注入；其余 4 个 provider 仅适配签名 | 低，header 对现有 vLLM 无影响 |
| `boom-main` | routes.rs 4 处调用点各加 `&ctx`（chat/messages × stream/non-stream） | 低，逻辑不变 |
| `boom-dashboard` | 不涉及 Provider trait，无需修改 | 无 |
| `boom-limiter` / `boom-routing` / `boom-audit` | 不涉及 | 无 |

---

## 4. 验证方案

1. **单元测试**：在 `boom-provider` 中增加 mock 测试，验证 `is_vip=true` 时请求 header 包含 `X-Gateway-Priority: vip`。
2. **集成验证**：用 `tcpdump` / `wireshark` 抓 BoomGateway → LLM-LA 流量，确认 header 存在。
3. **回归验证**：灰度期间直连 vLLM 的 90% 流量，观察错误率和延迟无变化。
