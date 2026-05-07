# BooMGateway — System Design Document

**Version:** 1.0  
**Branch:** `system_design`  
**Last Updated:** May 2026  
**Authors:** Intelligence Boom Platform Team

---

## Table of Contents

1. [Background & Motivation](#1-background--motivation)
2. [Requirements](#2-requirements)
3. [High-Level Design](#3-high-level-design)
4. [Low-Level Design](#4-low-level-design)
5. [Data Model](#5-data-model)
6. [API Reference](#6-api-reference)
7. [Key Algorithms & Patterns](#7-key-algorithms--patterns)
8. [Observability & Audit](#8-observability--audit)
9. [Operational Guide](#9-operational-guide)
10. [Benchmarks & Performance Characteristics](#10-benchmarks--performance-characteristics)
11. [Future Work](#11-future-work)

---

## 1. Background & Motivation

### 1.1 Problem Statement

Large Language Model (LLM) APIs from vendors like OpenAI, Anthropic, and AWS Bedrock are increasingly central to AI-powered products. Operating them at scale introduces several pain points:

- **Multi-provider fragmentation**: each vendor has different API shapes, authentication schemes, and response formats.
- **Cost & quota management**: teams share API keys without visibility into who consumes what quota.
- **Rate limiting at the platform level**: vendor-side rate limits are opaque; platform-side limits per team or per user are absent.
- **Auditability**: no centralized log of which key sent which prompt, how long it took, how many tokens were used.
- **Zero-downtime configuration updates**: adding or removing a model deployment should not require restarting the server.

### 1.2 Existing Solutions and Gaps

[LiteLLM](https://github.com/BerriAI/litellm) provides a Python-based proxy that unifies LLM APIs. However, for an organization that:
- requires sub-millisecond gateway overhead,
- operates a large number of concurrent streaming requests,
- needs fine-grained per-deployment flow control (token-budget queuing),
- wants to remain compatible with existing LiteLLM API keys and team tables,

...a Python runtime becomes a bottleneck and Python's GIL limits true parallelism for IO-heavy workloads.

### 1.3 Solution: BooMGateway

**BooMGateway** is a high-performance LLM API gateway written in **Rust**. It:

- Presents a **unified OpenAI-compatible API** (`/v1/chat/completions`, `/v1/messages`, etc.) regardless of which backend provider handles the request.
- Is **LiteLLM key-compatible**: reads authentication tokens directly from the LiteLLM PostgreSQL schema (`LiteLLM_VerificationToken`, `LiteLLM_TeamTable`).
- Implements **plan-based rate limiting** with concurrency guards and multi-window sliding window counters — fully in-memory with periodic DB persistence.
- Supports **per-deployment flow control** with a context-budget queue and VIP priority lanes.
- Enables **zero-downtime hot reload** of configuration via SIGHUP or `POST /admin/config/reload`.
- Ships a built-in **Web Dashboard** for admin operations (model management, plan management, usage monitoring).

---

## 2. Requirements

### 2.1 Functional Requirements

| ID | Requirement |
|----|-------------|
| F-01 | Accept OpenAI-compatible requests (`/v1/chat/completions`, `/v1/completions`, `/v1/messages`) and route them to the correct backend provider. |
| F-02 | Support multiple LLM providers: OpenAI, Anthropic, Azure OpenAI, AWS Bedrock, Google Gemini. |
| F-03 | Translate Anthropic Messages API format to internal OpenAI format and back. |
| F-04 | Authenticate requests using LiteLLM-compatible API keys (SHA-256 hashed lookup in PostgreSQL), team keys, or a master key. |
| F-05 | Enforce per-key and per-plan rate limits: RPM, concurrency, and arbitrary sliding-window token budgets. |
| F-06 | Support model aliases: a user-facing name that resolves to one or more backend deployments. |
| F-07 | Route to one of multiple deployments for a given model name using pluggable scheduling policies (round-robin, key-affinity). |
| F-08 | Queue requests per-deployment when in-flight count or context-char budget is exceeded (flow control). |
| F-09 | Persist all requests to an audit log table (`boom_request_log`) with token counts and latency. |
| F-10 | Support hot reload of all model/alias/plan configuration without dropping connections. |
| F-11 | Provide a Web Dashboard for admin operations: model CRUD, alias management, plan management, key assignment, live inflight view, prompt log browsing. |
| F-12 | Automatically disable a deployment after 3 consecutive upstream failures. |
| F-13 | Expose health check endpoints (`/health`, `/health/live`, `/health/ready`). |

### 2.2 Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NF-01 | **Latency**: Gateway overhead ≤ 1 ms p99 on the hot path (auth + rate limit check + routing), excluding network I/O. |
| NF-02 | **Throughput**: Handle ≥ 10,000 concurrent streaming connections per instance. |
| NF-03 | **Availability**: Zero downtime on config reload; graceful process restart supported. |
| NF-04 | **Observability**: Structured JSON logs (tracing), per-request audit log in PostgreSQL. |
| NF-05 | **Correctness**: Rate limit counters must not be lost on hot reload. |
| NF-06 | **Safety**: No cross-module table ownership violations; each module owns its DB tables. |
| NF-07 | **Security**: Master key required for all admin operations; Dashboard uses short-lived JWTs. |

---

## 3. High-Level Design

### 3.1 Deployment Topology

```
                ┌──────────────────────────────────────────────┐
                │              Client (AI Application)         │
                └──────────────────┬───────────────────────────┘
                                   │  HTTPS / HTTP
                                   ▼
                ┌──────────────────────────────────────────────┐
                │        Pingora Load Balancer (misc/LB)       │
                │  • TLS termination                           │
                │  • Host/path/IP-based routing rules          │
                │  • Hot-reloads routes.yaml via inotify       │
                └──────────────────┬───────────────────────────┘
                                   │  HTTP (plain)
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
          ┌──────────────────┐        ┌──────────────────┐
          │  BooMGateway #1  │        │  BooMGateway #2  │   (N instances)
          │  (boom-main)     │        │  (boom-main)     │
          └────────┬─────────┘        └────────┬─────────┘
                   │                           │
                   └───────────┬───────────────┘
                               │
              ┌────────────────┼───────────────────┐
              ▼                ▼                   ▼
       ┌────────────┐  ┌─────────────┐   ┌─────────────────┐
       │ OpenAI API │  │Anthropic API│   │  AWS Bedrock /  │
       │            │  │             │   │  Azure / Gemini  │
       └────────────┘  └─────────────┘   └─────────────────┘
              
       ┌────────────────────────────────────┐
       │         PostgreSQL Database        │
       │  • LiteLLM auth tables (read-only) │
       │  • boom_request_log (audit)        │
       │  • boom_model_deployment           │
       │  • boom_model_alias                │
       │  • boom_rate_limit_plan            │
       │  • boom_rate_limit_state           │
       │  • boom_key_plan_assignment        │
       │  • boom_config                     │
       └────────────────────────────────────┘
```

### 3.2 Request Lifecycle

A standard chat completion request flows through the following stages:

```
Client Request
     │
     ▼
[1] TLS Termination (Pingora LB)
     │
     ▼
[2] HTTP Routing → BooMGateway instance
     │
     ▼
[3] Authentication (boom-auth)
    • Extract Bearer token from Authorization header
    • SHA-256 hash lookup in LiteLLM_VerificationToken (DB)
    • Resolve team membership, model whitelist, RPM limit
     │
     ▼
[4] Model Access Check (boom-routing)
    • Direct match → key's model whitelist
    • Alias resolution → target model whitelist
    • Public model bypass
     │
     ▼
[5] Rate Limit Check (boom-limiter)
    • Resolve plan: key assignment → default plan → no plan
    • Concurrency guard (RAII, held until stream end)
    • Sliding-window RPM + custom window budget counters
     │
     ▼
[6] Provider Selection (boom-routing)
    • Exact model name → list of provider instances
    • Alias → target model → provider instances
    • Unknown model → wildcard "*" catch-all
    • SchedulePolicy selects one (round-robin or key-affinity)
     │
     ▼
[7] Flow Control (boom-flowcontrol)
    • Per-deployment in-flight count & context-char budget
    • VIP queue (priority) + normal queue
    • Queues request if limits exceeded; times out after 1200s
     │
     ▼
[8] Provider I/O (boom-provider)
    • Build HTTP request to upstream vendor API
    • Streaming: returns ChatStream (async Stream of chunks)
    • Non-streaming: returns ChatCompletionResponse
     │
     ▼
[9] Response Wrapping (boom-main/routes.rs)
    • Non-stream: log immediately, return JSON
    • Stream: wrap in LoggedStream (logs on Drop),
              InFlightStream (releases inflight counter on Drop),
              FlowControlledStream (releases FC guard on Drop),
              GuardedStream (releases concurrency guard on Drop)
    • Anthropic path: transcode OpenAI chunks → Anthropic SSE events
     │
     ▼
[10] Audit Log (boom-audit)
     • Written asynchronously (fire-and-forget spawn)
     • Fields: request_id, key_hash, model, tokens, duration_ms
```

### 3.3 Module Dependency Graph

```
boom-core  ◄──── boom-auth
           ◄──── boom-config
           ◄──── boom-provider
           ◄──── boom-limiter
           ◄──── boom-routing
           ◄──── boom-audit
           ◄──── boom-flowcontrol
           ◄──── boom-promptlog
           ◄──── boom-dashboard

boom-main  ────► boom-core
           ────► boom-auth
           ────► boom-config
           ────► boom-provider
           ────► boom-limiter
           ────► boom-routing
           ────► boom-audit
           ────► boom-flowcontrol
           ────► boom-promptlog
           ────► boom-dashboard
```

**Key invariant**: `boom-core` is the only leaf dependency. All feature modules depend only on `boom-core`. `boom-main` is the only root that wires everything together.

### 3.4 AppState Layout

```
AppState (Clone, heap-allocated via Arc, survives full process lifetime)
  │
  ├─ config_path: String                    -- path for hot-reload
  ├─ inner: Arc<ArcSwap<AppStateInner>>     -- hot-swappable config+auth
  │    ├─ config: Config
  │    ├─ auth: Arc<dyn Authenticator>
  │    └─ health: HealthStatus
  │
  ├─ db_pool: Option<PgPool>                -- survives reload
  ├─ limiter: Arc<SlidingWindowLimiter>     -- survives reload (preserves counters)
  ├─ plan_store: Arc<PlanStore>             -- survives reload (preserves assignments)
  ├─ deployment_store: Arc<DeploymentStore> -- survives reload (DashMap)
  ├─ alias_store: Arc<AliasStore>           -- survives reload (DashMap)
  ├─ router: Arc<Router>                    -- wraps above stores; policy hot-swappable
  ├─ inflight: Arc<InFlightTracker>         -- per-model/per-deployment inflight counts
  ├─ flow_controller: Arc<FlowController>  -- per-deployment queue (survives reload)
  ├─ failure_counter: Arc<DashMap<...>>     -- deployment failure counts
  ├─ debug_store: Arc<DebugErrorStore>      -- on-demand debug capture
  ├─ prompt_log_writer: PromptLogWriter     -- full prompt/response capture
  └─ request_count: Arc<AtomicU64>          -- 60s request summary counter
```

---

## 4. Low-Level Design

### 4.1 boom-core

**Role**: Defines the shared trait interface and public types. Has zero business logic.

Key types:
- `trait Provider`: `async fn chat(req)` + `async fn chat_stream(req)` + `fn deployment_id()` — the single abstraction over all LLM backends.
- `trait Authenticator`: `async fn authenticate(key)` — returns `AuthIdentity`.
- `trait RateLimiter`: checked-and-recorded rate limit decision.
- `ChatCompletionRequest / ChatCompletionResponse / ChatStream` — canonical internal request/response types.
- `GatewayError` — unified error enum with `status_code()` and `error_type()` methods; maps to OpenAI error format.
- `DebugErrorStore` — ring buffer for capturing upstream error details on demand.

### 4.2 boom-auth

**Role**: API key authentication with LiteLLM compatibility.

Flow:
1. Extract `Authorization: Bearer sk-...` header.
2. SHA-256 hash the key.
3. Query `LiteLLM_VerificationToken` by `token = $hash` (DB lookup, cached per request).
4. If no DB or query fails, fall back to master key comparison.
5. Resolve team-level metadata from `LiteLLM_TeamTable` (joined via `team_id`).
6. Return `AuthIdentity` containing: `key_hash`, `key_name`, `key_alias`, `team_id`, `models: Vec<String>`, `rpm_limit`, `metadata`.

`DbAuthenticator` is the concrete implementation. On reload, a new `DbAuthenticator` is constructed and atomically swapped into `AppStateInner`.

### 4.3 boom-config

**Role**: YAML configuration loading and environment variable expansion.

The `Config` struct covers:
- `general_settings`: host, port, master_key, database_url, public_models.
- `model_list`: list of `ModelEntry` (model_name + litellm_params + model_info + flow_control + enabled).
- `router_settings`: schedule_policy, key_affinity thresholds, model_group_alias map.
- `rate_limit`: global window_limits fallback.
- `plan_settings`: named plans with concurrency/rpm/window limits and time-based schedules.
- `prompt_log`: optional full-capture prompt logging config.

Environment variable expansion: any value starting with `${` is resolved at load time via `std::env::var`.

### 4.4 boom-provider

**Role**: Constructs concrete `Provider` trait objects from configuration.

Supported providers (detected by `litellm_model` prefix):
| Prefix | Provider |
|--------|----------|
| `openai/` or no prefix | OpenAI (`/v1/chat/completions`) |
| `azure/` | Azure OpenAI |
| `anthropic/` | Anthropic |
| `bedrock/` | AWS Bedrock (SigV4 auth) |
| `gemini/` | Google Gemini |

`create_provider(litellm_model, api_key, api_base, timeout, extra, deployment_id)` is the single factory function. Returns `Arc<dyn Provider>`.

Each provider translates the internal `ChatCompletionRequest` into the vendor-specific HTTP request body, dispatches via `reqwest`, and parses the response back into `ChatCompletionResponse` or `ChatStream`.

### 4.5 boom-routing

**Role**: Maintains the model→providers mapping and selects a provider per request.

**DeploymentStore**: `DashMap<String, Vec<Arc<dyn Provider>>>` — maps `model_name` to the list of available provider instances. Supports `add_deployment`, `get_providers`, `contains`, `len`, and `set_quota_ratio`.

**AliasStore**: `DashMap<String, AliasTarget>` — maps alias names to target model names and visibility flags.

**Router**: Unified routing logic:
1. Exact match in DeploymentStore.
2. Alias resolution via AliasStore → DeploymentStore.
3. Wildcard `"*"` catch-all (only for completely unknown model names; never used as fallback for a configured-but-down model).

**SchedulePolicy** trait with two implementations:
- `RoundRobinPolicy`: atomic counter mod list length. Simple and fair.
- `KeyAffinityPolicy`: prefers the deployment already handling long-context requests from the same key. Falls back to round-robin when no affinity match; can rebalance when load skew exceeds a threshold. Uses `InFlightTracker` and `FlowController.total_load()` for load awareness.

**InFlightTracker**: `DashMap<String, DashMap<String, AtomicU64>>` — tracks `(model → deployment_id → inflight_context_chars)` and `(model → deployment_id → inflight_count)`. Used by `KeyAffinityPolicy` and the Dashboard.

### 4.6 boom-limiter

**Role**: In-memory rate limiting with PostgreSQL persistence.

**SlidingWindowLimiter**: Implements sliding window counters per `RateLimitKey {key_hash, model}`.
- **Window**: each `(key, model, window_secs)` maps to a ring of `[current_bucket_count, previous_bucket_count, bucket_start_ts]`.
- `check_and_record(key, rpm_limit, window_limits, weight)`: atomically records the request and returns `RateLimitDecision { allowed, remaining, limit, rejected_window_secs, retry_after_secs }`.
- `rollback_plan_windows(key, windows, weight)`: decrements window counters when an upstream failure occurs (RPM counters are never rolled back — DDoS protection).
- **Persistence**: `sync_counters_to_db` upserts counter snapshots to `boom_rate_limit_state`; `restore_counters_from_db` reloads them at startup.

**PlanStore**: Manages rate limit plans and key-to-plan assignments.
- Plans: `DashMap<String, RateLimitPlan>` — each plan has `concurrency_limit`, `rpm_limit`, `window_limits`, and optional time-based `schedule` (different limits for different hours of day).
- Assignments: `DashMap<String, String>` — maps `key_hash` to `plan_name`.
- `resolve_plan(key_hash)`: returns the effective plan for a key (assignment → default plan).
- `effective_limits()`: evaluates schedule slots against current UTC hour to pick active limits.
- `try_acquire(key_hash, limit)`: returns a `ConcurrencyGuard` (RAII decrement on drop) or `None` if at limit.

### 4.7 boom-flowcontrol

**Role**: Per-deployment request queuing with context-budget awareness and VIP priority.

**FlowController**: `DashMap<String, FlowControlSlot>` — one slot per `deployment_id`.

**FlowControlSlot**: A `Mutex<SlotInner>` containing:
- `max_inflight: u32` — max simultaneous in-flight requests.
- `max_context: u64` — max total context chars across all in-flight requests.
- `vip_queue: VecDeque<QueuedRequest>` — VIP (priority) waiters.
- `normal_queue: VecDeque<QueuedRequest>` — standard waiters.

No separate counters — **the queue IS the source of truth**. A request marked `dispatched=true` counts as in-flight; removing it from the queue is the only rollback needed.

**Acquire flow**:
1. Reject immediately if `context_chars > max_context` (can never fit).
2. Append `QueuedRequest` to the appropriate queue.
3. Call `dispatch()` to greedily fill available capacity.
4. Await `oneshot::Receiver` with a 1200s timeout.
5. If dispatched before timeout: return `FlowControlGuard`.
6. On timeout: check if already dispatched; return guard or `Timeout` error.

**RAII safety**: `AcquireCleanup` is a `Drop` guard on the acquire future. If the request is cancelled (client disconnect), it removes the request from the queue and re-triggers dispatch.

**FlowControlGuard**: Removes request from queue on `Drop`, triggering dispatch to fill freed capacity.

**Periodic dispatch**: A background task calls `flow_controller.periodic_dispatch()` every 1s to ensure queued requests are dispatched even when no guard drops (e.g. very long-running requests).

### 4.8 boom-audit

**Role**: Asynchronous request log writes to PostgreSQL.

`log_request(pool, RequestLog)` spawns a Tokio task to insert one row into `boom_request_log`. Fire-and-forget — never blocks the response path.

`LoggedStream<S>` wraps a response stream. On `Drop` (stream fully consumed or connection dropped), it computes the real duration from `Instant::now() - start` and spawns the audit write. This ensures streaming responses record the **actual end time**, not the time the stream was created.

### 4.9 boom-dashboard

**Role**: Web management UI + REST admin API.

- **Frontend**: Single-page app (plain JS, no build step) embedded in the binary via `include_str!`. Served at `/dashboard`.
- **Authentication**: `POST /dashboard/login` returns a short-lived JWT signed with the master key. All dashboard API routes verify the JWT.
- **Admin operations**: Dashboard handlers send `AdminCommand` variants over an `mpsc::channel` to `boom-main`'s `admin_command_handler`. This decouples dashboard from `boom-provider` and `boom-config`.

`AdminCommand` variants include:
- `CreateDeployment` / `UpdateDeployment` / `DeleteDeployment`
- `CreateAlias` / `DeleteAlias`
- `EnableDeployment` / `DisableDeployment`
- `ReloadConfig`
- `SetDebugMode`

### 4.10 boom-promptlog

**Role**: Optional full-fidelity capture of request and response bodies for audit/debugging.

`PromptLogWriter` spawns a background Tokio task that receives `PromptLogEntry` values from a channel and writes them to a configurable sink (file, S3, etc.).

`PromptLogStream<S>` wraps a response stream. On stream completion, it serializes the accumulated SSE chunks into a response body and sends a `PromptLogEntry` to the writer.

Capture is filtered by key hash or team ID via `should_capture(key_hash, team_id)`.

### 4.11 boom-main (Assembly Layer)

**Role**: Wires all modules together, owns the Axum router, manages background tasks, and handles hot reload.

**Startup sequence** (`AppState::from_config`):
1. Connect PostgreSQL pool.
2. Initialize all stores (limiter, plan_store, deployment_store, alias_store, inflight, flow_controller).
3. Build deployments/aliases/plans from YAML into memory stores.
4. Run DB migrations.
5. `sync_yaml_to_db()` — upsert `source='yaml'` rows, resolve name conflicts.
6. `load_db_only_*()` — layer `source='db'` records on top of YAML-built stores.
7. Restore runtime state (assignments, counters) from DB.
8. Build `AppStateInner` (config + auth + health) and wrap in `ArcSwap`.

**Background tasks**:
- `spawn_sighup_listener`: triggers `AppState::reload()` on SIGHUP (Unix only).
- `spawn_sync_task`: every 10 minutes, snapshots rate limit counters and plan assignments to DB; cleans up expired in-memory entries.
- `spawn_request_summary`: every 60 seconds, logs total requests in the last minute.
- `spawn_periodic_fc_dispatch`: every 1 second, calls `flow_controller.periodic_dispatch()`.

---

## 5. Data Model

### 5.1 Database Tables

#### boom_request_log (owned by boom-audit)
| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| request_id | TEXT | UUID v4 |
| key_hash | TEXT | SHA-256 of the API key |
| key_name | TEXT | human-readable key name |
| key_alias | TEXT | optional alias |
| team_id | TEXT | LiteLLM team ID |
| model | TEXT | requested model name |
| model_name | TEXT | resolved deployment model name |
| api_path | TEXT | `/v1/chat/completions` etc. |
| is_stream | BOOL | |
| status_code | INT | HTTP status |
| error_type | TEXT | null on success |
| error_message | TEXT | null on success |
| input_tokens | INT | null for streaming until completion |
| output_tokens | INT | |
| duration_ms | INT | wall-clock from request start to last byte |
| deployment_id | TEXT | which backend deployment handled this |
| created_at | TIMESTAMPTZ | default now() |

#### boom_model_deployment (owned by boom-routing)
| Column | Type | Notes |
|--------|------|-------|
| id | BIGSERIAL PK | |
| deployment_id | TEXT UNIQUE | stable user-assigned ID |
| model_name | TEXT | the API-facing name |
| litellm_model | TEXT | e.g. `openai/gpt-4o` |
| source | TEXT | `'yaml'` or `'db'` |
| enabled | BOOL | |
| api_key | TEXT | |
| api_base | TEXT | |
| rpm / tpm | BIGINT | per-deployment rate limits |
| timeout | BIGINT | seconds |
| max_inflight_queue_len | INT | flow control |
| max_context_len | BIGINT | flow control (chars) |
| quota_count_ratio | BIGINT | quota cost multiplier |

#### boom_model_alias (owned by boom-routing)
| Column | Type |
|--------|------|
| alias_name | TEXT PK |
| target_model | TEXT |
| hidden | BOOL |
| source | TEXT |

#### boom_rate_limit_plan (owned by boom-limiter)
| Column | Type |
|--------|------|
| name | TEXT PK |
| concurrency_limit | INT |
| rpm_limit | BIGINT |
| window_limits | JSONB |
| schedule | JSONB |
| is_default | BOOL |
| source | TEXT |

#### boom_key_plan_assignment (owned by boom-limiter)
| Column | Type |
|--------|------|
| key_hash | TEXT PK |
| plan_name | TEXT |

#### boom_rate_limit_state (owned by boom-limiter)
Snapshot of in-memory sliding window counters for crash recovery.

#### boom_config (owned by boom-dashboard)
Key-value store for dashboard-managed settings.

#### LiteLLM_VerificationToken (read-only, owned by LiteLLM)
The external auth table. BooMGateway only reads this.

### 5.2 In-Memory State

```
SlidingWindowLimiter
  DashMap<RateLimitKey, Arc<Mutex<WindowState>>>
    RateLimitKey { key_hash, model }
    WindowState { buckets: Vec<Bucket> }

PlanStore
  DashMap<plan_name, RateLimitPlan>      -- plan definitions
  DashMap<key_hash, plan_name>           -- assignments
  DashMap<key_hash, AtomicU32>           -- concurrency counters

DeploymentStore
  DashMap<model_name, Vec<Arc<dyn Provider>>>
  DashMap<model_name, u64>               -- quota_count_ratio

AliasStore
  DashMap<alias, AliasTarget { model, hidden }>

FlowController
  DashMap<deployment_id, FlowControlSlot>

InFlightTracker
  DashMap<model, DashMap<deployment_id, InFlightEntry { count, context_chars }>>
```

---

## 6. API Reference

### 6.1 LLM Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/v1/chat/completions` | Bearer key | Chat completions (OpenAI format) |
| POST | `/chat/completions` | Bearer key | Same, without `/v1` prefix |
| POST | `/v1/completions` | Bearer key | Legacy text completions |
| POST | `/completions` | Bearer key | Same, without `/v1` prefix |
| POST | `/v1/messages` | Bearer key | Anthropic Messages API |
| GET | `/v1/models` | Bearer key | List accessible models |
| GET | `/v1/models/{id}` | Bearer key | Get model info |

### 6.2 Health Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | None | Full health status (JSON) |
| GET | `/health/live` | None | Liveness probe (plain `ok`) |
| GET | `/health/ready` | None | Readiness probe (200 / 503) |

### 6.3 Admin Endpoints

All admin endpoints require the master key.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/admin/config/reload` | Hot-reload config from YAML |
| GET | `/admin/plans` | List all rate limit plans |
| PUT | `/admin/plans` | Create/update a plan |
| DELETE | `/admin/plans/{name}` | Delete a plan |
| POST | `/admin/plans/assign` | Assign key to plan |
| DELETE | `/admin/plans/assign/{key_hash}` | Unassign key from plan |
| GET | `/admin/plans/assignments` | List all key-plan assignments |

### 6.4 Dashboard Endpoints

Available under `/dashboard/` prefix. Full CRUD for models, aliases, plans, plus live monitoring views and prompt log browsing.

---

## 7. Key Algorithms & Patterns

### 7.1 Zero-Downtime Hot Reload

```
AppState.reload():
  1. Re-read config YAML from disk.
  2. Rebuild deployment/alias/plan stores in-place (clear + repopulate).
     → DashMap operations are lock-free; in-flight reads complete uninterrupted.
  3. sync_yaml_to_db() — upsert source='yaml' rows.
  4. load_db_only_*() — layer source='db' records on top.
  5. Build new AppStateInner (config + new auth instance + updated health).
  6. inner.store(Arc::new(new_inner))  ← ArcSwap atomic swap
     → New requests immediately see new config.
     → In-flight requests hold a reference to the old AppStateInner arc,
        which stays alive until all holders drop it. Zero races.
  7. Rate limit counters, plan assignments, inflight guards → untouched.
```

### 7.2 Sliding Window Rate Limiting

The algorithm uses a **two-bucket sliding window**:
- `current_bucket`: covers requests in the current `window_secs` interval.
- `previous_bucket`: covers the preceding interval.
- Effective count = `previous_bucket * (1 - elapsed/window_secs) + current_bucket`.

This gives a smooth rate estimate without the hard resets of a fixed window. The `weight` parameter allows fractional quota deduction (e.g. a heavy model costs 2× the quota of a lighter one via `quota_count_ratio`).

### 7.3 Flow Control Queue (context-budget scheduling)

The FlowController solves the **"too many long contexts clog the deployment"** problem:

- Each deployment slot has `max_inflight` (request count) and `max_context` (total chars) limits.
- Requests that would exceed either limit are queued, not rejected.
- The `dispatch()` function scans queues greedily: it skips requests whose context would temporarily overflow the budget, waiting until current load drops.
- Because `context_chars ≤ max_context` is checked at enqueue time, every queued request is guaranteed to eventually be dispatchable — no starvation.
- VIP requests (keys with `metadata.vip = true`) are placed in a priority queue dispatched before normal requests.

### 7.4 Key Affinity Routing

When `schedule_policy: key_affinity` is configured, the router prefers to send requests from the same API key to the same deployment:

1. Find deployments that already have inflight requests from this key.
2. Among those, pick the one with the fewest total queue entries.
3. If context of the next request would exceed a `key_affinity_context_threshold` and the affinity deployment is too loaded (> `key_affinity_rebalance_threshold`), rebalance to the least-loaded deployment instead.
4. If no affinity match, fall back to round-robin.

This reduces context fragmentation across deployments and improves cache locality on stateful LLM backends.

### 7.5 RAII Guard Chaining for Streaming Responses

For a streaming response, multiple resources must be released precisely when the stream ends (whether via normal completion or client disconnect):

```
provider.chat_stream(req)
  └─> InFlightStream        [drops InFlightGuard → decrements inflight counter]
        └─> FlowControlledStream  [drops FlowControlGuard → removes from FC queue, triggers dispatch]
              └─> GuardedStream       [drops ConcurrencyGuard → decrements concurrency counter]
                    └─> LoggedStream       [drops → writes audit log with real duration]
                          └─> PromptLogStream    [drops → sends full response to prompt log writer]
                                └─> Sse (axum)
```

Each wrapper implements `futures::Stream` by delegating `poll_next` to its inner stream. When `poll_next` returns `Poll::Ready(None)`, the wrapper drops its guard. Rust's ownership model guarantees this happens exactly once, with no possibility of double-release or leak.

### 7.6 Auto-Disable on Consecutive Failures

```
On each upstream error that qualifies as a deployment failure
  (5xx, timeout, connection refused — not 4xx):

  failure_counter[deployment_id] += 1
  if counter >= AUTO_DISABLE_THRESHOLD (3):
    spawn async task:
      UPDATE boom_model_deployment SET enabled=false WHERE deployment_id=?
      deployment_store.remove_deployment(deployment_id)
      failure_counter.remove(deployment_id)

On successful response:
  failure_counter[deployment_id] = 0
```

---

## 8. Observability & Audit

### 8.1 Structured Logs

All log output is **JSON** via `tracing_subscriber::fmt().json()`. Uses a non-blocking writer (offloaded to a dedicated thread) to prevent Docker log driver back-pressure from blocking the Tokio runtime.

Log levels:
- `INFO`: request accepted (with key, model, plan, RPM remaining, input chars).
- `WARN`: rate limit exceeded, model access denied, deployment failure, auth warning.
- `ERROR`: upstream error, DB error, config reload failure.
- `DEBUG`: rate limit decisions, model access check details.

### 8.2 Audit Log

Every request — success or failure — produces one row in `boom_request_log`. The write is async and never blocks the response path. For streaming responses, the row is written after the last byte is sent (via `LoggedStream::drop`).

### 8.3 Health Endpoint

`GET /health` returns:
```json
{
  "status": "ok",
  "version": "1.x.y",
  "uptime_secs": 3600,
  "db_connected": true,
  "models_count": 42,
  "reload_count": 5,
  "last_reload_at": "2026-05-06T14:00:00Z"
}
```

### 8.4 Dashboard Monitoring

The built-in dashboard provides:
- Live inflight request counts per model and deployment.
- Flow control queue depth and waiters per deployment.
- Recent request log with filtering by key, model, status.
- Rate limit plan usage summary.
- Prompt log browsing (when enabled).

---

## 9. Operational Guide

### 9.1 Configuration

The primary configuration file is `config.yaml`. Key sections:

```yaml
general_settings:
  host: "0.0.0.0"
  port: 4000
  master_key: "${MASTER_KEY}"
  database_url: "${DATABASE_URL}"

model_list:
  - model_name: gpt-4o
    litellm_params:
      model: openai/gpt-4o
      api_key: "${OPENAI_API_KEY}"
      timeout: 120
      rpm: 500
    model_info:
      id: deploy-gpt4o-001     # stable deployment_id for flow control
    flow_control:
      model_queue_limit: 50    # max concurrent in-flight
      model_context_limit: 2000000  # max total context chars
    enabled: true

router_settings:
  schedule_policy: key_affinity
  model_group_alias:
    gpt4: gpt-4o              # alias → target

plan_settings:
  default_plan: standard
  plans:
    standard:
      concurrency_limit: 5
      rpm_limit: 60
    premium:
      concurrency_limit: 20
      rpm_limit: 300
      schedule:
        - hours: "9-17"
          concurrency_limit: 30
          rpm_limit: 500
```

### 9.2 Hot Reload

Config reload can be triggered two ways:
1. `kill -SIGHUP <pid>` — signal-based, no authentication required, works on Unix.
2. `POST /admin/config/reload` with master key — HTTP-based, works everywhere.

Reload preserves:
- All in-flight requests (complete with old config).
- Rate limit counters.
- Plan assignments.
- DB connection pool.

### 9.3 Graceful Restart

`boom-gateway --reboot --port 4000`:
1. Sends SIGTERM to any existing `boom-gateway` process on that port.
2. Probes `/health` to detect when the old process has fully shut down.
3. Detects frozen processes (no response) and sends SIGKILL.
4. Starts the new instance once the port is free.

### 9.4 Deployment Modes

**YAML-only** (no DB):
- All deployments, aliases, and plans from `config.yaml`.
- Master key authentication only.
- No audit log persistence.

**YAML + DB** (recommended for production):
- YAML is the source of truth for `source='yaml'` rows.
- Dashboard and API can add `source='db'` rows that persist across reloads.
- Full audit logging, rate limit state persistence, LiteLLM key authentication.

### 9.5 Load Balancer (misc/LB)

The Pingora-based LB provides:
- TLS termination with configurable certificate.
- HTTP/2 support.
- Route matching by host, path prefix, and client IP CIDR.
- Hot-reload of `routes.yaml` via `inotify` (no restart required).

Start with: `./misc/LB/start.sh start` (auto-generates self-signed cert if none provided).

---

## 10. Benchmarks & Performance Characteristics

### 10.1 Runtime Configuration

- **Tokio worker threads**: 32 (`#[tokio::main(worker_threads = 32)]`).
- **DB pool**: max 30 connections, 10s acquire timeout, 10min idle timeout, 30min max lifetime.
- **Flow control timeout**: 1200s (20 minutes) — reflects long-running LLM inference.

### 10.2 Concurrency Primitives

| Primitive | Usage | Why |
|-----------|-------|-----|
| `DashMap` | All hot-path stores | Lock-free sharded hashmap; ~16 shards by default. No global lock. |
| `ArcSwap` | AppStateInner, Router policy | Fully lock-free reads; swap is a single pointer store. |
| `Arc<AtomicU64>` | Request counter, inflight counts | Wait-free. |
| `tokio::sync::Mutex` | FlowControlSlot inner | Async-aware; held only for the duration of queue mutation (microseconds). |
| `std::sync::Mutex` | Window state buckets | Non-async; held only for bucket read-modify-write (nanoseconds). |

### 10.3 Hot-Path Allocations

The chat completion handler is designed to minimize allocations:
- Auth result (`AuthIdentity`) is a stack-allocated struct (no Box).
- Rate limit key is stack-allocated.
- Provider selection returns `Arc<dyn Provider>` — clone of a reference-counted pointer (no deep copy).
- SSE chunks are forwarded via a Tokio `mpsc::channel(32)` — bounded to avoid unbounded buffering.

### 10.4 Streaming Latency

For SSE streaming responses:
- The gateway adds one channel hop (provider stream → mpsc sender → SSE receiver) with a buffer of 32 chunks.
- `KeepAlive` pings are sent automatically by Axum's `Sse` wrapper to prevent proxy timeouts.
- Tool call argument buffering (consolidating fragmented JSON) adds negligible overhead.

---

## 11. Future Work

### 11.1 Retry & Failover
Currently, if a provider returns an error, the request fails immediately. A future iteration could implement automatic retry on a different deployment for idempotent errors (connection timeout, 502 from provider), while carefully avoiding double-billing for non-idempotent calls.

### 11.2 Token-Based Rate Limiting
The current rate limiting operates on request count. Adding **token-based limits** (limit on `prompt_tokens + completion_tokens` per window) would allow finer cost control but requires estimating or tracking tokens before the response completes — particularly complex for streaming responses.

### 11.3 Multi-Region Active-Active
Each BooMGateway instance maintains its rate limit state in-memory with periodic DB persistence. In a multi-region deployment, rate limit counters from different regions are not synchronized in real time. A Redis-based counter layer (or a CRDTs-based approach) could enable globally consistent rate limits at the cost of added latency.

### 11.4 Prompt Caching Awareness
Some LLM providers (Anthropic, OpenAI) offer prompt caching with reduced costs for cached prefixes. BooMGateway could track cache hit hints from provider responses and adjust quota consumption accordingly.

### 11.5 Circuit Breaker
The current auto-disable mechanism (3 consecutive failures → disable) is simplistic. A proper circuit breaker with half-open state, configurable thresholds, and automatic re-enable after a cooldown period would improve resilience.

### 11.6 Metrics Export
Currently, observability is through structured logs and the built-in dashboard. Exporting Prometheus metrics (request rate, error rate, latency histograms, inflight counts, queue depths) would enable integration with existing monitoring stacks (Grafana, Datadog, etc.).

### 11.7 Dynamic Policy Switching
The scheduling policy (round-robin vs. key-affinity) is set at startup and changed only via config reload. A future enhancement could allow per-model policy configuration and dynamic switching through the Dashboard without a full reload.

### 11.8 Semantic Caching
A semantic cache layer could deduplicate near-identical requests across keys, significantly reducing upstream API costs for workloads with repetitive prompts (e.g. customer support bots).

---

*This document reflects the current state of the `system_design` branch. As the codebase evolves, corresponding sections should be updated.*
