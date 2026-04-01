# BooMGateway Architecture

## Module Dependency Graph

```
                        ┌──────────────┐
                        │  boom-core   │  traits, types, errors
                        └──────┬───────┘
                               │
         ┌──────────┬──────────┼──────────┬──────────┬──────────┐
         │          │          │          │          │          │
  ┌──────▼─────┐ ┌──▼───────┐ ┌▼────────┐ ┌▼────────┐ ┌▼───────┐ ┌▼──────────┐
  │ boom-auth  │ │boom-prov │ │boom-cfg │ │boom-lim │ │boom-rt  │ │boom-audit │
  │ DB auth    │ │HTTP req  │ │YAML cfg │ │sliding  │ │deploy   │ │req log   │
  │ master_key │ │OpenAI/   │ │env vars │ │window   │ │store    │ │write +   │
  │ token cache│ │Anthropic │ │model    │ │concyrre │ │alias    │ │read      │
  │            │ │Bedrock   │ │list     │ │PlanStore│ │store    │ │          │
  └────────────┘ └──────────┘ └─────────┘ └─────────┘ └─────────┘ └──────────┘
                                                                │
       ┌────────────────────────────────────────────────────────┘
       │
  ┌────▼──────────────────────────────────────────────────────────────────────┐
  │ boom-dashboard                                                            │
  │  Web UI (SPA) + REST API + JWT auth                                       │
  │  depends: boom-core, boom-limiter, boom-routing, boom-audit               │
  │  does NOT depend on: boom-provider, boom-config                           │
  │  writes via: AdminCommand channel → boom-main                             │
  └────┬──────────────────────────────────────────────────────────────────────┘
       │
  ┌────▼──────────────────────────────────────────────────────────────────────┐
  │ boom-main (binary)                                                        │
  │  axum HTTP server, router assembly, hot-reload, background tasks          │
  │  depends on: ALL boom-* crates                                            │
  │  ┌───────────┐ ┌───────────┐ ┌──────────┐ ┌───────────┐ ┌────────────┐  │
  │  │ /v1/*     │ │ /health/* │ │ /admin/* │ │/dashboard/│ │ AppState   │  │
  │  │ API proxy │ │ checks    │ │ plans    │ │ Web UI    │ │ hot-reload │  │
  │  └───────────┘ └───────────┘ └──────────┘ └───────────┘ └────────────┘  │
  └───────────────────────────────────────────────────────────────────────────┘
```

## Core Traits (boom-core)

```rust
Provider      :: chat(req) → response | chat_stream(req) → stream
Authenticator :: authenticate(key) → AuthIdentity
RateLimiter   :: check_and_record(key, limits) → RateLimitDecision
```

## Request Flow

```
Client Request
     │
     ▼
┌──────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐   ┌──────────┐
│ Extract  │──▶│   Auth    │──▶│  Rate     │──▶│ Model    │──▶│  Route   │
│ API Key  │   │ DB/master │   │  Limit    │   │ Access   │   │ Dispatch │
└──────────┘   └───────────┘   └───────────┘   │ Check    │   └──────────┘
                                                └──────────┘        │
                                                     ┌──────────────▼──────────────┐
                                                     │  Provider.chat() / stream() │
                                                     │  (upstream LLM API)         │
                                                     └──────────────┬──────────────┘
                                                                    │
                                                     ┌──────────────▼──────────────┐
                                                     │  Response / SSE Stream      │
                                                     │  LoggedStream → audit on Drop│
                                                     │  → client                   │
                                                     └─────────────────────────────┘
```

### Stream Duration Recording

Streaming requests use `LoggedStream<S>` — a Drop-based wrapper that records the real
duration when the stream is fully consumed (or the connection drops), not when the
stream starts:

```
Request start ──▶ Provider.chat_stream() ──▶ LoggedStream wraps SSE ──▶ Client
                                                                    │
                                                         Drop::drop()
                                                                    │
                                                         log_request(real_duration)
```

This applies to all stream paths: `chat_completions`, `messages`, `pt_chat_completions`,
and `pt_messages` (pass-through SSE).

## AppState Structure

```
AppState (Clone, survives reload)
  ├─ config_path                  // YAML file path
  ├─ inner: Arc<ArcSwap<Inner>>   // hot-swappable: config + auth + health
  ├─ db_pool: Option<PgPool>      // survives reload
  ├─ limiter: Arc<SlidingWindowLimiter>       // survives reload
  ├─ plan_store: Arc<PlanStore>               // survives reload
  ├─ deployment_store: Arc<DeploymentStore>   // survives reload
  ├─ alias_store: Arc<AliasStore>             // survives reload
  └─ request_count: Arc<AtomicU64>            // survives reload, /v1/ + /admin/ only

AppStateInner (rebuilt on reload)
  ├─ config: Config
  ├─ auth: Arc<dyn Authenticator>
  └─ health: HealthStatus
```

### DeploymentStore (boom-routing)

```
DashMap<String, Vec<Arc<dyn Provider>>>   // model_name → provider list
DashMap<String, AtomicUsize>              // model_name → round-robin counter

Methods:
  select(model)        → round-robin provider selection
  add_deployment()     → add provider to model group
  set_deployments()    → replace all providers for a model
  remove_deployments() → remove all providers for a model
  clear()              → reset all (before full reload)
```

### AliasStore (boom-routing)

```
DashMap<String, String>    // alias_name → target_model
DashSet<String>            // hidden aliases

Methods:
  resolve(alias)      → Option<target_model>
  set_alias()         → create/update alias
  remove_alias()      → delete alias
  visible_names()     → non-hidden aliases
  clear()             → reset all (before full reload)
```

## AdminCommand Channel (Decoupling Pattern)

Dashboard needs to perform write operations (model CRUD, alias CRUD, config updates)
but must NOT depend on boom-provider or boom-config. Solution:

```
┌──────────────────┐    mpsc::channel    ┌──────────────────────────────┐
│ boom-dashboard   │ ────AdminCommand───▶ │ boom-main                    │
│                  │                      │ admin_command_handler()      │
│ has: DB, stores  │                      │ has: ALL modules, providers  │
│ lacks: provider  │                      │ can create providers, reload │
│ lacks: config    │                      │                              │
└──────────────────┘                      └──────────────────────────────┘

AdminCommand variants:
  UpsertModel(deployment)    → DB + DeploymentStore + build Provider
  DeleteModel(id, name)      → DB + DeploymentStore cleanup
  UpsertAlias(alias)         → DB + AliasStore
  DeleteAlias(name)          → DB + AliasStore cleanup
  UpsertPlan(plan)           → PlanStore + DB dual-write
  DeletePlan(name)           → PlanStore + DB cleanup
```

## Config Source: YAML vs DB

Controlled by `general_settings.store_model_in_db` in YAML config.

### YAML-first mode (`store_model_in_db: false`, default)

```
Startup:
  config.yaml → build providers → DeploymentStore (memory)
  config.yaml → build aliases → AliasStore (memory)
  config.yaml → build plans → PlanStore (memory)
  DB → restore assignments + rate limit counters

Reload:
  re-read YAML → rebuild DeploymentStore + AliasStore + PlanStore
  DB only stores runtime state (assignments, counters)

Changes via Dashboard:
  Models/aliases/plans created via Dashboard API are DB-only
  They persist in DB but are NOT in YAML
```

### DB-first mode (`store_model_in_db: true`, litellm-style)

```
Startup (first run):
  config.yaml → seed into DB tables (source='yaml')
  mark boom_config.db_seeded = true

Startup (subsequent):
  boom_model_deployment → build providers → DeploymentStore
  boom_model_alias → AliasStore
  boom_rate_limit_plan → PlanStore
  boom_key_plan_assignment → PlanStore.assignments
  boom_rate_limit_state → Limiter.counters

Reload:
  re-read YAML → reseed source='yaml' rows only
  source='db' rows (created via Dashboard) are preserved
  reload all stores from DB

Runtime CRUD:
  Dashboard creates/updates/deletes → write DB + update memory
  Changes take effect immediately, survive restart
```

## Auth Flow (boom-auth)

```
API Key (raw)
     │  ┌─ master_key? ──▶ constant-time compare ──▶ admin identity
     ├──┤
     │  └─ sk-xxx? ──▶ SHA-256 ──▶ DB lookup ──▶ user identity
     │
     └─ LiteLLM_VerificationToken (PostgreSQL, read-only)
        ├─ blocked / expired / budget checks
        └─ model access: key.models → team.models → wildcard
```

## Rate Limiting (boom-limiter)

```
Plan (YAML / API defined)
  ├─ concurrency_limit   ──▶ AtomicU32 per key    (RAII ConcurrencyGuard)
  ├─ rpm_limit           ──▶ 60s sliding window   (DashMap counters)
  ├─ window_limits[]     ──▶ custom windows       (e.g. 100 req / 5h)
  ├─ schedule[]          ──▶ time-based overrides (e.g. 9:00-21:00)
  └─ effective_limits()  ──▶ merge schedule × base

Key Assignment:  key_hash ──▶ plan_name ──▶ plan limits
Fallback:        explicit assignment → default_plan → config defaults
```

## Dashboard (boom-dashboard)

```
┌─── State Injection ────────────────────────────────────┐
│  DashboardState (Extension<Arc<...>>)                  │
│  ├─ db_pool           (shared, survives reload)        │
│  ├─ plan_store        (shared, survives reload)        │
│  ├─ limiter           (shared, survives reload)        │
│  ├─ deployment_store  (shared, survives reload)        │
│  ├─ alias_store       (shared, survives reload)        │
│  ├─ admin_tx          (mpsc::Sender<AdminCommand>)     │
│  ├─ jwt_secret        (derived from master_key)        │
│  └─ master_key        (admin login verification)       │
└────────────────────────────────────────────────────────┘

┌─── Auth ───────────────────────────────────────────────┐
│  Admin: "admin" + master_key → JWT cookie (2h)         │
│  User:  user_id + API key → SHA-256 → DB lookup → JWT  │
│  Session: HttpOnly cookie, verified per request         │
└────────────────────────────────────────────────────────┘

┌─── Endpoints ──────────────────────────────────────────┐
│  /dashboard/                → SPA (index.html embedded) │
│  /dashboard/api/auth/*      → login / logout / me       │
│  /dashboard/api/user/*      → plan, usage, key-info     │
│  /dashboard/api/admin/models    → model CRUD            │
│  /dashboard/api/admin/aliases   → alias CRUD            │
│  /dashboard/api/admin/config    → KV config CRUD        │
│  /dashboard/api/admin/plans     → plan CRUD (DB dual-w) │
│  /dashboard/api/admin/keys      → key management        │
│  /dashboard/api/admin/assignments → key-plan assignment  │
│  /dashboard/api/admin/logs      → request log query     │
└────────────────────────────────────────────────────────┘
```

## Hot Reload (boom-main)

```
SIGHUP / POST /admin/config/reload
     │
     ▼
  re-read config.yaml
     │
     ├─ store_model_in_db=true:
     │    reseed YAML rows in DB (source='yaml' only)
     │    reload DeploymentStore + AliasStore + PlanStore from DB
     │
     └─ store_model_in_db=false:
          rebuild DeploymentStore + AliasStore + PlanStore from YAML
     │
     ▼
  build new AppStateInner (config + auth + health)
     │
     ▼
  ArcSwap::store()  ──▶ atomic swap, zero downtime
     │
     └─ preserved: db_pool, limiter counters, plan assignments,
                   deployment_store, alias_store (rebuilt from source)
```

## DB Schema & DDL Ownership

### litellm Compatible (read-only for auth)

```sql
LiteLLM_VerificationToken   ← boom-auth reads for key verification
  ├─ token (SHA-256 hash, primary key)
  ├─ key_name, user_id, team_id
  ├─ models (JSON array, may contain special names)
  ├─ spend, max_budget, blocked, expires
  └─ rpm_limit, tpm_limit, metadata

LiteLLM_TeamTable           ← boom-auth reads for team model resolution
  └─ team_id → models (JSON array)
```

### BooMGateway Tables (full CRUD, by module)

```sql
-- boom-audit owns:
boom_request_log
  ├─ id BIGSERIAL PRIMARY KEY
  ├─ request_id, key_hash, key_name, team_id
  ├─ model, api_path
  ├─ is_stream, status_code
  ├─ error_type, error_message
  ├─ input_tokens, output_tokens, duration_ms
  └─ created_at TIMESTAMPTZ DEFAULT NOW()

-- boom-routing owns:
boom_model_deployment
  ├─ id UUID PRIMARY KEY
  ├─ model_name, litellm_model
  ├─ api_key, api_key_env (env reference flag)
  ├─ api_base, api_version
  ├─ aws_region_name, aws_access_key_id, aws_secret_access_key
  ├─ rpm, tpm, timeout, headers (JSONB)
  ├─ temperature, max_tokens, enabled
  ├─ source TEXT ('yaml' | 'db')
  └─ created_at, updated_at

boom_model_alias
  ├─ alias_name TEXT PRIMARY KEY
  ├─ target_model, hidden, source
  └─ updated_at

-- boom-limiter owns:
boom_rate_limit_plan
  ├─ name TEXT PRIMARY KEY
  ├─ concurrency_limit, rpm_limit
  ├─ window_limits JSONB, schedule JSONB
  ├─ is_default, source
  └─ updated_at

boom_key_plan_assignment
  ├─ key_hash TEXT PRIMARY KEY
  ├─ plan_name, assigned_at

boom_rate_limit_state
  ├─ cache_key TEXT PRIMARY KEY
  ├─ count, window_start, window_secs
  └─ updated_at

-- boom-dashboard owns:
boom_config
  ├─ key TEXT PRIMARY KEY
  ├─ value JSONB
  └─ updated_at TIMESTAMPTZ
  -- stores: db_seeded, rate_limit, plan_settings.default_plan, etc.
```

## Anthropic API Compatibility

The gateway supports Anthropic Messages API (`POST /v1/messages`) for clients like
Claude Code and opencode:

```
Anthropic Request → deserialize AnthropicMessagesRequest
  → convert to ChatCompletionRequest (OpenAI format)
  → auth / rate-limit / route (same pipeline)
  → convert response back to Anthropic format (non-stream or SSE)
```

Anthropic stream uses `AnthropicStreamTranscoder` to convert OpenAI SSE chunks
into Anthropic event types (`message_start`, `content_block_delta`, `message_delta`,
`message_stop`).

## Pass-Through Mode

When `pass_through.enabled=true`, requests are forwarded raw to an upstream gateway:

```
Client → auth + rate limit → forward raw bytes to upstream gateway → return response
```

- Model access check and rate limiting still run locally.
- SSE streaming is forwarded chunk-by-chunk with `LoggedStream` for accurate duration.
- No token usage is recorded (response body is not parsed).
```
