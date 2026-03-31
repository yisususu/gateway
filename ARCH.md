# BooMGateway Architecture

## Crate Dependency Graph

```
                        ┌──────────────┐
                        │  boom-core   │  traits, types, errors
                        └──────┬───────┘
                               │
              ┌────────────────┼────────────────┐─────────────────┐
              │                │                │                 │
      ┌───────▼──────┐ ┌──────▼──────┐ ┌───────▼───────┐ ┌──────▼──────┐
      │ boom-config  │ │  boom-auth  │ │ boom-limiter  │ │boom-provider│
      │ YAML config  │ │ DB auth     │ │ sliding win   │ │ HTTP client │
      │ model_list   │ │ master_key  │ │ concurrency   │ │ OpenAI/etc  │
      │ store_model  │ │ token cache │ │ PlanStore     │ └─────────────┘
      └──────────────┘ │ token cache │ │ DeployStore   │
                       │             │ │ AliasStore    │
                       └─────────────┘ └───────┬───────┘
                                               │
                                       ┌───────▼────────┐
                                       │ boom-dashboard │  Web UI + REST API
                                       │ JWT auth       │  SPA (embedded)
                                       │ admin + user   │
                                       │ models CRUD    │
                                       │ aliases CRUD   │
                                       │ config CRUD    │
                                       └───────┬────────┘
                                               │
      ┌────────────────────────────────────────┼──────────────────────────────────┐
      │                                 boom-gateway                            │
      │  main binary — axum HTTP server                                         │
      │  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
      │  │ /v1/*    │  │ /health/* │  │ /admin/* │  │/dashboard│  │ AppState │  │
      │  │ API proxy│  │ checks    │  │ plans    │  │ Web UI   │  │ hot-reload│ │
      │  └──────────┘  └───────────┘  └──────────┘  └──────────┘  └──────────┘  │
      └───────────────────────────────────────────────────────────────────────────┘
```

## Core Traits (boom-core)

```rust
Provider     :: chat(req) → response | stream     // upstream LLM
Authenticator:: authenticate(key) → AuthIdentity   // verify API key
RateLimiter  :: check_and_record(key, limits) → decision  // rate limit
```

## Request Flow

```
Client Request
     │
     ▼
┌──────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐
│ Extract  │──▶│   Auth    │──▶│  Rate     │──▶│  Route   │
│ API Key  │   │ DB/master │   │  Limit    │   │ Dispatch │
└──────────┘   └───────────┘   └───────────┘   └──────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │  Provider.chat()    │
                                         │  (upstream LLM API) │
                                         └──────────┬──────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │  Response / Stream  │
                                         │  → client           │
                                         └─────────────────────┘
```

## AppState Structure

```
AppState (Clone, survives reload)
  ├─ config_path                  // YAML file path
  ├─ inner: Arc<ArcSwap<Inner>>   // hot-swappable: config + auth + health
  ├─ db_pool: Option<PgPool>      // survives reload
  ├─ limiter: Arc<SlidingWindowLimiter>   // survives reload
  ├─ plan_store: Arc<PlanStore>           // survives reload
  ├─ deployment_store: Arc<DeploymentStore>  // survives reload (NEW)
  └─ alias_store: Arc<AliasStore>           // survives reload (NEW)

AppStateInner (rebuilt on reload)
  ├─ config: Config
  ├─ auth: Arc<dyn Authenticator>
  └─ health: HealthStatus
```

### DeploymentStore (boom-limiter)

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

### AliasStore (boom-limiter)

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
│  ├─ deployment_store  (shared, survives reload) [NEW]  │
│  ├─ alias_store       (shared, survives reload) [NEW]  │
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
│  /dashboard/api/admin/models    → model CRUD [NEW]      │
│  /dashboard/api/admin/aliases   → alias CRUD [NEW]      │
│  /dashboard/api/admin/config    → KV config CRUD [NEW]  │
│  /dashboard/api/admin/plans     → plan CRUD (DB dual-write) │
│  /dashboard/api/admin/keys      → key management        │
│  /dashboard/api/admin/assignments → key-plan assignment  │
└────────────────────────────────────────────────────────┘
```

## Hot Reload (boom-gateway)

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

## DB Schema

### litellm Compatible (read-only for auth)

```sql
LiteLLM_VerificationToken   ← key auth, dashboard key management
  ├─ token (SHA-256 hash, primary key)
  ├─ key_name, user_id, team_id
  ├─ models (JSON array, may contain special names)
  ├─ spend, max_budget, blocked, expires
  └─ rpm_limit, tpm_limit, metadata

LiteLLM_TeamTable           ← team model group resolution
  └─ team_id → models (JSON array)
```

### BooMGateway Tables (full CRUD)

```sql
boom_config                 ← KV config store
  ├─ key TEXT PRIMARY KEY
  ├─ value JSONB
  └─ updated_at TIMESTAMPTZ
  -- stores: db_seeded, rate_limit, plan_settings.default_plan, etc.

boom_model_deployment       ← model deployment definitions
  ├─ id UUID PRIMARY KEY
  ├─ model_name, litellm_model
  ├─ api_key, api_key_env (env reference flag)
  ├─ api_base, api_version
  ├─ aws_region_name, aws_access_key_id, aws_secret_access_key
  ├─ rpm, tpm, timeout, headers (JSONB)
  ├─ temperature, max_tokens, enabled
  ├─ source TEXT ('yaml' | 'db')
  └─ created_at, updated_at

boom_model_alias            ← model alias mapping
  ├─ alias_name TEXT PRIMARY KEY
  ├─ target_model, hidden, source
  └─ updated_at

boom_rate_limit_plan        ← plan persistence
  ├─ name TEXT PRIMARY KEY
  ├─ concurrency_limit, rpm_limit
  ├─ window_limits JSONB, schedule JSONB
  ├─ is_default, source
  └─ updated_at

boom_key_plan_assignment    ← key → plan mapping
  ├─ key_hash TEXT PRIMARY KEY
  ├─ plan_name, assigned_at

boom_rate_limit_state       ← rate limit counter snapshots
  ├─ cache_key TEXT PRIMARY KEY
  ├─ count, window_start, window_secs
  └─ updated_at
```
