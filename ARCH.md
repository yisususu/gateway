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
      └──────────────┘ │ token cache │ │ PlanStore     │ └─────────────┘
                       └─────────────┘ └───────┬───────┘
                                               │
                                       ┌───────▼────────┐
                                       │ boom-dashboard │  Web UI + REST API
                                       │ JWT auth       │  SPA (embedded)
                                       │ admin + user   │
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
Provider     :: chat(req) → response | stream     // 对接上游 LLM
Authenticator:: authenticate(key) → AuthIdentity   // 验证 API key
RateLimiter  :: check_and_record(key, limits) → decision  // 限流判定
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
│  ├─ db_pool      (shared with gateway, survives reload)│
│  ├─ plan_store   (shared with gateway, survives reload)│
│  ├─ limiter      (shared with gateway, survives reload)│
│  ├─ jwt_secret   (derived from master_key)             │
│  └─ master_key   (admin login verification)            │
└────────────────────────────────────────────────────────┘

┌─── Auth ───────────────────────────────────────────────┐
│  Admin: "admin" + master_key → JWT cookie (2h)         │
│  User:  user_id + API key → SHA-256 → DB lookup → JWT  │
│  Session: HttpOnly cookie, verified per request         │
└────────────────────────────────────────────────────────┘

┌─── Endpoints ──────────────────────────────────────────┐
│  /dashboard/              → SPA (index.html embedded)  │
│  /dashboard/api/auth/*    → login / logout / me        │
│  /dashboard/api/user/*    → plan, usage, key-info      │
│  /dashboard/api/admin/*   → plans, keys, assignments   │
└────────────────────────────────────────────────────────┘
```

## Hot Reload (boom-gateway)

```
SIGHUP / POST /admin/config/reload
     │
     ▼
  re-read config.yaml
     │
     ▼
  build new AppStateInner (providers, auth, deployments)
     │
     ▼
  ArcSwap::store()  ──▶ atomic swap, zero downtime
     │
     └─ preserved: db_pool, limiter counters, plan assignments
```

## DB Schema (litellm compatible, read-only for auth)

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
