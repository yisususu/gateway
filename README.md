# BooMGateway — 高性能 LLM API 网关

Rust 实现的高性能 LLM API 网关，提供统一的请求入口来访问多个 LLM 提供商（OpenAI、Anthropic、Gemini、Bedrock 等）。兼容 litellm 密钥体系，自建速率限制、套餐管理、Web Dashboard 和审计日志。

## 功能特性

- **多提供商路由** — OpenAI / Anthropic / Azure / Gemini / Bedrock / vLLM / Ollama 等 20+ 提供商
- **负载均衡** — 同名多部署自动轮询，支持 key_affinity 会话亲和策略
- **速率限制** — 滑动窗口 + 并发控制 + 自定义时间窗口 + 时段调度
- **套餐系统** — 灵活的 Plan 管理，key → plan 分配，三级 fallback
- **配额倍率** — 按模型设定 `quota_count_ratio`，大杯模型消耗更多配额
- **Anthropic 兼容** — 原生支持 `/v1/messages` 端点（Claude Code / opencode 兼容）
- **Web Dashboard** — SPA 管理面板，密钥/模型/套餐/日志实时管理
- **热重载** — SIGHUP 或 API 触发，零停机配置更新
- **DB 权威模式** — 可选 `store_model_in_db: true`，Dashboard CRUD 直接持久化
- **请求审计** — 完整请求日志（token 用量、耗时、状态码），流式请求真实 duration
- **容器化部署** — Docker 多阶段构建，openEuler 运行时

## 快速开始

### 前置要求

- Docker
- PostgreSQL（用于密钥认证和持久化，可选）

### 构建与运行

```bash
# 构建 Docker 镜像
docker build -t boom-gateway boom-gateway/

# 运行（最简配置）
docker run -d --name boom-gateway \
  -p 4000:4000 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  boom-gateway

# 运行（带数据库）
docker run -d --name boom-gateway \
  -p 4000:4000 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -e DATABASE_URL=postgres://user:pass@db:5432/litellm \
  boom-gateway
```

### 最小配置

```yaml
model_list:
  - model_name: gpt-4o
    litellm_params:
      model: openai/gpt-4o
      api_key: ${OPENAI_API_KEY}

general_settings:
  master_key: ${MASTER_KEY}
```

完整配置参考见 [CONFIG_EXAMPLE.md](CONFIG_EXAMPLE.md)。

## 项目结构

```
BooMGateway/
├── boom-gateway/              Rust workspace 根目录
│   ├── boom-core/             核心 trait 和公共类型
│   ├── boom-auth/             密钥认证（litellm 兼容）
│   ├── boom-config/           YAML 配置解析
│   ├── boom-provider/         LLM Provider 实现
│   ├── boom-limiter/          速率限制 + 并发控制 + PlanStore
│   ├── boom-routing/          DeploymentStore + AliasStore + 调度策略
│   ├── boom-audit/            请求日志读写
│   ├── boom-dashboard/        Web 管理 UI + REST API
│   └── boom-main/             主程序入口
├── misc/LB/                   Pingora 负载均衡代理（可选前置层）
├── config.example.yaml        配置示例
├── CONFIG_EXAMPLE.md          配置字段参考
├── ARCH.md                    架构设计文档
├── DESCRIPTOR.md              详细架构描述
└── CLAUDE.md                  开发规范
```

## 技术栈

- **语言**: Rust (edition 2021)
- **HTTP 框架**: Axum
- **异步运行时**: Tokio (32 worker threads)
- **数据库**: PostgreSQL (sqlx)
- **并发数据结构**: DashMap
- **热重载**: ArcSwap
- **缓存**: moka

## API 端点

### 客户端 API（需 API key）

| 端点 | 说明 |
|---|---|
| `POST /v1/chat/completions` | OpenAI 格式聊天（流式/非流式） |
| `POST /v1/messages` | Anthropic Messages API |
| `POST /v1/completions` | OpenAI 格式补全 |
| `GET /v1/models` | 模型列表 |

### 管理 API

| 端点 | 说明 |
|---|---|
| `POST /admin/config/reload` | 热重载配置 |
| `/admin/plans` | 套餐 CRUD |
| `/admin/plans/assign` | Key-套餐分配 |

### Dashboard API

| 端点 | 说明 |
|---|---|
| `/dashboard/api/admin/models` | 模型部署 CRUD |
| `/dashboard/api/admin/aliases` | 别名 CRUD |
| `/dashboard/api/admin/keys` | 密钥管理 |
| `/dashboard/api/admin/logs` | 请求日志查询 |
| `/dashboard/api/admin/teams` | 团队统计 |
| `/dashboard/api/admin/stats/*` | 模型统计 + 实时请求 |
| `/dashboard/api/admin/limits/reset` | 限流窗口重置 |
| `/dashboard/api/admin/config` | KV 配置管理 |

### 健康检查

| 端点 | 说明 |
|---|---|
| `GET /health` | 完整健康状态 |
| `GET /health/live` | 存活探针 |
| `GET /health/ready` | 就绪探针 |

## 热重载

修改配置文件后，两种方式触发：

1. **信号**: `kill -HUP <pid>`
2. **API**: `POST /admin/config/reload`

热重载原子交换内部状态（ArcSwap），零停机，不丢失运行时计数器。

---

## 负载均衡代理 (misc/LB)

基于 [Pingora](https://github.com/cloudflare/pingora) 的独立负载均衡器，可作为网关前置层。

### 启动

```bash
# 使用默认配置启动（自动构建镜像、生成证书）
./misc/LB/start.sh start

# 使用自定义配置启动
./misc/LB/start.sh start /path/to/my-routes.yaml

# 查看状态 / 停止 / 重启
./misc/LB/start.sh status
./misc/LB/start.sh stop
./misc/LB/start.sh restart
```

### 配置示例

```yaml
listen_port: 6198

tls:
  port: 6443
  cert: "/etc/gateway/server.crt"
  key: "/etc/gateway/server.key"

default_backend: "127.0.0.1:4000"

routes:
  - host: "api.example.com"
    backend: "10.0.0.1:4000"
  - client_ip: "10.0.0.0/24"
    backend: "10.0.1.1:4000"
  - path: "/api/"
    backend: "10.0.0.3:3000"
```

路由匹配维度：`host`（支持通配符 `*.example.com`）、`path`（前缀匹配）、`client_ip`（CIDR），按顺序匹配，先匹配到的生效。

## 文档索引

| 文档 | 说明 |
|---|---|
| [ARCH.md](ARCH.md) | 架构设计（模块图、请求流、状态管理、DB schema） |
| [DESCRIPTOR.md](DESCRIPTOR.md) | 详细架构描述（模块、API 端点、请求流程） |
| [CONFIG_EXAMPLE.md](CONFIG_EXAMPLE.md) | 配置字段参考和示例 |
| [CLAUDE.md](CLAUDE.md) | 开发规范和架构原则 |
