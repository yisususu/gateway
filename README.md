# Intelligence Boom Gateway

基于 [Pingora](https://github.com/cloudflare/pingora)（Cloudflare 开源的高性能 HTTP 代理框架）构建的反向代理与负载均衡网关。

## 功能特性

- **YAML 配置驱动** — 通过配置文件定义路由规则，无需修改代码
- **多维路由匹配** — 支持按域名（Host）、路径前缀（Path）、客户端源 IP（CIDR）组合匹配
- **TLS/HTTPS** — 可选启用 TLS 监听，支持 HTTP/2
- **配置热加载** — 修改配置文件后自动生效，无需重启
- **通配符域名** — 支持 `*.example.com` 形式的子域名匹配
- **容器化部署** — Docker 多阶段构建，运行时基于 openEuler 24.03

## 快速开始

### 前置要求

- Docker
- OpenSSL（用于生成 TLS 证书，启动脚本可自动生成）

### 启动

```bash
# 使用默认配置启动（自动构建镜像、生成证书）
./misc/LB/start.sh start

# 使用自定义配置启动
./misc/LB/start.sh start /path/to/my-routes.yaml

# 查看状态
./misc/LB/start.sh status

# 停止
./misc/LB/start.sh stop

# 重启
./misc/LB/start.sh restart
```

### 手动 Docker 启动

```bash
# 构建镜像
docker build -t gateway-lb misc/LB/

# 生成自签名证书
mkdir -p misc/LB/certs
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout misc/LB/certs/server.key \
  -out misc/LB/certs/server.crt \
  -subj "/CN=gateway-lb" \
  -addext "subjectAltName=DNS:*,DNS:localhost,IP:127.0.0.1"

# 启动容器
docker run -d --name gateway-lb \
  --restart unless-stopped \
  -v $(pwd)/misc/LB/config.yaml:/etc/gateway/routes.yaml:ro \
  -v $(pwd)/misc/LB/certs/server.crt:/etc/gateway/server.crt:ro \
  -v $(pwd)/misc/LB/certs/server.key:/etc/gateway/server.key:ro \
  -p 6198:6198 \
  -p 6443:6443 \
  gateway-lb
```

## 配置说明

配置文件为 YAML 格式，通过环境变量 `CONFIG_PATH` 指定路径（默认 `/etc/gateway/routes.yaml`）。

### 完整示例

```yaml
listen_port: 6198

tls:
  port: 6443
  cert: "/etc/gateway/server.crt"
  key: "/etc/gateway/server.key"

default_backend: "127.0.0.1:8080"

routes:
  - client_ip: "10.0.0.0/24"
    backend: "10.0.1.1:8080"
  - client_ip: "192.168.1.0/24"
    backend: "10.0.2.1:8080"
  - client_ip: "10.0.5.100"
    host: "api.example.com"
    backend: "10.0.3.1:8080"
  - host: "api.example.com"
    backend: "10.0.0.1:8080"
  - host: "*.example.com"
    backend: "10.0.0.2:8080"
  - path: "/api/"
    backend: "10.0.0.3:3000"
  - host: "app.example.com"
    path: "/assets/"
    backend: "10.0.0.4:80"
```

### 字段说明

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `listen_port` | integer | 否 | HTTP 监听端口，默认 `6198`。配置了 `tls` 时不生效 |
| `tls` | object | 否 | TLS 配置。配置后监听 HTTPS，未配置则监听 HTTP |
| `tls.port` | integer | 是 | HTTPS 监听端口 |
| `tls.cert` | string | 是 | TLS 证书文件路径（PEM 格式） |
| `tls.key` | string | 是 | TLS 私钥文件路径（PEM 格式） |
| `default_backend` | string | 是 | 所有路由均未匹配时的默认后端地址（`host:port` 格式） |
| `routes` | array | 是 | 路由规则列表，按顺序匹配，先匹配到的生效 |
| `routes[].host` | string | 否 | 匹配请求的 Host 头（去掉端口部分） |
| `routes[].path` | string | 否 | 匹配请求的 URI 路径前缀 |
| `routes[].client_ip` | string | 否 | 匹配客户端源 IP |
| `routes[].backend` | string | 是 | 匹配成功后转发的后端地址（`host:port` 格式） |

### 路由匹配规则

**匹配维度：**

| 维度 | 说明 | 示例 |
|---|---|---|
| `host` | 精确匹配或通配符匹配（`*.domain.com`），不区分大小写 | `api.example.com`、`*.example.com` |
| `path` | URI 路径前缀匹配 | `/api/` 匹配 `/api/users` |
| `client_ip` | 客户端源 IP 匹配，支持 CIDR 和单 IP | `10.0.0.0/24`、`192.168.1.100` |

**匹配逻辑：**

1. 按路由列表顺序依次匹配
2. 单条路由中，所有指定的维度都必须匹配才算命中
3. 未指定的维度视为不限制（自动通过）
4. 所有路由均未命中时，使用 `default_backend`

### HTTP / HTTPS 模式

- **HTTP 模式**：不配置 `tls` 段，网关监听 `listen_port` 端口
- **HTTPS 模式**：配置 `tls` 段，网关监听 `tls.port` 端口，自动启用 HTTP/2（ALPN 协商）

### 配置热加载

网关启动后自动监听配置文件变更。修改配置文件后，新的路由规则即时生效，无需重启进程。

- 变更检测带 200ms 防抖，避免编辑器保存时产生多次重载
- 配置文件语法错误时仅记录日志，不影响当前生效的配置
- 需要重启才能生效的配置变更：`listen_port`、`tls`（监听端口和证书在启动时确定）

## 项目结构

```
gateway/
├── misc/LB/              # 负载均衡代理
│   ├── src/main.rs       # Pingora 反向代理主程序
│   ├── Cargo.toml        # Rust 项目配置
│   ├── Dockerfile        # 多阶段 Docker 构建
│   ├── config.yaml       # 路由配置文件
│   └── start.sh          # 启动/停止脚本
├── docs/                 # 文档（待填充）
├── src/                  # 源码（待填充）
└── README.md
```

## 技术栈

- **Pingora 0.8** — Cloudflare 高性能 HTTP 代理框架
- **Rust 1.85** — 编译语言
- **OpenSSL** — TLS 实现
- **openEuler 24.03** — 容器运行时基础镜像
