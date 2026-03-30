# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**Intelligence Boom Gateway** — 基于 Pingora (Cloudflare) 的反向代理与负载均衡网关。Rust 实现，Docker 容器化部署。

## Build & Run

```bash
# 构建 Docker 镜像
docker build -t gateway-lb misc/LB/

# 通过启动脚本运行（自动构建 + 生成证书）
./misc/LB/start.sh start
```

运行时依赖：Docker。构建镜像内使用 rsproxy.cn 作为 crates.io 镜像。

## Architecture

- **Pingora 0.8** (`pingora-core` + `pingora-proxy`, openssl feature) 作为 HTTP 代理框架
- `misc/LB/src/main.rs` — 全部逻辑：YAML 配置加载、路由匹配（host/path/client_ip）、TLS 监听、配置热加载（notify crate + RwLock）
- `misc/LB/config.yaml` — 路由配置（routes 列表按顺序匹配，支持 host 通配符、path 前缀、client_ip CIDR）
- `misc/LB/Dockerfile` — 多阶段构建：rust:1.85-bookworm 编译 + openEuler 24.03 运行时
- `misc/LB/start.sh` — 启停脚本，自动生成自签名证书

## Key patterns

- 配置通过 `Arc<RwLock<Config>>` 共享，后台 watcher 线程写、代理请求线程读
- 路由匹配：`host`（精确/`*.wildcard`）+ `path`（starts_with）+ `client_ip`（IpNet::contains），全部指定维度需同时满足
- TLS 配置为 `TlsSettings::intermediate()` + `enable_h2()`

## Repository layout

- `misc/LB/` — 负载均衡代理（Pingora）
- `src/` — placeholder
- `docs/` — placeholder
