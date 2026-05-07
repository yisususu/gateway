# BooMGateway E2E 测试教程（最小可跑版）

这份文档目标是：**你在一台服务器上，最快跑通一个可 debug 的端到端链路**：

`curl -> BooMGateway -> Python mock upstream -> BooMGateway -> curl`

同时能验证：
- routing 是否把请求转发到你指定的 upstream
- limiter 是否会返回 429
- queue（flow control）是否会发生“第二个请求等待第一个请求结束”

---

## 1) 当前仓库有测试集吗？

有一些 **unit tests**，但目前没有完整的 e2e 测试套件（没有现成的 integration/e2e 目录和测试脚本）。

你现在仓库里能看到的测试主要在：
- `boom-gateway/boom-limiter/src/sliding_window.rs`
- `boom-gateway/boom-config/src/lib.rs`
- `boom-gateway/boom-core/src/anthropic.rs`
- `boom-gateway/boom-core/src/normalize.rs`

如果你想先跑单元测试：

```bash
cargo test -p boom-limiter
cargo test -p boom-config
cargo test -p boom-core
```

---

## 2) 最小 e2e 思路

我们不依赖真实 vLLM，先用一个 Python 进程模拟 OpenAI `/chat/completions`：

1. 起一个 mock server（随机返回内容，可配置延迟）
2. 起 BooMGateway，`model_name: Qwen3.5` 指向这个 mock server
3. 用 `curl` 调网关，检查返回
4. 并发打两条请求，观察 queue 等待

---

## 3) 准备文件

在仓库根目录执行（`BooMGateway/`）：

### 3.1 创建 mock 上游服务

新建 `SystemDesign/e2e_mock_upstream.py`：

```python
#!/usr/bin/env python3
import json
import random
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "0.0.0.0"
PORT = 8008

REPLIES = [
    "mock: hello from upstream",
    "mock: random answer A",
    "mock: random answer B",
    "mock: routing success",
]


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path not in ("/v1/chat/completions", "/chat/completions"):
            self._send_json(404, {"error": f"unknown path {self.path}"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        req = json.loads(raw.decode("utf-8"))

        # 可选：模拟慢模型，触发 gateway queue 等待
        # 客户端在最后一条 user message 中包含 [SLEEP=5] 这种标记即可
        sleep_s = 0
        try:
            msgs = req.get("messages", [])
            if msgs and isinstance(msgs, list):
                last_content = msgs[-1].get("content", "")
                if isinstance(last_content, str) and "[SLEEP=" in last_content:
                    start = last_content.index("[SLEEP=") + len("[SLEEP=")
                    end = last_content.index("]", start)
                    sleep_s = int(last_content[start:end])
        except Exception:
            pass

        if sleep_s > 0:
            time.sleep(sleep_s)

        model = req.get("model", "unknown-model")
        text = random.choice(REPLIES)

        resp = {
            "id": f"chatcmpl-mock-{random.randint(1000, 9999)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": text},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": random.randint(10, 30),
                "completion_tokens": random.randint(5, 20),
                "total_tokens": 0,
            },
        }
        resp["usage"]["total_tokens"] = (
            resp["usage"]["prompt_tokens"] + resp["usage"]["completion_tokens"]
        )

        print(
            f"[mock] path={self.path} model={model} sleep={sleep_s}s -> {text}",
            flush=True,
        )
        self._send_json(200, resp)


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"[mock] listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
```

### 3.2 创建最小网关配置

新建 `SystemDesign/e2e_config.yaml`：

```yaml
model_list:
  - model_name: "Qwen3.5"
    model_info:
      id: "instance-01"
    flow_control:
      model_queue_limit: 1
      model_context_limit: 1000000
    litellm_params:
      model: "openai/Qwen3.5"
      api_base: "http://127.0.0.1:8008/v1"
      api_key: "dummy-upstream-key"
      timeout: 120

general_settings:
  master_key: "sk-master-e2e"
  # 不填 database_url => master-key-only 模式，最简启动

router_settings:
  schedule_policy: "round_robin"

server:
  host: "0.0.0.0"
  port: 4000
  workers: 4

rate_limit:
  enabled: true
  default_rpm: 2
  window_limits:
    - [3, 60]

plan_settings:
  default_plan: "basic"
  plans:
    basic:
      concurrency_limit: 2
      rpm_limit: 2
      window_limits:
        - [3, 60]
```

---

## 4) 启动步骤

开两个终端。

### 终端 A：启动 mock upstream

```bash
cd /path/to/BooMGateway
python3 SystemDesign/e2e_mock_upstream.py
```

期望看到：

```text
[mock] listening on http://0.0.0.0:8008
```

### 终端 B：启动 BooMGateway

```bash
cd /path/to/BooMGateway
RUST_LOG=info cargo run -p boom-main --bin boom-gateway -- --config SystemDesign/e2e_config.yaml
```

期望看到：
- `BooMGateway listening on 0.0.0.0:4000`
- `No database URL — running in master-key-only auth mode`

---

## 5) 最简单 e2e（单请求）

```bash
curl -sS http://127.0.0.1:4000/v1/chat/completions \
  -H "Authorization: Bearer sk-master-e2e" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen3.5",
    "messages": [{"role":"user","content":"hello e2e"}],
    "stream": false
  }'
```

成功标准：
- `curl` 收到 `choices[0].message.content`（mock 随机文本）
- mock 终端打印一行 `[mock] path=/v1/chat/completions model=Qwen3.5 ...`

说明：
- 这一步证明了 routing 已经把 `Qwen3.5` 转发给你的 Python 进程。

---

## 6) 验证 429（limiter）

当前配置里默认 plan `rpm_limit=2`，你在 60 秒内连续打 3 次，第三次应触发 429。

```bash
for i in 1 2 3; do
  echo "---- req $i ----"
  curl -i -sS http://127.0.0.1:4000/v1/chat/completions \
    -H "Authorization: Bearer sk-master-e2e" \
    -H "Content-Type: application/json" \
    -d '{"model":"Qwen3.5","messages":[{"role":"user","content":"limiter test"}]}'
  echo
done
```

期望：
- 前两次 `HTTP/1.1 200 OK`
- 第三次 `HTTP/1.1 429 Too Many Requests`

---

## 7) 验证 queue 等待（flow control）

我们配置了：
- `model_queue_limit: 1`（同一 deployment 同时只允许 1 个 in-flight）

### 7.1 先发一个慢请求（占住 slot）

终端 C 执行（不要结束）：

```bash
curl -sS http://127.0.0.1:4000/v1/chat/completions \
  -H "Authorization: Bearer sk-master-e2e" \
  -H "Content-Type: application/json" \
  -d '{
    "model":"Qwen3.5",
    "messages":[{"role":"user","content":"first req [SLEEP=5]"}],
    "stream": false
  }'
```

### 7.2 紧接着发第二个请求（应等待）

另一个终端立即执行：

```bash
time curl -sS http://127.0.0.1:4000/v1/chat/completions \
  -H "Authorization: Bearer sk-master-e2e" \
  -H "Content-Type: application/json" \
  -d '{
    "model":"Qwen3.5",
    "messages":[{"role":"user","content":"second req"}],
    "stream": false
  }'
```

期望：
- 第二个请求不会立刻返回，耗时大约接近第一个请求剩余 sleep 时间。
- 这就是 gateway 内部 flow queue 的等待。

---

## 8) Debug 观察点（推荐）

## 8.1 提高 gateway 日志级别

```bash
RUST_LOG=debug cargo run -p boom-main --bin boom-gateway -- --config SystemDesign/e2e_config.yaml
```

关注：
- 请求摘要日志（包含 request_id、model、plan、rpm 剩余等）
- rate limit 拒绝日志
- flow control timeout / context exceeded 错误日志

## 8.2 mock 日志校验 routing 命中

mock 每次会打印 path 和 model。若 gateway 没路由过去，这边不会有日志。

## 8.3 健康检查

```bash
curl -sS http://127.0.0.1:4000/health
curl -sS http://127.0.0.1:4000/health/ready -i
```

---

## 9) 常见问题排查

## 9.1 请求 401/403

- 检查 `Authorization: Bearer sk-master-e2e` 是否和 `general_settings.master_key` 一致。
- master-key-only 模式下，最简单就是只用 master key 做 e2e。

## 9.2 请求 404 model not found

- 检查 `model` 是否填成 `Qwen3.5`（和 `model_list[].model_name` 一致）。

## 9.3 gateway 返回 upstream unavailable

- 检查 Python mock 是否在 `127.0.0.1:8008` 监听。
- 检查 `api_base` 是否是 `http://127.0.0.1:8008/v1`。

## 9.4 第二个请求没等待

- 确认 `flow_control.model_queue_limit: 1` 生效。
- 确认第一个请求带了 `[SLEEP=5]`，mock 确实 sleep 了。

---

## 10) 进阶：模拟多 instance routing

如果你后续要模拟“一个模型对应 10 台机器”，可以：

1. 启动多个 mock 进程（例如 8008、8009、8010）
2. 在 `model_list` 为同一个 `model_name: Qwen3.5` 配多条 deployment（不同 `api_base`、不同 `model_info.id`）
3. `schedule_policy` 改成 `round_robin` 或 `key_affinity`
4. 观察每个 mock 终端的命中分布

这样就能在不启动 vLLM 的情况下验证 BooMGateway 的 routing 和 queue 行为。

---

## 11) 用 `run_e2e.sh` 一键跑（脚本说明）

仓库里提供了 `SystemDesign/run_e2e.sh`，用来**自动拉起 mock、向已运行的 Gateway 发请求并做简单断言**。使用前仍需**手动启动 BooMGateway**（脚本不负责启动网关进程）。

### 11.1 前置：先起 Gateway（必须用 `e2e_config.yaml`）

```bash
cd /path/to/BooMGateway
RUST_LOG=info cargo run -p boom-main --bin boom-gateway -- --config SystemDesign/e2e_config.yaml
```

### 11.2 再跑脚本（一键测试）

在另一个终端：

```bash
cd /path/to/BooMGateway
chmod +x SystemDesign/run_e2e.sh   # 只需第一次

./SystemDesign/run_e2e.sh basic    # 单次 chat，校验返回 JSON
./SystemDesign/run_e2e.sh queue    # 并发两条，校验 queue 等待（约 ≥4s）
./SystemDesign/run_e2e.sh rate     # 连续 3 次，校验第三次 429
./SystemDesign/run_e2e.sh all      # basic → queue → 等待 61s → rate（全流程）
```

可选环境变量（与脚本默认值一致时可不写）：

| 变量 | 默认 | 含义 |
|------|------|------|
| `GATEWAY_URL` | `http://127.0.0.1:4000` | Gateway 地址 |
| `API_KEY` | `sk-master-e2e` | 须与 `e2e_config.yaml` 里 `master_key` 一致 |
| `MODEL_NAME` | `Qwen3.5` | 请求的 model 名 |

脚本会把 mock 日志写到 `SystemDesign/mock_upstream.log`；queue 测试会临时生成 `queue_req1.out`、`queue_req2.out`。

### 11.3 `run_e2e.sh` 会不会调用 yaml 和 upstream Python？

**会间接用到这两个文件，但方式不同：**

| 文件 | 脚本是否「调用」 | 说明 |
|------|------------------|------|
| `SystemDesign/e2e_mock_upstream.py` | **会** | 若 `127.0.0.1:8008` 上没有可用的 HTTP 服务，脚本会执行 `python3 SystemDesign/e2e_mock_upstream.py` 后台启动 mock；若端口已在跑则跳过。脚本退出时会尝试结束它自己拉起的 mock 进程。 |
| `SystemDesign/e2e_config.yaml` | **不会由脚本执行加载** | Gateway **必须由你自己**用 `--config SystemDesign/e2e_config.yaml` 启动；脚本只在你未起 Gateway 时，在报错提示里打印这条示例命令。yaml 的内容是在网关进程里读的，不是 shell 去读。 |

总结：**脚本负责 mock + curl 测试；yaml 只给 `boom-gateway` 用，须单独启动网关。**
