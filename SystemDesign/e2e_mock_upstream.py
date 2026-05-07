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

        # Optional: simulate slow upstream with marker in last user message.
        # Example marker: [SLEEP=5]
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
