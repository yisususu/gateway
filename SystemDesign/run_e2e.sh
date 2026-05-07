#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MOCK_SCRIPT="${SCRIPT_DIR}/e2e_mock_upstream.py"
CONFIG_FILE="${SCRIPT_DIR}/e2e_config.yaml"

GATEWAY_URL="${GATEWAY_URL:-http://127.0.0.1:4000}"
API_KEY="${API_KEY:-sk-master-e2e}"
MODEL_NAME="${MODEL_NAME:-Qwen3.5}"
MODE="${1:-all}" # basic | rate | queue | all

MOCK_PID=""

cleanup() {
  if [[ -n "${MOCK_PID}" ]]; then
    echo "[cleanup] stopping mock upstream pid=${MOCK_PID}"
    kill "${MOCK_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "[error] command not found: $1"
    exit 1
  fi
}

post_chat() {
  local content="$1"
  curl -sS -X POST "${GATEWAY_URL}/v1/chat/completions" \
    -H "Authorization: Bearer ${API_KEY}" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"${MODEL_NAME}\",
      \"messages\": [{\"role\":\"user\",\"content\":\"${content}\"}],
      \"stream\": false
    }"
}

post_chat_with_code() {
  local content="$1"
  curl -sS -w $'\n%{http_code}' -X POST "${GATEWAY_URL}/v1/chat/completions" \
    -H "Authorization: Bearer ${API_KEY}" \
    -H "Content-Type: application/json" \
    -d "{
      \"model\": \"${MODEL_NAME}\",
      \"messages\": [{\"role\":\"user\",\"content\":\"${content}\"}],
      \"stream\": false
    }"
}

start_mock_if_needed() {
  if curl -sS --max-time 1 "http://127.0.0.1:8008/" >/dev/null 2>&1; then
    echo "[info] mock upstream already reachable at :8008, skip start"
    return
  fi

  echo "[info] starting mock upstream: ${MOCK_SCRIPT}"
  python3 "${MOCK_SCRIPT}" >"${SCRIPT_DIR}/mock_upstream.log" 2>&1 &
  MOCK_PID=$!

  for _ in {1..20}; do
    if curl -sS --max-time 1 "http://127.0.0.1:8008/" >/dev/null 2>&1; then
      echo "[ok] mock upstream started (pid=${MOCK_PID})"
      return
    fi
    sleep 0.2
  done

  echo "[error] mock upstream did not become ready"
  exit 1
}

check_gateway_ready() {
  if ! curl -sS --max-time 2 "${GATEWAY_URL}/health" >/dev/null 2>&1; then
    cat <<EOF
[error] gateway not reachable at ${GATEWAY_URL}

Start gateway first (example):
  cd "${ROOT_DIR}"
  RUST_LOG=info cargo run -p boom-main --bin boom-gateway -- --config "${CONFIG_FILE}"
EOF
    exit 1
  fi
  echo "[ok] gateway is reachable at ${GATEWAY_URL}"
}

run_basic() {
  echo "== BASIC E2E =="
  local out
  out="$(post_chat "hello e2e basic")"
  echo "${out}"
  if [[ "${out}" != *"chat.completion"* ]]; then
    echo "[error] basic response does not look like OpenAI completion JSON"
    exit 1
  fi
  echo "[ok] basic e2e passed"
}

run_rate() {
  echo "== RATE LIMIT (expect 429 on 3rd req within 60s) =="
  local r1 r2 r3 c1 c2 c3 b1 b2 b3

  r1="$(post_chat_with_code "rate test #1")"; b1="${r1%$'\n'*}"; c1="${r1##*$'\n'}"
  r2="$(post_chat_with_code "rate test #2")"; b2="${r2%$'\n'*}"; c2="${r2##*$'\n'}"
  r3="$(post_chat_with_code "rate test #3")"; b3="${r3%$'\n'*}"; c3="${r3##*$'\n'}"

  echo "[resp1] code=${c1}"
  echo "[resp2] code=${c2}"
  echo "[resp3] code=${c3}"
  echo "[resp3 body] ${b3}"

  if [[ "${c1}" != "200" || "${c2}" != "200" || "${c3}" != "429" ]]; then
    echo "[error] expected codes: 200, 200, 429"
    exit 1
  fi
  echo "[ok] rate-limit behavior matched expectation"
}

run_queue() {
  echo "== FLOW QUEUE (2nd request should wait) =="
  local t1 t2 elapsed
  local f1="${SCRIPT_DIR}/queue_req1.out"
  local f2="${SCRIPT_DIR}/queue_req2.out"

  rm -f "${f1}" "${f2}"

  t1="$(date +%s)"
  post_chat "queue req #1 [SLEEP=5]" >"${f1}" &
  local pid1=$!
  sleep 0.2
  post_chat "queue req #2 should wait" >"${f2}" &
  local pid2=$!

  wait "${pid1}"
  wait "${pid2}"
  t2="$(date +%s)"
  elapsed=$((t2 - t1))

  echo "[info] total elapsed=${elapsed}s"
  echo "[resp1] $(cat "${f1}")"
  echo "[resp2] $(cat "${f2}")"

  if (( elapsed < 4 )); then
    echo "[error] queue wait did not happen (elapsed < 4s)"
    exit 1
  fi
  echo "[ok] queue wait observed"
}

main() {
  require_cmd curl
  require_cmd python3

  case "${MODE}" in
    basic|rate|queue|all) ;;
    *)
      echo "[error] invalid mode: ${MODE}"
      echo "Usage: $0 [basic|rate|queue|all]"
      exit 1
      ;;
  esac

  start_mock_if_needed
  check_gateway_ready

  if [[ "${MODE}" == "basic" ]]; then
    run_basic
    exit 0
  fi
  if [[ "${MODE}" == "rate" ]]; then
    run_rate
    exit 0
  fi
  if [[ "${MODE}" == "queue" ]]; then
    run_queue
    exit 0
  fi

  # all mode:
  # queue and rate share the same limiter budget (default rpm=2 in e2e_config.yaml),
  # so we run queue first, then wait for limiter window reset before rate test.
  run_basic
  run_queue
  echo "[info] waiting 61s for limiter window reset before rate test..."
  sleep 61
  run_rate
  echo "[done] all e2e checks passed"
}

main "$@"
