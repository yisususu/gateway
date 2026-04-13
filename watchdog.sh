#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <port> <restart_script> [restart_script_args...]"
    echo "  Watchdog: probe /health every 3s (2s timeout)."
    echo "  Restart after 3 consecutive failures."
    exit 1
fi

PORT="$1"
RESTART_SCRIPT="$2"
shift 2

URL="http://127.0.0.1:${PORT}/health"
FAIL=0

echo "[watchdog] port=${PORT} url=${URL} restart=${RESTART_SCRIPT}"

while true; do
    START=$(date +%s%N)
    if curl -sf --max-time 2 "$URL" > /dev/null 2>&1; then
        if [ "$FAIL" -gt 0 ]; then
            echo "[watchdog] recovered after ${FAIL} failure(s)"
        fi
        FAIL=0
    else
        FAIL=$((FAIL + 1))
        echo "[watchdog] FAIL ${FAIL}/3 — $(date '+%Y-%m-%d %H:%M:%S')"
        if [ "$FAIL" -ge 3 ]; then
            echo "[watchdog] 3 consecutive failures — restarting..."
            bash "$RESTART_SCRIPT" "$@" 2>&1 || true
            echo "[watchdog] restart script exited, waiting 10s before resuming probes..."
            sleep 10
            FAIL=0
        fi
    fi

    ELAPSED=$(( ($(date +%s%N) - START) / 1000000 ))
    REMAINING=$(( 3000 - ELAPSED ))
    if [ "$REMAINING" -gt 0 ]; then
        sleep $(( REMAINING / 1000 )).$(( REMAINING % 1000 ))
    fi
done
