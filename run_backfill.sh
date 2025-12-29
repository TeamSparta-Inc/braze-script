#!/bin/bash
# venv 활성화 필요

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/backfill_$(date +%Y%m%d_%H%M%S).log"

nohup python3 "$SCRIPT_DIR/user_backfill.py" "$@" > "$LOG_FILE" 2>&1 &

PID=$!
echo "Started backfill process (PID: $PID)"
echo "Log file: $LOG_FILE"
echo "$PID" > "$SCRIPT_DIR/backfill.pid"
