#!/bin/bash
# Runs INSIDE the wa-relay cmux workspace (so it can talk to cmux). Two jobs:
#   - relay: drain cmux_outbox and deliver to sessions (foreground, restart on crash)
#   - heal:  periodically repair stale cmux subscriptions after cmux restarts
cd "$(dirname "$0")" || exit 1
PY=/usr/bin/python3
echo "[wa-supervisor] started pid=$$ $(date '+%F %T')"
( while true; do "$PY" cmux-wa-heal.py >/dev/null 2>&1; sleep 60; done ) &
while true; do
  echo "[wa-supervisor] launching cmux-relay.py"
  "$PY" cmux-relay.py
  echo "[wa-supervisor] relay exited ($?); restart in 2s"
  sleep 2
done
