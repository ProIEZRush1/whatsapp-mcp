#!/bin/bash
# Watchdog: ensure exactly one wabridge instance is running.
# Invoked by launchd on a StartInterval; starts the bridge (detached) if absent.
set -euo pipefail

DIR="/Users/ech/.local/share/whatsapp-mcp/whatsapp-bridge"
BIN="$DIR/wabridge"
LOG="$DIR/bridge.out"

cd "$DIR"

# Already running? nothing to do.
if pgrep -f "$BIN" >/dev/null 2>&1; then
  exit 0
fi

# Free the port if a half-dead listener lingers.
if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
  exit 0
fi

# Start the bridge detached so it survives this script exiting.
nohup "$BIN" >> "$LOG" 2>&1 &
disown || true
exit 0
