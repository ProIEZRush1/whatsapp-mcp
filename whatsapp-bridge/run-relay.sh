#!/bin/bash
# Supervisor for the WhatsApp -> cmux delivery relay.
# The relay MUST run inside a cmux terminal surface (cmux only accepts socket
# writes from its own descendant processes). This wrapper keeps it alive across
# crashes. If you close this surface or restart cmux, re-run this script in any
# cmux terminal:  bash ~/.local/share/whatsapp-mcp/whatsapp-bridge/run-relay.sh
cd "$(dirname "$0")" || exit 1
echo "[run-relay] supervisor started; pid=$$"
while true; do
  echo "[run-relay] launching cmux-relay.py"
  python3 cmux-relay.py
  code=$?
  echo "[run-relay] relay exited (code=$code); restarting in 2s"
  sleep 2
done
