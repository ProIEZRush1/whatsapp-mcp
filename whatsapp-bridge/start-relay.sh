#!/bin/bash
# Ensure the WhatsApp -> cmux push relay workspace exists.
# MUST be run from inside a cmux session (the relay does `cmux send`, which only
# works for processes living inside the cmux app session tree).
D="$HOME/.local/share/whatsapp-mcp/whatsapp-bridge"
CMUX="/Applications/cmux.app/Contents/Resources/bin/cmux"
if "$CMUX" tree --all 2>/dev/null | grep -q '"wa-relay"'; then
  echo "wa-relay ya está corriendo."
  exit 0
fi
"$CMUX" workspace create --name "wa-relay" \
  --description "WhatsApp -> cmux push relay (mantener abierta)" \
  --cwd "$D" \
  --command "while true; do python3 $D/cmux-relay.py; echo '[relay restart]'; sleep 2; done" \
  --focus false
echo "wa-relay creada."
