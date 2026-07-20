#!/bin/bash
# Idempotent: ensure the wa-relay workspace (relay + self-heal) is running.
# Must run INSIDE a cmux session. Safe under concurrent calls (mkdir lock).
D="$HOME/.local/share/whatsapp-mcp/whatsapp-bridge"
CMUX="/Applications/cmux.app/Contents/Resources/bin/cmux"
[ -x "$CMUX" ] || exit 0
"$CMUX" tree --all 2>/dev/null | grep -q '"wa-relay"' && exit 0
LOCK="$D/.wa-relay.lockdir"
if [ -d "$LOCK" ]; then
  age=$(( $(date +%s) - $(stat -f %m "$LOCK" 2>/dev/null || echo 0) ))
  [ "$age" -gt 120 ] && rmdir "$LOCK" 2>/dev/null
fi
mkdir "$LOCK" 2>/dev/null || exit 0
trap 'rmdir "$LOCK" 2>/dev/null' EXIT
"$CMUX" tree --all 2>/dev/null | grep -q '"wa-relay"' && exit 0
"$CMUX" workspace create --name "wa-relay" \
  --description "WhatsApp relay + self-heal (no cerrar)" \
  --cwd "$D" --command "bash $D/wa-supervisor.sh" --focus false
echo "wa-relay (re)creada."

exit 0
