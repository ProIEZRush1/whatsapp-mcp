#!/bin/bash
# WhatsApp MCP maintenance — keeps the bridge DB compact and logs/media small.
# Run daily via com.ech.whatsapp-maint LaunchAgent.
set -e
B="$HOME/.local/share/whatsapp-mcp/whatsapp-bridge"
SQLITE=/usr/bin/sqlite3

# 1. Checkpoint the WAL so messages.db absorbs pending writes (prevents huge -wal).
[ -f "$B/store/messages.db" ] && "$SQLITE" "$B/store/messages.db" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null 2>&1 || true
[ -f "$B/store/whatsapp.db" ] && "$SQLITE" "$B/store/whatsapp.db" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null 2>&1 || true

# 2. Truncate bridge logs if larger than 5 MB (the bridge appends, so this is safe).
for f in "$B/bridge.out" "$B/bridge.err"; do
  if [ -f "$f" ] && [ "$(stat -f%z "$f" 2>/dev/null || echo 0)" -gt 5242880 ]; then : > "$f"; fi
done

# 3. Delete downloaded media older than 30 days (re-downloadable on demand).
find "$B/store" -type f \( -name '*.jpg' -o -name '*.jpeg' -o -name '*.png' -o -name '*.webp' \
  -o -name '*.mp4' -o -name '*.ogg' -o -name '*.opus' -o -name '*.pdf' -o -name '*.docx' \
  -o -name '*.xlsx' -o -name '*.zip' \) -mtime +30 -delete 2>/dev/null || true

# 4. Remove any stray bridge build/backup artifacts that pile up.
rm -f "$B"/wabridge.bak* "$B"/wabridge.prev "$B"/wabridge.new "$B"/wabridge.lidfix \
  "$B"/wabridge.*.bak "$B"/*.repair.out "$B"/store/*.prerepair-session \
  "$B"/store/*.prelidmigrate "$B"/store/*.20*_repair.bak 2>/dev/null || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] maintenance done; disk: $(du -sh "$HOME/.local/share/whatsapp-mcp" 2>/dev/null | cut -f1)"
