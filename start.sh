#!/bin/bash
# Auto-start the Go bridge + Python MCP server together
DIR="$(cd "$(dirname "$0")" && pwd)"

# Start the Go bridge in background if not already running
if ! lsof -i :8080 -sTCP:LISTEN >/dev/null 2>&1; then
  cd "$DIR/whatsapp-bridge"
  go run main.go &
  BRIDGE_PID=$!

  # Wait for bridge to be ready (up to 30s)
  for i in $(seq 1 30); do
    if curl -s http://localhost:8080/api/status >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  # Cleanup bridge when script exits
  trap "kill $BRIDGE_PID 2>/dev/null" EXIT
fi

# Run the Python MCP server
cd "$DIR/whatsapp-mcp-server"
exec /Users/ech/.local/bin/uv run main.py
