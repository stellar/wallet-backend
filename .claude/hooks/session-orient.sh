#!/usr/bin/env bash
# session-orient.sh — SessionStart hook
# Injects workspace tree and surfaces pending tasks for orientation

set -euo pipefail

VAULT_ROOT="docs/knowledge"
OPS_DIR="$VAULT_ROOT/ops"

# Print vault tree for orientation
if command -v tree &>/dev/null; then
  echo "=== Knowledge Vault ==="
  tree -L 3 "$VAULT_ROOT" --dirsfirst -I "*.arscontexta" 2>/dev/null || true
else
  echo "=== Knowledge Vault ==="
  find "$VAULT_ROOT" -type f -name "*.md" | sort | head -40 2>/dev/null || true
fi

# Surface reminders if they exist
if [[ -f "$OPS_DIR/reminders.md" ]]; then
  echo ""
  echo "=== Reminders ==="
  cat "$OPS_DIR/reminders.md"
fi

# Surface pending tasks from tasks/
TASKS_DIR="$OPS_DIR/tasks"
if [[ -d "$TASKS_DIR" ]]; then
  PENDING=$(find "$TASKS_DIR" -name "*.md" -newer "$VAULT_ROOT/.arscontexta" 2>/dev/null | head -5)
  if [[ -n "$PENDING" ]]; then
    echo ""
    echo "=== Pending Tasks ==="
    echo "$PENDING"
  fi
fi

exit 0
