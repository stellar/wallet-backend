#!/usr/bin/env bash
# session-orient.sh — SessionStart hook
# Tracks session start + injects workspace tree, pending tasks, and condition signals

set -euo pipefail

VAULT_ROOT="docs/knowledge"
OPS_DIR="$VAULT_ROOT/ops"
SESSIONS_DIR="$OPS_DIR/sessions"

# ── Session tracking (silent — no stdout) ──────────────────────
# Read stdin before any echo (SessionStart provides JSON with session_id)
STDIN_DATA=$(timeout 5 cat 2>/dev/null || true)
SESSION_ID=""
if command -v jq &>/dev/null && [[ -n "$STDIN_DATA" ]]; then
  SESSION_ID=$(echo "$STDIN_DATA" | jq -r '.session_id // empty' 2>/dev/null || true)
fi

if [[ -n "$SESSION_ID" ]]; then
  mkdir -p "$SESSIONS_DIR"
  TIMESTAMP=$(date -u +"%Y%m%d-%H%M%S")
  CURRENT_JSON="$SESSIONS_DIR/current.json"

  # Promote previous session to timestamped archive if different ID
  if [[ -f "$CURRENT_JSON" ]]; then
    PREV_ID=""
    PREV_STARTED=""
    if command -v jq &>/dev/null; then
      PREV_ID=$(jq -r '.id // empty' "$CURRENT_JSON" 2>/dev/null || true)
      PREV_STARTED=$(jq -r '.started // empty' "$CURRENT_JSON" 2>/dev/null || true)
    fi
    if [[ -n "$PREV_ID" ]] && [[ "$PREV_ID" != "$SESSION_ID" ]]; then
      ARCHIVE_TS="${PREV_STARTED:-$TIMESTAMP}"
      mv "$CURRENT_JSON" "$SESSIONS_DIR/${ARCHIVE_TS}.json"
    fi
  fi

  # Write current session
  cat > "$CURRENT_JSON" << EOF
{
  "id": "$SESSION_ID",
  "started": "$TIMESTAMP",
  "status": "active"
}
EOF
fi

# ── Context injection (stdout → conversation) ──────────────────

# Print vault entry listing for orientation
echo "=== Knowledge Vault ==="
if command -v tree &>/dev/null; then
  tree -L 3 "$VAULT_ROOT" --dirsfirst -I "*.arscontexta" 2>/dev/null || true
else
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

# ── Condition-based maintenance signals (ported from plugin) ───
OBS_COUNT=$(ls -1 "$OPS_DIR/observations/"*.md 2>/dev/null | wc -l | tr -d ' ' || echo 0)
TENS_COUNT=$(ls -1 "$OPS_DIR/tensions/"*.md 2>/dev/null | wc -l | tr -d ' ' || echo 0)
SESS_COUNT=$(ls -1 "$SESSIONS_DIR/"*.json 2>/dev/null | grep -cv current 2>/dev/null || echo 0)

if [[ "$OBS_COUNT" -ge 10 ]]; then
  echo "CONDITION: $OBS_COUNT pending observations. Consider /retrospect."
fi
if [[ "$TENS_COUNT" -ge 5 ]]; then
  echo "CONDITION: $TENS_COUNT unresolved tensions. Consider /retrospect."
fi
if [[ "$SESS_COUNT" -ge 5 ]]; then
  echo "CONDITION: $SESS_COUNT unprocessed sessions. Consider /remember --mine-sessions."
fi

exit 0
