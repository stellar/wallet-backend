#!/usr/bin/env bash
# session-capture.sh — Stop hook
# Persists rich session summary to docs/knowledge/ops/sessions/
# Receives JSON on stdin: { session_id, transcript_path, last_assistant_message, cwd, stop_hook_active }

set -euo pipefail

# ── Read stdin (with timeout to avoid hanging if stdin is empty) ──────────────
STDIN_DATA=$(timeout 5 cat 2>/dev/null || true)

# ── Guard: exit immediately if stop_hook_active to prevent re-entrant loops ───
STOP_HOOK_ACTIVE=$(echo "$STDIN_DATA" | jq -r '.stop_hook_active // false' 2>/dev/null || echo "false")
if [[ "$STOP_HOOK_ACTIVE" == "true" ]]; then
  exit 0
fi

# ── Extract fields from stdin JSON ────────────────────────────────────────────
SESSION_ID=$(echo "$STDIN_DATA" | jq -r '.session_id // ""' 2>/dev/null || true)
TRANSCRIPT_PATH=$(echo "$STDIN_DATA" | jq -r '.transcript_path // ""' 2>/dev/null || true)
LAST_MESSAGE=$(echo "$STDIN_DATA" | jq -r '.last_assistant_message // ""' 2>/dev/null || true)
CWD=$(echo "$STDIN_DATA" | jq -r '.cwd // ""' 2>/dev/null || true)

# ── Timestamps ────────────────────────────────────────────────────────────────
SESSION_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SESSION_STAMP=$(date -u +"%Y%m%d-%H%M%S")

SESSIONS_DIR="docs/knowledge/ops/sessions"
mkdir -p "$SESSIONS_DIR"
SESSION_FILE="$SESSIONS_DIR/session-${SESSION_STAMP}.md"

# ── Get session start time from current.json ──────────────────────────────────
CURRENT_JSON="ops/sessions/current.json"
SESSION_STARTED=""
if [[ -f "$CURRENT_JSON" ]]; then
  SESSION_STARTED=$(jq -r '.started // ""' "$CURRENT_JSON" 2>/dev/null || true)
fi

# ── Git activity since session start ──────────────────────────────────────────
GIT_COMMITS=""
GIT_FILES_CHANGED=""
KNOWLEDGE_ENTRIES_TOUCHED=""

if [[ -n "$SESSION_STARTED" ]] && git rev-parse --git-dir &>/dev/null 2>&1; then
  # Convert YYYYMMDD-HHMMSS to a format git --since understands
  # e.g. 20260223-221338 → "2026-02-23 22:13:38"
  GIT_SINCE="${SESSION_STARTED:0:4}-${SESSION_STARTED:4:2}-${SESSION_STARTED:6:2} ${SESSION_STARTED:9:2}:${SESSION_STARTED:11:2}:${SESSION_STARTED:13:2}"

  GIT_COMMITS=$(git log --oneline --since="$GIT_SINCE" 2>/dev/null || true)
  GIT_FILES_CHANGED=$(git log --name-only --since="$GIT_SINCE" --pretty=format: 2>/dev/null \
    | sort -u | grep -v '^$' || true)
  KNOWLEDGE_ENTRIES_TOUCHED=$(echo "$GIT_FILES_CHANGED" \
    | grep "^docs/knowledge/entries/" || true)
fi

COMMIT_COUNT=$(echo "$GIT_COMMITS" | grep -c . 2>/dev/null || echo 0)
FILES_COUNT=$(echo "$GIT_FILES_CHANGED" | grep -c . 2>/dev/null || echo 0)
ENTRIES_COUNT=$(echo "$KNOWLEDGE_ENTRIES_TOUCHED" | grep -c . 2>/dev/null || echo 0)

# ── Lightweight transcript analysis ──────────────────────────────────────────
TOTAL_ENTRIES=0
USER_TURNS=0
ASSISTANT_TURNS=0
TOOLS_SUMMARY=""
FILES_WRITTEN=""

if [[ -f "$TRANSCRIPT_PATH" ]]; then
  TOTAL_ENTRIES=$(wc -l < "$TRANSCRIPT_PATH" 2>/dev/null | tr -d ' ' || echo 0)

  USER_TURNS=$(jq -r 'select(.type == "user") | .type' "$TRANSCRIPT_PATH" 2>/dev/null \
    | wc -l | tr -d ' ' || echo 0)
  ASSISTANT_TURNS=$(jq -r 'select(.type == "assistant") | .type' "$TRANSCRIPT_PATH" 2>/dev/null \
    | wc -l | tr -d ' ' || echo 0)

  # Tool usage frequency (top 5)
  TOOLS_SUMMARY=$(jq -r '
    select(.type == "assistant") |
    .message.content[]? |
    select(.type == "tool_use") |
    .name
  ' "$TRANSCRIPT_PATH" 2>/dev/null \
    | sort | uniq -c | sort -rn | head -5 \
    | awk '{printf "  - %s: %d\n", $2, $1}' || true)

  # Files written or edited via Write/Edit tool calls
  FILES_WRITTEN=$(jq -r '
    select(.type == "assistant") |
    .message.content[]? |
    select(.type == "tool_use") |
    select(.name == "Write" or .name == "Edit") |
    .input.file_path // .input.path // ""
  ' "$TRANSCRIPT_PATH" 2>/dev/null \
    | grep -v '^$' | sort -u \
    | awk '{printf "  - %s\n", $0}' || true)
fi

# ── Truncate last_assistant_message to 2000 chars ─────────────────────────────
LAST_MESSAGE_TRUNCATED="${LAST_MESSAGE:0:2000}"
if [[ ${#LAST_MESSAGE} -gt 2000 ]]; then
  LAST_MESSAGE_TRUNCATED="${LAST_MESSAGE_TRUNCATED}... [truncated]"
fi

# ── Format optional sections ──────────────────────────────────────────────────
format_section() {
  local title="$1"
  local count="$2"
  local content="$3"
  if [[ -n "$content" ]]; then
    echo "## ${title} (${count})"
    echo "$content"
    echo ""
  fi
}

# ── Write session file ────────────────────────────────────────────────────────
{
  cat <<FRONTMATTER
---
date: ${SESSION_DATE}
session_id: ${SESSION_ID}
type: session-capture
mined: false
cwd: ${CWD}
transcript: ${TRANSCRIPT_PATH}
---

# Session Capture — ${SESSION_STAMP}

FRONTMATTER

  if [[ -n "$LAST_MESSAGE_TRUNCATED" ]]; then
    echo "## Last Assistant Message"
    echo ""
    echo "$LAST_MESSAGE_TRUNCATED"
    echo ""
  fi

  if [[ -n "$GIT_COMMITS" ]]; then
    echo "## Commits (${COMMIT_COUNT})"
    echo ""
    echo "$GIT_COMMITS"
    echo ""
  fi

  if [[ -n "$GIT_FILES_CHANGED" ]]; then
    echo "## Files Changed (${FILES_COUNT})"
    echo ""
    echo "$GIT_FILES_CHANGED" | awk '{print "  - " $0}'
    echo ""
  fi

  if [[ -n "$KNOWLEDGE_ENTRIES_TOUCHED" ]]; then
    echo "## Knowledge Entries Touched (${ENTRIES_COUNT})"
    echo ""
    echo "$KNOWLEDGE_ENTRIES_TOUCHED" | awk '{print "  - " $0}'
    echo ""
  fi

  if [[ "$TOTAL_ENTRIES" -gt 0 || "$USER_TURNS" -gt 0 ]]; then
    echo "## Transcript Analysis"
    echo ""
    echo "- Total entries: ${TOTAL_ENTRIES}, User turns: ${USER_TURNS}, Assistant turns: ${ASSISTANT_TURNS}"
    if [[ -n "$TOOLS_SUMMARY" ]]; then
      echo "- Tools used by frequency:"
      echo "$TOOLS_SUMMARY"
    fi
    if [[ -n "$FILES_WRITTEN" ]]; then
      echo "- Files written/edited:"
      echo "$FILES_WRITTEN"
    fi
    echo ""
  fi

} > "$SESSION_FILE"

# ── Update current.json: mark session completed ───────────────────────────────
if [[ -f "$CURRENT_JSON" ]]; then
  UPDATED=$(jq \
    --arg status "completed" \
    --arg ended "$SESSION_STAMP" \
    --arg file "$SESSION_FILE" \
    '.status = $status | .ended = $ended | .session_file = $file' \
    "$CURRENT_JSON" 2>/dev/null || cat "$CURRENT_JSON")
  echo "$UPDATED" > "$CURRENT_JSON"
fi

exit 0
