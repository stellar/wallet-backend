---
description: Configuration reference for the wallet-backend knowledge system
type: reference
vault: docs/knowledge
---

# Configuration

## Primary Configuration

Edit `docs/knowledge/ops/config.yaml` to adjust system behavior.

## Hooks

Hooks are configured in `.claude/settings.json`. Current hooks:
- `SessionStart`: `bash .claude/hooks/session-orient.sh` — vault orientation
- `PostToolUse (Write)`: `validate-note.sh` + `auto-commit.sh` — schema check + commit
- `PostToolUse (Edit)`: `track-code-changes.sh` — subsystem change tracking
- `Stop`: `session-capture.sh` — session state persistence

## Subsystem Mapping

`track-code-changes.sh` maps file paths to subsystems:

| File Pattern | Subsystem | Reference Doc |
|--------------|-----------|---------------|
| `internal/services/ingest*.go`, `internal/indexer/**` | ingestion | ingestion-pipeline.md |
| `internal/ingest/**` | ingestion | ingestion-pipeline.md |
| `internal/serve/graphql/**` | graphql | graphql-api.md |
| `internal/data/**`, `internal/db/migrations/**` | data-layer | data-layer.md |
| `internal/signing/**` | signing | signing-and-channels.md |
| `internal/services/*.go` (non-ingest) | services | services.md |
| `pkg/auth/**`, `internal/serve/middleware/auth*` | auth | authentication.md |

## Entry Schema

Required fields: `description`, `type`, `status`, `subsystem`, `areas`
Optional fields: `confidence`, `superseded_by`

Valid enum values:
- `type`: decision, insight, pattern, gotcha, reference
- `status`: current, superseded, experimental
- `confidence`: proven, likely, experimental
- `subsystem`: ingestion, graphql, data-layer, signing, services, auth
