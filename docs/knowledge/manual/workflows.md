---
description: Common workflows for the wallet-backend knowledge system
type: reference
vault: docs/knowledge
---

# Common Workflows

## Debugging a Production Issue

1. Start debugging session normally
2. As you discover root causes, capture to `captures/`
3. After resolving: `/document` to create a `gotcha` or `insight` entry
4. `/connect` to link to the relevant subsystem map
5. Update reference doc if a fundamental misunderstanding was exposed

**Example entry types from debugging:**
- "Ingestion falls behind when RPC returns 504" → `gotcha` in ingestion subsystem
- "TimescaleDB chunk skipping only works when chunk_skipping extension is enabled" → `gotcha` in data-layer

## Adding a New Feature

Before coding:
1. Read relevant reference docs from `docs/knowledge/references/`
2. Check `entries/` for relevant decisions and patterns

After coding:
1. `/document` any new patterns or decisions made during implementation
2. `/retrospect` if the feature was non-trivial

## Making an Architecture Decision

1. Capture the decision context in `captures/`
2. `/document` as a `decision` entry with:
   - Context: what problem were we solving?
   - Decision: what did we choose?
   - Alternatives considered
   - Consequences
3. `/connect` to link to superseded alternatives (if any)

## After Code Review Feedback

If review reveals a misunderstanding:
1. `/document` the corrected understanding as an `insight`
2. Mark old entry as `superseded` if it contradicts the new understanding

## Handling Review Tasks (from hooks)

The `track-code-changes` hook creates tasks like:
```
- [ ] Review: references/ingestion-pipeline.md may need updating (internal/services/ingest_backfill.go changed, 2026-02-23)
```

Workflow:
1. Read the reference doc
2. Read the changed source file
3. Update the reference doc if needed
4. Check off the task in `ops/tasks.md`
