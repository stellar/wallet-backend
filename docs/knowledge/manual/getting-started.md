---
description: Getting started with the wallet-backend knowledge system — first 10 minutes
type: reference
vault: docs/knowledge
---

# Getting Started

## First 10 Minutes

### 1. Orient (1 min)

The vault opens every session with an orientation. You'll see:
- Pending review tasks from `ops/tasks.md`
- Recent captures awaiting processing
- Subsystem maps with entry counts

Start at `[[entries/index]]` if you want to explore.

### 2. Capture (2 min)

If you have something to capture — a debugging insight, a gotcha, a decision you just made — drop it in `captures/`:

```markdown
# Quick capture
Date: 2026-02-23
Subsystem: ingestion

Backfill skips ledgers that already have a cursor — check IngestService.calculateGaps().
```

Don't worry about formatting. Process it later with `/document`.

### 3. Document (5 min)

Turn a capture or a fresh observation into a structured entry:

```
/document
```

The skill guides you through:
- Choosing an entry type (decision, insight, pattern, gotcha)
- Filling required frontmatter (type, status, subsystem, areas)
- Writing structured content (context, details, implications)
- Linking to related entries and reference docs

### 4. Connect (2 min)

After documenting, connect the new entry to the relevant knowledge map:

```
/connect
```

The skill finds related entries and updates the subsystem map.

## Entry Types

| Type | Use for |
|------|---------|
| `decision` | Why we chose X over Y |
| `insight` | Non-obvious understanding of how something works |
| `pattern` | Reusable implementation pattern |
| `gotcha` | Trap or surprising behavior to avoid |
| `reference` | Comprehensive overview (references/ only) |
