---
description: Processing methodology for the wallet-backend knowledge system
type: reference
vault: docs/knowledge
---

# Methodology

The knowledge system follows a four-phase processing cycle adapted for engineering context.

## Processing Cycle

### 1. Document
Capture raw engineering knowledge — architecture decisions, debugging sessions, code patterns — and structure it as entries in `entries/`. Each entry has YAML frontmatter with type, status, subsystem, and confidence.

**When to document:**
- After resolving a non-obvious bug
- After making an architectural decision
- After discovering a gotcha or RPC quirk
- After optimizing a query or fixing a performance issue

### 2. Connect
Link entries to each other and to reference docs. Update knowledge maps (subsystem maps in `entries/`) when new entries are added. Use `[[wiki-links]]` to create navigable connections.

**When to connect:**
- After documenting a new entry
- When you notice an entry relates to an existing pattern or decision

### 3. Revisit
Review entries that may have become stale due to code changes. The `track-code-changes` hook creates review tasks in `ops/tasks.md` when subsystem code is edited. Process these tasks periodically.

**Triggers for revisit:**
- Hook-generated review tasks in `ops/tasks.md`
- Entries older than 90 days with `status: current`
- After major refactors

### 4. Verify
Confirm entries against source code. Check that method names, constants, and behaviors documented in entries match the actual code.

**Verification cadence:** Before using an entry to inform a major code change, verify it's still accurate.

## Entry Schema

```yaml
---
description: <one-line summary>
type: decision|insight|pattern|gotcha|reference
status: current|superseded|experimental
confidence: proven|likely|experimental
subsystem: ingestion|graphql|data-layer|signing|services|auth
areas: [<tags>]
---
```

## Three-Space Boundaries

| Space | Folder | Contains |
|-------|--------|----------|
| Working | `captures/` | Raw notes, unprocessed observations |
| Reference | `references/` + `entries/` | Structured knowledge |
| Archive | `archive/` | Superseded entries |
