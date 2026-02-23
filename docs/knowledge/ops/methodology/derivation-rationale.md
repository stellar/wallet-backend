---
description: Rationale for each configuration dimension in the knowledge system derivation
type: reference
vault: docs/knowledge
---

# Derivation Rationale

Why each dimension was set to its current position.

## Granularity → Moderate

**Chosen:** Per-decision/per-insight entries, not atomic claims.

**Rationale:** Wallet-backend engineering knowledge is inherently contextual. An atomic "TimescaleDB uses hypertables" claim is less useful than an entry that explains *why* hypertables were chosen, what the tradeoffs were, and what chunk interval was selected. Moderate granularity preserves the "why" alongside the "what."

**Not chosen:** Fine-grained atomic claims would fragment context across too many entries, making it hard to understand decisions holistically.

## Organization → Flat

**Chosen:** Flat entries/ directory, no subsystem sub-folders.

**Rationale:** Wallet-backend knowledge crosses subsystem boundaries frequently (e.g., a gotcha about Stellar RPC affects both ingestion and transaction services). Flat organization with `subsystem` metadata in frontmatter allows filtering without artificial hierarchy.

**Not chosen:** Hierarchical folders would force entries into a single subsystem even when they span multiple.

## Linking → Explicit + Implicit

**Chosen:** Explicit wiki-links for known relationships; implicit deferred until tooling is installed.

**Rationale:** Dual audience (human + AI) benefits from explicit links that make relationships navigable without tooling. The AI agent audience specifically needs explicit links to traverse the knowledge graph without semantic search.

## Processing → Heavy

**Chosen:** Both WHY and HOW are captured; dual audience extraction.

**Rationale:** Engineering knowledge degrades fastest when only "what" is captured. The wallet-backend has non-obvious design decisions (channel account locking, hypertable chunk intervals, Stellar RPC quirks) where the "why" is critical to avoid repeating mistakes.

## Navigation → 3-tier

**Chosen:** Hub (index.md) → subsystem knowledge maps → entries/references.

**Rationale:** Three tiers match the three levels of engineering question: "What area is this?" → "What do I know about ingestion?" → "Why did we choose X?"

## Maintenance → Condition-based

**Chosen:** Trigger maintenance on code changes (via hooks) rather than on schedule.

**Rationale:** Engineering context changes when code changes, not on a calendar schedule. The track-code-changes hook detects subsystem edits and creates review tasks, ensuring reference docs stay accurate after code changes.

## Schema → Moderate-Dense

**Chosen:** Required fields (description, type, status, subsystem, areas) + optional fields.

**Rationale:** AI agents need structured metadata to decide whether to load a document without reading its full content. The `subsystem` field enables agents to load only relevant docs for a given task.

## Automation → Full

**Chosen:** Full automation via Claude Code hooks.

**Rationale:** Manual knowledge maintenance in an agentic coding context is unsustainable. Hooks automate session orientation, note validation, and code-change tracking, ensuring the system is maintained continuously without extra effort.
