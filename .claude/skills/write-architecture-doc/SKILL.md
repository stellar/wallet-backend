---
name: write-architecture-doc
description: Write or complete reference documentation with Mermaid diagrams for a wallet-backend subsystem. Use when creating new docs in docs/knowledge/references/ or filling in TODO sections in existing ones.
---

# Task: Write or Complete Architecture Documentation

Write (or complete) an architecture doc at `docs/knowledge/references/{TARGET_FILE}`. The user will specify the target file and the subsystem scope. If the file already exists, improve it — replace any TODO comments, fill gaps, and add missing sections. If the file is new, create it from scratch based on source code analysis.

## Reference Example

`docs/knowledge/references/ingestion-pipeline.md` is the gold standard. Read it before starting — it demonstrates the expected patterns:
- Diagram-first sections (Mermaid diagrams are primary content, not decoration)
- Concise prose that explains "why" and design decisions
- Reference tables for constants, methods, and config
- Source file references for deeper exploration

## Audience

These docs are read by **Claude Code agents** working on the wallet-backend codebase. They are loaded on-demand from CLAUDE.md when an agent needs context about a subsystem. This means:
- **Optimize for AI comprehension**: clear data flows, explicit relationships, named constants
- **Avoid redundancy with source code**: document architecture and decisions, not line-by-line code
- **Be precise**: agents will use these docs to make code changes — inaccurate docs cause incorrect edits

## Process

### Phase 1: Scope and Read Source Code (mandatory)

1. **Determine scope**: If the user specifies source files/directories, start there. Otherwise, explore the relevant directories using `get_symbols_overview` and `find_symbol` to identify the key types, interfaces, and functions.
2. **Check change tracking**: If `docs/knowledge/ops/reference-doc-changes.yaml` exists, read the `files` list for this doc's key. Use the listed files as priority targets for source code reading — they represent what changed since the last update, enabling targeted section updates rather than a full rewrite.
3. **Read the code**: Read every relevant source file. Focus on:
   - Public interfaces and their implementations
   - Constructor/factory functions (reveal dependency wiring)
   - Constants (retry limits, batch sizes, thresholds)
   - SQL queries (exact queries, not paraphrased)
   - Error handling patterns and state machines
4. **If the file already exists**: Read it first, identify TODOs and thin sections that need expansion.

**Do NOT start writing until you have read all relevant source files.**

### Phase 2: Design Document Structure

Plan the sections and diagram types before writing:

1. **Start with a high-level overview diagram** showing the subsystem's place in the broader architecture
2. **One section per major flow or component**, each with at least one diagram
3. **Choose diagram types based on what the code does**:

| Pattern in Code | Diagram Type | When to Use |
|----------------|--------------|-------------|
| Data flow, pipelines, fan-out | `flowchart TD` or `flowchart LR` | Request → middleware → handler → DB |
| Decision trees, branching logic | `flowchart TD` | if/switch statements with multiple paths |
| Temporal ordering between actors | `sequenceDiagram` | Multi-step transactions, request/response between services |
| State machines, retry loops | `stateDiagram-v2` | Retry with backoff, lifecycle states, connection states |
| Struct/interface relationships | `flowchart` with subgraphs | Interface → multiple implementations |

4. **Add sections the user didn't ask for** if the code reveals important subsystems. The goal is completeness for the subsystem scope.

### Phase 3: Write the Document

For each section:
1. **Mermaid diagram first** — this is the primary content
2. **Prose after** — explain design decisions, key constants, and things the diagram can't show
3. **Tables for reference data** — method inventories, constant values, config options
4. **Source file references** — list key files so readers can dive deeper

Writing rules:
- Use exact values from source code (constants, method names, SQL queries)
- Keep prose concise — if a diagram shows it, don't repeat it in text
- Do NOT add speculative or aspirational content — only document what exists in code
- Do NOT describe trivial getters/setters — focus on non-obvious architecture
- If a file already existed, ensure every original TODO comment is addressed and replaced
- Add YAML frontmatter to every reference doc: `---\ntitle: [Subsystem Name]\ntype: reference\nsubsystem: [subsystem]\nupdated: YYYY-MM-DD\n---`

### Phase 4: Validate Mermaid Diagrams

Run validation after writing:
```
mmdc -i docs/knowledge/references/{TARGET_FILE} -o /tmp/mermaid-validation.svg 2>&1
```

Fix any errors and re-validate until all diagrams pass. Common pitfalls:
- Special characters in labels need quotes: `Node["Label with (parens)"]`
- Subgraph titles need quotes if they contain special chars
- Arrow labels use `-->|"label"|` syntax
- No empty nodes or dangling arrows
- `stateDiagram-v2` notes use `note left of` / `note right of` syntax

### Phase 5: Spot-Check Accuracy

Re-read 2-3 key source files and verify:
- Method names and signatures match the code
- Constants have correct values
- SQL queries match the actual implementations
- Data flow arrows reflect the real call chain

### Phase 6: Reset Change Tracking

After successfully writing or updating the doc:

1. **Update `updated:` in YAML frontmatter** to today's date (format: YYYY-MM-DD).
2. **Reset the change counter** in `docs/knowledge/ops/reference-doc-changes.yaml`: set `changes.<doc_key>` to `{ count: 0, files: [] }` where `<doc_key>` matches the filename without `.md` (e.g., `signing-and-channels` for `references/signing-and-channels.md`).
3. This closes the staleness loop — `/next` will auto-close the `stale_references` maintenance task on its next evaluation when it sees the count is below the threshold.

Example reset for `references/signing-and-channels.md`:
```yaml
# In reference-doc-changes.yaml, change:
  signing-and-channels: { count: 0, files: [] }
```

## Quality Checklist

- [ ] Every section has at least one Mermaid diagram
- [ ] All Mermaid diagrams pass `mmdc` validation
- [ ] Constants and method names are accurate (verified against source)
- [ ] Prose explains "why" (design decisions), not just "what" (which the diagram shows)
- [ ] No speculative or aspirational content — only documents what exists
- [ ] Key source files are referenced so readers can find the code
- [ ] If the file already existed: all original TODO comments are replaced with content
- [ ] The doc is registered in CLAUDE.md under "Reference Docs" with a one-line description

## Registering New Docs

After writing a new doc, add it to the Reference Docs section in `CLAUDE.md`:
```
- @docs/knowledge/references/{TARGET_FILE} — {one-line description of scope}
```
This is how future agents discover the doc. The one-line description acts as an index entry — make it specific enough that an agent can decide whether to load the doc based on the description alone.
