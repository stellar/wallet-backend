# Knowledge System

This directory contains a structured knowledge base for the wallet-backend codebase. It captures architecture decisions, debugging insights, code patterns, and gotchas — the kind of institutional knowledge that normally lives in people's heads or gets buried in Slack threads. Instead of losing it when someone moves to a different team, or rediscovering the same pitfall six months later, the knowledge graph makes it searchable, linkable, and durable.

The system is powered by [Ars Contexta v0.8.0](https://arscontexta.org) and integrated with Claude Code via automation hooks and slash commands. When you open this repo in Claude Code, hooks automatically surface relevant knowledge at session start, validate new entries against the schema, auto-commit knowledge files to git, track when source code changes make reference docs stale, and capture a session summary when you close.

**At a glance:** ~124 entries, 8 reference docs, 7 knowledge maps, 18+ slash commands, 5 automation hooks.

---

## What's In Here

```
docs/knowledge/
├── README.md                    # This file
├── references/                  # Comprehensive subsystem overviews with Mermaid diagrams
│   ├── overview.md              # High-level architecture and directory structure
│   ├── ingestion-pipeline.md    # Live/backfill ingestion flow, processors, retry logic
│   ├── graphql-api.md           # Request flow, schema, resolvers, dataloaders, mutations
│   ├── data-layer.md            # TimescaleDB hypertables, models, migrations, connection pool
│   ├── signing-and-channels.md  # Signing providers (ENV/KMS), channel account lifecycle
│   ├── services.md              # Service pattern, inventory, dependency graph
│   ├── authentication.md        # JWT/Ed25519 auth flow, client library
│   └── state-changes.md         # Unified state change model, processor taxonomy
├── entries/                     # Individual insights (one idea per file)
│   ├── index.md                 # Hub knowledge map — start here
│   ├── ingestion.md             # Knowledge map: ingestion subsystem
│   ├── graphql-api.md           # Knowledge map: GraphQL API
│   ├── data-layer.md            # Knowledge map: data layer
│   ├── signing.md               # Knowledge map: signing and channels
│   └── *.md                     # ~124 individual insight entries
├── manual/                      # Human-readable documentation for using the system
│   ├── getting-started.md       # First 10 minutes walkthrough
│   ├── skills.md                # Complete slash command reference
│   ├── workflows.md             # Detailed workflow patterns
│   ├── configuration.md         # Configuration reference
│   ├── meta-skills.md           # Health checks, stats, graph analysis
│   └── troubleshooting.md       # Common problems and solutions
├── ops/                         # System operations — pipeline state, queue, config
│   ├── config.yaml              # System configuration
│   ├── derivation.md            # Rationale for configuration choices
│   ├── tasks/                   # Pending knowledge work tasks
│   ├── queries/                 # Diagnostic shell scripts
│   ├── sessions/                # Per-session capture summaries
│   └── methodology/             # Full methodology specification
└── self/                        # Agent identity and methodology docs
```

The knowledge base has three content tiers. **Reference docs** (`references/`) are comprehensive subsystem overviews — the first thing to read before touching a subsystem. They contain Mermaid architecture diagrams, processor inventories, source file paths, and cross-references. **Entries** (`entries/`) are atomic insights — each file captures exactly one decision, pattern, or gotcha, and entries link to each other via wiki-links. **Knowledge maps** (the named `.md` files inside `entries/`) are navigation hubs that organize entries by subsystem with context explaining why each entry matters.

---

## Reference Docs

Before working on any subsystem, read the corresponding reference doc. These are not summaries — they contain the full architecture with diagrams.

| Subsystem | Reference Doc | Entries |
|-----------|--------------|---------|
| Ingestion pipeline | `references/ingestion-pipeline.md` | 52 |
| GraphQL API | `references/graphql-api.md` | 21 |
| Data layer | `references/data-layer.md` | 26 |
| Signing & channels | `references/signing-and-channels.md` | 21 |
| State changes | `references/state-changes.md` | 4 |
| Services | `references/services.md` | 0 |
| Authentication | `references/authentication.md` | 0 |
| Architecture overview | `references/overview.md` | — |

Each reference doc contains: a Mermaid architecture diagram showing component relationships, an inventory of relevant source files with their roles, configuration tables, and cross-references to related entries. They are generated and maintained via the `/write-architecture-doc` slash command. When source code changes accumulate (tracked automatically via the `track-code-changes` hook), a reference doc is flagged as potentially stale and `/next` will surface it for regeneration.

---

## How Entries Work

### Prose-as-title pattern

Entry filenames are written as complete, specific claims rather than topic labels. This is intentional. A file named `"channel account exhaustion causes silent transaction drops during bursts"` is fundamentally different from a file named `"channel accounts"`. The claim form lets the title do real work in wiki-links:

> *Since [[channel account exhaustion causes silent transaction drops during bursts]], burst submission requires backpressure.*

Compare that to: *"See [[channel accounts]] for more."* The first form uses the entry as an argument in reasoning. The second just points somewhere. The prose-as-title pattern makes every link a thought, not a footnote. Yes, the filenames are long — that's the point.

### Entry structure

Every entry is a markdown file with a YAML frontmatter block followed by a prose body. Here's the advisory lock entry as an example:

```yaml
---
context: FNV-64a hash of "wallet-backend-ingest-<network>" gives testnet/pubnet separate locks; uses pg_try_advisory_lock (non-blocking)
type: decision
status: current
subsystem: ingestion
areas: [ingestion, data-layer, postgresql]
created: 2026-02-24
---
```

The `context` field is required. It must add new information beyond what the title already says — scope, mechanism, or implication. A context that restates the title (`"Exhausting channel accounts causes transactions to be dropped silently"`) is invalid. A good context adds specificity the title omits: the lock hash function, which variant of the PostgreSQL function is used, and the non-blocking behavior.

### Entry types

| Type | Meaning |
|------|---------|
| `decision` | Why we chose X over Y — captures the reasoning at the time so future readers don't have to reverse-engineer it |
| `insight` | Non-obvious understanding of how something works — the kind of thing you'd wish you'd known before debugging |
| `pattern` | A reusable implementation approach — Options struct DI, dataloader batching, factory functions |
| `gotcha` | A trap or surprise — Stellar RPC quirks, migration ordering constraints, schema subtleties |

### Wiki-link convention

Links work as arguments, not pointers. When connecting entries, write the link into the sentence so it carries meaning:

```markdown
Since [[crash recovery relies on atomic transactions to make ledger re-processing idempotent]],
a crash-and-restart cycle is safe: the lock releases, the new process acquires it, and
re-processes the last incomplete ledger from scratch.
```

Not: `"See [[crash recovery relies on atomic transactions...]] for related information."`

---

## Automation

Five Claude Code hooks automate the routine maintenance work that would otherwise go undone.

| Hook | Trigger | What It Does | Output |
|------|---------|--------------|--------|
| `session-orient.sh` | Session start (`SessionStart`) | Surfaces vault tree, pending tasks, reminders, and recent sessions | Displayed in Claude Code context |
| `validate-note.sh` | Writing to `entries/` (`PostToolUse: Write`) | Checks YAML schema — required fields, enum values, context quality | Inline warnings (non-blocking) |
| `auto-commit.sh` | Writing to `docs/knowledge/` (`PostToolUse: Write`) | Auto-commits the file to git so knowledge is never accidentally lost | Git history |
| `track-code-changes.sh` | Editing `.go` files (`PostToolUse: Edit`) | Increments change counter per reference doc, creates review tasks when a doc accumulates 5+ changes | `ops/reference-doc-changes.yaml`, `ops/tasks/` |
| `session-capture.sh` | Session end (`Stop`) | Captures session summary: commits made, files changed, tool usage patterns | `ops/sessions/` |

The change-tracking lifecycle works as follows: every time you edit a `.go` file in a subsystem, the hook increments a counter in `ops/reference-doc-changes.yaml` for the corresponding reference doc. When a doc accumulates 5 changes, it's flagged as potentially stale — the source code has drifted enough that the architecture doc may no longer be accurate. Running `/next` will surface this as a recommended action, and `/write-architecture-doc` regenerates the reference doc from current source code, resetting the counter.

---

## Processing Pipeline

New knowledge flows through four phases. Each phase has a distinct purpose — mixing them degrades quality.

1. **Capture** — Raw notes drop into `captures/`. No structure required. Write down whatever you learned, observed, or noticed. Format doesn't matter at this stage.

2. **Document** (`/document`) — Extract structured entries from the raw material. Each extracted entry gets a prose title, YAML frontmatter, and a body that explains the insight with source file context. The target extraction rate for domain-relevant material is above 90% — if something is genuinely about the wallet-backend, it probably belongs as an entry.

3. **Connect** (`/connect`) — Find relationships between the new entries and existing entries. Update knowledge maps to include the new entries with context phrases. This is where isolated entries become part of the knowledge graph.

4. **Verify** (`/verify`) — Quality gate: cold-read test (can you predict the entry's content from its title and context alone?), schema validation, and link health check. Entries that fail the cold-read test have context that restates the title and need to be sharpened.

For most developers, `/document` followed by `/connect` covers the common case. The full pipeline from raw capture to verified, linked entry can run end-to-end via `/pipeline`, or in batches via `/ralph` (which spawns isolated subagents to prevent context contamination between entries).

---

## Claude Code Skills

All interaction with the knowledge system happens through slash commands in Claude Code. There are three tiers.

### Core workflow

These are the commands you'll use regularly:

| Command | What it does |
|---------|-------------|
| `/document` | Extract structured entries from a source file, observation, or debugging session |
| `/connect` | Find relationships between entries and update knowledge maps |
| `/verify` | Quality-check an entry: cold-read test, schema validation, link health |
| `/next` | Get the single highest-priority recommendation for knowledge work |
| `/write-architecture-doc` | Generate or update a reference doc from current source code |

### Discovery and capture

| Command | What it does |
|---------|-------------|
| `/learn` | Research a topic, file results to `captures/` with provenance |
| `/remember` | Capture a correction or friction pattern as a methodology entry |
| `/seed` | Add a source file to the processing queue |
| `/pipeline` | Run the full capture-to-verify pipeline on a source |

### Maintenance and diagnostics

| Command | What it does |
|---------|-------------|
| `/health` | Run vault diagnostics: schema compliance, orphans, links, coverage |
| `/stats` | Show vault statistics and growth metrics |
| `/graph` | Analyze knowledge graph structure: hubs, bridges, clusters |
| `/tasks` | View and manage pending knowledge tasks |
| `/revisit` | Update older entries with newer connections |
| `/retrospect` | Review accumulated observations and challenge assumptions |
| `/ralph` | Batch-process queue items with isolated subagents |
| `/validate` | Schema-only validation (lighter than `/verify`) |
| `/refactor` | Plan vault restructuring from configuration changes |

See `manual/skills.md` for detailed documentation on each command, including arguments and examples.

---

## Common Workflows

### Before working on a subsystem

Read the reference doc first. If you're about to touch the ingestion pipeline, open `references/ingestion-pipeline.md`. It has a Mermaid diagram of the processor fan-out, the full processor inventory with their source files, notes on retry logic, and links to the entries that explain non-obvious design decisions. Reading it takes five minutes and can save hours of archaeology through the codebase.

### After debugging something non-obvious

Run `/document` in Claude Code. Describe what you learned: what you expected, what actually happened, and why. The system will guide you through creating a structured entry. Then run `/connect` to link it into the graph — future sessions (and teammates) will find it when they hit the same issue. The auto-commit hook ensures it's in git immediately.

### When reference docs feel outdated

Run `/health` to check for stale reference docs, or inspect `ops/reference-doc-changes.yaml` directly. Use `/write-architecture-doc` to regenerate a specific reference doc from current source code. The hook tracks which subsystem each source file belongs to, so the regenerated doc reflects the current implementation rather than the state at the time the doc was last written.

See `manual/workflows.md` for more patterns: code review knowledge extraction, architecture decision documentation, and production debugging debriefs.

---

## Diagnostic Scripts

Six shell scripts in `ops/queries/` answer common maintenance questions. Run them from the repo root.

| Script | What it finds |
|--------|--------------|
| `orphan-entries.sh` | Entries with no incoming wiki-links — isolated nodes in the graph |
| `stale-decisions.sh` | `status: current` entries not modified in 90+ days |
| `stale-references.sh` | Reference docs with ≥5 accumulated source code changes |
| `subsystem-coverage.sh` | Entry counts by subsystem and type |
| `todo-references.sh` | Reference docs with remaining TODO sections |
| `superseded-chains.sh` | Decision supersession chains |

```bash
bash docs/knowledge/ops/queries/orphan-entries.sh
bash docs/knowledge/ops/queries/stale-decisions.sh
bash docs/knowledge/ops/queries/stale-references.sh
```

---

## Configuration

The system configuration lives in a few places:

- **`ops/config.yaml`** — System configuration: vocabulary dimensions, entry schema, subsystem mapping, and hook declarations. This is the source of truth for how the system is structured.
- **`ops/derivation.md`** — The rationale for each configuration choice. Read this before changing `config.yaml` to understand why the current structure was chosen.
- **`.claude/rules/knowledge-system.md`** — Entry design rules and methodology, loaded into every Claude Code session. This is what the agent reads to know how to create and connect entries.
- **`.claude/settings.json`** — Hook registration: maps Claude Code events (`SessionStart`, `PostToolUse`, `Stop`) to the hook scripts.

See `manual/configuration.md` for the full configuration reference, including all schema fields and vocabulary terms.

---

## Further Reading

- **`manual/getting-started.md`** — First 10 minutes walkthrough: create your first entry, run your first pipeline
- **`manual/skills.md`** — Complete slash command reference with arguments and examples
- **`manual/workflows.md`** — Detailed workflow patterns for common scenarios
- **`manual/configuration.md`** — Configuration reference for `config.yaml` and schema fields
- **`manual/meta-skills.md`** — Health checks, stats, and graph analysis in depth
- **`manual/troubleshooting.md`** — Common problems and solutions
- **`ops/methodology/`** — Full methodology specification covering pipeline mechanics, task management, maintenance, and the self-extension loop
