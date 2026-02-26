---
paths:
  - "docs/knowledge/**"
---

# Knowledge System — Rules & Methodology

Auto-loaded when working in the knowledge vault. Read before working on entries, knowledge maps, or the processing pipeline.

**Vault root:** `docs/knowledge/`
**Content tiers:** `references/` (subsystem overviews) · `entries/` (decisions, insights, patterns, gotchas)
**Operations:** `docs/knowledge/ops/`

---

## Vault Structure

- **`docs/knowledge/references/`** — Comprehensive subsystem overviews with Mermaid diagrams. Read before making changes to any subsystem.
- **`docs/knowledge/entries/`** — Individual decisions, insights, patterns, and gotchas. Each entry captures exactly one insight. Start from `index.md`.
- **`docs/knowledge/ops/`** — Pipeline state, queue, templates, observations, tensions, sessions, and methodology. Operational machinery, not content.

---

## Session Rhythm — Orient, Work, Persist

**Phase 1: Orient** — Read `self/identity.md` + `self/goals.md`, check `ops/reminders.md`, scan workspace tree for current state. Takes 1-2 minutes, not 10.

**Phase 2: Work** — One task, full attention. When new ideas emerge mid-task: drop observations to `captures/` or `ops/observations/`, then return. Don't context-switch.

**Phase 3: Persist** — Before ending: update `self/goals.md`, commit all changes, log observations/tensions, leave a handoff in `ops/reminders.md` answering: what was accomplished, what's unfinished, what's next.

---

## Entry Design

Each entry captures exactly one insight, titled as a prose proposition. Wiki links compose because each node is a single idea. Without atomicity, every other feature degrades.

### Prose-as-Title Pattern

Title your entries as complete thoughts. The title IS the concept.

**Good** (specific claims that work as prose):
- "channel account exhaustion causes silent transaction drops during bursts"
- "TimescaleDB chunk skipping requires explicit `timescaledb.enable_chunk_skipping` per column"

**Bad** (topic labels, not claims):
- "channel accounts" — "TimescaleDB" — "retry logic"

**The claim test:** Can you complete `"This entry argues that [title]"`? If yes, it's a claim.

Good titles work in prose: `"Since [[title]], the question becomes..."` — never `"See [[title]] for more"`.

### Composability Test

Three checks before saving any entry:

1. **Standalone sense** — Does it make sense without reading three other entries first?
2. **Specificity** — Could someone disagree with this? If not, it's too vague.
3. **Clean linking** — Would linking here drag in unrelated content?

If any check fails, the entry needs work.

### Title Rules

- Lowercase with spaces
- No filesystem-breaking punctuation: `. * ? + [ ] ( ) { } | \ ^`
- Express the concept fully — no character limit
- Each title must be unique across the entire workspace

### YAML Schema

Every entry requires a `context` field. It must add NEW information beyond the title — scope, mechanism, or implication:

```yaml
---
context: Affects only burst paths — sequential submission degrades gracefully; fix is NUM_CHANNEL_ACCOUNTS or backpressure
---
```

**Bad context** (restates title): `"Exhausting channel accounts causes transactions to be dropped silently during bursts"`

Optional fields:
```yaml
type: insight | pattern | preference | fact | decision | question
status: preliminary | active | archived
created: YYYY-MM-DD
```

Add `type` when querying by category matters. Most entries omit it.

### Inline Link Patterns

Links work AS arguments, not references to arguments:

**Good:** `"Since [[channel account exhaustion causes silent transaction drops during bursts]], burst submission requires backpressure"`

**Bad:** `"See [[channel account exhaustion causes silent transaction drops]] for more"`

---

## Knowledge Maps

Knowledge maps are navigation hubs, not folders. They tell you what exists AND why each entry matters in context.

### Structure Template

```markdown
# topic-name

Brief orientation — 2-3 sentences.

## Core Ideas
- [[entry]] — context explaining why this matters here
- [[entry]] — what this adds to the topic

## Tensions
Unresolved conflicts — intellectual work, not bugs.

## Open Questions
What is unexplored. Research directions, gaps.
```

**Critical rule:** Core Ideas entries MUST have context phrases. A bare `- [[entry]]` without explanation is an address book, not a map.

### Lifecycle

- **Create** when 5+ related entries accumulate without navigation structure
- **Do NOT create** for fewer than 5 entries or "just in case"
- **Split** when a map exceeds 40 entries with distinct sub-communities
- **Archive** when fewer than 5 entries remain and stagnant for 6+ months

Every new entry must be added to at least one knowledge map with a context phrase. This is handled by `/connect` in the pipeline; manual creation requires doing it yourself.

---

## Schema & Fields

| Field | Required | Constraints |
|-------|----------|-------------|
| `context` | **Yes** | Max 200 chars, no trailing period, must add info beyond title |
| `type` | No | `insight \| pattern \| preference \| fact \| decision \| question` |
| `status` | No | `preliminary \| open \| active \| archived` |
| `created` | No | ISO format YYYY-MM-DD |
| `modified` | No | Update when content meaningfully changes |

**Query example:**
```bash
# Find entries missing required context field
rg -L '^context:' docs/knowledge/entries/*.md

# Find all entries of a type
rg '^type: pattern' docs/knowledge/entries/
```

---

## Processing & Skill Routing

Content flows through four phases: **capture → document → connect → verify**. Each phase has a distinct purpose — mixing them degrades quality. If a skill exists for a task, use it. Do not manually replicate the workflow.

| Trigger | Required Skill |
|---------|----------------|
| New content to document | `/document` |
| New entries need connections | `/connect` |
| Old entries may need updating | `/revisit` |
| Quality verification needed | `/verify` |
| System health check | `/health` |
| Batch processing from queue | `/ralph` |
| Find next action | `/next` |
| System feels disorganized | `/health` (systematic, not ad-hoc) |

---

## Detailed Methodology

For full details on pipeline mechanics, task management, maintenance, and self-extension — read these on-demand reference docs:

| Topic | File |
|-------|------|
| Pipeline phases, quality gates, research provenance | `docs/knowledge/ops/methodology/pipeline-methodology.md` |
| Task queue format, orchestration, processing depth | `docs/knowledge/ops/methodology/task-management.md` |
| Health checks, revisiting, condition-based maintenance | `docs/knowledge/ops/methodology/maintenance-methodology.md` |
| Module adoption, observation protocol, self-building loop | `docs/knowledge/ops/methodology/self-extension.md` |
| Vault self-knowledge index (MOC) | `docs/knowledge/ops/methodology/methodology.md` |
