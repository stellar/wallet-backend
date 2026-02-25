---
name: next
description: Surface the most valuable next action by combining task stack, queue state, inbox pressure, health, and goals. Recommends one specific action with rationale. Triggers on "/next", "what should I do", "what's next".
version: "1.0"
generated_from: "arscontexta-v1.6"
user-invocable: true
context: fork
model: sonnet
allowed-tools: Read, Grep, Glob, Bash
---

## Runtime Configuration (Step 0 — before any processing)

Read these files to configure domain-specific behavior:

1. **`docs/knowledge/ops/derivation.md`** — vocabulary mapping, domain context
   - Use `entries` for the entries folder name
   - Use `captures/` for the captures folder name
   - Use `entry` for the entry type name in output
   - Use `knowledge map` for knowledge map references
   - Use `/document` for process/extract command
   - Use `/connect` for connection-finding command
   - Use `/revisit` for backward-pass command
   - Use `retrospect` for retrospect command name

2. **`ops/config.yaml`** — thresholds, processing preferences
   - `self_evolution.observation_threshold` (default: 10)
   - `self_evolution.tension_threshold` (default: 5)

If these files don't exist, use universal defaults and generic command names.

## EXECUTE NOW

**INVARIANT: /next recommends, it does not execute.** Present one recommendation with rationale. The user decides what to do. This prevents cognitive outsourcing where the system makes all work decisions and the user becomes a rubber stamp.

**Execute these steps IN ORDER:**

---

### Step 1: Read Vocabulary

Read `docs/knowledge/ops/derivation.md` (or fall back to `ops/derivation.md`) for domain vocabulary mapping. All output must use domain-native terms. If neither file exists, use universal terms (entries, captures, knowledge map, etc).

---

### Step 2: Reconcile Maintenance Queue

Before collecting state, evaluate all maintenance conditions and reconcile the queue. This ensures maintenance tasks are current before the recommendation engine runs.

**Read queue file** (`ops/queue/queue.json` or `ops/queue.yaml`). If `schema_version` < 3, migrate:
- Add `maintenance_conditions` section with default thresholds
- Add `priority` field to existing tasks (default: "pipeline")
- Set `schema_version: 3`

**For each condition in maintenance_conditions:**

1. **Evaluate the condition:**

| Condition | Evaluation Method |
|-----------|------------------|
| orphan_notes | For each entry in entries/, count incoming [[links]]. Zero = orphan. |
| dangling_links | Extract all [[links]], verify targets exist as files. Missing = dangling. |
| inbox_pressure | Count *.md in captures/. |
| observation_accumulation | Count status: pending in ops/observations/. |
| tension_accumulation | Count status: pending or open in ops/tensions/. |
| pipeline_stalled | Queue tasks with status: pending unchanged across sessions. |
| unprocessed_sessions | Count files in docs/knowledge/ops/sessions/ without mined: true. |
| moc_oversize | For each knowledge map, count linked entries. |
| stale_notes | Entries not modified in 30+ days with < 2 links. |
| low_link_density | Average link count across all entries. |
| methodology_drift | Compare config.yaml modification time vs newest ops/methodology/ entry modification time. If config is newer, methodology may be stale. |
| stale_references | Read ops/reference-doc-changes.yaml. For each doc with count >= 5, flag as stale. Threshold: 5 changes. Recommendation format: `/write-architecture-doc references/{doc_file}` with rationale citing count and changed files list. Auto-close when count drops below threshold after /write-architecture-doc resets it. |

2. **If condition exceeds threshold AND no pending task with this condition_key exists:**

Create maintenance task:
```bash
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
MAINT_MAX=$(jq '[.tasks[] | select(.id | startswith("maint-")) | .id | ltrimstr("maint-") | tonumber] | max // 0' ops/queue/queue.json)
NEXT_MAINT=$((MAINT_MAX + 1))

jq --arg id "maint-$(printf '%03d' $NEXT_MAINT)" \
   --arg priority "{priority}" \
   --arg key "{condition_key}" \
   --arg target "{description}" \
   --arg action "{recommended command}" \
   --arg ts "$TIMESTAMP" \
   '.tasks += [{"id": $id, "type": "maintenance", "priority": $priority, "status": "pending", "condition_key": $key, "target": $target, "action": $action, "auto_generated": true, "created": $ts}]' \
   ops/queue/queue.json > tmp.json && mv tmp.json ops/queue/queue.json
```

3. **If condition is satisfied AND a pending task with this condition_key exists:**

Auto-close it:
```bash
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
jq --arg key "{condition_key}" --arg ts "$TIMESTAMP" \
   '(.tasks[] | select(.condition_key == $key and .status == "pending")).status = "done" |
    (.tasks[] | select(.condition_key == $key and .status == "pending")).completed = $ts' \
    ops/queue/queue.json > tmp.json && mv tmp.json ops/queue/queue.json
```

4. **If condition fires AND a pending task already exists:**

Update the target description (specifics may have changed):
```bash
jq --arg key "{condition_key}" --arg target "{new description}" \
   '(.tasks[] | select(.condition_key == $key and .status == "pending")).target = $target' \
   ops/queue/queue.json > tmp.json && mv tmp.json ops/queue/queue.json
```

---

### Step 3: Collect Vault State

Gather all signals. Run independent checks in parallel where possible. Record each signal even if the check returns zero — absence of signal is itself informative.

| Signal | How to Check | What to Record |
|--------|--------------|----------------|
| **Task stack** | Read `ops/tasks.md` — current priorities and open items | Top items, open count, any deadlines |
| **Queue state** | Read `ops/queue.yaml` or `ops/queue/queue.json` — pending pipeline tasks | Total pending, by phase (create, connect, revisit, verify), blocked phases |
| **Captures pressure** | Count `*.md` files in captures/, find oldest by mtime | Count per subdirectory, age of oldest item in days |
| **Entry count** | Count `*.md` in entries/ | Total entries for context |
| **Orphan entries** | For each entry, grep for `[[filename]]` across all files — zero hits = orphan | Count, first 5 names |
| **Dangling links** | Extract all `[[links]]` from entries/, verify each target file exists | Count, first 5 targets |
| **Stale entries** | Entries not modified recently AND with low link density (< 2 links) | Count |
| **Goals** | Read `self/goals.md` or `ops/goals.md` — current priorities, active threads | Priority list, active research directions |
| **Observations** | Count files with `status: pending` in `ops/observations/` | Count |
| **Tensions** | Count files with `status: pending` or `status: open` in `ops/tensions/` | Count |
| **Methodology** | Check `ops/methodology/` for recent captures (files modified in last 7 days) | Count of recent, total count |
| **Health** | Read most recent report in `ops/health/` — note timestamp and issues | Last run date, issue count, any critical issues |
| **Sessions** | Check `docs/knowledge/ops/sessions/` for files without `mined: true` in frontmatter | Count of unmined sessions |
| **Recent /next** | Read `ops/next-log.md` (if exists) — last 3 recommendations | Previous suggestions to avoid repetition |

**Adaptation rules:**
- Directory names adapt to domain vocabulary (e.g., captures/ instead of hardcoded "inbox")
- Skip checks silently for directories that do not exist — do not report "docs/knowledge/ops/sessions/ not found"
- A missing directory means that feature is not active, which is valid state

**Signal collection commands:**

```bash
# Captures pressure (adapt path to vocabulary)
INBOX_COUNT=$(find captures/ -name "*.md" -maxdepth 2 2>/dev/null | wc -l | tr -d ' ')
OLDEST_INBOX=$(find captures/ -name "*.md" -maxdepth 2 -exec stat -f "%m %N" {} \; 2>/dev/null | sort -n | head -1)

# Entry count
NOTE_COUNT=$(ls -1 entries/*.md 2>/dev/null | wc -l | tr -d ' ')

# Pending observations
OBS_COUNT=$(grep -rl '^status: pending' ops/observations/ 2>/dev/null | wc -l | tr -d ' ')

# Pending tensions
TENSION_COUNT=$(grep -rl '^status: pending\|^status: open' ops/tensions/ 2>/dev/null | wc -l | tr -d ' ')

# Unmined sessions
SESSION_COUNT=$(grep -rL '^mined: true' docs/knowledge/ops/sessions/*.md 2>/dev/null | wc -l | tr -d ' ')
```

---

### Step 4: Classify by Consequence Speed

Evaluate every signal against consequence speed — how fast does inaction degrade the system?

| Speed | Signals | Threshold | Why This Priority |
|-------|---------|-----------|-------------------|
| **Session** | Captures > 5 items, orphan entries (any), dangling links (any), 10+ pending observations, 5+ pending tensions, unprocessed sessions > 3 | Immediate — these degrade work quality right now | Orphans are invisible to traversal. Dangling links confuse navigation. Captures pressure means lost ideas. Observation/tension thresholds mean the system is accumulating unprocessed friction. |
| **Multi-session** | Pipeline queue backlog > 10, research gaps identified in goals, stale entries > 10, captures items aging > 7 days, methodology captures > 5 in same category, stale_references (any doc with count >= 5) | Soon — these compound over days | Unfinished pipeline batches block downstream connections. Stale entries represent decaying knowledge. Stale reference docs cause Claude Code agents to load inaccurate architecture context — errors compound as divergence grows. |
| **Slow** | Health check not run in 14+ days, knowledge map oversized (>40 entries), link density below 2.0 average, low entry count relative to time | Background — annoying but not blocking | These are maintenance tasks. Important for long-term health but not urgent. |

**Threshold rule:** 10+ pending observations OR 5+ pending tensions is ALWAYS session-priority. Recommend /retrospect in this case.

**Signal interaction rules:**
- Task stack items ALWAYS override automated recommendations (user-set priorities beat system-detected urgency)
- Multiple session-priority signals: pick the one with highest impact (most items affected)
- If captures pressure AND queue backlog: recommend processing captures first (pipeline needs input before it can process)

---

### Step 5: Generate Recommendation

Select the SINGLE most valuable action. The recommendation must be specific enough to execute immediately — a concrete command invocation, not a vague suggestion.

**Priority cascade:**

#### 1. Task Stack First

If `ops/tasks.md` has open items, recommend from the task stack. User-set priorities override all automated recommendations because:
- The user has context the system does not
- Ignoring explicit priorities erodes trust
- Task stack items represent deliberate decisions, not automated detection

Format: Recommend the specific task with context about why it was in the stack.

#### 1.5. Session-Priority Maintenance Tasks

Read queue for maintenance tasks with `priority: "session"` and `status: "pending"`. These represent vault health conditions that degrade THIS session.

Pick the highest-impact one:
- orphan_notes: "{N} entries invisible to traversal"
- dangling_links: "{N} broken links confusing navigation"
- inbox_pressure: "{N} items aging in captures"

Recommend the `action` field from the queue entry.

#### 2. Session-Priority Signals

If no task stack items, pick the highest-impact session-priority signal:

| Signal | Recommendation | Rationale Template |
|--------|---------------|-------------------|
| Dangling links / orphans | /health or specific fix command | "You have [N] orphan entries invisible to traversal. Connecting them increases graph density and retrieval quality." |
| 10+ observations or 5+ tensions | /retrospect | "[N] pending observations have accumulated. Pattern detection requires processing this backlog to evolve the system." |
| Captures > 5 items | /document [specific file] | "Your captures has [N] items (oldest: [age]). [File X] has the highest connection potential based on [reason]." |
| Unprocessed sessions > 3 | /remember --mine-sessions | "[N] sessions have uncaptured friction patterns. Mining them prevents methodology regressions." |

**When recommending captures processing:** Choose the specific captures item that aligns best with current goals or has the most connection potential to existing entries. Recommend a concrete file, not "process some captures."

#### 3. Multi-Session Signals

If no session-priority items:

| Signal | Recommendation | Rationale Template |
|--------|---------------|-------------------|
| Queue backlog > 10 | /ralph [N] | "[N] pipeline tasks are pending. Your newest entries lack connections, which means they can't participate in synthesis." |
| Stale entries > 10 | /revisit [specific entry] | "[N] entries haven't been touched since [date]. [Entry X] has the most connections and would benefit most from updating." |
| Research gaps | /document [file aligned with goals] | "Your goals mention [topic] but your graph has few entries there. [Captures item] addresses this gap." |
| Methodology convergence | /retrospect | "[N] methodology captures in the [category] area suggest a pattern worth elevating." |
| Stale reference docs (count >= 5) | /write-architecture-doc references/{doc_file} | "{count} source file changes in {source_dirs} since last update. Changed files: {file_list}. Reference docs loaded by Claude Code agents become misleading when they diverge from the code — pick the doc with the highest change count first." |

**When recommending revisiting:** Choose the most-connected stale entry (highest link density + oldest modification). Revisiting high-connectivity entries has the highest ripple effect.

#### 4. Slow Signals

If nothing pressing:

| Signal | Recommendation | Rationale Template |
|--------|---------------|-------------------|
| No recent health check | /health | "Last health check was [date]. Running one now catches structural issues before they compound." |
| Knowledge map oversized | Restructuring suggestion | "[Knowledge map X] has [N] entries. Splitting into sub-knowledge-maps improves navigation and reduces cognitive load." |
| Low link density | /revisit on lowest-density entry | "Your graph has an average link density of [N]. Revisiting sparse entries increases traversal paths." |

#### 5. Everything Clean

If all signals are healthy:

```
next

  All signals healthy.
  Captures: 0 | Queue: 0 pending | Orphans: 0 | Dangling: 0

  No urgent work detected.

  Suggested: Explore a new direction from goals.md
  or revisit older entries to deepen the graph.
```

**Rationale is always mandatory.** Every recommendation must explain:
1. WHY this action over alternatives
2. What DEGRADES if this action is deferred
3. How it connects to goals (if applicable)

---

### Step 6: Deduplicate

Read `ops/next-log.md` (if it exists). Check the last 3 entries.

**Deduplication rules:**
- If the same recommendation appeared in the last 2 entries, select the next-best action instead
- This prevents the system from getting stuck recommending the same thing repeatedly when the user has chosen not to act on it
- If the same recommendation is genuinely the highest priority (e.g., captures pressure keeps growing), add an explicit note: "This was recommended previously. The signal has grown stronger since then ([before] → [now])."

---

### Step 7: Output

```
next

  State:
    Captures: [count] items (oldest: [age])
    Queue: [count] pending ([phase breakdown])
    Orphans: [count] | Dangling: [count]
    Observations: [count] | Tensions: [count]
    [any other decision-relevant signals]

  Recommended: [specific command/action]

  Rationale: [2-3 sentences — why this action,
  how it connects to goals, what degrades if deferred]

  After that: [second priority, if relevant]
  [optional: alignment with goals.md priority]
```

**Command specificity is mandatory.** Recommendations must be concrete invocations:

| Good | Bad |
|------|-----|
| `/document captures/article-on-spaced-repetition.md` | "process some captures items" |
| `/ralph 5` | "work on the queue" |
| `/retrospect` | "review your observations" |
| `/revisit [[entry title here]]` | "update some old entries" |

**State display rules:**
- Show only 2-4 decision-relevant signals — not all 14 checks
- Zero-count signals that are healthy can be omitted (don't show "Orphans: 0" unless contrasting with a problem)
- Non-zero signals at session or multi-session priority should always be shown

---

### Step 8: Log the Recommendation

Append to `ops/next-log.md` (create if missing):

```markdown
## YYYY-MM-DD HH:MM

**State:** Captures: [N] | Entries: [N] | Orphans: [N] | Dangling: [N] | Stale: [N] | Obs: [N] | Tensions: [N] | Queue: [N]
**Recommended:** [action]
**Rationale:** [one sentence]
**Priority:** session | multi-session | slow
```

**Why log?** The log serves three purposes:
1. Deduplication — prevents recommending the same action repeatedly
2. Evolution tracking — shows what signals have been persistent vs transient
3. /retrospect evidence — persistent recommendations that go unacted-on may reveal misalignment between what the system detects and what the user values

---

## Edge Cases

### Empty Vault (0-5 entries)

Recommend capturing or processing content. Maintenance is premature with < 5 entries — the graph does not have enough nodes for meaningful analysis.

```
next

  State:
    Entries: [N] — early stage vault

  Recommended: Capture or /document content
  Rationale: Your graph has [N] entries. At this stage, adding
  content matters more than maintaining structure. Health checks,
  revisiting, and retrospect become valuable after ~10 entries.
```

### Everything Clean

Say so explicitly. Recommend exploratory work aligned with goals, or reflective work on older entries:

```
  No urgent work detected. Consider:
  - Exploring a research direction from goals.md
  - Revisiting older entries to deepen connections
  - Reviewing and updating goals.md itself
```

### No Goals File

Recommend creating `self/goals.md` or `ops/goals.md` first. Without priorities, recommendations lack grounding and the system cannot distinguish between "important to the user" and "detected by automation."

```
  Recommended: Create ops/goals.md
  Rationale: Without goals, /next can only recommend based on
  automated detection. Goals let the system align recommendations
  with what actually matters to you.
```

### No docs/knowledge/ops/derivation.md

Use universal vocabulary. Do not fail — /next should always produce a recommendation regardless of configuration state.

### Queue Not Active

Skip queue checks silently. Not all vaults use the pipeline architecture — some rely on manual processing. Do not report "queue not found" as an issue.

### Multiple Session-Priority Signals

When several signals are at session priority simultaneously, pick the one that unblocks the most downstream work:
- Dangling links block graph traversal → fix first
- Observation threshold → retrospect prevents methodology drift
- Captures pressure → processing prevents idea loss

If genuinely equal priority, pick the one the user has not been recommended recently (check next-log.md).

### Stale /next Log

If `ops/next-log.md` has not been updated in 14+ days, the user may not be running /next regularly. Note this but do not make it a recommendation — /next is optional, not mandatory.

---

## Anti-Patterns

These are patterns that /next must avoid:

| Anti-Pattern | Why It Is Wrong | What to Do Instead |
|-------------|----------------|-------------------|
| Recommending everything | Overwhelms the user, defeats the purpose of "single most valuable action" | Pick ONE. Mention a second only as "after that" |
| Vague recommendations | "Process captures" gives no actionable starting point | Name the specific file, entry, or command |
| Ignoring task stack | User-set priorities exist for a reason | Always check ops/tasks.md first |
| Repeating the same rec | If the user did not act on it, recommending it again is nagging | Deduplicate via next-log.md |
| Recommending maintenance too early | A 5-entry vault does not need health checks | Scale recommendations to vault maturity |
| Cognitive outsourcing | Making all decisions for the user | Recommend and explain — never execute |
