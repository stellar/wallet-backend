---
context: EffectsProcessor uses SDK effect stream (semantic interpretation); TrustlinesProcessor uses raw ledger entry diffs (storage view); both can see the same trustline state change
type: tension
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# effects-based processing vs ledger-change-based processing produces overlapping but different views of the same data

The indexer runs both `EffectsProcessor` (effect-stream-based) and `TrustlinesProcessor` (ledger-change-based) in the same pipeline, and both can observe the same underlying account state changes.

**Effect-based view** (`EffectsProcessor`): The Stellar SDK interprets XDR operation results and emits typed effects (e.g., `TrustlineCreated`, `TrustlineRemoved`, `TrustlineAuthorized`). These effects are semantic — they represent *what happened* in business terms. However, effects can be lossy: some ledger changes do not generate effects, and the effect type vocabulary is fixed to what the SDK implementation recognizes.

**Ledger-change-based view** (`TrustlinesProcessor`): Reads raw Pre/Post entries directly from the XDR ledger close metadata. This is complete and authoritative — every state transition produces a ledger change. But it requires application-level interpretation of flag bit changes to understand *what kind* of change occurred.

The overlap creates a question that is not fully resolved: which source should be authoritative for trustline authorization changes? `EffectsProcessor` produces `BALANCE_AUTHORIZATION` state changes from the effect stream, and `SACEventsProcessor` produces overlapping records from SAC events. The current design stores both, leaving deduplication to downstream consumers.

---

Relevant Notes:
- [[EffectsProcessor is the most complex processor handling seven distinct categories of account state changes]] — effects-based processor
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — ledger-change-based processor
- [[SACEventsProcessor and EffectsProcessor both generate BALANCE_AUTHORIZATION state changes for trustline flag changes through different paths]] — the concrete overlap instance

Areas:
- [[entries/ingestion]]
