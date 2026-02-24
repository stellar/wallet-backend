---
context: Protocol upgrade changed topic layout for set_authorized events; processor checks meta version and applies correct offset rather than parsing both formats
type: gotcha
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# SACEventsProcessor adapts to tx meta version differences for set_authorized event topic layout

`SACEventsProcessor` contains version-specific logic for parsing `set_authorized` event topics from SAC diagnostic events. The topic layout for `set_authorized` events changed between transaction meta versions — an older format uses a different number of topic entries and a different offset for the account/trustline fields.

The processor inspects the transaction meta version field and selects the correct parsing path:
- **Old format**: Fewer topics, fields at lower indices
- **New format**: Additional topic entry prepended, shifting all fields by one

This branching exists because Stellar's protocol upgrades can change the ABI-level layout of contract events without bumping the XDR type version. Ledgers processed during the transition period (or historical backfill reaching that era) will contain both formats. A single parsing strategy would misread one or the other.

This is a maintenance risk: future protocol changes to SAC event layouts would require adding another version branch here.

---

Relevant Notes:
- [[SACEventsProcessor handles authorization differently for classic accounts vs contract accounts because their authorization storage differs]] — another complexity in the same processor
- [[SACEventsProcessor silently continues on extraction errors rather than failing the entire transaction]] — how parsing failures are handled

Areas:
- [[entries/ingestion]]
