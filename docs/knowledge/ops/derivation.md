# Derivation State — Ars Contexta Knowledge System

Generated: 2026-02-23
System: wallet-backend
Plugin version: arscontexta 0.8.0

## Configuration Summary

| Dimension | Position | Signal |
|-----------|----------|--------|
| Granularity | Moderate | Per-decision/per-insight entries, not atomic claims |
| Organization | Flat | 1-2 repos, knowledge crosses subsystem boundaries |
| Linking | Explicit + implicit | Dual audience; implicit deferred until qmd installed |
| Processing | Heavy | Both WHY and HOW; dual audience needs thorough extraction |
| Navigation | 3-tier | Hub → subsystem maps → entries/references |
| Maintenance | Condition-based | Engineering context changes frequently |
| Schema | Moderate-Dense | AI agents need structured metadata to navigate |
| Automation | Full | Claude Code platform, agentic coding use case |

## Personality

Neutral-helpful. Professional, task-focused.

## Coherence

All hard and soft constraint checks pass.

## Use Case

Wallet-backend engineering knowledge system. Dual audience:
1. **Human recall** — architecture decisions, debugging insights, patterns
2. **AI agent context** — structured context for AI agents working on this codebase

## Vault Root

`docs/knowledge/`

## Vault Marker

`docs/knowledge/.arscontexta`

## Content Tiers

- **references/** — comprehensive subsystem overviews with Mermaid diagrams (moved from docs/architecture/)
- **entries/** — individual decisions, insights, patterns, gotchas

## Vocabulary Mapping

| Universal | Wallet-Backend Engineering |
|-----------|---------------------------|
| notes/ | entries/ |
| inbox/ | captures/ |
| archive/ | archive/ |
| note | entry |
| MOC | knowledge map |
| reduce | document |
| reflect | connect |
| reweave | revisit |
| verify | verify |
| rethink | retrospect |
| description | context |
| topics | areas |

## Entry Types

`decision`, `insight`, `pattern`, `gotcha`, `reference`

## Extraction Categories

1. Architecture decisions — why TimescaleDB, why channel accounts, why schema-first GraphQL
2. Debugging insights — ingestion failures, transaction signing issues, query performance
3. Code patterns — indexer processor pattern, Options struct DI, dataloader pattern
4. Gotchas — Stellar RPC quirks, TimescaleDB chunk behavior, migration ordering
5. API/service knowledge — Stellar RPC behavior, GraphQL resolver patterns
6. Performance learnings — query optimization, connection pooling, hypertable tuning

## Subsystems

ingestion, graphql, data-layer, signing, services, auth

## Integration Points

- Knowledge system rules: `.claude/rules/knowledge-system.md`
- Skills: `.claude/skills/` (16 new skills)
- Hooks: `.claude/hooks/` (5 scripts)
- Hook config: `.claude/settings.json`

## Derivation Date

2026-02-23
