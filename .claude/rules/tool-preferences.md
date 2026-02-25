# Tool Preferences

Prefer specialized tools over built-in defaults:

| Task | Use | Instead of |
|------|-----|------------|
| Find symbols, classes, methods | Serena `find_symbol`, `get_symbols_overview` | Grep/Glob |
| Trace call sites and usages | Serena `find_referencing_symbols` | Grep |
| Code edits (symbol-level) | Serena `replace_symbol_body`, `insert_after_symbol` | Edit |
| Regex search across codebase | Serena `search_for_pattern` | Grep |
| Library/framework docs & examples | Context7 `resolve-library-id` → `query-docs` | WebSearch/WebFetch |
| PostgreSQL/TimescaleDB docs & best practices | pg-aiguide `search_docs` | WebSearch/WebFetch |
| Query optimization, connection issues, PG best practices | `postgres` skill via Skill tool | ad-hoc troubleshooting |
| DB schema design, hypertables, migrations, search | `pg:*` skills via Skill tool | ad-hoc implementation |
| Non-code files (YAML, MD, Docker) | Built-in Grep/Glob | — |
| General web content, blog posts | WebSearch/WebFetch | — |

## Notes
- Context7: Automatically use for code generation, setup/configuration steps, or library/API docs — without the user having to ask
- Context7: Always call `resolve-library-id` first, then `query-docs`
- Context7: Only fall back to WebSearch if Context7 returns no results
- Serena: Only fall back to Grep/Glob for non-code files
- pg-aiguide: Automatically use `search_docs` for any PostgreSQL or TimescaleDB question — set `source` to "postgres" or "tiger" as appropriate
- `postgres` skill: Invoke for general PostgreSQL work — query optimization, connection troubleshooting, performance improvement
- `pg:*` skills: Invoke the matching specific skill before designing tables, creating hypertables, writing migrations, or setting up search
