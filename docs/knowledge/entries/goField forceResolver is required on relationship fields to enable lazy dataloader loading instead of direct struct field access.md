---
context: Without this directive gqlgen resolves the field by reading the struct field directly, bypassing the dataloader and causing N+1 queries
type: pattern
created: 2026-02-24
---

gqlgen's default behavior for a field that matches a struct field name is to access the struct field directly — no resolver is called. The `@goField(forceResolver: true)` directive overrides this, forcing gqlgen to emit a resolver method call instead. On relationship fields (e.g., `operations` on `Transaction`), this is required to route the resolution through the dataloader, which batches N queries into one. Omitting the directive silently falls through to direct struct access, producing N+1 database queries. The same directive is also required on enum fields — see [[goField forceResolver on enum fields enables type conversion from DB integer representation to GraphQL enum in the resolver]].
