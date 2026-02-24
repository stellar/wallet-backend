---
context: Clients who omit pagination args are scored as 10 items; this may undercount actual query cost for large datasets
type: insight
created: 2026-02-24
---

The GraphQL complexity calculation assigns a cost of `first` or `last` to paginated fields when those args are present. When neither is provided, the complexity scorer defaults to 10 rather than 0 or infinity. This means unbounded queries are not blocked by the complexity limit (they get a finite score of 10) but are also not accurately scored — a query that returns 10,000 rows incurs the same complexity budget as one that returns 10. Clients can exploit this by omitting pagination args entirely. The default-to-10 behavior is a deliberate choice to avoid blocking all unpaginated queries, but it is an approximation.
