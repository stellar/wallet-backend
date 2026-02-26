---
context: Four balance types (native, issued asset, liquidity pool, claimable balance) share one interface; avoids requiring four separate queries per account
type: decision
created: 2026-02-24
status: current
subsystem: graphql
areas: [graphql, data-layer]
---

The `Balance` GraphQL interface is implemented by `NativeBalance`, `IssuedAssetBalance`, `LiquidityPoolBalance`, and `ClaimableBalance`. The `balancesByAccountAddress` query returns `[Balance!]!`, allowing the resolver to mix all four types in one response. Without an interface, clients would need four separate queries to reconstruct an account's complete balance picture. The tradeoff is that clients must use inline fragments (`... on NativeBalance { ... }`) to access type-specific fields.
