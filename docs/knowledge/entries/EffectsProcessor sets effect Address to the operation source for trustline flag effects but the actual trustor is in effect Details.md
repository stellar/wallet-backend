---
context: Stellar effect stream populates Address with the operation source account; for set_trust_line_flags the affected trustor is a different account found in Details.trustor
type: gotcha
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# EffectsProcessor sets effect Address to the operation source for trustline flag effects but the actual trustor is in effect Details

For `SetTrustLineFlagsOp` operations and related trustline authorization effects, the Stellar SDK's effect stream populates `effect.Address` with the **operation source account** (the account that submitted the operation) rather than the **trustor** (the account whose trustline is being modified).

The trustor — the account that actually holds the trustline — is stored in `effect.Details["trustor"]`.

This is a footgun for code that assumes `effect.Address` is always the affected party. `EffectsProcessor` must explicitly check the effect type and extract `Details["trustor"]` when building `StateChange` records for `BALANCE_AUTHORIZATION` category:

```go
if effect.Type == effects.TypeTrustlineAuthorized ||
   effect.Type == effects.TypeTrustlineDeauthorized {
    address = effect.Details["trustor"].(string)  // NOT effect.Address
}
```

Code that uses `effect.Address` without this check will attribute the authorization change to the wrong account, making the trustline change invisible to the trustor when querying `state_changes WHERE address = trustor_address`.

---

Relevant Notes:
- [[EffectsProcessor is the most complex processor handling seven distinct categories of account state changes]] — context for which effects require this workaround
- [[effects-based processing vs ledger-change-based processing produces overlapping but different views of the same data]] — why TrustlinesProcessor is an alternative that avoids this issue

Areas:
- [[entries/ingestion]]
