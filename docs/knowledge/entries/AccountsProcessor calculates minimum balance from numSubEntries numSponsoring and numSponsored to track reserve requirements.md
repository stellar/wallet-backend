---
context: Stellar reserve requirement is (2 + numSubEntries + numSponsoring - numSponsored) * baseReserve XLM; storing derived minBalance avoids recomputing in wallet queries
type: insight
status: active
subsystem: ingestion
areas: [ingestion, indexer]
created: 2026-02-24
---

# AccountsProcessor calculates minimum balance from numSubEntries numSponsoring and numSponsored to track reserve requirements

`AccountsProcessor` derives and stores a `MinBalance` field when processing each `AccountChange`. This field is not stored directly in Stellar ledger entries — it is computed from three raw fields present in the account entry:

```
minBalance = (2 + numSubEntries + numSponsoring - numSponsored) × baseReserve
```

- `numSubEntries`: number of trustlines, offers, data entries owned by the account
- `numSponsoring`: number of entries this account sponsors for others (increases reserve)
- `numSponsored`: number of entries sponsored by others for this account (decreases reserve)
- `baseReserve`: network-level constant (0.5 XLM as of current mainnet)

Storing `minBalance` directly avoids requiring wallet query code to know the reserve calculation formula. The `account_balances` hypertable row becomes self-contained: the spendable balance is simply `balance - minBalance` without any additional arithmetic or network parameter lookup.

The calculation uses the `Post` entry from the ledger change (not `Pre`), so `minBalance` reflects the reserve requirement AFTER the operation completes — the correct value for determining if the account can afford subsequent transactions.

---

Relevant Notes:
- [[TrustlinesProcessor and AccountsProcessor read absolute values from ledger change Post entries not deltas]] — explains why Post is used here
- [[the unified StateChange struct uses nullable fields and a category discriminator to store all state change types in a single database table]] — AccountChange is a sibling struct stored in a separate table

Areas:
- [[entries/ingestion]]
