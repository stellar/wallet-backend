---
context: IsAccountFeeBumpEligible queries the accounts table; accounts not in the ingested set are ineligible regardless of their on-chain state
type: insight
created: 2026-02-24
---

The `FeeBump` mutation first calls `IsAccountFeeBumpEligible`, which checks the `accounts` table (populated by the ingestion pipeline). Only accounts that appear in the ingested dataset are eligible. This prevents the distribution account from being used as a fee sponsor for arbitrary Stellar accounts that haven't been registered with this wallet-backend instance. The boundary is intentional: fee-bump is a service for managed accounts, not a public fee proxy.

Since [[the distribution account and channel accounts are two independent signing identities with different providers]], the eligibility check guards the distribution account specifically — channel accounts are not used for fee-bump sponsorship. The ingestion dependency means fee-bump is unavailable for accounts until they have at least one confirmed on-chain transaction indexed by the pipeline.

Areas: [[entries/signing]] | [[entries/graphql-api]]
