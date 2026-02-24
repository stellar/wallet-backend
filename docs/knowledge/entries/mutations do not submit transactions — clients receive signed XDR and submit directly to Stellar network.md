---
context: wallet-backend never holds user private keys; the distribution account signs with its own key but the final transaction goes to Stellar RPC from the client
type: decision
created: 2026-02-24
---

The `BuildAndSignTransaction` and `FeeBump` mutations return a signed XDR envelope to the caller rather than submitting it to Stellar RPC. This is a deliberate security boundary: wallet-backend manages only its own channel account pool; user funds are controlled by client-held keys. Submission is the client's responsibility. The implication is that if a client crashes after receiving the XDR but before submitting, the transaction is lost — the channel account lock will auto-release after 30 seconds (see [[channel account time-bounded locks auto-release after 30 seconds if a client crashes after receiving the signed XDR]]). The two mutations form a sequential pair: [[createFeeBumpTransaction requires the inner transaction to already be signed before wrapping and returns ErrNoSignaturesProvided otherwise]] — the signed XDR from `BuildAndSignTransaction` is the prerequisite for a subsequent `FeeBump` call.

Areas: [[entries/signing]] | [[entries/graphql-api]]
