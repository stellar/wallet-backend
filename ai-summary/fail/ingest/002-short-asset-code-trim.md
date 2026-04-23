# H002: Short trustline asset codes suppress inherited authorization state on trustline creation

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: wallet-data integrity
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

When a new trustline is created, ingest should derive the correct inherited authorization/clawback flags regardless of whether the asset code is shorter than the fixed XDR width for AlphaNum4/AlphaNum12 assets.

## Mechanism

`getTrustlineFlagsFromChanges` compares the asset code extracted from effect details against the code reconstructed from the trustline entry. If the trustline-side code retained its NUL padding, short asset codes could fail to match and the inherited balance-authorization state change would be skipped.

## Trigger

1. Create a trustline for a short asset code (for example, `USD`).
2. Let ingest generate the synthetic balance-authorization state change for trustline creation.
3. Check whether the trustline lookup fails because the trustline-side code still contains fixed-width padding.

## Target Code

- `internal/indexer/processors/effects.go:getTrustlineFlagsFromChanges:520-576` — trustline lookup by `(trustor, assetCode, assetIssuer)`

## Evidence

The code reconstructs `entryAssetCode` from fixed-width XDR asset-code arrays before matching it against the effect detail values. That initially looks vulnerable to padded-string mismatches for short codes.

## Anti-Evidence

Right before the comparison, the code strips NUL bytes from `entryAssetCode` (`for i, c := range entryAssetCode { if c == 0 { entryAssetCode = entryAssetCode[:i]; break } }`). The normalized value is then compared against `assetCode`, preventing the mismatch.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The suspected short-code mismatch is already handled by explicit NUL-byte trimming before the asset-code equality check. Short AlphaNum4/12 assets should therefore still match correctly.

### Lesson Learned

In this codebase, several XDR fixed-width string paths already normalize padding before comparison. Confirm the normalization step before filing asset-code mismatch hypotheses.
