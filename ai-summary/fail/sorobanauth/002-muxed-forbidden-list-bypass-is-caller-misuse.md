# H002: Muxed Forbidden Signer Aliases Can Miss G-Address Matches

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: High
**Impact**: integrity / forbidden-signer bypass
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

`CheckForForbiddenSigners` should canonicalize both the operation source account and the forbidden-signer blacklist so that `M...` and `G...` representations of the same underlying Ed25519 key are treated identically. A caller-supplied blacklist should not be bypassable by choosing a different StrKey representation for the same signer.

## Mechanism

The function canonicalizes `opSourceAccount` through `ResolveToGAddress`, but it compares address-credential signers directly against the raw `forbiddenSigners` slice. If a caller supplies a forbidden signer as an `M...` address while the auth entry stringifies to the underlying `G...` address, the `slices.Contains` test will miss the match.

## Trigger

1. Call `CheckForForbiddenSigners` with `forbiddenSigners = []string{"M...forbidden..."}`.
2. Pass an address-credential auth entry whose signer is the same underlying account but stringifies as `G...`.
3. Observe that the function does not flag the signer as forbidden.

## Target Code

- `pkg/sorobanauth/sorobanauth.go:135-167` — canonicalizes only `opSourceAccount`, not `forbiddenSigners`.
- `pkg/utils/utils.go:121-131` — provides the canonicalization helper used on the source-account side.

## Evidence

The source-account branch calls `ResolveToGAddress(opSourceAccount)`, while the address-credentials branch compares `auth.Credentials.Address.Address.String()` directly against the caller-supplied strings. No normalization step is applied to the blacklist itself.

## Anti-Evidence

There are no in-tree production callers of `CheckForForbiddenSigners`, and the exploit requires the caller to build the blacklist from attacker-influenced `M...` input rather than from fixed privileged-account configuration.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

This is a caller-misuse hazard, not a wallet-backend server exploit on `main`: no production code invokes the function, and the attacker would need influence over the blacklist input itself.

### Lesson Learned

For `CheckForForbiddenSigners`, reachability matters more than theoretical mismatch behavior. A viable finding here needs a concrete submission path in production code that actually depends on this helper and accepts attacker-shaped blacklist input.
