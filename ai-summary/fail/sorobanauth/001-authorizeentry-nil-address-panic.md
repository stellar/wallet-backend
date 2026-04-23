# H001: AuthorizeEntry Panics On Address Credentials With Nil Address

**Date**: 2026-04-21
**Subsystem**: sorobanauth
**Severity**: Medium
**Impact**: availability
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

`AuthSigner.AuthorizeEntry` should return a descriptive error when it is asked to sign a `SorobanCredentialsTypeSorobanCredentialsAddress` entry whose `Credentials.Address` pointer is nil. Malformed authorization entries should not be able to crash a caller process.

## Mechanism

The function only checks `auth.Credentials.Type` and then immediately dereferences `auth.Credentials.Address.Address` while rebuilding the entry. A caller that feeds an address-typed entry with a nil `Address` pointer would hit a nil-pointer panic instead of receiving a typed error.

## Trigger

Call `(*AuthSigner).AuthorizeEntry` with:
- `auth.Credentials.Type = xdr.SorobanCredentialsTypeSorobanCredentialsAddress`
- `auth.Credentials.Address = nil`

## Target Code

- `pkg/sorobanauth/sorobanauth.go:33-50` — dereferences `auth.Credentials.Address.Address` without a nil guard.

## Evidence

Unlike `CheckForForbiddenSigners`, which explicitly checks `auth.Credentials.Address == nil`, `AuthorizeEntry` does not validate the pointer before dereferencing it.

## Anti-Evidence

The only in-repo callers are integration-test helpers that sign RPC simulation results, and there are no production imports of `pkg/sorobanauth` on `main`. I did not find a live server path that feeds attacker-controlled auth entries into this function.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Failed At**: hypothesis
**Novelty**: PASS — not previously investigated

### Why It Failed

The panic is real at the package boundary, but I could not connect it to a production wallet-backend request path on `main`; the code is only imported by integration infrastructure in-tree.

### Lesson Learned

For this subsystem, exported-library robustness bugs are not enough on their own. They need a concrete production caller on `main` or an explicit server path that consumes attacker-controlled `SorobanAuthorizationEntry` values.
