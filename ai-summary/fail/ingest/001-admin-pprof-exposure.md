# H001: Unauthenticated ingest pprof endpoints are exposed on every interface

**Date**: 2026-04-21
**Subsystem**: ingest
**Severity**: Medium
**Impact**: availability / confidentiality
**Hypothesis by**: gpt-5.4, high

## Expected Behavior

Profiling and tracing endpoints should not be reachable by arbitrary network clients. If ingest exposes pprof at all, it should either bind only to loopback/private admin infrastructure or enforce authentication before serving goroutine dumps, heap snapshots, CPU profiles, and execution traces.

## Mechanism

When `AdminPort > 0`, `startServers` binds `adminServer` to `:%d` and mounts the standard library pprof handlers directly, with no auth or network scoping. Any client that can reach that port can dump runtime state (`/debug/pprof/goroutine`, `/debug/pprof/heap`) or force expensive profiling work (`/debug/pprof/profile`, `/debug/pprof/trace`), creating both an information-disclosure surface and a request-driven DoS primitive against the ingest process.

## Trigger

1. Run ingest with `AdminPort` configured on a reachable interface.
2. Send unauthenticated requests such as:
   - `GET /debug/pprof/goroutine?debug=2`
   - `GET /debug/pprof/heap`
   - `GET /debug/pprof/profile?seconds=30`
3. Observe that profiling data is returned or CPU profiling work is started without any auth gate.

## Target Code

- `internal/ingest/ingest.go:startServers:300-317` — starts an admin HTTP server on `:%d`
- `internal/ingest/ingest.go:registerAdminHandlers:323-329` — mounts raw pprof handlers

## Evidence

The admin server is created with `Addr: fmt.Sprintf(":%d", cfg.AdminPort)` and `Handler: adminMux`, then `registerAdminHandlers(adminMux)` wires `/debug/pprof/*` directly. There is no auth middleware, IP restriction, or loopback-only bind in this path.

## Anti-Evidence

The surface is only enabled when `AdminPort > 0`, and it is split onto a separate port rather than the main ingest port. Operators who keep that port unreachable reduce exposure, but the code itself does not enforce that boundary.

---

## Review

**Verdict**: NOT_VIABLE
**Date**: 2026-04-21
**Reviewed by**: claude-opus-4-6, high
**Novelty**: PASS — not previously investigated
**Failed At**: reviewer

### Trace Summary

Traced the full admin server startup path in `internal/ingest/ingest.go`. Confirmed that `startServers` (lines 274-321) creates an admin HTTP server binding to all interfaces on `AdminPort` when configured, and `registerAdminHandlers` (lines 324-330) mounts the five standard `net/http/pprof` handlers (`Index`, `Cmdline`, `Profile`, `Symbol`, `Trace`) with no authentication middleware. The code exactly matches what the hypothesis describes.

### Code Paths Examined

- `internal/ingest/ingest.go:startServers:274-321` — confirmed admin server binds `0.0.0.0:AdminPort` with no auth
- `internal/ingest/ingest.go:registerAdminHandlers:324-330` — confirmed raw pprof handler mounting
- `internal/ingest/ingest.go:Configs:50-103` — confirmed `AdminPort` is opt-in (default zero)
- `internal/ingest/ingest.go:setupDeps:137-270` — confirmed admin server is only started via `startServers` call

### Why It Failed

The finding is real but does not meet the minimum **Medium** severity threshold required by the objective. The actual impact is Informational/Low:

1. **No crash/panic**: Go's `net/http/pprof` handlers do not crash or panic the process. They consume bounded resources during profiling but return normally.
2. **No OOM**: Heap dumps, goroutine dumps, and CPU profiles produce output proportional to existing process state, not attacker-controlled amplification. No realistic path to OOM the ingest process through these endpoints.
3. **Information disclosure is Low-severity**: Goroutine stacks and heap profiles reveal internal function names and memory layout — useful for reconnaissance but not directly exploitable against wallet-backend's security model (no secrets in heap, no auth tokens leaked via pprof).
4. **Operator-deployment concern**: The admin port is opt-in (`AdminPort > 0`), deliberately separated from the main ingest port, and follows standard Go service patterns. This is analogous to the objective's explicit OUT_OF_SCOPE: "Missing rate limits, missing WAF, missing TLS configuration — these are operator-deployment concerns, not wallet-backend code bugs."
5. **Cannot argue to Medium**: None of the Medium criteria (crash/panic from unauth input, Redis desync, migration CAS violation, JWT replay, Soroban sim crash, RPC-induced OOM) apply to unauthenticated pprof access.

### Lesson Learned

Unauthenticated pprof on a dedicated admin port is standard Go operational practice and is an operator-deployment concern, not a code-level security bug. Only flag pprof exposure if the endpoints are on the main API port (bypassing auth middleware) or if a specific pprof handler can be shown to crash/OOM the process under realistic conditions.
