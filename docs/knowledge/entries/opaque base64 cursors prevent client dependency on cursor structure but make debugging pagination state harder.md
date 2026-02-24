---
context: Opacity is deliberate (prevents clients from constructing cursors) but adds indirection when inspecting raw API responses during development
type: question
created: 2026-02-24
---

All pagination cursors in the API are base64-encoded opaque strings. The opacity prevents clients from relying on cursor internals (which could change with schema evolution) but it has a real cost: debugging a pagination issue requires decoding the base64 string to inspect the position encoded inside. This tension is unresolved — the Relay spec recommends opaque cursors precisely for this reason, but developer tooling (e.g., browser devtools inspection) becomes harder. A potential mitigation is a debug header that logs decoded cursor values server-side without exposing them in the response.
