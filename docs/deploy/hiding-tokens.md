# Hiding tokens

For an operator who wants the API to stop returning a Soroban token contract. After reading it you can add a contract to the hidden list, remove it again, and check that `serve` loaded the list.

## Contents

- [What hiding does](#what-hiding-does)
- [When to hide a token](#when-to-hide-a-token)
- [Add or remove a token](#add-or-remove-a-token)
- [File format](#file-format)
- [Startup errors](#startup-errors)
- [Where in the code](#where-in-the-code)

## What hiding does

Hiding is a read filter in `serve`. Ingestion keeps processing the contract and still writes its current state and history.

| The API | For a hidden contract |
| --- | --- |
| Current state written by the entry's protocol (SEP-41 balances and allowances) | Not returned |
| State changes in the entry's protocol range | Not returned |
| Transactions and operations that touched an account only through the token | Still listed, with no state changes under them |

Because nothing is deleted, removing a contract from the list shows everything again, including what was written while it was hidden.

## When to hide a token

- It matches a protocol's interface, but its balances cannot be derived from its events the way the processor derives them. The Comet LP pool `CAS3FL6TLZKDGGSISDBWGGPXT3NRR4DYTZD7YOD3HMYO6LTJUVGRVEAM` is on the built-in list for this reason: it mints LP shares without emitting an event, so SEP-41 event arithmetic drives its balances negative.
- It is a malicious token you do not want shown.

## Add or remove a token

The binary ships with a built-in list. `HIDDEN_CONTRACTS_FILE` (or `--hidden-contracts-file`) points `serve` at your own file instead, and that file replaces the built-in list entirely. Copy the built-in entries you want to keep from `internal/serve/hidden_contracts.yaml`.

1. Write the list and make it readable by `serve`. With the image, mount it into the container.

   ```bash
   docker run --rm -v "$PWD/hidden_contracts.yaml:/config/hidden_contracts.yaml:ro" \
     -e HIDDEN_CONTRACTS_FILE=/config/hidden_contracts.yaml \
     -e DATABASE_URL=<DATABASE_URL> -e RPC_URL=<RPC_URL> -e NETWORK_PASSPHRASE=<NETWORK_PASSPHRASE> \
     public.ecr.aws/stellar/wallet-backend:<VERSION> serve
   ```

2. Restart every `serve` instance. The list is read at startup only.

3. Verify: the startup log lists every entry.

   ```text
   Hiding <N> contracts: [<CONTRACT_ID> (SEP41) ...]
   ```

To remove a token, delete its entry and restart `serve`. Nothing needs rebuilding.

## File format

```yaml
hidden_contracts:
  - contract_id: C...
    protocol: SEP41
    reason: why it is hidden, and what would let it be shown again
    added: 2026-10-07
```

| Field | Required | Notes |
| --- | --- | --- |
| `contract_id` | yes | A `C...` contract address |
| `protocol` | yes | The protocol whose rows are hidden. `SEP41` is the only value today |
| `reason` | yes | Not empty |
| `added` | no | Informational |

Use `hidden_contracts: []` to hide nothing.

An entry hides only the rows that one protocol's processor wrote for the contract. When a dedicated processor later takes the token over, its rows show while the old processor's rows stay hidden, with nothing to delete. A token tracked by two processors at once, for example during a backfill handover, gets one entry per protocol.

## Startup errors

`serve` refuses to start on any of these, so a typo cannot silently widen or empty the list.

| Problem | Error |
| --- | --- |
| File missing or unreadable | `reading hidden contracts file <PATH>: <cause>` |
| Bad YAML or an unknown field | `parsing hidden contracts: <cause>` |
| No `hidden_contracts` key | ``parsing hidden contracts: the hidden_contracts key is missing (use `hidden_contracts: []` for none)`` |
| A second YAML document | `parsing hidden contracts: more than one YAML document in the file` |
| Not a contract address | `hidden contract <N>: invalid contract_id "<VALUE>": <cause>` |
| Empty reason | `hidden contract <N> (<CONTRACT_ID>): reason is empty` |
| No protocol | `hidden contract <N> (<CONTRACT_ID>): protocol is required` |
| Unknown protocol | `hidden contract <N> (<CONTRACT_ID>): unknown protocol "<VALUE>"` |
| Same contract and protocol twice | `hidden contract <N> (<CONTRACT_ID>): listed more than once for <PROTOCOL>` |

## Where in the code

| Path | Role |
| --- | --- |
| `internal/serve/hidden_contracts.yaml` | The built-in list |
| `internal/serve/hidden_contracts.go` | Loading, validation, and wiring the filter into the read models |
| `internal/data/statechanges.go` | Skips hidden state-change ranges |
| `internal/data/sep41/balances.go`, `allowances.go` | Skip hidden SEP-41 contracts |
