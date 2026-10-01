# Support

For anyone running or building on wallet-backend who hit a problem or has a question. After reading you know where to ask and what to include.

## Where to go

| Need | Where |
| --- | --- |
| Bug report or feature request | GitHub Issues, using the issue templates |
| Question | The [Stellar Developer Discord](https://discord.gg/stellardev) |
| Security issue | Report it privately as described in the [Stellar security policy](https://github.com/stellar/.github/blob/master/SECURITY.md). Do not open a public issue. |

## What to include in a report

- Output of `wallet-backend version`.
- Network: testnet, pubnet, or other.
- stellar-rpc version.
- `LEDGER_BACKEND_TYPE`: `rpc` or `datastore`.
- Relevant log lines, captured with `LOG_LEVEL=DEBUG`.
- The GraphQL query and variables, if the problem is in the API.

Remove secret keys, seed phrases, API keys, and JWTs before you post. Public keys and transaction hashes are fine.
