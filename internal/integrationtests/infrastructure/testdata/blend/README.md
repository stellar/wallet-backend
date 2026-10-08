# Blend Auxiliary Contract WASM Files

This directory contains pre-compiled Soroban smart contract WASM files for the auxiliary
contracts that surround the Blend Capital v2 protocol (pool factory, emitter, Comet AMM,
and a mock SEP-40 oracle). They are used by integration tests that deploy the full Blend
protocol stack on a standalone Stellar network.

## Source

- **Repository**: [blend-capital/blend-utils](https://github.com/blend-capital/blend-utils)
- **Pinned commit**: `b05242df30b6b6caf9d317646f754541824a5a8b`

The two Comet files are the exception: they come from the Comet v1.1 GitHub release, see their
entries below.

## Files

### pool_factory.wasm
- **Source path**: `wasm_v2/pool_factory.wasm`
- **sha256**: `31328050548831f63d2b72e37bcfd0bb7371b7907135755dbe09ed434d755ca9`
- **Purpose**: Deploys and tracks Blend v2 lending pool instances.

### emitter.wasm
- **Source path**: `wasm_v1/emitter.wasm` (Blend v2 reuses the v1 emitter contract)
- **sha256**: `438a5528cff17ede6fe515f095c43c5f15727af17d006971485e52462e7e7b89`
- **Purpose**: Distributes BLND emissions to the backstop module of reward-zone pools.

### comet.wasm
- **Source**: [CometDEX/comet-contracts-v1](https://github.com/CometDEX/comet-contracts-v1) release `v1.1.0_contracts_pkg1.0.0_cli25.1.0`, asset `contracts_v1.0.0.wasm`
- **sha256**: `d735c3395f59510172cf5cf838823a1389b97cfec6bf24e580e9bd77d2b3e687`
- **Purpose**: Balancer-style weighted-pool AMM used to hold the BLND/USDC backstop LP token.

### comet_factory.wasm
- **Source**: [CometDEX/comet-contracts-v1](https://github.com/CometDEX/comet-contracts-v1) release `v1.1.0_contracts_pkg1.0.0_cli25.1.0`, asset `factory_v1.0.0.wasm`
- **sha256**: `a640ba6249ba89dd7527e6fb9b66b2a0d747721f1f7f3fbdf9c2ae723a5f970d`
- **Purpose**: Deploys new Comet pool instances from a pinned Comet wasm hash.

### oracle.wasm
- **Source path**: `src/external/oracle.wasm`
- **sha256**: `66c0b87b5eb481be594175d59e66ec9a9ac8945be0fec4e09f6c28bf7a1708be`
- **Purpose**: Mock SEP-40 price oracle used to feed reserve/backstop asset prices to a pool.

## Blend pool and backstop wasms are not duplicated here

The Blend v2 pool and backstop contract wasms already live at
`internal/services/blend/testdata/blend_pool_v2.wasm` and
`internal/services/blend/testdata/blend_backstop_v2.wasm`. They are byte-identical to
`wasm_v2/pool.wasm` and `wasm_v2/backstop.wasm` in blend-utils at the pinned commit above
(verified via sha256):

| File | sha256 |
|---|---|
| `blend-utils/wasm_v2/pool.wasm` | `a41fc53d6753b6c04eb15b021c55052366a4c8e0e21bc72700f461264ec1350e` |
| `internal/services/blend/testdata/blend_pool_v2.wasm` | `a41fc53d6753b6c04eb15b021c55052366a4c8e0e21bc72700f461264ec1350e` |
| `blend-utils/wasm_v2/backstop.wasm` | `c1f4502a757e25c611f5a159bc1ab0eef64085adac6c68123dca66e87faffbc2` |
| `internal/services/blend/testdata/blend_backstop_v2.wasm` | `c1f4502a757e25c611f5a159bc1ab0eef64085adac6c68123dca66e87faffbc2` |

## Updating WASM Files

1. Clone `blend-capital/blend-utils` and pin to the desired commit, or download the Comet
   release assets.
2. Copy the updated wasm files from the sources listed above.
3. Recompute sha256 (`shasum -a 256 <file>`) and update this README.
4. Re-verify the pool/backstop byte-identity claim above and update the table.
