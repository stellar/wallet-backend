# Integration Test WASM Contracts

This directory contains pre-compiled Soroban smart contract WASM files used in integration tests.

## Files

### soroban_token_contract.wasm
- **Source**: stellar/soroban-examples v22.0.1 (built from source)
- **Purpose**: Standard SEP-41 compliant token contract for integration testing
- **Version**: Protocol 24 compatible (Soroban SDK v22.0.8)
- **Usage**: Deployed to test SEP-41 token operations (initialize, mint, transfer, burn)
- **Interface**: Full SEP-41 implementation with metadata support
  - **Constructor**: `__constructor(admin, decimal, name, symbol)` - Must be called after deployment
  - **Mint**: `mint(to, amount)` - Mints tokens to an address (admin only)
  - **Transfer**: `transfer(from, to, amount)` - Transfers tokens between addresses
  - **Burn**: `burn(from, amount)` - Burns tokens from an address
  - **Query**: `balance(address)`, `decimals()`, `name()`, `symbol()`, `allowance(from, spender)`
  - **Admin**: `set_admin(new_admin)`, `approve(from, spender, amount, expiration)`
- **Token Configuration**:
  - Name: "USD Coin"
  - Symbol: "USDC"
  - Decimals: 7
  - Admin: Master test account
- **Contract Hash**: `e80840a63de88eda39d2a2525e8f059201e17066e02777e0d410f1959d888c1d`
- **References**:
  - [SEP-41 Standard](https://stellar.org/protocol/sep-41)
  - [soroban-examples/token](https://github.com/stellar/soroban-examples/tree/v22.0.1/token)

### soroban_increment_contract.wasm
- **Source**: stellar/go SDK integration tests
- **Purpose**: Simple counter contract used as a token holder
- **Usage**: Deployed to test C-address token balances
- **Note**: Any contract can hold tokens - no special interface needed

## Why Pre-Compiled WASM?

These files are copied from the stellar/go SDK to avoid:
- Requiring Rust toolchain for tests
- Building contracts during CI/CD
- Version mismatches between stellar-core and contracts

## Updating WASM Files

### For soroban_token_contract.wasm:
1. Clone the soroban-examples repository:
   ```bash
   git clone -b v22.0.1 https://github.com/stellar/soroban-examples.git
   cd soroban-examples/token
   ```
2. Build the contract:
   ```bash
   stellar contract build
   ```
3. Copy the WASM file:
   ```bash
   cp target/wasm32v1-none/release/soroban_token_contract.wasm \
      [wallet-backend]/internal/integrationtests/infrastructure/testdata/
   ```
4. Update this README with the new contract hash and any interface changes

### For other WASM files:
1. Locate files in stellar/go SDK at:
   `services/horizon/internal/integration/testdata/`
2. Copy updated WASM files to this directory
3. Update this README with any interface changes
