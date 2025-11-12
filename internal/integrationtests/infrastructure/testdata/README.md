# Integration Test WASM Contracts

This directory contains pre-compiled Soroban smart contract WASM files used in integration tests.

## Files

### soroban_sac_test.wasm
- **Source**: stellar/go SDK integration tests
- **Purpose**: SEP-41 compliant token contract for testing custom tokens
- **Usage**: Deployed to test SEP-41 token balances for both G-addresses and C-addresses
- **Interface**: Implements standard token functions (mint, transfer, balance, etc.)

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

If contracts need updating:
1. Locate files in stellar/go SDK at:
   `services/horizon/internal/integration/testdata/`
2. Copy updated WASM files to this directory
3. Update this README with any interface changes
