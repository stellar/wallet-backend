#!/bin/bash
set -e

# Handle environment logic for Soroban RPC
if [ "$NETWORK" = "testnet" ]; then
    CONFIG_FILE="/config/soroban-rpc-testnet-config.toml"
elif [ "$NETWORK" = "pubnet" ]; then
    CONFIG_FILE="/config/soroban-rpc-pubnet-config.toml"
else
    echo "Unknown environment: $NETWORK"
    exit 1
fi

# Start Soroban RPC
exec /usr/bin/stellar-soroban-rpc --config-path "$CONFIG_FILE"
