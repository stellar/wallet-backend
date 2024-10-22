#!/bin/bash
set -e

# Handle environment logic for Soroban RPC
if [ "$NETWORK" = "testnet" ]; then
    CONFIG_FILE="/config/soroban-rpc-config.toml"
elif [ "$NETWORK" = "prod" ]; then
    CONFIG_FILE="/config/soroban-prod-rpc-config.toml"
else
    echo "Unknown environment: $NETWORK"
    exit 1
fi

# Start Soroban RPC
./app/soroban-rpc --config-path "$CONFIG_FILE"
