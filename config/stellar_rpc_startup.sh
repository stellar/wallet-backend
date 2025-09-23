#!/bin/bash
set -e

# Handle environment logic for Stellar RPC
if [ "$NETWORK" = "testnet" ]; then
    CONFIG_FILE="/config/stellar-rpc-testnet-config.toml"
elif [ "$NETWORK" = "pubnet" ]; then
    CONFIG_FILE="/config/stellar-rpc-pubnet-config.toml"
else
    echo "Unknown environment: $NETWORK"
    exit 1
fi

# Start Soroban RPC
exec /usr/bin/stellar-rpc --config-path "$CONFIG_FILE"
