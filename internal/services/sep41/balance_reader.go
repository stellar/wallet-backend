// Package sep41 — balance_reader.go reads a holder's authoritative balance by
// simulating the contract's balance(id) view function. The repair path writes
// this value over event-derived rows.
//
// The value is the raw i128 as a decimal string, no decimals scaling — the
// same form the processor persists. Scaling here would make the two
// incomparable.
package sep41

import (
	"context"
	"fmt"

	"github.com/stellar/wallet-backend/internal/services"
	"github.com/stellar/wallet-backend/internal/utils"
)

// BalanceReader reads on-chain SEP-41 balances via RPC simulation.
type BalanceReader struct {
	rpc services.ContractMetadataService
}

// NewBalanceReader returns a reader backed by the supplied
// ContractMetadataService, which provides the generic simulation primitive.
func NewBalanceReader(rpc services.ContractMetadataService) *BalanceReader {
	if rpc == nil {
		return nil
	}
	return &BalanceReader{rpc: rpc}
}

// ReadBalance simulates balance(holder) and returns the raw i128 decimal
// string plus the ledger it is true at. tokenContractAddress is a C-address;
// holderAddress is a G- or C-address. Any failure (RPC error, revert, non-i128
// result) is returned; callers treat it as skip-and-report.
func (r *BalanceReader) ReadBalance(ctx context.Context, tokenContractAddress, holderAddress string) (string, uint32, error) {
	if r == nil {
		return "", 0, fmt.Errorf("sep41: nil BalanceReader")
	}

	holderArg, err := utils.AddressScVal(holderAddress)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: reading balance of %s on %s: %w", holderAddress, tokenContractAddress, err)
	}

	val, ledger, err := r.rpc.FetchSingleFieldWithLedger(ctx, tokenContractAddress, "balance", holderArg)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: simulating balance(%s) on %s: %w", holderAddress, tokenContractAddress, err)
	}

	balance, err := extractI128(val)
	if err != nil {
		return "", 0, fmt.Errorf("sep41: balance(%s) on %s returned an unexpected result: %w", holderAddress, tokenContractAddress, err)
	}

	return balance.String(), ledger, nil
}
