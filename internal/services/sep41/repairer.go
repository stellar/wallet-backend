// Package sep41 — repairer.go implements the current-state repair seam for
// SEP-41: a unit is a (holder, token) pair from sep41_balances, truth is a
// balance(holder) simulation, and Apply is the conditional absolute write.
package sep41

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"
)

// repairUnit is one (holder, token) pair to verify and repair.
type repairUnit struct {
	holder       string // G... or C... strkey
	tokenAddress string // C... strkey, the simulation target
	tokenUUID    uuid.UUID
}

// String renders the unit for engine log lines.
func (u repairUnit) String() string {
	return fmt.Sprintf("%s balance of %s", u.tokenAddress, u.holder)
}

// repairer implements services.ProtocolCurrentStateRepair for SEP-41.
type repairer struct {
	reader   *BalanceReader
	balances sep41data.BalanceModelInterface
}

var _ services.ProtocolCurrentStateRepair = (*repairer)(nil)

func newRepairer(deps services.ProtocolDeps) *repairer {
	return &repairer{
		reader:   NewBalanceReader(deps.ContractMetadataService),
		balances: deps.Models.SEP41.Balances,
	}
}

// ListUnits pages (holder, token) pairs from sep41_balances. The table is the
// work list: repair verifies known pairs, it does not discover holders.
func (r *repairer) ListUnits(ctx context.Context, scope services.RepairScope, cursor string, limit int) ([]services.RepairUnit, string, error) {
	var filterContract *uuid.UUID
	if scope.ContractAddress != "" {
		id := data.DeterministicContractID(scope.ContractAddress)
		filterContract = &id
	}

	after, err := decodePairCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	pairs, err := r.balances.ListPairs(ctx, filterContract, scope.AccountAddress, after, int32(limit))
	if err != nil {
		return nil, "", fmt.Errorf("listing SEP-41 repair units: %w", err)
	}

	units := make([]services.RepairUnit, len(pairs))
	for i, p := range pairs {
		units[i] = repairUnit{holder: string(p.AccountID), tokenAddress: p.TokenID, tokenUUID: p.ContractID}
	}

	next := ""
	if len(pairs) == limit {
		last := pairs[len(pairs)-1]
		next = encodePairCursor(string(last.AccountID), last.ContractID)
	}
	return units, next, nil
}

// FetchTruth simulates balance(holder) on the token contract.
func (r *repairer) FetchTruth(ctx context.Context, unit services.RepairUnit) (services.Truth, uint32, error) {
	u, ok := unit.(repairUnit)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected repair unit type %T", unit)
	}
	value, ledger, err := r.reader.ReadBalance(ctx, u.tokenAddress, u.holder)
	if err != nil {
		return nil, 0, err
	}
	return value, ledger, nil
}

// Apply conditionally writes the simulated balance. Zeros become permanent
// rows (the stamp keeps shielding against stale deltas); GetByAccount hides
// them from readers.
func (r *repairer) Apply(ctx context.Context, dbTx pgx.Tx, unit services.RepairUnit, truth services.Truth, ledger uint32) (bool, error) {
	u, ok := unit.(repairUnit)
	if !ok {
		return false, fmt.Errorf("unexpected repair unit type %T", unit)
	}
	value, ok := truth.(string)
	if !ok {
		return false, fmt.Errorf("unexpected truth type %T for %s", truth, u)
	}

	bal := sep41data.Balance{
		AccountID:    types.AddressBytea(u.holder),
		ContractID:   u.tokenUUID,
		Balance:      value,
		LedgerNumber: ledger,
	}
	applied, err := r.balances.ApplyAbsolute(ctx, dbTx, bal)
	if err != nil {
		return false, fmt.Errorf("writing repaired balance for %s: %w", u, err)
	}
	return applied, nil
}

// encodePairCursor / decodePairCursor round-trip the keyset position through the
// engine's opaque string cursor as "<holder strkey>|<contract uuid>".
func encodePairCursor(holder string, contractID uuid.UUID) string {
	return holder + "|" + contractID.String()
}

func decodePairCursor(cursor string) (*sep41data.Balance, error) {
	if cursor == "" {
		return nil, nil
	}
	holder, rawUUID, found := strings.Cut(cursor, "|")
	if !found {
		return nil, fmt.Errorf("malformed repair cursor %q", cursor)
	}
	id, err := uuid.Parse(rawUUID)
	if err != nil {
		return nil, fmt.Errorf("malformed repair cursor %q: %w", cursor, err)
	}
	return &sep41data.Balance{AccountID: types.AddressBytea(holder), ContractID: id}, nil
}
