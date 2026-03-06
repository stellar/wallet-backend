package services

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// BatchChanges holds data collected from a backfill batch for catchup mode.
// This data is processed after all parallel batches complete to ensure proper ordering.
type BatchChanges struct {
	TrustlineChangesByKey     map[indexer.TrustlineChangeKey]types.TrustlineChange
	ContractChanges           []types.ContractChange
	AccountChangesByAccountID map[string]types.AccountChange
	SACBalanceChangesByKey    map[indexer.SACBalanceChangeKey]types.SACBalanceChange
	UniqueTrustlineAssets     map[uuid.UUID]data.TrustlineAsset
	UniqueContractTokensByID  map[string]types.ContractType
	SACContractsByID          map[string]*data.Contract // SAC contract metadata extracted from instance entries
}

// mergeTrustlineChanges merges source trustline changes into dest, keeping highest OperationID per key.
// Handles ADD→REMOVE no-op case where a trustline is created and removed in the same batch.
func mergeTrustlineChanges(dest, source map[indexer.TrustlineChangeKey]types.TrustlineChange) {
	for key, change := range source {
		existing, exists := dest[key]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.TrustlineOpRemove && existing.Operation == types.TrustlineOpAdd {
			delete(dest, key)
			continue
		}
		dest[key] = change
	}
}

// mergeAccountChanges merges source account changes into dest, keeping highest OperationID per account.
// Handles CREATE→REMOVE no-op case where an account is created and removed in the same batch.
func mergeAccountChanges(dest, source map[string]types.AccountChange) {
	for accountID, change := range source {
		existing, exists := dest[accountID]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle CREATE→REMOVE no-op case
		if exists && change.Operation == types.AccountOpRemove && existing.Operation == types.AccountOpCreate {
			delete(dest, accountID)
			continue
		}
		dest[accountID] = change
	}
}

// mergeSACBalanceChanges merges source SAC balance changes into dest, keeping highest OperationID per key.
// Handles ADD→REMOVE no-op case where a SAC balance is created and removed in the same batch.
func mergeSACBalanceChanges(dest, source map[indexer.SACBalanceChangeKey]types.SACBalanceChange) {
	for key, change := range source {
		existing, exists := dest[key]
		if exists && existing.OperationID > change.OperationID {
			continue
		}
		// Handle ADD→REMOVE no-op case
		if exists && change.Operation == types.SACBalanceOpRemove && existing.Operation == types.SACBalanceOpAdd {
			delete(dest, key)
			continue
		}
		dest[key] = change
	}
}

// processBatchChanges processes aggregated batch changes after all parallel batches complete.
// Unique assets and contracts are pre-collected during batch processing.
func (m *ingestService) processBatchChanges(
	ctx context.Context,
	dbTx pgx.Tx,
	trustlineChangesByKey map[indexer.TrustlineChangeKey]types.TrustlineChange,
	contractChanges []types.ContractChange,
	accountChangesByAccountID map[string]types.AccountChange,
	sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange,
	uniqueAssets map[uuid.UUID]data.TrustlineAsset,
	uniqueContractTokens map[string]types.ContractType,
	sacContracts map[string]*data.Contract,
) error {
	// 1. Convert unique assets map to slice for BatchInsert
	assetSlice := make([]data.TrustlineAsset, 0, len(uniqueAssets))
	for _, asset := range uniqueAssets {
		assetSlice = append(assetSlice, asset)
	}

	// 2. Insert unique trustline assets
	if len(assetSlice) > 0 {
		if txErr := m.models.TrustlineAsset.BatchInsert(ctx, dbTx, assetSlice); txErr != nil {
			return fmt.Errorf("inserting trustline assets: %w", txErr)
		}
	}

	// 3. Insert new contract tokens (filter existing, fetch metadata, insert)
	if len(uniqueContractTokens) > 0 {
		contracts, txErr := m.prepareNewContractTokens(ctx, dbTx, uniqueContractTokens, sacContracts)
		if txErr != nil {
			return fmt.Errorf("preparing contracts: %w", txErr)
		}
		if len(contracts) > 0 {
			if txErr := m.models.Contract.BatchInsert(ctx, dbTx, contracts); txErr != nil {
				return fmt.Errorf("inserting contracts: %w", txErr)
			}
		}
	}

	// 4. Apply token changes to PostgreSQL
	if txErr := m.tokenIngestionService.ProcessTokenChanges(ctx, dbTx, trustlineChangesByKey, contractChanges, accountChangesByAccountID, sacBalanceChangesByKey); txErr != nil {
		return fmt.Errorf("processing token changes: %w", txErr)
	}

	log.Ctx(ctx).Infof("Processed batch changes: %d trustline, %d contract, %d account, %d SAC balance changes",
		len(trustlineChangesByKey), len(contractChanges), len(accountChangesByAccountID), len(sacBalanceChangesByKey))

	return nil
}
