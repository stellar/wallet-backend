// Account token caching service - manages PostgreSQL storage of account token holdings
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
package services

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/stellar/go-stellar-sdk/support/log"

	wbdata "github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// TokenIngestionService provides write access to account token storage during live ingestion.
// The balance writes are split by where their foreign-key parents are staged: each balance
// family runs on the transaction that carries its parent rows, so every FK check passes at
// that transaction's own commit. Trustline balances ride the sibling that inserts
// trustline_assets, pool-share balances ride the sibling that upserts liquidity_pools, and
// SAC balances stay on the coordinating transaction because their parent (contract_tokens)
// is also written by the classification path there — splitting those writers across
// concurrent transactions could deadlock at the commit barrier on a same-key insert.
type TokenIngestionService interface {
	// ProcessTrustlineChanges applies trustline balance changes. dbTx must be the
	// transaction on which this ledger's trustline_assets rows are staged: the balance
	// rows' foreign key is checked against committed state at commit, and a transaction
	// committing before its parents would fail with SQLSTATE 23503.
	ProcessTrustlineChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange) error
	// ProcessSACBalanceChanges applies SAC balance changes. dbTx must be the transaction
	// on which this ledger's contract_tokens rows are staged.
	ProcessSACBalanceChanges(ctx context.Context, dbTx pgx.Tx, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error
	// ProcessNativeAndPoolChanges applies native-balance and liquidity-pool changes.
	// Pools are upserted before pool-share balances, so the share rows' foreign key is
	// satisfied within this same transaction; native balances have no parent.
	ProcessNativeAndPoolChanges(ctx context.Context, dbTx pgx.Tx, accountChangesByAccountID map[string]types.AccountChange, lpShareChangesByKey map[indexer.LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange, lpChangesByPoolID map[string]types.LiquidityPoolChange) error
}

// Verify interface compliance at compile time
var _ TokenIngestionService = (*tokenIngestionService)(nil)

// TokenIngestionServiceConfig holds configuration for creating a TokenIngestionService.
type TokenIngestionServiceConfig struct {
	TrustlineBalanceModel     wbdata.TrustlineBalanceModelInterface
	NativeBalanceModel        wbdata.NativeBalanceModelInterface
	SACBalanceModel           wbdata.SACBalanceModelInterface
	LiquidityPoolModel        wbdata.LiquidityPoolModelInterface
	LiquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface
	NetworkPassphrase         string
}

// tokenIngestionService implements TokenIngestionService.
type tokenIngestionService struct {
	trustlineBalanceModel     wbdata.TrustlineBalanceModelInterface
	nativeBalanceModel        wbdata.NativeBalanceModelInterface
	sacBalanceModel           wbdata.SACBalanceModelInterface
	liquidityPoolModel        wbdata.LiquidityPoolModelInterface
	liquidityPoolBalanceModel wbdata.LiquidityPoolBalanceModelInterface
	networkPassphrase         string
}

// NewTokenIngestionService creates a TokenIngestionService for ingestion.
func NewTokenIngestionService(cfg TokenIngestionServiceConfig) *tokenIngestionService {
	return &tokenIngestionService{
		trustlineBalanceModel:     cfg.TrustlineBalanceModel,
		nativeBalanceModel:        cfg.NativeBalanceModel,
		sacBalanceModel:           cfg.SACBalanceModel,
		liquidityPoolModel:        cfg.LiquidityPoolModel,
		liquidityPoolBalanceModel: cfg.LiquidityPoolBalanceModel,
		networkPassphrase:         cfg.NetworkPassphrase,
	}
}

// ProcessTrustlineChanges applies trustline balance changes on the transaction
// that stages their trustline_assets parent rows.
func (s *tokenIngestionService) ProcessTrustlineChanges(ctx context.Context, dbTx pgx.Tx, trustlineChangesByTrustlineKey map[indexer.TrustlineChangeKey]types.TrustlineChange) error {
	return s.processTrustlineChanges(ctx, dbTx, trustlineChangesByTrustlineKey)
}

// ProcessSACBalanceChanges applies SAC balance changes on the transaction that
// stages their contract_tokens parent rows.
func (s *tokenIngestionService) ProcessSACBalanceChanges(ctx context.Context, dbTx pgx.Tx, sacBalanceChangesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	return s.processSACBalanceChanges(ctx, dbTx, sacBalanceChangesByKey)
}

// ProcessNativeAndPoolChanges applies native-balance and liquidity-pool changes; the
// target tables have no foreign keys, so any transaction may carry them.
func (s *tokenIngestionService) ProcessNativeAndPoolChanges(ctx context.Context, dbTx pgx.Tx, accountChangesByAccountID map[string]types.AccountChange, lpShareChangesByKey map[indexer.LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange, lpChangesByPoolID map[string]types.LiquidityPoolChange) error {
	if err := s.processNativeBalanceChanges(ctx, dbTx, accountChangesByAccountID); err != nil {
		return err
	}
	if err := s.processLiquidityPoolChanges(ctx, dbTx, lpChangesByPoolID); err != nil {
		return err
	}
	if err := s.processLiquidityPoolShareChanges(ctx, dbTx, lpShareChangesByKey); err != nil {
		return err
	}
	return nil
}

// processTrustlineChanges handles trustline balance upserts and deletes.
func (s *tokenIngestionService) processTrustlineChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.TrustlineChangeKey]types.TrustlineChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.TrustlineBalance
	var deletes []wbdata.TrustlineBalance
	for key, change := range changesByKey {
		fullData := wbdata.TrustlineBalance{
			AccountID:          types.AddressBytea(change.AccountID),
			AssetID:            key.TrustlineID,
			Balance:            change.Balance,
			Limit:              change.Limit,
			BuyingLiabilities:  change.BuyingLiabilities,
			SellingLiabilities: change.SellingLiabilities,
			Flags:              change.Flags,
			LedgerNumber:       change.LedgerNumber,
		}
		if change.Operation == types.TrustlineOpRemove {
			deletes = append(deletes, fullData)
		} else {
			upserts = append(upserts, fullData)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.trustlineBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting trustline balances: %w", err)
		}
	}
	log.Ctx(ctx).Debugf("upserted %d trustlines, deleted %d trustlines", len(upserts), len(deletes))
	return nil
}

// processNativeBalanceChanges handles native XLM balance upserts and deletes.
func (s *tokenIngestionService) processNativeBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByAccountID map[string]types.AccountChange) error {
	if len(changesByAccountID) == 0 {
		return nil
	}

	var upserts []wbdata.NativeBalance
	var deletes []types.AddressBytea
	for _, change := range changesByAccountID {
		if change.Operation == types.AccountOpRemove {
			deletes = append(deletes, types.AddressBytea(change.AccountID))
		} else {
			upserts = append(upserts, wbdata.NativeBalance{
				AccountID:          types.AddressBytea(change.AccountID),
				Balance:            change.Balance,
				MinimumBalance:     change.MinimumBalance,
				BuyingLiabilities:  change.BuyingLiabilities,
				SellingLiabilities: change.SellingLiabilities,
				NumSubEntries:      change.NumSubEntries,
				LedgerNumber:       change.LedgerNumber,
			})
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.nativeBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting native balances: %w", err)
		}
		log.Ctx(ctx).Debugf("upserted %d native balances, deleted %d native balances", len(upserts), len(deletes))
	}
	return nil
}

// processSACBalanceChanges handles SAC balance upserts and deletes for contract addresses.
func (s *tokenIngestionService) processSACBalanceChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.SACBalanceChangeKey]types.SACBalanceChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.SACBalance
	var deletes []wbdata.SACBalance
	for _, change := range changesByKey {
		contractID := wbdata.DeterministicContractID(change.ContractID)
		sacBal := wbdata.SACBalance{
			AccountID:         types.AddressBytea(change.AccountID),
			ContractID:        contractID,
			Balance:           change.Balance,
			IsAuthorized:      change.IsAuthorized,
			IsClawbackEnabled: change.IsClawbackEnabled,
			LedgerNumber:      change.LedgerNumber,
		}
		if change.Operation == types.SACBalanceOpRemove {
			deletes = append(deletes, sacBal)
		} else {
			upserts = append(upserts, sacBal)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.sacBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting SAC balances: %w", err)
		}
		log.Ctx(ctx).Debugf("upserted %d SAC balances, deleted %d SAC balances", len(upserts), len(deletes))
	}
	return nil
}

// processLiquidityPoolChanges handles liquidity pool reserve upserts and deletes.
func (s *tokenIngestionService) processLiquidityPoolChanges(ctx context.Context, dbTx pgx.Tx, changesByPoolID map[string]types.LiquidityPoolChange) error {
	if len(changesByPoolID) == 0 {
		return nil
	}

	var upserts []wbdata.LiquidityPool
	var deletes []wbdata.LiquidityPool
	for _, change := range changesByPoolID {
		pool := wbdata.LiquidityPool{
			PoolID:       change.PoolID,
			AssetA:       change.AssetA,
			AmountA:      change.ReserveA,
			AssetB:       change.AssetB,
			AmountB:      change.ReserveB,
			LedgerNumber: change.LedgerNumber,
		}
		if change.Operation == types.LiquidityPoolOpRemove {
			deletes = append(deletes, pool)
		} else {
			upserts = append(upserts, pool)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.liquidityPoolModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting liquidity pools: %w", err)
		}
		log.Ctx(ctx).Debugf("upserted %d liquidity pools, deleted %d liquidity pools", len(upserts), len(deletes))
	}
	return nil
}

// processLiquidityPoolShareChanges handles pool-share balance upserts and deletes.
func (s *tokenIngestionService) processLiquidityPoolShareChanges(ctx context.Context, dbTx pgx.Tx, changesByKey map[indexer.LiquidityPoolShareChangeKey]types.LiquidityPoolShareChange) error {
	if len(changesByKey) == 0 {
		return nil
	}

	var upserts []wbdata.LiquidityPoolBalance
	var deletes []wbdata.LiquidityPoolBalance
	for _, change := range changesByKey {
		balance := wbdata.LiquidityPoolBalance{
			AccountID:    types.AddressBytea(change.AccountID),
			PoolID:       change.PoolID,
			Shares:       change.Shares,
			LedgerNumber: change.LedgerNumber,
		}
		if change.Operation == types.LiquidityPoolShareOpRemove {
			deletes = append(deletes, balance)
		} else {
			upserts = append(upserts, balance)
		}
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		if err := s.liquidityPoolBalanceModel.BatchUpsert(ctx, dbTx, upserts, deletes); err != nil {
			return fmt.Errorf("upserting liquidity pool balances: %w", err)
		}
		log.Ctx(ctx).Debugf("upserted %d liquidity pool balances, deleted %d liquidity pool balances", len(upserts), len(deletes))
	}
	return nil
}
