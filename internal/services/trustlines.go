package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

const (
	archiveURL = "https://history.stellar.org/prd/core-live/core_live_001/"
)

// TrustlinesService defines the interface for trustlines operations.
type TrustlinesService interface {
	GetCheckpointLedger() uint32
	PopulateTrustlinesAndSAC(ctx context.Context) error
	Add(ctx context.Context, accountAddress string, assets []string) error
	GetTrustlines(ctx context.Context, accountAddress string) ([]string, error)
	GetContracts(ctx context.Context, accountAddress string) ([]string, error)
	ProcessChangesBatch(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
}

var _ TrustlinesService = (*trustlinesService)(nil)

type trustlinesService struct {
	checkpointLedger uint32
	archive          historyarchive.ArchiveInterface
	redisStore       *store.RedisStore
	trustlinesPrefix string
	contractsPrefix  string
}

func NewTrustlinesService(networkPassphrase string, redisStore *store.RedisStore) (TrustlinesService, error) {
	archive, err := historyarchive.Connect(
		archiveURL,
		historyarchive.ArchiveOptions{
			NetworkPassphrase: networkPassphrase,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to history archive: %w", err)
	}
	return &trustlinesService{
		checkpointLedger: 0,
		archive:          archive,
		redisStore:       redisStore,
		trustlinesPrefix: "trustlines:",
		contractsPrefix:  "contracts:",
	}, nil
}

// AddTrustlines adds trustlines for an account to Redis.
func (s *trustlinesService) Add(ctx context.Context, key string, assets []string) error {
	if len(assets) == 0 {
		return nil
	}
	if err := s.redisStore.SAdd(ctx, key, assets...); err != nil {
		return fmt.Errorf("adding trustlines for key %s: %w", key, err)
	}
	return nil
}

// GetTrustlines retrieves all trustlines for an account from Redis.
func (s *trustlinesService) GetTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	key := s.trustlinesPrefix + accountAddress
	trustlines, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	return trustlines, nil
}

// GetContracts retrieves all contract tokens for an account from Redis.
func (s *trustlinesService) GetContracts(ctx context.Context, accountAddress string) ([]string, error) {
	key := s.contractsPrefix + accountAddress
	contracts, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting contracts for account %s: %w", accountAddress, err)
	}
	return contracts, nil
}

func (s *trustlinesService) GetCheckpointLedger() uint32 {
	return s.checkpointLedger
}

func (s *trustlinesService) PopulateTrustlinesAndSAC(ctx context.Context) error {
	latestCheckpointLedger, err := getLatestCheckpointLedger(s.archive)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Infof("Populating trustlines from ledger %d", latestCheckpointLedger)
	s.checkpointLedger = latestCheckpointLedger
	reader, err := ingest.NewCheckpointChangeReader(
		context.Background(),
		s.archive,
		latestCheckpointLedger,
	)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			// Log error but don't override the function's return error
			fmt.Printf("error closing reader: %v\n", closeErr)
		}
	}()

	// trustlines is a map of G-... account address to a list of asset codes
	trustlines := make(map[string][]string, 0)
	// contracts is a map of both G-... and C-... contract addresses to a list of contract IDs
	contracts := make(map[string][]string, 0)
	entries := 0
	startTime := time.Now()
	for {
		change, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading checkpoint changes: %w", err)
		}

		switch change.Type {
		case xdr.LedgerEntryTypeTrustline:
			trustlineEntry := change.Post.Data.MustTrustLine()
			accountAddress := trustlineEntry.AccountId.Address()
			asset := trustlineEntry.Asset
			if asset.Type == xdr.AssetTypeAssetTypePoolShare {
				continue
			}
			var assetType, assetCode, assetIssuer string
			err = trustlineEntry.Asset.Extract(&assetType, &assetCode, &assetIssuer)
			if err != nil {
				continue
			}
			entries++
			assetStr := fmt.Sprintf("%s:%s", assetCode, assetIssuer)
			trustlines[accountAddress] = append(trustlines[accountAddress], assetStr)
		case xdr.LedgerEntryTypeContractData:
			contractDataEntry := change.Post.Data.MustContractData()

			if contractDataEntry.Key.Type != xdr.ScValTypeScvVec {
				continue
			}

			// Extract the account/contract address from the contract data entry key
			holderAddress, err := exctractHolderAddress(contractDataEntry.Key)
			if err != nil {
				// Skip invalid balance entries
				continue
			}

			// Extract the SAC contract ID from the contract data entry
			contractAddress, err := extractContractID(contractDataEntry)
			if err != nil {
				// Skip if we can't extract contract ID
				continue
			}

			entries++
			// Store SAC balance: map holder address to the contract address
			contracts[holderAddress] = append(contracts[holderAddress], contractAddress)
		default:
			continue
		}
	}
	fmt.Printf("Processed %d entries in %v minutes\n", entries, time.Since(startTime).Minutes())

	// Store trustlines in Redis using parallel worker pool
	pool := pond.NewPool(100, pond.WithQueueSize(1000))
	defer pool.Stop()

	group := pool.NewGroupContext(ctx)
	var errs []error
	errMu := sync.Mutex{}

	startTime = time.Now()
	for accountAddress, assets := range trustlines {
		accAddr := accountAddress
		accAssets := assets
		group.Submit(func() {
			if err := s.Add(ctx, s.trustlinesPrefix+accAddr, accAssets); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("storing trustlines for account %s: %w", accAddr, err))
				errMu.Unlock()
			}
		})
	}
	for accountAddress, contracts := range contracts {
		accAddr := accountAddress
		accContracts := contracts
		group.Submit(func() {
			if err := s.Add(ctx, s.contractsPrefix+accAddr, accContracts); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("storing contracts for account %s: %w", accAddr, err))
				errMu.Unlock()
			}
		})
	}

	if err := group.Wait(); err != nil {
		return fmt.Errorf("waiting for trustlines storage: %w", err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("storing trustlines: %w", errors.Join(errs...))
	}

	fmt.Printf("Stored token info in Redis in %v minutes\n", time.Since(startTime).Minutes())

	return nil
}

// ProcessChangesBatch processes multiple changes efficiently using Redis pipelining.
// This reduces network round trips from N operations to 1, significantly improving performance
// when processing large batches of trustline changes during ingestion.
func (s *trustlinesService) ProcessChangesBatch(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// Convert trustline changes to Redis pipeline operations
	operations := make([]store.SetPipelineOperation, 0, len(trustlineChanges)+len(contractChanges))
	for _, change := range trustlineChanges {
		key := s.trustlinesPrefix + change.AccountID
		var op store.SetOperation

		switch change.Operation {
		case types.TrustlineOpAdd:
			op = store.SetOpAdd
		case types.TrustlineOpRemove:
			op = store.SetOpRemove
		default:
			return fmt.Errorf("unsupported trustline operation: %s", change.Operation)
		}

		operations = append(operations, store.SetPipelineOperation{
			Op:      op,
			Key:     key,
			Members: []string{change.Asset},
		})
	}

	// Convert contract changes to Redis pipeline operations
	for _, change := range contractChanges {
		// For contract changes, we always add the contract IDs and not remove them.
		operations = append(operations, store.SetPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.contractsPrefix + change.AccountID,
			Members: []string{change.ContractID},
		})
	}

	// Execute all operations in a single pipeline
	if err := s.redisStore.ExecuteSetPipeline(ctx, operations); err != nil {
		return fmt.Errorf("executing trustline changes pipeline: %w", err)
	}

	return nil
}

func getLatestCheckpointLedger(archive historyarchive.ArchiveInterface) (uint32, error) {
	// Get latest ledger from archive
	latestLedger, err := archive.GetLatestLedgerSequence()
	if err != nil {
		return 0, fmt.Errorf("getting latest ledger sequence: %w", err)
	}

	// Get checkpoint manager
	manager := archive.GetCheckpointManager()

	// Return the latest checkpoint (on or before latest ledger)
	if manager.IsCheckpoint(latestLedger) {
		return latestLedger, nil
	}
	return manager.PrevCheckpoint(latestLedger), nil
}

// exctractHolderAddress extracts the account address from a Stellar Asset Contract balance entry.
// Balance entries have a key that is a ScVec with 2 elements:
// - First element: ScSymbol("Balance")
// - Second element: ScAddress (the account/contract holder address)
// Returns the holder address as a Stellar-encoded string, or empty string if invalid.
func exctractHolderAddress(key xdr.ScVal) (string, error) {
	// Verify the key is a vector
	keyVecPtr, ok := key.GetVec()
	if !ok || keyVecPtr == nil {
		return "", fmt.Errorf("key is not a vector")
	}
	keyVec := *keyVecPtr

	// Balance entries should have exactly 2 elements
	if len(keyVec) != 2 {
		return "", fmt.Errorf("key vector length is %d, expected 2", len(keyVec))
	}

	// First element should be the symbol "Balance"
	sym, ok := keyVec[0].GetSym()
	if !ok || sym != "Balance" {
		return "", fmt.Errorf("first element is not 'Balance' symbol")
	}

	// Second element is the ScAddress of the balance holder
	scAddress, ok := keyVec[1].GetAddress()
	if !ok {
		return "", fmt.Errorf("second element is not a valid address")
	}

	// Convert ScAddress to Stellar string format
	// This handles both account addresses (G...) and contract addresses (C...)
	holderAddress, err := scAddress.String()
	if err != nil {
		return "", fmt.Errorf("converting address to string: %w", err)
	}

	return holderAddress, nil
}

// extractContractID extracts the contract ID from a ContractData entry and returns it
// as a Stellar-encoded contract address (C...).
func extractContractID(contractData xdr.ContractDataEntry) (string, error) {
	if contractData.Contract.ContractId == nil {
		return "", fmt.Errorf("contract ID is nil")
	}

	contractID := *contractData.Contract.ContractId
	// Encode as a Stellar contract address (C...)
	contractAddress, err := strkey.Encode(strkey.VersionByteContract, contractID[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract ID: %w", err)
	}

	return contractAddress, nil
}
