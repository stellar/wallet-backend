// Package services provides account token management with Redis caching.
// This file handles trustlines and Stellar Asset Contract (SAC) balance tracking.
package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/sac"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"

	"github.com/stellar/go/support/log"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/store"
)

const (
	// Redis key prefixes for account token storage
	trustlinesKeyPrefix      = "trustlines:"
	contractsKeyPrefix       = "contracts:"
	contractTypePrefix       = "contract_type:"
	evictedBalancesKeyPrefix = "evicted_balances:"

	// redisPipelineBatchSize is the number of operations to batch in a single Redis pipeline.
	redisPipelineBatchSize = 50000

	// progressInterval is the number of checkpoint entries to process before logging progress.
	progressInterval = 100000
)

// AccountTokenService manages Redis caching of account token holdings,
// including both classic Stellar trustlines and Stellar Asset Contract (SAC) balances.
type AccountTokenService interface {
	// GetCheckpointLedger returns the ledger sequence number of the checkpoint
	// used to populate the initial account token cache.
	GetCheckpointLedger() uint32

	// PopulateAccountTokens performs initial Redis cache population from Stellar
	// history archive for the latest checkpoint. This is a long-running operation
	// that may take several minutes and should be called during service initialization.
	// It reads all trustlines and contract balances and stores them in Redis.
	PopulateAccountTokens(ctx context.Context) error

	// Add adds token identifiers (trustline assets or contract IDs) to an account's
	// Redis set. For trustlines, assets should be formatted as "CODE:ISSUER".
	// For contracts, assets should be contract addresses starting with "C".
	// Returns nil if assets is empty (no-op).
	Add(ctx context.Context, accountAddress string, assets []string) error

	// GetAccountTrustlines retrieves all classic trustline assets for an account.
	// Returns a slice of assets formatted as "CODE:ISSUER", or empty slice if none exist.
	GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error)

	// GetAccountContracts retrieves all Stellar Asset Contract (SAC) balance contract
	// IDs for an account. Returns contract addresses (C...) or empty slice if none exist.
	GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error)

	// GetContractType determines the token type (SAC, SEP41, or UNKNOWN) for a contract.
	// Returns ContractTypeUnknown if the contract ID is not found in the cache.
	GetContractType(ctx context.Context, contractID string) (types.ContractType, error)

	// GetEvictedBalances retrieves all evicted SAC balances for an account from Redis.
	// Returns a slice of evicted balances with contract ID, balance, token type, and authorization info.
	// Returns empty slice if no evicted balances exist for the account.
	GetEvictedBalances(ctx context.Context, accountAddress string) ([]EvictedBalance, error)

	// ProcessTokenChanges applies trustline and contract balance changes to Redis cache
	// using pipelining for performance. This is called by the indexer for each ledger's
	// state changes during live ingestion.
	ProcessTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error
}

var _ AccountTokenService = (*accountTokenService)(nil)

type EvictedBalance struct {
	ContractID        string
	Balance           string
	TokenType         types.ContractType
	IsAuthorized      bool
	IsClawbackEnabled bool
}

type accountTokenService struct {
	checkpointLedger      uint32
	archive               historyarchive.ArchiveInterface
	contractSpecValidator ContractSpecValidator
	redisStore            *store.RedisStore
	networkPassphrase     string
	trustlinesPrefix      string
	contractsPrefix       string
	contractTypePrefix    string
	evictedBalancesPrefix string
}

func NewAccountTokenService(networkPassphrase string, archiveURL string, redisStore *store.RedisStore, contractSpecValidator ContractSpecValidator, checkpointFrequency uint32) (AccountTokenService, error) {
	var archive historyarchive.ArchiveInterface

	// Only connect to archive if URL is provided (needed for ingest, not for serve)
	if archiveURL != "" {
		var err error
		archive, err = historyarchive.Connect(
			archiveURL,
			historyarchive.ArchiveOptions{
				NetworkPassphrase:   networkPassphrase,
				CheckpointFrequency: checkpointFrequency,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("connecting to history archive: %w", err)
		}
	}

	return &accountTokenService{
		checkpointLedger:      0,
		archive:               archive,
		contractSpecValidator: contractSpecValidator,
		redisStore:            redisStore,
		networkPassphrase:     networkPassphrase,
		trustlinesPrefix:      trustlinesKeyPrefix,
		contractsPrefix:       contractsKeyPrefix,
		contractTypePrefix:    contractTypePrefix,
		evictedBalancesPrefix: evictedBalancesKeyPrefix,
	}, nil
}

// PopulateAccountTokens performs initial Redis cache population from Stellar history archive.
// This reads the latest checkpoint ledger and extracts all trustlines and contracts that an account has.
// It should be called during service initialization before processing live ingestion.
// Warning: This is a long-running operation that may take several minutes.
func (s *accountTokenService) PopulateAccountTokens(ctx context.Context) error {
	if s.archive == nil {
		return fmt.Errorf("history archive not configured - PopulateAccountTokens requires archive connection")
	}

	defer func() {
		if err := s.contractSpecValidator.Close(ctx); err != nil {
			log.Ctx(ctx).Errorf("error closing contract spec validator: %v", err)
		}
	}()

	latestCheckpointLedger, err := getLatestCheckpointLedger(s.archive)
	if err != nil {
		return err
	}

	log.Ctx(ctx).Infof("Populating account token cache from checkpoint ledger = %d", latestCheckpointLedger)
	s.checkpointLedger = latestCheckpointLedger

	// Create checkpoint change reader
	reader, err := ingest.NewCheckpointChangeReader(ctx, s.archive, latestCheckpointLedger)
	if err != nil {
		return fmt.Errorf("creating checkpoint change reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Ctx(ctx).Errorf("error closing checkpoint reader: %v", closeErr)
		}
	}()

	// Collect account tokens from checkpoint
	trustlines, contracts, contractTypesByContractID, contractIDsByWasmHash, err := s.collectAccountTokensFromCheckpoint(ctx, reader)
	if err != nil {
		return err
	}

	evictedBalances, err := s.getEvictedContractBalances(ctx)
	if err != nil {
		return fmt.Errorf("getting evicted contract balances: %w", err)
	}

	if err := s.enrichContractTypes(ctx, contractTypesByContractID, contractIDsByWasmHash); err != nil {
		return err
	}

	if err := s.storeAccountTokensInRedis(ctx, trustlines, contracts, contractTypesByContractID); err != nil {
		return err
	}

	return s.storeEvictedBalancesInRedis(ctx, evictedBalances)
}

// ProcessTokenChanges processes token changes efficiently using Redis pipelining.
// This reduces network round trips from N operations to 1, significantly improving performance
// during live ingestion. Called by the indexer for each ledger's state changes.
//
// For trustlines: handles both ADD (new trustline created) and REMOVE (trustline deleted).
// For SAC balances: only ADD operations are processed (contract tokens are never explicitly removed).
func (s *accountTokenService) ProcessTokenChanges(ctx context.Context, trustlineChanges []types.TrustlineChange, contractChanges []types.ContractChange) error {
	if len(trustlineChanges) == 0 && len(contractChanges) == 0 {
		return nil
	}

	// Calculate capacity: each trustline = 1 op, each contract = 2 ops (set membership + contract type)
	trustlineOpsCount := len(trustlineChanges)
	contractOpsCount := len(contractChanges) * 2
	operations := make([]store.RedisPipelineOperation, 0, trustlineOpsCount+contractOpsCount)

	// Convert trustline changes to Redis pipeline operations
	for _, change := range trustlineChanges {
		if change.Asset == "" {
			log.Ctx(ctx).Warnf("Skipping trustline change with empty asset for account %s", change.AccountID)
			continue
		}

		key := s.buildTrustlineKey(change.AccountID)
		var op store.RedisOperation

		switch change.Operation {
		case types.TrustlineOpAdd:
			op = store.SetOpAdd
		case types.TrustlineOpRemove:
			op = store.SetOpRemove
		default:
			return fmt.Errorf("unsupported trustline operation: %s", change.Operation)
		}

		operations = append(operations, store.RedisPipelineOperation{
			Op:      op,
			Key:     key,
			Members: []string{change.Asset},
		})
	}

	// Convert contract changes to Redis pipeline operations
	for _, change := range contractChanges {
		if change.ContractID == "" {
			log.Ctx(ctx).Warnf("Skipping contract change with empty contract ID for account %s", change.AccountID)
			continue
		}

		// For contract changes, we always add contract IDs and never remove them.
		// This is because contract balance entries persist in the ledger even when balance is zero,
		// unlike trustlines which can be completely deleted. We track all contracts an account has
		// ever interacted with.
		operations = append(operations, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildContractKey(change.AccountID),
			Members: []string{change.ContractID},
		}, store.RedisPipelineOperation{
			Op:    store.OpSet,
			Key:   s.buildContractTypeKey(change.ContractID),
			Value: string(change.ContractType),
		})
	}

	// Execute all operations in a single pipeline
	if err := s.redisStore.ExecutePipeline(ctx, operations); err != nil {
		return fmt.Errorf("executing token changes pipeline: %w", err)
	}

	return nil
}

// buildTrustlineKey constructs the Redis key for an account's trustlines set.
func (s *accountTokenService) buildTrustlineKey(accountAddress string) string {
	return s.trustlinesPrefix + accountAddress
}

// buildContractKey constructs the Redis key for an account's contracts set.
func (s *accountTokenService) buildContractKey(accountAddress string) string {
	return s.contractsPrefix + accountAddress
}

// buildContractTypeKey constructs the Redis key for a contract's type.
func (s *accountTokenService) buildContractTypeKey(contractID string) string {
	return s.contractTypePrefix + contractID
}

// buildEvictedBalancesKey constructs the Redis key for an account's evicted balances.
func (s *accountTokenService) buildEvictedBalancesKey(accountAddress string) string {
	return s.evictedBalancesPrefix + accountAddress
}

// Add adds token identifiers to an account's Redis set.
// For trustlines, assets are formatted as "CODE:ISSUER".
// For SAC balances, assets are contract addresses (C...).
// Returns nil if assets is empty (no-op).
func (s *accountTokenService) Add(ctx context.Context, redisKey string, assets []string) error {
	if len(assets) == 0 {
		return nil
	}
	if err := s.redisStore.SAdd(ctx, redisKey, assets...); err != nil {
		return fmt.Errorf("adding members to set %s: %w", redisKey, err)
	}
	return nil
}

// GetAccountTrustlines retrieves all trustlines for an account from Redis.
func (s *accountTokenService) GetAccountTrustlines(ctx context.Context, accountAddress string) ([]string, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("account address cannot be empty")
	}
	key := s.buildTrustlineKey(accountAddress)
	trustlines, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting trustlines for account %s: %w", accountAddress, err)
	}
	return trustlines, nil
}

// GetAccountContracts retrieves all Stellar Asset Contract (SAC) balance contract IDs for an account from Redis.
func (s *accountTokenService) GetAccountContracts(ctx context.Context, accountAddress string) ([]string, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("account address cannot be empty")
	}
	key := s.buildContractKey(accountAddress)
	contracts, err := s.redisStore.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting contracts for account %s: %w", accountAddress, err)
	}
	return contracts, nil
}

// GetContractType retrieves the token type (SAC or CUSTOM) for a given contract ID from Redis.
func (s *accountTokenService) GetContractType(ctx context.Context, contractID string) (types.ContractType, error) {
	if contractID == "" {
		return types.ContractTypeUnknown, fmt.Errorf("contract ID cannot be empty")
	}
	key := s.buildContractTypeKey(contractID)
	tokenType, err := s.redisStore.Get(ctx, key)
	if err != nil {
		return types.ContractTypeUnknown, fmt.Errorf("getting contract type for %s: %w", contractID, err)
	}
	if tokenType == "" {
		return types.ContractTypeUnknown, nil
	}
	return types.ContractType(tokenType), nil
}

// GetEvictedBalances retrieves all evicted SAC balances for an account from Redis.
func (s *accountTokenService) GetEvictedBalances(ctx context.Context, accountAddress string) ([]EvictedBalance, error) {
	if accountAddress == "" {
		return nil, fmt.Errorf("account address cannot be empty")
	}

	key := s.buildEvictedBalancesKey(accountAddress)
	val, err := s.redisStore.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("getting evicted balances for account %s: %w", accountAddress, err)
	}

	// No evicted balances for this account
	if val == "" {
		return []EvictedBalance{}, nil
	}

	var balances []EvictedBalance
	if err := json.Unmarshal([]byte(val), &balances); err != nil {
		return nil, fmt.Errorf("unmarshaling evicted balances: %w", err)
	}

	return balances, nil
}

func (s *accountTokenService) GetCheckpointLedger() uint32 {
	return s.checkpointLedger
}

func (s *accountTokenService) getEvictedContractBalances(ctx context.Context) (map[string][]EvictedBalance, error) {
	balancesByAccountAddress := make(map[string][]EvictedBalance)
	archiveIterator := ingest.NewHotArchiveIterator(ctx, s.archive, s.checkpointLedger)
	for ledgerEntry, err := range archiveIterator {
		if err != nil {
			return nil, fmt.Errorf("reading ledger entry: %w", err)
		}

		switch ledgerEntry.Data.Type {
		case xdr.LedgerEntryTypeContractData:
			contractDataEntry := ledgerEntry.Data.MustContractData()
			contractID, ok := contractDataEntry.Contract.GetContractId()
			if !ok {
				continue
			}
			contractIDStr := strkey.MustEncode(strkey.VersionByteContract, contractID[:])

			holderAddress, balance, ok := sac.ContractBalanceFromContractData(ledgerEntry, s.networkPassphrase)
			if !ok {
				continue
			}

			// Now that we have confirmed the contract is an SAC, we extract the authorization fields
			isAuthorized, isClawbackEnabled, err := s.extractAuthorizationFields(contractDataEntry)
			if err != nil {
				continue
			}

			holderAddressStr := strkey.MustEncode(strkey.VersionByteAccountID, holderAddress[:])
			balancesByAccountAddress[holderAddressStr] = append(balancesByAccountAddress[holderAddressStr], EvictedBalance{
				ContractID:        contractIDStr,
				Balance:           balance.String(),
				TokenType:         types.ContractTypeSAC,
				IsAuthorized:      isAuthorized,
				IsClawbackEnabled: isClawbackEnabled,
			})
		default:
			continue
		}

	}

	return balancesByAccountAddress, nil
}

// collectAccountTokensFromCheckpoint reads from a ChangeReader and collects all trustlines and contract balances.
// Returns maps of trustlines, contracts, contract types, and contract IDs grouped by WASM hash.
func (s *accountTokenService) collectAccountTokensFromCheckpoint(
	ctx context.Context,
	reader ingest.ChangeReader,
) (
	trustlines map[string][]string,
	contracts map[string][]string,
	contractTypesByContractID map[string]types.ContractType,
	contractIDsByWasmHash map[xdr.Hash][]string,
	err error,
) {
	// trustlines maps account addresses (G...) to their trustline assets formatted as "CODE:ISSUER"
	trustlines = make(map[string][]string)
	// contracts maps holder addresses (account G... or contract C...) to contract IDs (C...) they hold balances in
	contracts = make(map[string][]string)
	// contractTypes tracks the token type for each unique contract ID
	contractTypesByContractID = make(map[string]types.ContractType)
	contractIDsByWasmHash = make(map[xdr.Hash][]string)
	uniqueWasmHashes := set.NewSet[xdr.Hash]()

	entries := 0
	startTime := time.Now()
	lastLogTime := startTime

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, nil, nil, nil, fmt.Errorf("checkpoint processing cancelled: %w", ctx.Err())
		default:
		}

		change, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("reading checkpoint changes: %w", err)
		}

		//exhaustive:ignore
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

			//exhaustive:ignore
			switch contractDataEntry.Key.Type {
			case xdr.ScValTypeScvVec:
				// Extract the account/contract address from the contract data entry key.
				// We parse using the [Balance, holder_address] format that is followed by SEP-41 tokens.
				// However, this could also be valid for any non-SEP41 contract that mimics the same format.
				holderAddress, err := s.extractHolderAddress(contractDataEntry.Key)
				if err != nil {
					continue
				}

				// Extract the contract ID from the contract data entry
				contractAddress, err := s.extractContractID(contractDataEntry)
				if err != nil {
					continue
				}

				// Store contract info: map holder address to contract address and type
				contracts[holderAddress] = append(contracts[holderAddress], contractAddress)
				entries++

			case xdr.ScValTypeScvLedgerKeyContractInstance:
				contractAddress, err := s.extractContractID(contractDataEntry)
				if err != nil {
					continue
				}
				ledgerEntry := change.Post
				_, isSAC := sac.AssetFromContractData(*ledgerEntry, s.networkPassphrase)
				if isSAC {
					contractTypesByContractID[contractAddress] = types.ContractTypeSAC // Verified SAC
				} else {
					// For non-SAC contracts, we need to validate the contract spec to determine if it is a SEP-41 token.
					contractInstance := contractDataEntry.Val.MustInstance()
					if contractInstance.Executable.Type == xdr.ContractExecutableTypeContractExecutableWasm {
						// Extract the WASM hash
						if contractInstance.Executable.WasmHash != nil {
							wasmHash := *contractInstance.Executable.WasmHash
							contractIDsByWasmHash[wasmHash] = append(contractIDsByWasmHash[wasmHash], contractAddress)
							uniqueWasmHashes.Add(wasmHash)
						}
					}
				}
				entries++
			}
		}

		// Progress logging every progressInterval entries
		if entries%progressInterval == 0 && entries > 0 {
			elapsed := time.Since(lastLogTime)
			log.Ctx(ctx).Infof("Processed %d entries (%.0f entries/sec)", entries, float64(progressInterval)/elapsed.Seconds())
			lastLogTime = time.Now()
		}
	}

	log.Ctx(ctx).Infof("Processed %d checkpoint entries in %.2f minutes", entries, time.Since(startTime).Minutes())
	return trustlines, contracts, contractTypesByContractID, contractIDsByWasmHash, nil
}

// enrichContractTypes validates contract specs and enriches the contractTypesByContractID map with SEP-41 classifications.
func (s *accountTokenService) enrichContractTypes(
	ctx context.Context,
	contractTypesByContractID map[string]types.ContractType,
	contractIDsByWasmHash map[xdr.Hash][]string,
) error {
	uniqueWasmHashes := make([]xdr.Hash, 0, len(contractIDsByWasmHash))
	for wasmHash := range contractIDsByWasmHash {
		uniqueWasmHashes = append(uniqueWasmHashes, wasmHash)
	}

	// Validate the contract spec against known contract types
	contractTypesByWasmHash, err := s.contractSpecValidator.Validate(ctx, uniqueWasmHashes)
	if err != nil {
		return fmt.Errorf("validating contract spec: %w", err)
	}

	// Map WASM hash types back to individual contract IDs
	for wasmHash, contractType := range contractTypesByWasmHash {
		for _, contractAddress := range contractIDsByWasmHash[wasmHash] {
			contractTypesByContractID[contractAddress] = contractType
		}
	}

	return nil
}

// storeAccountTokensInRedis stores all collected trustlines and contracts into Redis using pipelining.
func (s *accountTokenService) storeAccountTokensInRedis(
	ctx context.Context,
	trustlines map[string][]string,
	contracts map[string][]string,
	contractTypesByContractID map[string]types.ContractType,
) error {
	startTime := time.Now()

	// Calculate total operations: trustlines + contracts + contract types
	totalOps := len(trustlines) + len(contracts) + len(contractTypesByContractID)
	redisPipelineOps := make([]store.RedisPipelineOperation, 0, totalOps)

	// Add trustline operations
	for accountAddress, assets := range trustlines {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildTrustlineKey(accountAddress),
			Members: assets,
		})
	}

	// Add contract operations
	for accountAddress, contractAddresses := range contracts {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:      store.SetOpAdd,
			Key:     s.buildContractKey(accountAddress),
			Members: contractAddresses,
		})
	}

	// Add contract type operations
	for contractID, contractType := range contractTypesByContractID {
		redisPipelineOps = append(redisPipelineOps, store.RedisPipelineOperation{
			Op:    store.OpSet,
			Key:   s.buildContractTypeKey(contractID),
			Value: string(contractType),
		})
	}

	// Execute operations in batches
	for i := 0; i < len(redisPipelineOps); i += redisPipelineBatchSize {
		end := min(i+redisPipelineBatchSize, len(redisPipelineOps))
		if err := s.redisStore.ExecutePipeline(ctx, redisPipelineOps[i:end]); err != nil {
			return fmt.Errorf("executing account tokens pipeline: %w", err)
		}
	}

	log.Ctx(ctx).Infof("Stored %d trustlines and %d contracts in Redis in %.2f minutes", len(trustlines), len(contracts), time.Since(startTime).Minutes())
	return nil
}

// storeEvictedBalancesInRedis stores evicted SAC balances into Redis using JSON serialization.
// Each account's balances are stored as a JSON array under the key evicted_balances:{accountAddress}.
func (s *accountTokenService) storeEvictedBalancesInRedis(
	ctx context.Context,
	balancesByAccountAddress map[string][]EvictedBalance,
) error {
	if len(balancesByAccountAddress) == 0 {
		return nil
	}

	startTime := time.Now()
	operations := make([]store.RedisPipelineOperation, 0, len(balancesByAccountAddress))

	// Serialize each account's balances as JSON and create pipeline operations
	for accountAddr, balances := range balancesByAccountAddress {
		data, err := json.Marshal(balances)
		if err != nil {
			return fmt.Errorf("marshaling evicted balances for %s: %w", accountAddr, err)
		}
		operations = append(operations, store.RedisPipelineOperation{
			Op:    store.OpSet,
			Key:   s.buildEvictedBalancesKey(accountAddr),
			Value: string(data),
		})
	}

	// Execute operations in batches using existing pipeline
	for i := 0; i < len(operations); i += redisPipelineBatchSize {
		end := min(i+redisPipelineBatchSize, len(operations))
		if err := s.redisStore.ExecutePipeline(ctx, operations[i:end]); err != nil {
			return fmt.Errorf("executing evicted balances pipeline: %w", err)
		}
	}

	log.Ctx(ctx).Infof("Stored evicted balances for %d accounts in Redis in %.2f seconds", len(balancesByAccountAddress), time.Since(startTime).Seconds())
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

// extractHolderAddress extracts the account address from a Stellar Asset Contract balance entry.
// Balance entries have a key that is a ScVec with 2 elements:
// - First element: ScSymbol("Balance")
// - Second element: ScAddress (the account/contract holder address)
// Returns the holder address as a Stellar-encoded string, or empty string if invalid.
func (s *accountTokenService) extractHolderAddress(key xdr.ScVal) (string, error) {
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
func (s *accountTokenService) extractContractID(contractData xdr.ContractDataEntry) (string, error) {
	// Check if the Contract field is of type CONTRACT (not ACCOUNT)
	if contractData.Contract.Type != xdr.ScAddressTypeScAddressTypeContract {
		return "", fmt.Errorf("contract address type is %v, expected ScAddressTypeContract", contractData.Contract.Type)
	}

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

// extractAuthorizationFields extracts the authorized and clawback enabled fields from a SAC balance entry.
// SAC balance entries store these as boolean fields in a map with keys "authorized" and "clawback".
func (s *accountTokenService) extractAuthorizationFields(contractData xdr.ContractDataEntry) (isAuthorized bool, isClawbackEnabled bool, err error) {
	// Get the balance map from the contract data value
	balanceMap, ok := contractData.Val.GetMap()
	if !ok || balanceMap == nil {
		return false, false, fmt.Errorf("contract data value is not a map")
	}

	var authorizedFound, clawbackFound bool

	// Iterate through the map entries to find authorized and clawback fields
	for _, entry := range *balanceMap {
		// Check if the key is a symbol
		if entry.Key.Type != xdr.ScValTypeScvSymbol {
			continue
		}

		keySymbol := string(entry.Key.MustSym())
		switch keySymbol {
		case "authorized":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return false, false, fmt.Errorf("authorized field is not bool type")
			}
			boolVal, ok := entry.Val.GetB()
			if !ok {
				return false, false, fmt.Errorf("failed to extract authorized boolean value")
			}
			isAuthorized = boolVal
			authorizedFound = true

		case "clawback":
			if entry.Val.Type != xdr.ScValTypeScvBool {
				return false, false, fmt.Errorf("clawback field is not bool type")
			}
			boolVal, ok := entry.Val.GetB()
			if !ok {
				return false, false, fmt.Errorf("failed to extract clawback boolean value")
			}
			isClawbackEnabled = boolVal
			clawbackFound = true
		}
	}

	// Validate that both required fields were found
	if !authorizedFound {
		return false, false, fmt.Errorf("authorized field not found in balance map")
	}
	if !clawbackFound {
		return false, false, fmt.Errorf("clawback field not found in balance map")
	}

	return isAuthorized, isClawbackEnabled, nil
}
