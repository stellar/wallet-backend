// Package services provides business logic for the wallet-backend.
// This file implements ContractMetadataService for fetching and storing SAC/SEP-41 token metadata.
package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	// simulateTransactionBatchSize is the number of contracts to process in parallel
	// when fetching metadata via RPC simulation.
	simulateTransactionBatchSize = 20

	// batchSleepDuration is the delay between batches to avoid overwhelming the RPC.
	batchSleepDuration = 2 * time.Second
)

// ContractMetadata holds the metadata for a contract token (name, symbol, decimals).
type ContractMetadata struct {
	ContractID string
	Type       types.ContractType
	Code       string // For SAC: extracted from name (CODE:ISSUER)
	Issuer     string // For SAC: extracted from name (CODE:ISSUER)
	Name       string
	Symbol     string
	Decimals   uint32
}

// ContractMetadataService handles fetching and storing metadata (name, symbol, decimals)
// for Stellar Asset Contract (SAC) and SEP-41 token contracts via RPC simulation.
type ContractMetadataService interface {
	// FetchAndStoreMetadata fetches metadata for the given contracts and stores in the database.
	// Parameters:
	//   - contractTypesByID: map of contractID to contract type (SAC or SEP-41)
	// Returns error only for critical failures; individual fetch failures are logged.
	FetchAndStoreMetadata(ctx context.Context, contractTypesByID map[string]types.ContractType) error
	// FetchSingleField fetches a single contract method (name, symbol, decimals, balance, etc...) via RPC simulation.
	// The args parameter allows passing arguments to the contract function (e.g., address for balance(id) function).
	FetchSingleField(ctx context.Context, contractAddress, functionName string, args ...xdr.ScVal) (xdr.ScVal, error)
}

var _ ContractMetadataService = (*contractMetadataService)(nil)

type contractMetadataService struct {
	rpcService    RPCService
	contractModel data.ContractModelInterface
	pool          pond.Pool
	dummyAccount  *keypair.Full
}

// NewContractMetadataService creates a new ContractMetadataService instance.
func NewContractMetadataService(
	rpcService RPCService,
	contractModel data.ContractModelInterface,
	pool pond.Pool,
) (ContractMetadataService, error) {
	if rpcService == nil {
		return nil, fmt.Errorf("rpcService cannot be nil")
	}
	if contractModel == nil {
		return nil, fmt.Errorf("contractModel cannot be nil")
	}
	if pool == nil {
		return nil, fmt.Errorf("pool cannot be nil")
	}

	return &contractMetadataService{
		rpcService:    rpcService,
		contractModel: contractModel,
		pool:          pool,
		dummyAccount:  keypair.MustRandom(),
	}, nil
}

// FetchAndStoreMetadata fetches metadata for contracts and stores in database.
func (s *contractMetadataService) FetchAndStoreMetadata(ctx context.Context, contractTypesByID map[string]types.ContractType) error {
	if len(contractTypesByID) == 0 {
		log.Ctx(ctx).Info("No contracts to fetch metadata for")
		return nil
	}

	// Build initial metadata map and contract IDs slice
	metadataMap := make(map[string]ContractMetadata, len(contractTypesByID))
	contractIDs := make([]string, 0, len(contractTypesByID))
	for contractID, contractType := range contractTypesByID {
		metadataMap[contractID] = ContractMetadata{
			ContractID: contractID,
			Type:       contractType,
		}
		contractIDs = append(contractIDs, contractID)
	}

	// Fetch metadata in parallel batches
	start := time.Now()
	metadataMap = s.fetchBatch(ctx, metadataMap, contractIDs)
	log.Ctx(ctx).Infof("Fetched metadata for %d contracts in %.2f minutes", len(metadataMap), time.Since(start).Minutes())

	// Parse SAC code:issuer from name field
	s.parseSACMetadata(metadataMap)

	// Store in database
	start = time.Now()
	err := s.storeInDB(ctx, metadataMap)
	log.Ctx(ctx).Infof("Stored metadata for %d contracts in %.2f seconds", len(metadataMap), time.Since(start).Seconds())
	return err
}

// fetchMetadata fetches name, symbol, and decimals for a single contract in parallel.
// Returns ContractMetadata with the fetched values.
func (s *contractMetadataService) fetchMetadata(ctx context.Context, contractID string, contractType types.ContractType) (ContractMetadata, error) {
	group := s.pool.NewGroupContext(ctx)

	var (
		name     string
		symbol   string
		decimals uint32
		errs     []error
		mu       sync.Mutex
	)

	appendError := func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	// Fetch name
	group.Submit(func() {
		nameVal, err := s.FetchSingleField(ctx, contractID, "name")
		if err != nil {
			appendError(fmt.Errorf("fetching name: %w", err))
			return
		}
		nameStr, ok := nameVal.GetStr()
		if !ok {
			appendError(fmt.Errorf("name value is not a string"))
			return
		}
		mu.Lock()
		name = string(nameStr)
		mu.Unlock()
	})

	// Fetch symbol
	group.Submit(func() {
		symbolVal, err := s.FetchSingleField(ctx, contractID, "symbol")
		if err != nil {
			appendError(fmt.Errorf("fetching symbol: %w", err))
			return
		}
		symbolStr, ok := symbolVal.GetStr()
		if !ok {
			appendError(fmt.Errorf("symbol value is not a string"))
			return
		}
		mu.Lock()
		symbol = string(symbolStr)
		mu.Unlock()
	})

	// Fetch decimals
	group.Submit(func() {
		decimalsVal, err := s.FetchSingleField(ctx, contractID, "decimals")
		if err != nil {
			appendError(fmt.Errorf("fetching decimals: %w", err))
			return
		}
		decimalsU32, ok := decimalsVal.GetU32()
		if !ok {
			appendError(fmt.Errorf("decimals value is not a uint32"))
			return
		}
		mu.Lock()
		decimals = uint32(decimalsU32)
		mu.Unlock()
	})

	if err := group.Wait(); err != nil {
		return ContractMetadata{}, fmt.Errorf("waiting for metadata fetch group for contract %s: %w", contractID, err)
	}

	if len(errs) > 0 {
		return ContractMetadata{}, fmt.Errorf("fetching contract metadata for contract %s: %w", contractID, errors.Join(errs...))
	}

	return ContractMetadata{
		ContractID: contractID,
		Type:       contractType,
		Name:       name,
		Symbol:     symbol,
		Decimals:   decimals,
	}, nil
}

// FetchSingleField fetches a single contract method (name, symbol, decimals, balance, etc.) via RPC simulation.
// The args parameter allows passing arguments to the contract function (e.g., address for balance(id) function).
func (s *contractMetadataService) FetchSingleField(ctx context.Context, contractAddress, functionName string, args ...xdr.ScVal) (xdr.ScVal, error) {
	if err := ctx.Err(); err != nil {
		return xdr.ScVal{}, fmt.Errorf("context error: %w", err)
	}

	// Decode contract ID from string
	contractIDBytes, err := strkey.Decode(strkey.VersionByteContract, contractAddress)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("decoding contract address: %w", err)
	}
	contractID := xdr.ContractId(contractIDBytes)

	// Build invoke operation
	invokeOp := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol(functionName),
				Args:         xdr.ScVec(args),
			},
		},
	}

	// Build transaction with dummy source account (simulation doesn't need real account)
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: s.dummyAccount.Address(), Sequence: 0},
		Operations:           []txnbuild.Operation{invokeOp},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("building transaction: %w", err)
	}

	// Encode transaction to XDR
	txXDR, err := tx.Base64()
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("encoding transaction: %w", err)
	}

	// Simulate transaction
	result, err := s.rpcService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("simulating transaction: %w", err)
	}

	if result.Error != "" {
		return xdr.ScVal{}, fmt.Errorf("simulation failed: %s", result.Error)
	}

	if len(result.Results) == 0 {
		return xdr.ScVal{}, fmt.Errorf("no simulation results returned")
	}

	return result.Results[0].XDR, nil
}

// fetchBatch fetches metadata for multiple contracts in parallel using pond groups.
// Processes contracts in batches to limit RPC load.
func (s *contractMetadataService) fetchBatch(ctx context.Context, metadataMap map[string]ContractMetadata, contractIDs []string) map[string]ContractMetadata {
	var mu sync.Mutex

	for i := 0; i < len(contractIDs); i += simulateTransactionBatchSize {
		end := min(i+simulateTransactionBatchSize, len(contractIDs))
		contractIDsBatch := contractIDs[i:end]

		group := s.pool.NewGroupContext(ctx)
		for _, contractID := range contractIDsBatch {
			group.Submit(func() {
				existing := metadataMap[contractID]
				metadata, err := s.fetchMetadata(ctx, contractID, existing.Type)
				if err != nil {
					log.Ctx(ctx).Warnf("Failed to fetch metadata for contract %s: %v", contractID, err)
					return
				}

				mu.Lock()
				if metadata.Name != "" {
					existing.Name = metadata.Name
				}
				if metadata.Symbol != "" {
					existing.Symbol = metadata.Symbol
				}
				if metadata.Decimals != 0 {
					existing.Decimals = metadata.Decimals
				}
				metadataMap[contractID] = existing
				mu.Unlock()
			})
		}

		if err := group.Wait(); err != nil {
			log.Ctx(ctx).Warnf("Error waiting for batch metadata fetch: %v", err)
		}
		time.Sleep(batchSleepDuration)
	}
	return metadataMap
}

// storeInDB stores contract metadata in the contract_tokens database table.
func (s *contractMetadataService) storeInDB(ctx context.Context, metadataMap map[string]ContractMetadata) error {
	if len(metadataMap) == 0 {
		log.Ctx(ctx).Info("No contract metadata to store in database")
		return nil
	}

	// Build contracts slice from metadata map
	contracts := make([]*data.Contract, 0, len(metadataMap))
	for _, metadata := range metadataMap {
		contracts = append(contracts, &data.Contract{
			ID:       metadata.ContractID,
			Type:     string(metadata.Type),
			Code:     &metadata.Code,
			Issuer:   &metadata.Issuer,
			Name:     &metadata.Name,
			Symbol:   &metadata.Symbol,
			Decimals: metadata.Decimals,
		})
	}

	// Batch insert all contracts
	insertedIDs, err := s.contractModel.BatchInsert(ctx, nil, contracts)
	if err != nil {
		return fmt.Errorf("storing contract metadata in database: %w", err)
	}

	log.Ctx(ctx).Infof("Successfully stored metadata for %d/%d contracts in database", len(insertedIDs), len(metadataMap))
	return nil
}

// parseSACMetadata parses the code:issuer format from SAC token names and populates the Code and Issuer fields.
func (s *contractMetadataService) parseSACMetadata(metadataMap map[string]ContractMetadata) {
	for contractID, metadata := range metadataMap {
		if metadata.Type != types.ContractTypeSAC {
			continue
		}
		if metadata.Name == "" {
			continue
		}

		parts := strings.Split(metadata.Name, ":")
		if len(parts) != 2 {
			continue
		}

		metadata.Code = parts[0]
		metadata.Issuer = parts[1]
		metadataMap[contractID] = metadata
	}
}
