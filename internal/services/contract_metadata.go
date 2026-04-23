// Package services provides business logic for the wallet-backend.
// This file implements ContractMetadataService for fetching SAC token metadata via RPC.
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

// ContractMetadataService handles fetching metadata (name, symbol, decimals)
// for SAC token contracts via RPC simulation.
type ContractMetadataService interface {
	// FetchSACMetadata fetches metadata for SAC contracts by calling name() via RPC.
	// SAC name() returns "code:issuer" format (or "native" for XLM).
	// Returns []*data.Contract with Code, Issuer, Name, Symbol, and Decimals=7.
	FetchSACMetadata(ctx context.Context, contractIDs []string) ([]*data.Contract, error)
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

// FetchSACMetadata fetches metadata for SAC contracts by calling name() via RPC.
// SAC contracts return "code:issuer" format from name() (or "native" for XLM).
// Returns []*data.Contract with Code, Issuer, Name, Symbol, and Decimals=7 (hardcoded for Stellar assets).
// This function fails fast if any contract metadata fetch fails - partial results are not returned.
func (s *contractMetadataService) FetchSACMetadata(ctx context.Context, contractIDs []string) ([]*data.Contract, error) {
	if len(contractIDs) == 0 {
		return []*data.Contract{}, nil
	}

	start := time.Now()
	var (
		contracts   []*data.Contract
		mu          sync.Mutex
		fetchErrors []error
	)

	// Process in batches to avoid overwhelming the RPC
	for i := 0; i < len(contractIDs); i += simulateTransactionBatchSize {
		end := min(i+simulateTransactionBatchSize, len(contractIDs))
		batch := contractIDs[i:end]

		group := s.pool.NewGroupContext(ctx)
		for _, contractID := range batch {
			group.Submit(func() {
				contract, err := s.fetchSACMetadataForContract(ctx, contractID)
				if err != nil {
					mu.Lock()
					fetchErrors = append(fetchErrors, fmt.Errorf("contract %s: %w", contractID, err))
					mu.Unlock()
					return
				}
				mu.Lock()
				contracts = append(contracts, contract)
				mu.Unlock()
			})
		}

		if err := group.Wait(); err != nil {
			return nil, fmt.Errorf("error in SAC metadata batch: %w", err)
		}

		// Sleep between batches to avoid overwhelming the RPC (skip for last batch)
		if end < len(contractIDs) {
			time.Sleep(batchSleepDuration)
		}
	}

	// Fail if any contract metadata fetch failed - partial results are not acceptable
	if len(fetchErrors) > 0 {
		return nil, fmt.Errorf("failed to fetch metadata for %d SAC contracts: %w", len(fetchErrors), errors.Join(fetchErrors...))
	}

	log.Ctx(ctx).Infof("Fetched metadata for %d SAC contracts in %.4f seconds", len(contracts), time.Since(start).Seconds())
	return contracts, nil
}

// fetchSACMetadataForContract fetches metadata for a single SAC contract by calling name().
// Parses the name as "code:issuer" format or handles "native" as XLM.
func (s *contractMetadataService) fetchSACMetadataForContract(ctx context.Context, contractID string) (*data.Contract, error) {
	nameVal, err := s.FetchSingleField(ctx, contractID, "name")
	if err != nil {
		return nil, fmt.Errorf("fetching name: %w", err)
	}

	nameStr, ok := nameVal.GetStr()
	if !ok {
		return nil, fmt.Errorf("name value is not a string")
	}
	name := string(nameStr)

	var code, issuer string
	if name == "native" {
		// Native XLM asset
		code = "XLM"
		issuer = ""
	} else {
		// Parse "code:issuer" format
		parts := strings.SplitN(name, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed SAC name '%s': expected 'code:issuer' format", name)
		}
		code = parts[0]
		issuer = parts[1]
	}

	return &data.Contract{
		ID:         data.DeterministicContractID(contractID),
		ContractID: contractID,
		Type:       string(types.ContractTypeSAC),
		Code:       &code,
		Issuer:     &issuer,
		Name:       &name,
		Symbol:     &code,
		Decimals:   7, // Stellar assets always use 7 decimals
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
