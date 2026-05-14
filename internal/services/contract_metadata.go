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

// errors / strings imports above already cover everything the retry helpers need.

const (
	// simulateTransactionBatchSize is the number of contracts to process in parallel
	// when fetching metadata via RPC simulation.
	simulateTransactionBatchSize = 20

	// batchSleepDuration is the delay between batches to avoid overwhelming the RPC.
	batchSleepDuration = 2 * time.Second
)

// simulateMaxAttempts is the upper bound on retries for transient RPC failures
// inside a single FetchSingleField call. Permanent errors bail on the first
// attempt; transient errors retry with exponential backoff. Declared as a var
// (rather than const) so tests can override to 1 to keep mock-call expectations
// readable.
var simulateMaxAttempts = 3

// simulateInitialBackoff is the first sleep between retries; subsequent retries
// double it (200ms, 400ms in the worst case before giving up).
var simulateInitialBackoff = 200 * time.Millisecond

// transientSimulateErrorSubstrings are case-insensitive markers we treat as
// transient when surfaced from the RPC. Adding to this list is the supported
// way to widen retry coverage — keep it small and well-justified.
var transientSimulateErrorSubstrings = []string{
	"latency",                 // public RPC: "latency since last known ledger closed is too high"
	"timeout",                 // generic timeout
	"connection refused",      // local/temporary network failure
	"connection reset",        // transient TCP reset
	"i/o timeout",             // Go net read/write deadline
	"temporarily unavailable", // 503-style RPC backpressure
	"too many requests",       // 429
}

// isTransientSimulateErr reports whether an error from SimulateTransaction is
// worth retrying. Net-level failures and known-transient RPC error strings
// retry; everything else (bad inputs, missing functions, contract errors)
// bails on the first attempt to avoid masking real problems.
func isTransientSimulateErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false // caller cancelled — don't retry past their deadline
	}
	msg := strings.ToLower(err.Error())
	for _, s := range transientSimulateErrorSubstrings {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// ContractMetadataService handles fetching metadata for Stellar Asset Contract
// (SAC) tokens via RPC simulation. Protocol-specific metadata (e.g. SEP-41
// name/symbol/decimals) lives inside the per-protocol package — this surface
// is intentionally limited to SAC (a Stellar primitive) plus the
// FetchSingleField primitive that any per-protocol validator can compose on.
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

	// Simulate the transaction with bounded retries on transient RPC errors
	// (e.g., public-RPC stale-ledger/latency complaints). Permanent errors —
	// missing function, malformed args, contract reverts — bail on the first
	// attempt so we don't mask real problems behind retries.
	backoff := simulateInitialBackoff
	var lastErr error
	for attempt := 1; attempt <= simulateMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return xdr.ScVal{}, fmt.Errorf("context error: %w", err)
		}

		result, err := s.rpcService.SimulateTransaction(txXDR, entities.RPCResourceConfig{})
		if err != nil {
			if !isTransientSimulateErr(err) {
				return xdr.ScVal{}, fmt.Errorf("simulating transaction: %w", err)
			}
			lastErr = fmt.Errorf("simulating transaction: %w", err)
		} else if result.Error != "" {
			simErr := fmt.Errorf("simulation failed: %s", result.Error)
			if !isTransientSimulateErr(simErr) {
				return xdr.ScVal{}, simErr
			}
			lastErr = simErr
		} else if len(result.Results) == 0 {
			// Empty results aren't classified as transient — surface immediately.
			return xdr.ScVal{}, fmt.Errorf("no simulation results returned")
		} else {
			return result.Results[0].XDR, nil
		}

		if attempt < simulateMaxAttempts {
			log.Ctx(ctx).Debugf("simulate %s.%s transient err (attempt %d/%d): %v", contractAddress, functionName, attempt, simulateMaxAttempts, lastErr)
			select {
			case <-ctx.Done():
				return xdr.ScVal{}, fmt.Errorf("context error: %w", ctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
	return xdr.ScVal{}, fmt.Errorf("simulate %s.%s after %d attempts: %w", contractAddress, functionName, simulateMaxAttempts, lastErr)
}
