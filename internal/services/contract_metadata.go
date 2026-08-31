// Package services provides business logic for the wallet-backend.
// This file implements ContractMetadataService, an RPC-simulation helper for
// reading single contract fields (name, symbol, decimals, balance, ...).
package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/entities"
)

// errors / strings imports above already cover everything the retry helpers need.

const (
	// SimulateTransactionBatchSize is the number of contracts to process in parallel
	// when fetching metadata via RPC simulation. Exported so callers that size a
	// worker pool to serve this batch size (the SEP-41 validator's private pool)
	// reference the same value instead of a hardcoded copy that can drift out of sync.
	SimulateTransactionBatchSize = 20
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

// ContractMetadataService fetches contract state via RPC simulation.
// Protocol-specific metadata (e.g. SEP-41 name/symbol/decimals) lives inside
// the per-protocol package; this surface is intentionally limited to the
// FetchSingleField primitive that any per-protocol validator can compose on.
type ContractMetadataService interface {
	// FetchSingleField fetches a single contract method (name, symbol, decimals, balance, etc...) via RPC simulation.
	// The args parameter allows passing arguments to the contract function (e.g., address for balance(id) function).
	FetchSingleField(ctx context.Context, contractAddress, functionName string, args ...xdr.ScVal) (xdr.ScVal, error)
}

var _ ContractMetadataService = (*contractMetadataService)(nil)

type contractMetadataService struct {
	rpcService   RPCService
	dummyAccount *keypair.Full
}

// NewContractMetadataService creates a new ContractMetadataService instance.
func NewContractMetadataService(rpcService RPCService) (ContractMetadataService, error) {
	if rpcService == nil {
		return nil, fmt.Errorf("rpcService cannot be nil")
	}

	return &contractMetadataService{
		rpcService:   rpcService,
		dummyAccount: keypair.MustRandom(),
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
