// Package sep41 — metadata.go owns the SEP-41 token metadata fetch path. It
// uses services.ContractMetadataService.FetchSingleField (a generic helper)
// to invoke name(), symbol(), and decimals() via RPC simulation against
// SEP-41 contracts. Per-contract RPC failures are tolerated; missing entries
// in the returned map signal "could not fetch — caller should fall back to
// defaults."
//
// This path is private to the sep41 package — the framework knows nothing
// about token metadata or these particular view functions. Other protocols
// that need their own enrichment write their own equivalents.
package sep41

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/services"
)

const (
	// metadataBatchSize is the number of contracts processed in parallel per
	// RPC simulation batch. Tuned the same as the previous generic
	// simulateTransactionBatchSize so behavior is preserved verbatim.
	metadataBatchSize = 20

	// metadataBatchSleep is the delay between batches to avoid overwhelming
	// the RPC.
	metadataBatchSleep = 2 * time.Second

	// maxTokenDecimals caps SEP-41 decimals() at a realistic upper bound. Real
	// tokens use ≤ 18; this also keeps the value inside Postgres INTEGER range,
	// since SEP-41 technically permits a u32 that exceeds INT32_MAX. A contract
	// returning more than this is treated as malicious and dropped (caller
	// persists defaults).
	maxTokenDecimals uint32 = 70

	// maxTokenNameLength / maxTokenSymbolLength bound attacker-controlled
	// strings from SEP-41 name() and symbol(), measured in bytes. Real tokens
	// are well under these.
	maxTokenNameLength   = 128
	maxTokenSymbolLength = 32
)

// validateTokenString enforces the invariants a SEP-41 string field must
// satisfy before it can be safely persisted to a Postgres TEXT column:
// bounded length, valid UTF-8, and no NUL bytes. PG TEXT (in a UTF-8 DB)
// rejects both invalid UTF-8 and 0x00, so letting either through would wedge
// the ledger-persist transaction.
func validateTokenString(fieldName, value string, maxLen int) error {
	if len(value) > maxLen {
		return fmt.Errorf("%s exceeds %d byte cap (got %d)", fieldName, maxLen, len(value))
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", fieldName)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%s contains NUL byte", fieldName)
	}
	return nil
}

// metadataFetcher resolves token metadata for newly classified SEP-41
// contracts via RPC simulation. Holds an internal worker pool for parallel
// fetches inside a single batch.
type metadataFetcher struct {
	rpc  services.ContractMetadataService
	pool pond.Pool
}

// newMetadataFetcher returns a fetcher backed by the supplied
// ContractMetadataService (which provides the generic FetchSingleField
// primitive). pool is owned by the caller.
func newMetadataFetcher(rpc services.ContractMetadataService, pool pond.Pool) *metadataFetcher {
	if rpc == nil || pool == nil {
		return nil
	}
	return &metadataFetcher{rpc: rpc, pool: pool}
}

// FetchMetadata returns name/symbol/decimals for each contract, keyed by
// C-address. Per-contract failures are logged and the contract is omitted
// from the map; only context errors propagate.
func (f *metadataFetcher) FetchMetadata(ctx context.Context, contractIDs []string) (map[string]*data.Contract, error) {
	if f == nil || len(contractIDs) == 0 {
		return map[string]*data.Contract{}, nil
	}

	var (
		mu  sync.Mutex
		out = make(map[string]*data.Contract, len(contractIDs))
	)

	for i := 0; i < len(contractIDs); i += metadataBatchSize {
		end := i + metadataBatchSize
		if end > len(contractIDs) {
			end = len(contractIDs)
		}
		batch := contractIDs[i:end]

		group := f.pool.NewGroupContext(ctx)
		for _, contractID := range batch {
			contractID := contractID
			group.Submit(func() {
				contract, err := f.fetchOne(ctx, contractID)
				if err != nil {
					log.Ctx(ctx).Warnf("sep41 metadata fetch failed for %s: %v", contractID, err)
					return
				}
				mu.Lock()
				out[contractID] = contract
				mu.Unlock()
			})
		}

		if err := group.Wait(); err != nil {
			// Pool errors (typically ctx cancellation) are fatal — callers want to stop.
			return nil, fmt.Errorf("error in SEP-41 metadata batch: %w", err)
		}

		if end < len(contractIDs) {
			select {
			case <-ctx.Done():
				return out, fmt.Errorf("waiting between SEP-41 metadata batches: %w", ctx.Err())
			case <-time.After(metadataBatchSleep):
			}
		}
	}

	return out, nil
}

// fetchOne pulls name, symbol, and decimals for a single contract.
func (f *metadataFetcher) fetchOne(ctx context.Context, contractID string) (*data.Contract, error) {
	nameVal, err := f.rpc.FetchSingleField(ctx, contractID, "name")
	if err != nil {
		return nil, fmt.Errorf("fetching name: %w", err)
	}
	nameStr, ok := nameVal.GetStr()
	if !ok {
		return nil, fmt.Errorf("name is not a string")
	}
	if err := validateTokenString("name", string(nameStr), maxTokenNameLength); err != nil {
		return nil, err
	}

	symbolVal, err := f.rpc.FetchSingleField(ctx, contractID, "symbol")
	if err != nil {
		return nil, fmt.Errorf("fetching symbol: %w", err)
	}
	symbolStr, ok := symbolVal.GetStr()
	if !ok {
		return nil, fmt.Errorf("symbol is not a string")
	}
	if err := validateTokenString("symbol", string(symbolStr), maxTokenSymbolLength); err != nil {
		return nil, err
	}

	decimalsVal, err := f.rpc.FetchSingleField(ctx, contractID, "decimals")
	if err != nil {
		return nil, fmt.Errorf("fetching decimals: %w", err)
	}
	decimalsU32, ok := decimalsVal.GetU32()
	if !ok {
		return nil, fmt.Errorf("decimals is not a u32")
	}
	if uint32(decimalsU32) > maxTokenDecimals {
		return nil, fmt.Errorf("decimals exceeds cap of %d (got %d)", maxTokenDecimals, decimalsU32)
	}

	name := string(nameStr)
	symbol := string(symbolStr)
	return &data.Contract{
		ID:         data.DeterministicContractID(contractID),
		ContractID: contractID,
		Type:       contractTokenType,
		Name:       &name,
		Symbol:     &symbol,
		Decimals:   uint32(decimalsU32),
	}, nil
}
