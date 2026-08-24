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

// metadataBatchSize is the number of contracts processed in parallel per RPC
// simulation batch. metadataBatchSleep is the delay between batches to avoid
// overwhelming the RPC. Declared as vars (not consts) so tests can shorten
// them — mirrors simulateMaxAttempts / simulateInitialBackoff in
// contract_metadata.go.
var (
	metadataBatchSize  = 20
	metadataBatchSleep = 2 * time.Second
)

// metadataFailureBackoff is how long a contract whose metadata fetch failed
// is skipped before being tried again. Claimed contracts re-enter the fetch
// path on every ledger they are active in, so without this a contract whose
// name()/symbol() simulation persistently fails (unreachable RPC, reverting
// token) costs its full retry-with-backoff on every single ledger. A var so
// tests can shorten it.
var metadataFailureBackoff = 5 * time.Minute

const (
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
// fetches inside a single batch, plus two caches that bound repeat work:
// claimed contracts re-enter the fetch path on every ledger whose changes
// touch them, and Prefetch deliberately has no database access to check what
// is already persisted.
//   - fetched: contracts resolved once in this process are never fetched
//     again; their metadata is already persisted by Apply. A restart refetches
//     each contract once, which doubles as the refresh path for tokens whose
//     on-chain metadata changed.
//   - failedUntil: contracts whose fetch failed are skipped until the
//     metadataFailureBackoff deadline, so a persistently failing name()
//     simulation (unreachable RPC, reverting token) does not cost its full
//     retry-with-backoff on every single ledger.
type metadataFetcher struct {
	rpc  services.ContractMetadataService
	pool pond.Pool

	cacheMu     sync.Mutex
	fetched     map[string]struct{}
	failedUntil map[string]time.Time
}

// newMetadataFetcher returns a fetcher backed by the supplied
// ContractMetadataService (which provides the generic FetchSingleField
// primitive). pool is owned by the caller.
func newMetadataFetcher(rpc services.ContractMetadataService, pool pond.Pool) *metadataFetcher {
	if rpc == nil || pool == nil {
		return nil
	}
	return &metadataFetcher{
		rpc:         rpc,
		pool:        pool,
		fetched:     map[string]struct{}{},
		failedUntil: map[string]time.Time{},
	}
}

// filterCached drops contracts already fetched in this process and contracts
// still inside their failure-backoff window, pruning expired failure entries.
func (f *metadataFetcher) filterCached(contractIDs []string) []string {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	now := time.Now()
	kept := make([]string, 0, len(contractIDs))
	for _, id := range contractIDs {
		if _, done := f.fetched[id]; done {
			continue
		}
		until, failed := f.failedUntil[id]
		if failed && now.Before(until) {
			continue
		}
		if failed {
			delete(f.failedUntil, id)
		}
		kept = append(kept, id)
	}
	return kept
}

// markFetched records contracts whose metadata is already persisted, so
// later FetchMetadata calls skip them without an RPC attempt. Apply calls it
// with what the database holds — the durable complement to the in-process
// fetched set, which alone would re-fetch every known token once per process
// lifetime, and forever on a deployment whose RPC can never resolve metadata
// (the loadtest rig's dead endpoint with externally seeded rows).
func (f *metadataFetcher) markFetched(contractIDs []string) {
	if f == nil || len(contractIDs) == 0 {
		return
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	for _, id := range contractIDs {
		f.fetched[id] = struct{}{}
		delete(f.failedUntil, id)
	}
}

func (f *metadataFetcher) recordFailure(contractID string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.failedUntil[contractID] = time.Now().Add(metadataFailureBackoff)
}

func (f *metadataFetcher) recordSuccess(contractID string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.fetched[contractID] = struct{}{}
	delete(f.failedUntil, contractID)
}

// FetchMetadata returns name/symbol/decimals for each contract, keyed by
// C-address. Per-contract failures are logged and the contract is omitted
// from the map; only context errors propagate.
func (f *metadataFetcher) FetchMetadata(ctx context.Context, contractIDs []string) (map[string]*data.Contract, error) {
	if f == nil || len(contractIDs) == 0 {
		return map[string]*data.Contract{}, nil
	}
	contractIDs = f.filterCached(contractIDs)
	if len(contractIDs) == 0 {
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
					f.recordFailure(contractID)
					log.Ctx(ctx).Warnf("sep41 metadata fetch failed for %s (next attempt in %s): %v", contractID, metadataFailureBackoff, err)
					return
				}
				f.recordSuccess(contractID)
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
