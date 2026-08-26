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

// metadataFailureBackoff is how long we skip a contract after its metadata
// fetch fails. Every ledger that touches a known SEP-41 contract asks for its
// metadata again, so without this a contract we cannot reach costs 600ms of
// retry sleeps per ledger, forever. A var so tests can shorten it.
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

// fetchState is what the fetcher remembers about one contract.
type fetchState struct {
	// haveIt means the metadata is already stored, so never fetch this
	// contract again in this process.
	haveIt bool
	// retryAfter is when a failed fetch may be tried again. Ignored when
	// haveIt is set.
	retryAfter time.Time
}

// metadataFetcher resolves token metadata for SEP-41 contracts via RPC
// simulation, with a worker pool for parallel fetches inside one batch.
//
// Every ledger that touches a known SEP-41 contract asks for its metadata
// again, so the fetcher remembers what it has already settled: contracts whose
// metadata we hold are never fetched again in this process, and contracts whose
// fetch failed are skipped until their back-off passes. Prefetch seeds the
// first group from contract_tokens before any fetch runs, so a restart does not
// refetch what is already stored.
//
// A restart clears all of it, which is also how metadata that changed on chain
// gets picked up.
type metadataFetcher struct {
	rpc  services.ContractMetadataService
	pool pond.Pool

	cacheMu sync.Mutex
	state   map[string]fetchState
}

// newMetadataFetcher returns a fetcher backed by the supplied
// ContractMetadataService (which provides the generic FetchSingleField
// primitive). pool is owned by the caller.
func newMetadataFetcher(rpc services.ContractMetadataService, pool pond.Pool) *metadataFetcher {
	if rpc == nil || pool == nil {
		return nil
	}
	return &metadataFetcher{
		rpc:   rpc,
		pool:  pool,
		state: map[string]fetchState{},
	}
}

// filterCached drops contracts whose metadata we already have and contracts
// still inside their failure back-off, clearing back-off entries that have
// expired.
func (f *metadataFetcher) filterCached(contractIDs []string) []string {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	now := time.Now()
	kept := make([]string, 0, len(contractIDs))
	for _, id := range contractIDs {
		st, known := f.state[id]
		switch {
		case known && st.haveIt:
			continue
		case known && now.Before(st.retryAfter):
			continue
		case known:
			delete(f.state, id)
		}
		kept = append(kept, id)
	}
	return kept
}

// unknownAddrs returns the contracts whose metadata we do not already have.
// Prefetch asks contract_tokens about exactly these, so once every claimed
// contract is accounted for the query stops being issued at all.
func (f *metadataFetcher) unknownAddrs(contractIDs []string) []string {
	if f == nil {
		return nil
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	out := make([]string, 0, len(contractIDs))
	for _, id := range contractIDs {
		if st, known := f.state[id]; known && st.haveIt {
			continue
		}
		out = append(out, id)
	}
	return out
}

// markFetched records contracts whose metadata is already stored, so later
// FetchMetadata calls skip them without calling RPC. Prefetch passes in what it
// found in contract_tokens. Rows written by anything other than this process —
// an earlier run, or a direct SQL update — are only visible this way.
func (f *metadataFetcher) markFetched(contractIDs []string) {
	if f == nil || len(contractIDs) == 0 {
		return
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	for _, id := range contractIDs {
		f.state[id] = fetchState{haveIt: true}
	}
}

// recordFailure holds a contract back until the back-off passes. Only reached
// for a contract filterCached let through, which is never one we already have
// the metadata for.
func (f *metadataFetcher) recordFailure(contractID string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.state[contractID] = fetchState{retryAfter: time.Now().Add(metadataFailureBackoff)}
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
				f.markFetched([]string{contractID})
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
