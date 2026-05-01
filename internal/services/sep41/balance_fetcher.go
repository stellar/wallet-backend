// Package sep41 — balance_fetcher.go owns the authoritative-balance read path.
//
// SEP-41 only standardizes the interface and event topics, not the math
// relating event amounts to balances. Fee-on-transfer, rebasing, and
// interest-bearing tokens all emit a transfer "amount" that is not the actual
// balance change. The only correct way to populate sep41_balances is to call
// `balance(addr)` on the contract via Soroban simulation. This file wraps
// services.ContractMetadataService.FetchSingleField for that purpose.
package sep41

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/services"
)

// balanceFetchBatchSize bounds the number of concurrent simulate calls per
// batch. Matches the metadata fetcher's batch size; we don't sleep between
// batches because this fetcher runs on the ledger commit critical path.
const balanceFetchBatchSize = 20

// balanceFetcher resolves authoritative SEP-41 balances by calling
// `balance(addr)` on the contract via RPC simulation. Holds a worker pool for
// parallel per-pair fetches.
type balanceFetcher struct {
	rpc  services.ContractMetadataService
	pool pond.Pool
}

// newBalanceFetcher returns a fetcher backed by the supplied
// ContractMetadataService. pool is owned by the caller. Returns nil when
// either dependency is nil so callers can degrade gracefully on cmd paths
// that have no RPC configured.
func newBalanceFetcher(rpc services.ContractMetadataService, pool pond.Pool) *balanceFetcher {
	if rpc == nil || pool == nil {
		return nil
	}
	return &balanceFetcher{rpc: rpc, pool: pool}
}

// FetchBalances calls balance(addr) for each pair. Returns a map of
// successful results keyed by balanceKey; pairs whose RPC failed are absent
// from the map and logged. Per-pair errors are intentionally non-fatal —
// surfacing one bad contract as a ledger failure would block ingest. Only
// ctx-cancellation or pool-level errors propagate.
func (f *balanceFetcher) FetchBalances(ctx context.Context, pairs []balanceKey) (map[balanceKey]*big.Int, error) {
	if f == nil || len(pairs) == 0 {
		return map[balanceKey]*big.Int{}, nil
	}

	var (
		mu  sync.Mutex
		out = make(map[balanceKey]*big.Int, len(pairs))
	)

	for i := 0; i < len(pairs); i += balanceFetchBatchSize {
		end := i + balanceFetchBatchSize
		if end > len(pairs) {
			end = len(pairs)
		}
		batch := pairs[i:end]

		group := f.pool.NewGroupContext(ctx)
		for _, k := range batch {
			k := k
			group.Submit(func() {
				bal, err := f.fetchOne(ctx, k.ContractID, k.Account)
				if err != nil {
					if isArchivedEntryErr(err) {
						log.Ctx(ctx).Warnf("sep41 balance() archived for %s on %s; leaving existing row untouched", k.Account, k.ContractID)
					} else {
						log.Ctx(ctx).Warnf("sep41 balance() fetch failed for %s on %s: %v", k.Account, k.ContractID, err)
					}
					return
				}
				mu.Lock()
				out[k] = bal
				mu.Unlock()
			})
		}

		if err := group.Wait(); err != nil {
			return nil, fmt.Errorf("error in SEP-41 balance fetch batch: %w", err)
		}
	}
	return out, nil
}

// fetchOne invokes `balance(addr)` on the contract and decodes the i128
// result into a *big.Int.
func (f *balanceFetcher) fetchOne(ctx context.Context, contractAddr, accountAddr string) (*big.Int, error) {
	scAddr, err := addressStrkeyToScVal(accountAddr)
	if err != nil {
		return nil, fmt.Errorf("encoding account address %s: %w", accountAddr, err)
	}
	val, err := f.rpc.FetchSingleField(ctx, contractAddr, "balance", scAddr)
	if err != nil {
		return nil, err
	}
	return extractI128(val)
}

// addressStrkeyToScVal converts a Stellar strkey address (G... account or
// C... contract) into an xdr.ScVal of type ScvAddress, suitable for passing
// as an argument to a Soroban contract call.
func addressStrkeyToScVal(address string) (xdr.ScVal, error) {
	if len(address) != 56 {
		return xdr.ScVal{}, fmt.Errorf("invalid address length: expected 56, got %d", len(address))
	}
	var scAddr xdr.ScAddress
	switch address[0] {
	case 'G':
		accountID, err := xdr.AddressToAccountId(address)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("decoding account address: %w", err)
		}
		scAddr = xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &accountID,
		}
	case 'C':
		raw, err := strkey.Decode(strkey.VersionByteContract, address)
		if err != nil {
			return xdr.ScVal{}, fmt.Errorf("decoding contract address: %w", err)
		}
		var id xdr.ContractId
		copy(id[:], raw)
		scAddr = xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &id,
		}
	default:
		return xdr.ScVal{}, fmt.Errorf("unrecognized address prefix %q", address[0])
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &scAddr}, nil
}

// archivedEntryMarkers are case-insensitive substrings that mark a Soroban
// simulation failure as "ledger entry archived; restore needed." These come
// back as plain error strings from stellar-rpc, not a structured error code.
var archivedEntryMarkers = []string{
	"archived",
	"entry_archived",
	"restorepreamble",
	"restore preamble",
	"requires restore",
}

func isArchivedEntryErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, m := range archivedEntryMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}
