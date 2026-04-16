package resolvers

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/stellar/wallet-backend/internal/data"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	// maxBalancePageLimit is intentionally smaller than "load everything" behavior
	// because balances can fan out across trustlines and contract tokens for a single account.
	maxBalancePageLimit int32 = 100
	// balanceCursorPrefix versions the opaque cursor payload so future cursor
	// formats can be introduced without silently misreading old cursors.
	balanceCursorPrefix = "v1"
)

type balanceSource string

const (
	// The source order is the canonical connection order exposed by Account.balances.
	// Cursors are opaque to clients, but the resolver must keep this ordering stable
	// so forward/backward pagination works consistently across mixed balance types.
	balanceSourceNative  balanceSource = "native"
	balanceSourceClassic balanceSource = "classic"
	balanceSourceSAC     balanceSource = "sac"
	balanceSourceSEP41   balanceSource = "sep41"
)

// balanceCursor is the decoded form of our opaque cursor payload:
//
//	base64("v1:<source>:<id>")
//
// The cursor represents a position in the single logical Account.balances
// connection, not just a position inside one backing table. The <source> part
// tells the resolver which source owned the last returned edge, and the <id>
// part is that source's local keyset cursor.
//
// The ID is source-specific:
// - native -> literal "native"
// - classic -> trustline asset UUID
// - sac/sep41 -> contract UUID
//
// Example:
//   - canonical order: native, classic(A), classic(B), sep41(X)
//   - endCursor after returning classic(B): base64("v1:classic:<B-uuid>")
//   - next forward page skips native entirely, resumes classic after B, then
//     continues into later sources if it still needs more rows.
type balanceCursor struct {
	Source balanceSource
	ID     string
}

// balanceNode keeps the GraphQL node together with the internal key used to
// rebuild a stable cursor after the page has been assembled.
type balanceNode struct {
	Balance graphql1.Balance
	Source  balanceSource
	ID      string
}

func (n *balanceNode) CursorID() string {
	return fmt.Sprintf("%s:%s:%s", balanceCursorPrefix, n.Source, n.ID)
}

// balanceBadUserInputError normalizes pagination validation failures to the
// GraphQL error code used elsewhere in the API for client-correctable input issues.
func balanceBadUserInputError(message string) error {
	return &gqlerror.Error{
		Message: message,
		Extensions: map[string]interface{}{
			"code": "BAD_USER_INPUT",
		},
	}
}

// balanceInternalError hides storage/RPC details from clients while preserving
// a stable machine-readable error code for operational failures.
func balanceInternalError() error {
	return &gqlerror.Error{
		Message: ErrMsgBalancesFetchFailed,
		Extensions: map[string]interface{}{
			"code": "INTERNAL_ERROR",
		},
	}
}

// balanceSourcesForAddress returns the ordered set of balance sources that can
// apply to the requested address type.
//
// G-addresses can have native XLM, classic trustlines, and SEP-41 balances.
// C-addresses can hold SAC balances and SEP-41 balances, but never native/classic rows.
func balanceSourcesForAddress(address string) []balanceSource {
	if utils.IsContractAddress(address) {
		return []balanceSource{balanceSourceSAC, balanceSourceSEP41}
	}
	return []balanceSource{balanceSourceNative, balanceSourceClassic, balanceSourceSEP41}
}

// balanceSourceIndex maps a source to its canonical order position. The walkers
// use this to decide which sources fall before or after the decoded cursor.
func balanceSourceIndex(sources []balanceSource, source balanceSource) int {
	return slices.Index(sources, source)
}

// parseBalancePaginationParams layers balances-specific policy on top of the
// shared Relay pagination parser:
// - same first/after/last/before semantics as the other connections
// - string cursors instead of int/composite cursors
// - a field-specific max page size to protect this multi-source resolver
func parseBalancePaginationParams(first *int32, after *string, last *int32, before *string) (PaginationParams, error) {
	if first != nil && *first > maxBalancePageLimit {
		return PaginationParams{}, balanceBadUserInputError(fmt.Sprintf("first must be less than or equal to %d", maxBalancePageLimit))
	}
	if last != nil && *last > maxBalancePageLimit {
		return PaginationParams{}, balanceBadUserInputError(fmt.Sprintf("last must be less than or equal to %d", maxBalancePageLimit))
	}

	params, err := parsePaginationParams(first, after, last, before, CursorTypeString)
	if err != nil {
		return PaginationParams{}, balanceBadUserInputError(err.Error())
	}
	return params, nil
}

// parseBalanceCursor validates and decodes the inner cursor payload into a typed
// form the resolver can dispatch to the correct backing source. The outer
// base64 layer was already removed by parsePaginationParams.
//
// Validation is intentionally source-aware:
// - the source must exist in the canonical source list for this address type
// - the id must match the source's local key type
//
// That prevents a cursor from one balance collection shape from being replayed
// against another, such as passing a native/classic cursor to a contract address.
func parseBalanceCursor(cursor *string, sources []balanceSource) (*balanceCursor, error) {
	if cursor == nil {
		return nil, nil
	}

	parts := strings.SplitN(*cursor, ":", 3)
	if len(parts) != 3 {
		return nil, balanceBadUserInputError("invalid balance cursor")
	}
	if parts[0] != balanceCursorPrefix {
		return nil, balanceBadUserInputError("invalid balance cursor version")
	}

	source := balanceSource(parts[1])
	if balanceSourceIndex(sources, source) == -1 {
		return nil, balanceBadUserInputError("invalid balance cursor source")
	}

	switch source {
	case balanceSourceNative:
		if parts[2] != string(balanceSourceNative) {
			return nil, balanceBadUserInputError("invalid balance cursor id")
		}
	default:
		if _, err := uuid.Parse(parts[2]); err != nil {
			return nil, balanceBadUserInputError("invalid balance cursor id")
		}
	}

	return &balanceCursor{
		Source: source,
		ID:     parts[2],
	}, nil
}

// uuid converts cursor IDs for the UUID-backed sources. Native uses a sentinel
// string rather than a UUID because there is at most one native balance row.
func (c *balanceCursor) uuid() (*uuid.UUID, error) {
	if c == nil || c.Source == balanceSourceNative {
		return nil, nil
	}

	id, err := uuid.Parse(c.ID)
	if err != nil {
		return nil, fmt.Errorf("parsing balance cursor uuid: %w", err)
	}
	return &id, nil
}

// getAccountBalances is the main field implementation for Account.balances.
// It treats native/classic/SAC/SEP-41 balances as one logical Relay
// connection:
//  1. parse Relay args
//  2. decode the source-aware cursor into a global boundary
//  3. fetch requested+1 rows across the ordered sources
//  4. hand the assembled nodes to the shared Relay helper for edges/PageInfo
func (r *Resolver) getAccountBalances(ctx context.Context, address string, first *int32, after *string, last *int32, before *string) (*graphql1.BalanceConnection, error) {
	params, err := parseBalancePaginationParams(first, after, last, before)
	if err != nil {
		return nil, err
	}

	sources := balanceSourcesForAddress(address)
	cursor, err := parseBalanceCursor(params.StringCursor, sources)
	if err != nil {
		return nil, err
	}

	queryLimit := *params.Limit + 1
	networkPassphrase := r.rpcService.NetworkPassphrase()

	nodes, err := r.getAccountBalanceNodes(ctx, address, sources, cursor, params, queryLimit, networkPassphrase)
	if err != nil {
		return nil, err
	}

	conn := NewConnectionWithRelayPagination(nodes, params, func(node *balanceNode) string {
		return node.CursorID()
	})

	edges := make([]*graphql1.BalanceEdge, len(conn.Edges))
	for i, edge := range conn.Edges {
		edges[i] = &graphql1.BalanceEdge{
			Node:   edge.Node.Balance,
			Cursor: edge.Cursor,
		}
	}

	return &graphql1.BalanceConnection{
		Edges:    edges,
		PageInfo: conn.PageInfo,
	}, nil
}

// getAccountBalanceNodes delegates to separate forward/backward walkers because
// the backing data comes from multiple sources rather than one DB query.
//
// The important rule is that the decoded cursor marks one boundary in the
// global source-ordered list. Only the source that owns that boundary receives
// a source-local cursor; earlier/later sources are either skipped or scanned
// from their natural start depending on pagination direction.
//
// Both walkers gather requested+1 nodes so PageInfo can be computed by the
// shared Relay helper without loading the full balance set.
func (r *Resolver) getAccountBalanceNodes(
	ctx context.Context,
	address string,
	sources []balanceSource,
	cursor *balanceCursor,
	params PaginationParams,
	queryLimit int32,
	networkPassphrase string,
) ([]*balanceNode, error) {
	if params.ForwardPagination {
		return r.getAccountBalanceNodesForward(ctx, address, sources, cursor, queryLimit, networkPassphrase)
	}

	nodes, err := r.getAccountBalanceNodesBackward(ctx, address, sources, cursor, queryLimit, networkPassphrase)
	if err != nil {
		return nil, err
	}

	slices.Reverse(nodes)
	return nodes, nil
}

// getAccountBalanceNodesForward walks sources in canonical order and only uses
// the decoded cursor on the source where the previous page ended.
//
// Example:
// - canonical order: native -> classic -> sep41
// - after = classic:<uuid-B>
// Then the forward walk:
// - skips native (already fully consumed by earlier pages)
// - resumes classic after uuid-B
// - continues into sep41 only if classic does not fill the page
func (r *Resolver) getAccountBalanceNodesForward(
	ctx context.Context,
	address string,
	sources []balanceSource,
	cursor *balanceCursor,
	queryLimit int32,
	networkPassphrase string,
) ([]*balanceNode, error) {
	nodes := make([]*balanceNode, 0, queryLimit)
	remaining := queryLimit
	cursorSourceIndex := -1
	if cursor != nil {
		cursorSourceIndex = balanceSourceIndex(sources, cursor.Source)
	}

	for i, source := range sources {
		// All sources before the cursor source were already fully consumed by the
		// previous page, so they can be skipped entirely.
		if cursor != nil && i < cursorSourceIndex {
			continue
		}

		var sourceCursor *balanceCursor
		if cursor != nil && i == cursorSourceIndex {
			// Only the source that produced the end cursor should apply the
			// source-local keyset comparison for the next page.
			sourceCursor = cursor
		}

		sourceNodes, err := r.getBalanceNodesForSource(ctx, address, source, sourceCursor, data.ASC, remaining, networkPassphrase)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, sourceNodes...)
		if int32(len(sourceNodes)) >= remaining {
			break
		}
		remaining -= int32(len(sourceNodes))
	}

	return nodes, nil
}

// getAccountBalanceNodesBackward mirrors the forward walk but traverses sources
// in reverse. Each source fetch runs DESC in the DB, and the combined slice is
// reversed once at the end so the final connection still returns canonical ASC
// order.
//
// Example:
// - canonical order: native -> classic -> sep41
// - before = sep41:<uuid-X>
// Then the backward walk:
// - skips sources after sep41 in canonical order (none in this example)
// - resumes sep41 before uuid-X
// - falls back into classic, then native, until the page is full
func (r *Resolver) getAccountBalanceNodesBackward(
	ctx context.Context,
	address string,
	sources []balanceSource,
	cursor *balanceCursor,
	queryLimit int32,
	networkPassphrase string,
) ([]*balanceNode, error) {
	nodes := make([]*balanceNode, 0, queryLimit)
	remaining := queryLimit
	cursorSourceIndex := len(sources)
	if cursor != nil {
		cursorSourceIndex = balanceSourceIndex(sources, cursor.Source)
	}

	for i := len(sources) - 1; i >= 0; i-- {
		source := sources[i]
		// When paging backwards, sources that come after the cursor source in
		// canonical order were already consumed by the current page window.
		if cursor != nil && i > cursorSourceIndex {
			continue
		}

		var sourceCursor *balanceCursor
		if cursor != nil && i == cursorSourceIndex {
			// As with forward pagination, only one source uses the decoded cursor.
			sourceCursor = cursor
		}

		sourceNodes, err := r.getBalanceNodesForSource(ctx, address, source, sourceCursor, data.DESC, remaining, networkPassphrase)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, sourceNodes...)
		if int32(len(sourceNodes)) >= remaining {
			break
		}
		remaining -= int32(len(sourceNodes))
	}

	return nodes, nil
}

// getBalanceNodesForSource converts one backing source into connection nodes.
// Each source owns its own local keyset semantics, but all of them flow into
// the same balanceNode shape for final connection assembly.
//
// The caller only passes a non-nil cursor when this source owns the global page
// boundary. Every other source either starts from its natural beginning/end or
// is skipped entirely by the source walkers above.
func (r *Resolver) getBalanceNodesForSource(
	ctx context.Context,
	address string,
	source balanceSource,
	cursor *balanceCursor,
	sortOrder data.SortOrder,
	limit int32,
	networkPassphrase string,
) ([]*balanceNode, error) {
	if limit <= 0 {
		return nil, nil
	}

	switch source {
	case balanceSourceNative:
		// Native has at most one row — a cursor means it was already emitted.
		if cursor != nil {
			return nil, nil
		}
		return r.getNativeBalanceNodes(ctx, address, networkPassphrase)
	case balanceSourceClassic:
		return r.getTrustlineBalanceNodes(ctx, address, cursor, sortOrder, limit, networkPassphrase)
	case balanceSourceSAC:
		return r.getSACBalanceNodes(ctx, address, cursor, sortOrder, limit)
	case balanceSourceSEP41:
		return r.getSEP41BalanceNodes(ctx, address, cursor, sortOrder, limit)
	default:
		return nil, balanceBadUserInputError("invalid balance source")
	}
}

// getNativeBalanceNodes fetches the single native XLM balance for the account.
func (r *Resolver) getNativeBalanceNodes(ctx context.Context, address string, networkPassphrase string) ([]*balanceNode, error) {
	nativeBalance, err := r.balanceReader.GetNativeBalance(ctx, address)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get native balance for %s: %v", address, err)
		return nil, balanceInternalError()
	}
	if nativeBalance == nil {
		return nil, nil
	}

	nativeBalanceResult, err := buildNativeBalanceFromDB(nativeBalance, networkPassphrase)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to build native balance for %s: %v", address, err)
		return nil, balanceInternalError()
	}

	return []*balanceNode{{
		Balance: nativeBalanceResult,
		Source:  balanceSourceNative,
		ID:      string(balanceSourceNative),
	}}, nil
}

// getTrustlineBalanceNodes pages classic trustlines by their stable internal
// asset UUID, then converts each DB row into the GraphQL trustline shape.
func (r *Resolver) getTrustlineBalanceNodes(
	ctx context.Context,
	address string,
	cursor *balanceCursor,
	sortOrder data.SortOrder,
	limit int32,
	networkPassphrase string,
) ([]*balanceNode, error) {
	cursorID, err := cursor.uuid()
	if err != nil {
		return nil, balanceBadUserInputError("invalid balance cursor id")
	}

	trustlines, err := r.balanceReader.GetTrustlineBalances(ctx, address, &limit, cursorID, sortOrder)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get paginated trustline balances for %s: %v", address, err)
		return nil, balanceInternalError()
	}

	nodes := make([]*balanceNode, 0, len(trustlines))
	for _, trustline := range trustlines {
		trustlineBalance, balanceErr := buildTrustlineBalanceFromDB(trustline, networkPassphrase)
		if balanceErr != nil {
			log.Ctx(ctx).Errorf("failed to build trustline balance for %s and asset %s: %v", address, trustline.AssetID, balanceErr)
			return nil, balanceInternalError()
		}

		nodes = append(nodes, &balanceNode{
			Balance: trustlineBalance,
			Source:  balanceSourceClassic,
			ID:      trustline.AssetID.String(),
		})
	}

	return nodes, nil
}

// getSACBalanceNodes pages contract-holder SAC balances by contract UUID. Unlike
// SEP-41, all fields needed for the GraphQL node are already present in PostgreSQL.
func (r *Resolver) getSACBalanceNodes(
	ctx context.Context,
	address string,
	cursor *balanceCursor,
	sortOrder data.SortOrder,
	limit int32,
) ([]*balanceNode, error) {
	cursorID, err := cursor.uuid()
	if err != nil {
		return nil, balanceBadUserInputError("invalid balance cursor id")
	}

	sacBalances, err := r.balanceReader.GetSACBalances(ctx, address, &limit, cursorID, sortOrder)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get paginated SAC balances for %s: %v", address, err)
		return nil, balanceInternalError()
	}

	nodes := make([]*balanceNode, 0, len(sacBalances))
	for _, sacBalance := range sacBalances {
		nodes = append(nodes, &balanceNode{
			Balance: buildSACBalanceFromDB(sacBalance),
			Source:  balanceSourceSAC,
			ID:      sacBalance.ContractID.String(),
		})
	}

	return nodes, nil
}

// SEP-41 balances are the only source that still needs RPC work. To avoid
// spending RPC calls on the extra row fetched for hasNextPage detection, this
// method converts only the visible contracts and carries the final fetched row
// through as a cursor-only sentinel node.
func (r *Resolver) getSEP41BalanceNodes(
	ctx context.Context,
	address string,
	cursor *balanceCursor,
	sortOrder data.SortOrder,
	limit int32,
) ([]*balanceNode, error) {
	cursorID, err := cursor.uuid()
	if err != nil {
		return nil, balanceBadUserInputError("invalid balance cursor id")
	}

	contracts, err := r.accountContractTokensModel.GetSEP41ByAccount(ctx, address, &limit, cursorID, sortOrder)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get paginated SEP-41 contracts for %s: %v", address, err)
		return nil, balanceInternalError()
	}

	nodes := make([]*balanceNode, 0, len(contracts))
	visibleContracts := contracts
	var sentinel *balanceNode
	if len(contracts) == int(limit) {
		// The last fetched row is reserved as a cursor-only sentinel so Relay can
		// detect a next page without paying one extra RPC balance lookup.
		extraContract := contracts[len(contracts)-1]
		visibleContracts = contracts[:len(contracts)-1]
		sentinel = &balanceNode{
			Source: balanceSourceSEP41,
			ID:     extraContract.ID.String(),
		}
	}

	for _, contract := range visibleContracts {
		sep41Balance, balanceErr := getSep41Balance(ctx, address, r.contractMetadataService, contract)
		if balanceErr != nil {
			log.Ctx(ctx).Errorf("failed to get SEP-41 balance for %s and contract %s: %v", address, contract.ContractID, balanceErr)
			return nil, balanceInternalError()
		}

		nodes = append(nodes, &balanceNode{
			Balance: sep41Balance,
			Source:  balanceSourceSEP41,
			ID:      contract.ID.String(),
		})
	}

	if sentinel != nil {
		// The sentinel intentionally has no GraphQL node payload. It exists only
		// so NewConnectionWithRelayPagination can compute hasNextPage/endCursor.
		nodes = append(nodes, sentinel)
	}

	return nodes, nil
}
