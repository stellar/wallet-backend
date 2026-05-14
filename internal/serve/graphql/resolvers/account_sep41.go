package resolvers

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/wallet-backend/internal/data"
	sep41data "github.com/stellar/wallet-backend/internal/data/sep41"
	graphql1 "github.com/stellar/wallet-backend/internal/serve/graphql/generated"
)

const (
	// maxAllowancePageLimit caps a single sep41Allowances page. It matches
	// maxBalancePageLimit so both Account connections share the same request cap
	// and the same rule applies to the GraphQL complexity calculation.
	maxAllowancePageLimit int32 = 100
	// allowanceCursorPrefix versions the opaque cursor payload so future formats
	// can be introduced without silently misreading old cursors.
	allowanceCursorPrefix = "v1"
)

// allowanceCursor is the decoded form of the opaque pagination cursor:
//
//	base64("v1:<spender>:<contract_uuid>")
//
// (spender_address, contract_id) is unique inside a single owner's allowance
// list (owner+spender+contract_id is the table primary key), so that pair is a
// valid keyset cursor.
type allowanceCursor struct {
	SpenderAddress string
	ContractID     uuid.UUID
}

// parseAllowancePaginationParams layers the allowance-specific page cap on top
// of the shared Relay pagination parser. Mirrors parseBalancePaginationParams.
func parseAllowancePaginationParams(first *int32, after *string, last *int32, before *string) (PaginationParams, error) {
	if first != nil && *first > maxAllowancePageLimit {
		return PaginationParams{}, balanceBadUserInputError(fmt.Sprintf("first must be less than or equal to %d", maxAllowancePageLimit))
	}
	if last != nil && *last > maxAllowancePageLimit {
		return PaginationParams{}, balanceBadUserInputError(fmt.Sprintf("last must be less than or equal to %d", maxAllowancePageLimit))
	}

	params, err := parsePaginationParams(first, after, last, before, CursorTypeString)
	if err != nil {
		return PaginationParams{}, balanceBadUserInputError(err.Error())
	}
	return params, nil
}

// parseAllowanceCursor decodes the inner cursor payload. The outer base64 layer
// is already removed by parsePaginationParams.
func parseAllowanceCursor(cursor *string) (*allowanceCursor, error) {
	if cursor == nil {
		return nil, nil
	}

	parts := strings.SplitN(*cursor, ":", 3)
	if len(parts) != 3 {
		return nil, balanceBadUserInputError("invalid allowance cursor")
	}
	if parts[0] != allowanceCursorPrefix {
		return nil, balanceBadUserInputError("invalid allowance cursor version")
	}
	if parts[1] == "" {
		return nil, balanceBadUserInputError("invalid allowance cursor spender")
	}

	contractID, err := uuid.Parse(parts[2])
	if err != nil {
		return nil, balanceBadUserInputError("invalid allowance cursor contract id")
	}

	return &allowanceCursor{
		SpenderAddress: parts[1],
		ContractID:     contractID,
	}, nil
}

// encodeAllowanceCursorID produces the inner cursor payload for an allowance row.
// The outer base64 wrapping is applied by NewConnectionWithRelayPagination.
func encodeAllowanceCursorID(a sep41data.Allowance) string {
	return fmt.Sprintf("%s:%s:%s", allowanceCursorPrefix, a.SpenderAddress, a.ContractID)
}

// getSEP41Allowances is the main implementation for Account.sep41Allowances.
// It fetches a single keyset-paginated page from the DB, filters expired rows at
// the SQL level, and builds a Relay connection around the result.
//
// The "current ledger" used to filter expired allowances is the live ingestion
// high-watermark read from ingest_store. If ingestion hasn't advanced yet
// (fresh install) we pass 0, which returns allowances with any future
// expiration_ledger; this is the safest behavior for a brand-new database.
func (r *Resolver) getSEP41Allowances(ctx context.Context, address string, first *int32, after *string, last *int32, before *string) (*graphql1.SEP41AllowanceConnection, error) {
	if address == "" {
		return nil, balanceBadUserInputError("account has no address")
	}

	params, err := parseAllowancePaginationParams(first, after, last, before)
	if err != nil {
		return nil, err
	}

	inner, err := parseAllowanceCursor(params.StringCursor)
	if err != nil {
		return nil, err
	}

	currentLedger, err := r.models.IngestStore.Get(ctx, data.LatestLedgerCursorName)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to read latest ingested ledger for sep41Allowances: %v", err)
		return nil, balanceInternalError()
	}

	sep41Sort := sep41data.SortASC
	if params.SortOrder == data.DESC {
		sep41Sort = sep41data.SortDESC
	}

	var cursor *sep41data.AllowanceCursor
	if inner != nil {
		cursor = &sep41data.AllowanceCursor{
			SpenderAddress: inner.SpenderAddress,
			ContractID:     inner.ContractID,
		}
	}

	queryLimit := *params.Limit + 1
	allowances, err := r.balanceReader.GetSEP41Allowances(ctx, address, currentLedger, queryLimit, cursor, sep41Sort)
	if err != nil {
		log.Ctx(ctx).Errorf("failed to get SEP-41 allowances for %s: %v", address, err)
		return nil, balanceInternalError()
	}

	conn := NewConnectionWithRelayPagination(allowances, params, encodeAllowanceCursorID)

	edges := make([]*graphql1.SEP41AllowanceEdge, len(conn.Edges))
	for i, edge := range conn.Edges {
		edges[i] = &graphql1.SEP41AllowanceEdge{
			Node: &graphql1.SEP41Allowance{
				Owner:              edge.Node.OwnerAddress,
				Spender:            edge.Node.SpenderAddress,
				TokenID:            edge.Node.TokenID,
				Amount:             edge.Node.Amount,
				ExpirationLedger:   edge.Node.ExpirationLedger,
				LastModifiedLedger: edge.Node.LedgerNumber,
			},
			Cursor: edge.Cursor,
		}
	}

	return &graphql1.SEP41AllowanceConnection{
		Edges:    edges,
		PageInfo: conn.PageInfo,
	}, nil
}
