// Active auction storage for Blend v2 (blend_auctions).
package blend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// auctionsTable is the metrics label for blend_auctions queries.
const auctionsTable = "blend_auctions"

// AuctionKey identifies one active auction row.
type AuctionKey struct {
	Pool        types.AddressBytea
	User        types.AddressBytea
	AuctionType int32
}

// Auction mirrors one blend_auctions row. Bid and Lot map asset C-addresses to
// decimal-string amounts (JSONB).
type Auction struct {
	Pool               types.AddressBytea
	User               types.AddressBytea
	AuctionType        int32
	Bid                map[string]string
	Lot                map[string]string
	StartBlock         int32
	LastModifiedLedger int32
}

// AuctionModelInterface exposes Blend v2 active-auction storage operations.
type AuctionModelInterface interface {
	// BatchUpsert full-row replaces each auction (partial fills rewrite bid/lot).
	BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Auction) error
	// DeleteByKey removes exactly the given (pool, user, auction_type) rows.
	DeleteByKey(ctx context.Context, dbTx pgx.Tx, keys []AuctionKey) error
}

// AuctionModel implements AuctionModelInterface against blend_auctions.
type AuctionModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

var _ AuctionModelInterface = (*AuctionModel)(nil)

// marshalAuctionAmounts serializes a bid/lot asset-amount map for storage in a
// JSONB column. A nil/empty map serializes as "{}" (not SQL NULL or JSON null)
// so readers can always unmarshal the column into map[string]string without a
// null check.
func marshalAuctionAmounts(amounts map[string]string) (string, error) {
	if len(amounts) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(amounts)
	if err != nil {
		return "", fmt.Errorf("marshalling auction amounts: %w", err)
	}
	return string(b), nil
}

// BatchUpsert inserts or fully replaces blend_auctions rows. See
// AuctionModelInterface.
func (m *AuctionModel) BatchUpsert(ctx context.Context, dbTx pgx.Tx, rows []Auction) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(rows))
	users := make([][]byte, len(rows))
	auctionTypes := make([]int32, len(rows))
	bids := make([]string, len(rows))
	lots := make([]string, len(rows))
	startBlocks := make([]int32, len(rows))
	ledgers := make([]int32, len(rows))
	for i, r := range rows {
		poolBytes, err := addressToBytes(string(r.Pool))
		if err != nil {
			return fmt.Errorf("converting pool address for auction upsert: %w", err)
		}
		userBytes, err := addressToBytes(string(r.User))
		if err != nil {
			return fmt.Errorf("converting user address for auction upsert: %w", err)
		}
		bidJSON, err := marshalAuctionAmounts(r.Bid)
		if err != nil {
			return fmt.Errorf("marshalling bid for auction upsert: %w", err)
		}
		lotJSON, err := marshalAuctionAmounts(r.Lot)
		if err != nil {
			return fmt.Errorf("marshalling lot for auction upsert: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		auctionTypes[i] = r.AuctionType
		bids[i] = bidJSON
		lots[i] = lotJSON
		startBlocks[i] = r.StartBlock
		ledgers[i] = r.LastModifiedLedger
	}

	const upsertQuery = `
		INSERT INTO blend_auctions (pool_contract_id, user_account_id, auction_type, bid, lot, start_block, last_modified_ledger)
		SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::integer[], $4::text[]::jsonb[], $5::text[]::jsonb[], $6::integer[], $7::integer[])
		ON CONFLICT (pool_contract_id, user_account_id, auction_type) DO UPDATE SET
			bid                  = EXCLUDED.bid,
			lot                  = EXCLUDED.lot,
			start_block          = EXCLUDED.start_block,
			last_modified_ledger = EXCLUDED.last_modified_ledger`
	if _, err := dbTx.Exec(ctx, upsertQuery, pools, users, auctionTypes, bids, lots, startBlocks, ledgers); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchUpsert", auctionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting blend auctions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("BatchUpsert", auctionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("BatchUpsert", auctionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("BatchUpsert", auctionsTable).Observe(float64(len(rows)))
	return nil
}

// DeleteByKey removes exactly the given (pool, user, auction_type) rows from
// blend_auctions (auction fill or cancellation).
func (m *AuctionModel) DeleteByKey(ctx context.Context, dbTx pgx.Tx, keys []AuctionKey) error {
	if len(keys) == 0 {
		return nil
	}

	start := time.Now()

	pools := make([][]byte, len(keys))
	users := make([][]byte, len(keys))
	auctionTypes := make([]int32, len(keys))
	for i, k := range keys {
		poolBytes, err := addressToBytes(string(k.Pool))
		if err != nil {
			return fmt.Errorf("converting pool address for auction delete: %w", err)
		}
		userBytes, err := addressToBytes(string(k.User))
		if err != nil {
			return fmt.Errorf("converting user address for auction delete: %w", err)
		}
		pools[i] = poolBytes
		users[i] = userBytes
		auctionTypes[i] = k.AuctionType
	}

	const deleteQuery = `
		DELETE FROM blend_auctions
		WHERE (pool_contract_id, user_account_id, auction_type) IN (SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::integer[]))`
	if _, err := dbTx.Exec(ctx, deleteQuery, pools, users, auctionTypes); err != nil {
		m.Metrics.QueryErrors.WithLabelValues("DeleteByKey", auctionsTable, utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("deleting blend auctions: %w", err)
	}

	duration := time.Since(start).Seconds()
	m.Metrics.QueryDuration.WithLabelValues("DeleteByKey", auctionsTable).Observe(duration)
	m.Metrics.QueriesTotal.WithLabelValues("DeleteByKey", auctionsTable).Inc()
	m.Metrics.BatchSize.WithLabelValues("DeleteByKey", auctionsTable).Observe(float64(len(keys)))
	return nil
}
