package data

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/metrics"
	"github.com/stellar/wallet-backend/internal/utils"
)

// TradesModel writes executed fills and the per-token last fill.
type TradesModel struct {
	DB      *pgxpool.Pool
	Metrics *metrics.DBMetrics
}

// addressBytea encodes a Stellar address the way every BYTEA address column stores it
// (1 version byte + 32 key bytes), so trades rows join Blend's tables and resolvers' tokenId.
func addressBytea(addr string) ([]byte, error) {
	v, err := types.AddressBytea(addr).Value()
	if err != nil {
		return nil, fmt.Errorf("encoding address %s: %w", addr, err)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("encoding address %s: unexpected %T", addr, v)
	}
	return b, nil
}

// BatchCopy streams fills into trades with COPY. Like the other bulk tables it has no conflict
// handling: the persist path guarantees a ledger's fills are written once.
func (m *TradesModel) BatchCopy(ctx context.Context, dbTx pgx.Tx, trades []types.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	start := time.Now()
	copyCount, err := dbTx.CopyFrom(ctx, pgx.Identifier{"trades"},
		[]string{
			"ledger_created_at", "operation_id", "fill_index", "ledger_number", "base_token", "counter_token",
			"base_amount", "counter_amount", "base_qty", "usd_value", "venue",
		},
		pgx.CopyFromSlice(len(trades), func(i int) ([]any, error) {
			t := trades[i]
			base, err := addressBytea(t.BaseToken)
			if err != nil {
				return nil, err
			}
			counter, err := addressBytea(t.CounterToken)
			if err != nil {
				return nil, err
			}
			return []any{
				pgtype.Timestamptz{Time: t.LedgerClosed, Valid: true},
				pgtype.Int8{Int64: t.OperationID, Valid: true},
				pgtype.Int2{Int16: t.FillIndex, Valid: true},
				pgtype.Int4{Int32: int32(t.LedgerNumber), Valid: true},
				base,
				counter,
				pgtype.Numeric{Int: t.BaseAmount, Valid: true},
				pgtype.Numeric{Int: t.CounterAmount, Valid: true},
				nullableFloat(t.BaseQty),
				nullableFloat(t.USDValue),
				pgtype.Int2{Int16: int16(t.Venue), Valid: true},
			}, nil
		}),
	)
	m.Metrics.QueryDuration.WithLabelValues("BatchCopy", "trades").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("BatchCopy", "trades").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trades", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("pgx CopyFrom trades: %w", err)
	}
	if int(copyCount) != len(trades) {
		m.Metrics.QueryErrors.WithLabelValues("BatchCopy", "trades", "row_count_mismatch").Inc()
		return fmt.Errorf("expected %d trades copied, got %d", len(trades), copyCount)
	}
	return nil
}

func nullableFloat(f *float64) pgtype.Float8 {
	if f == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: *f, Valid: true}
}

// LastTrade is a token's most recent priced fill.
type LastTrade struct {
	Token           string
	PriceUSD        float64
	LedgerCreatedAt time.Time
	OperationID     int64
}

// UpsertLastTrades records each token's latest priced fill. The guard on operation_id makes the
// write order-independent, so replays and the backfill path can never move a token backwards.
func (m *TradesModel) UpsertLastTrades(ctx context.Context, dbTx pgx.Tx, last []LastTrade) error {
	if len(last) == 0 {
		return nil
	}
	tokens := make([][]byte, len(last))
	prices := make([]float64, len(last))
	times := make([]time.Time, len(last))
	opIDs := make([]int64, len(last))
	for i, l := range last {
		b, err := addressBytea(l.Token)
		if err != nil {
			return err
		}
		tokens[i] = b
		prices[i] = l.PriceUSD
		times[i] = l.LedgerCreatedAt
		opIDs[i] = l.OperationID
	}
	const q = `
		INSERT INTO token_last_trades (token, price_usd, ledger_created_at, operation_id)
		SELECT * FROM UNNEST($1::bytea[], $2::float8[], $3::timestamptz[], $4::bigint[])
		ON CONFLICT (token) DO UPDATE SET
			price_usd = EXCLUDED.price_usd,
			ledger_created_at = EXCLUDED.ledger_created_at,
			operation_id = EXCLUDED.operation_id
		WHERE EXCLUDED.operation_id > token_last_trades.operation_id`
	start := time.Now()
	_, err := dbTx.Exec(ctx, q, tokens, prices, times, opIDs)
	m.Metrics.QueryDuration.WithLabelValues("UpsertLastTrades", "token_last_trades").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("UpsertLastTrades", "token_last_trades").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("UpsertLastTrades", "token_last_trades", utils.GetDBErrorType(err)).Inc()
		return fmt.Errorf("upserting token_last_trades: %w", err)
	}
	return nil
}

// GetAllLastTrades returns every token's last priced fill.
func (m *TradesModel) GetAllLastTrades(ctx context.Context) ([]LastTrade, error) {
	start := time.Now()
	rows, err := m.DB.Query(ctx, `SELECT token, price_usd, ledger_created_at, operation_id FROM token_last_trades`)
	m.Metrics.QueryDuration.WithLabelValues("GetAllLastTrades", "token_last_trades").Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("GetAllLastTrades", "token_last_trades").Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetAllLastTrades", "token_last_trades", utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying token_last_trades: %w", err)
	}
	defer rows.Close()
	var out []LastTrade
	for rows.Next() {
		var l LastTrade
		var token types.AddressBytea
		if err := rows.Scan(&token, &l.PriceUSD, &l.LedgerCreatedAt, &l.OperationID); err != nil {
			return nil, fmt.Errorf("scanning token_last_trades: %w", err)
		}
		l.Token = string(token)
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating token_last_trades: %w", err)
	}
	return out, nil
}

// Candle is one USD price bucket of a token. VWAP is 0 when the bucket has no base volume.
type Candle struct {
	Bucket    time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	VWAP      float64
	USDVolume float64
	Trades    int64
}

// CandleView names the continuous aggregate a candle query reads.
const (
	CandleView1m = "trades_1m"
	CandleView1h = "trades_1h"
	CandleView1d = "trades_1d"
)

// GetCandles returns the token's candles with from <= bucket < to in ascending order, at most
// limit of them. view must be one of the CandleView constants.
func (m *TradesModel) GetCandles(ctx context.Context, view string, token string, from, to time.Time, limit int) ([]Candle, error) {
	var q string
	switch view {
	case CandleView1m:
		q = candlesQuery(CandleView1m)
	case CandleView1h:
		q = candlesQuery(CandleView1h)
	case CandleView1d:
		q = candlesQuery(CandleView1d)
	default:
		return nil, fmt.Errorf("unknown candle view %q", view)
	}
	tokenBytes, err := addressBytea(token)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	rows, err := m.DB.Query(ctx, q, tokenBytes, from, to, limit)
	m.Metrics.QueryDuration.WithLabelValues("GetCandles", view).Observe(time.Since(start).Seconds())
	m.Metrics.QueriesTotal.WithLabelValues("GetCandles", view).Inc()
	if err != nil {
		m.Metrics.QueryErrors.WithLabelValues("GetCandles", view, utils.GetDBErrorType(err)).Inc()
		return nil, fmt.Errorf("querying %s: %w", view, err)
	}
	defer rows.Close()
	var out []Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.Bucket, &c.Open, &c.High, &c.Low, &c.Close, &c.VWAP, &c.USDVolume, &c.Trades); err != nil {
			return nil, fmt.Errorf("scanning %s: %w", view, err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating %s: %w", view, err)
	}
	return out, nil
}

// candlesQuery is only ever called with the CandleView constants, never user input.
func candlesQuery(view string) string {
	return `SELECT bucket, open, high, low, close,
		COALESCE(usd_volume / NULLIF(base_volume, 0), 0), usd_volume, trades::bigint
		FROM ` + view + `
		WHERE base_token = $1 AND bucket >= $2 AND bucket < $3
		ORDER BY bucket ASC LIMIT $4`
}
