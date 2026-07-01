package indexer

import (
	"fmt"
	"testing"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// Benchmark fixtures model a ledger's worth of per-transaction results with a realistic mix
// (participants, one operation, a state change, and account/trustline/SAC balance changes each).
// They are pre-built so the benchmarks measure only buffer folding, not fixture construction.
const (
	benchTxCount       = 200
	benchParticipantsN = 100
	benchAsset         = "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
)

var (
	benchParticipants [benchParticipantsN]string
	benchResults      [benchTxCount]*TransactionResult
)

func init() {
	for i := range benchParticipantsN {
		benchParticipants[i] = fmt.Sprintf("GPARTICIPANT%053d", i)
	}
	for i := range benchTxCount {
		opID := int64(i*10 + 1)
		account := fmt.Sprintf("GACCOUNT%056d", i%benchParticipantsN)
		tx := &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i)), ToID: int64(i)}
		op := &types.Operation{ID: opID}

		benchResults[i] = &TransactionResult{
			Transaction:    tx,
			TxParticipants: []string{benchParticipants[i%benchParticipantsN], benchParticipants[(i+1)%benchParticipantsN]},
			Operations:     map[int64]*types.Operation{opID: op},
			OpParticipants: map[int64][]string{opID: {benchParticipants[i%benchParticipantsN]}},
			StateChanges: []types.StateChange{
				{ToID: int64(i), AccountID: types.AddressBytea(account), OperationID: opID},
			},
			AccountChanges: []types.AccountChange{
				{AccountID: account, SortKey: opID, Operation: types.AccountOpUpdate, Balance: int64(i)},
			},
			TrustlineChanges: []types.TrustlineChange{
				{AccountID: account, Asset: benchAsset, OperationID: opID, Operation: types.TrustlineOpAdd, Balance: int64(i)},
			},
			SACBalanceChanges: []types.SACBalanceChange{
				{AccountID: account, ContractID: "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP", Balance: "100", Operation: types.SACBalanceOpAdd, OperationID: opID},
			},
			ParticipantCount: 2,
		}
	}
}

// BenchmarkIngestTransactionResult measures folding a single transaction's result into the buffer.
func BenchmarkIngestTransactionResult(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.IngestTransactionResult(benchResults[i%benchTxCount])
		buf.Clear()
	}
}

// BenchmarkFoldLedger measures folding a full ledger's worth of results into one buffer and then
// clearing it — the per-ledger cost the serial fold pays after the parallel workers finish.
func BenchmarkFoldLedger(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, result := range benchResults {
			buf.IngestTransactionResult(result)
		}
		buf.Clear()
	}
}

// BenchmarkPushTransaction isolates the canonical-pointer + participant-set path.
func BenchmarkPushTransaction(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := benchResults[i%benchTxCount]
		buf.PushTransaction(result.TxParticipants[0], result.Transaction)
	}
}

// BenchmarkPushAccountChange isolates the tombstone-aware dedup path.
func BenchmarkPushAccountChange(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PushAccountChange(benchResults[i%benchTxCount].AccountChanges[0])
	}
}
