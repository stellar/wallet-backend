package indexer

import (
	"fmt"
	"testing"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func BenchmarkPushTransaction(b *testing.B) {
	buf := NewIndexerBuffer()
	tx := &types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PushTransaction(fmt.Sprintf("participant-%d", i%100), tx)
	}
}

func BenchmarkPushOperation(b *testing.B) {
	buf := NewIndexerBuffer()
	tx := &types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
	op := &types.Operation{ID: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PushOperation(fmt.Sprintf("participant-%d", i%100), op, tx)
	}
}

func BenchmarkMerge(b *testing.B) {
	for _, nTx := range []int{1, 10, 50} {
		b.Run(fmt.Sprintf("txs=%d", nTx), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dst := NewIndexerBuffer()
				src := NewIndexerBuffer()
				for j := 0; j < nTx; j++ {
					tx := &types.Transaction{
						Hash: types.HashBytea(fmt.Sprintf("hash-%d", j)),
						ToID: int64(j),
					}
					src.PushTransaction(fmt.Sprintf("p-%d", j), tx)
				}
				b.StartTimer()
				dst.Merge(src)
			}
		})
	}
}

func BenchmarkBatchPushChanges(b *testing.B) {
	buf := NewIndexerBuffer()
	trustlines := []types.TrustlineChange{
		{AccountID: "alice", Asset: "USD:GISSUER", OperationID: 1, Operation: types.TrustlineOpAdd},
	}
	accounts := []types.AccountChange{
		{AccountID: "alice", OperationID: 1, Operation: types.AccountOpCreate},
	}
	sacBalances := []types.SACBalanceChange{
		{AccountID: "alice", ContractID: "CCONTRACT", OperationID: 1, Operation: types.SACBalanceOpAdd},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.BatchPushChanges(trustlines, accounts, sacBalances, nil)
	}
}

func BenchmarkClearAndReuse(b *testing.B) {
	buf := NewIndexerBuffer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i%10)), ToID: int64(i % 10)}
		buf.PushTransaction("alice", tx)
		buf.Clear()
	}
}
