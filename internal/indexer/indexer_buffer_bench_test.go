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

func BenchmarkConcurrentPushTransaction(b *testing.B) {
	buf := NewIndexerBuffer()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i)), ToID: int64(i)}
			buf.PushTransaction(fmt.Sprintf("p-%d", i%100), tx)
			i++
		}
	})
}

func BenchmarkConcurrentPushOperation(b *testing.B) {
	buf := NewIndexerBuffer()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i)), ToID: int64(i)}
			op := &types.Operation{ID: int64(i)}
			buf.PushOperation(fmt.Sprintf("p-%d", i%100), op, tx)
			i++
		}
	})
}

func BenchmarkConcurrentBatchPushChanges(b *testing.B) {
	buf := NewIndexerBuffer()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			trustlines := []types.TrustlineChange{
				{AccountID: fmt.Sprintf("acct-%d", i), Asset: "USD:GISSUER", OperationID: int64(i), Operation: types.TrustlineOpAdd},
			}
			accounts := []types.AccountChange{
				{AccountID: fmt.Sprintf("acct-%d", i), OperationID: int64(i), Operation: types.AccountOpCreate},
			}
			sacBalances := []types.SACBalanceChange{
				{AccountID: fmt.Sprintf("acct-%d", i), ContractID: "CCONTRACT", OperationID: int64(i), Operation: types.SACBalanceOpAdd},
			}
			buf.BatchPushChanges(trustlines, accounts, sacBalances, nil)
			i++
		}
	})
}

func BenchmarkConcurrentPushStateChange(b *testing.B) {
	buf := NewIndexerBuffer()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i)), ToID: int64(i)}
			op := &types.Operation{ID: int64(i)}
			sc := types.StateChange{
				ToID:        int64(i),
				AccountID:   types.AddressBytea(fmt.Sprintf("acct-%d", i%100)),
				OperationID: int64(i),
			}
			buf.PushStateChange(tx, op, sc)
			i++
		}
	})
}
