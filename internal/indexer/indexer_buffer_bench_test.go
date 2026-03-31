package indexer

import (
	"fmt"
	"testing"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

const (
	benchParticipants = 100
	benchTxCount      = 200
)

// Pre-allocated test data to isolate buffer performance from allocation noise.
var (
	benchTxs             [benchTxCount]*types.Transaction
	benchOps             [benchTxCount]*types.Operation
	benchStateChanges    [benchTxCount]types.StateChange
	benchParticipantStrs [benchParticipants]string
	benchTrustlines      [benchTxCount][]types.TrustlineChange
	benchAccounts        [benchTxCount][]types.AccountChange
	benchSACBalances     [benchTxCount][]types.SACBalanceChange
)

func init() {
	for i := range benchParticipants {
		benchParticipantStrs[i] = fmt.Sprintf("participant-%d", i)
	}
	for i := range benchTxCount {
		benchTxs[i] = &types.Transaction{Hash: types.HashBytea(fmt.Sprintf("hash-%d", i)), ToID: int64(i)}
		benchOps[i] = &types.Operation{ID: int64(i)}
		benchStateChanges[i] = types.StateChange{
			ToID:        int64(i),
			AccountID:   types.AddressBytea(fmt.Sprintf("acct-%d", i%100)),
			OperationID: int64(i),
		}
		benchTrustlines[i] = []types.TrustlineChange{
			{AccountID: fmt.Sprintf("acct-%d", i), Asset: "USD:GISSUER", OperationID: int64(i), Operation: types.TrustlineOpAdd},
		}
		benchAccounts[i] = []types.AccountChange{
			{AccountID: fmt.Sprintf("acct-%d", i), OperationID: int64(i), Operation: types.AccountOpCreate},
		}
		benchSACBalances[i] = []types.SACBalanceChange{
			{AccountID: fmt.Sprintf("acct-%d", i), ContractID: "CCONTRACT", OperationID: int64(i), Operation: types.SACBalanceOpAdd},
		}
	}
}

func BenchmarkPushTransaction(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.PushTransaction(benchParticipantStrs[i%benchParticipants], benchTxs[i%benchTxCount])
	}
}

func BenchmarkPushOperation(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % benchTxCount
		buf.PushOperation(benchParticipantStrs[i%benchParticipants], benchOps[idx], benchTxs[idx])
	}
}

func BenchmarkBatchPushChanges(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % benchTxCount
		buf.BatchPushChanges(benchTrustlines[idx], benchAccounts[idx], benchSACBalances[idx], nil)
	}
}

func BenchmarkConcurrentPushTransaction(b *testing.B) {
	buf := NewIndexerBuffer()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			buf.PushTransaction(benchParticipantStrs[i%benchParticipants], benchTxs[i%benchTxCount])
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
			idx := i % benchTxCount
			buf.PushOperation(benchParticipantStrs[i%benchParticipants], benchOps[idx], benchTxs[idx])
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
			idx := i % benchTxCount
			buf.BatchPushChanges(benchTrustlines[idx], benchAccounts[idx], benchSACBalances[idx], nil)
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
			idx := i % benchTxCount
			buf.PushStateChange(benchTxs[idx], benchOps[idx], benchStateChanges[idx])
			i++
		}
	})
}
