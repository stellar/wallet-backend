package indexer

import (
	"fmt"
	"sync"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func buildStateChange(toID int64, reason types.StateChangeReason, accountID string, operationID int64) types.StateChange {
	return types.StateChange{
		ToID:                toID,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		AccountID:           types.AddressBytea(accountID),
		OperationID:         operationID,
		SortKey:             fmt.Sprintf("%d:%s:%s", toID, types.StateChangeCategoryBalance, accountID),
	}
}

func TestIndexerBuffer_PushTransaction(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("alice", tx2)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("bob", tx2) // duplicate is a no-op

		// Assert participants by transaction
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.ToID])

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, indexerBuffer.GetTransactions())
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			indexerBuffer.PushTransaction("alice", tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushTransaction("alice", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushTransaction("bob", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushTransaction("bob", tx2) // duplicate is a no-op
			wg.Done()
		}()
		wg.Wait()

		// Assert participants by transaction
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.ToID])

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
	})
}

func TestIndexerBuffer_PushOperation(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2) // duplicate operation ID is a no-op

		// Assert participants by operation
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])

		// Assert transactions were also added
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("bob", "chuck"), txParticipants[tx2.ToID])
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		wg := sync.WaitGroup{}
		wg.Add(4)
		go func() {
			indexerBuffer.PushOperation("alice", op1, tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushOperation("bob", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushOperation("chuck", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushOperation("chuck", op2, tx2) // duplicate operation ID is a no-op
			wg.Done()
		}()
		wg.Wait()

		// Assert participants by operation
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])
	})
}

func TestIndexerBuffer_PushStateChange(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1}

		indexerBuffer.PushStateChange(tx, op, sc1)
		indexerBuffer.PushStateChange(tx, op, sc2)
		indexerBuffer.PushStateChange(tx, op, sc3)

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChange(tx, op, sc1)
		}()

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChange(tx, op, sc2)
			indexerBuffer.PushStateChange(tx, op, sc3)
		}()

		wg.Wait()

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Len(t, allStateChanges, 3)
		assert.ElementsMatch(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢 with operations and transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 3}
		op2 := types.Operation{ID: 4}
		op3 := types.Operation{ID: 5}
		indexerBuffer.PushOperation("someone", op1, tx1)
		indexerBuffer.PushOperation("someone", op2, tx2)

		sc1 := buildStateChange(3, types.StateChangeReasonCredit, "alice", op1.ID)
		sc2 := buildStateChange(4, types.StateChangeReasonDebit, "alice", op2.ID)
		sc3 := buildStateChange(4, types.StateChangeReasonCredit, "eve", op3.ID)
		// These are fee state changes, so they don't have an operation ID.
		sc4 := buildStateChange(1, types.StateChangeReasonDebit, "bob", 0)
		sc5 := buildStateChange(2, types.StateChangeReasonDebit, "bob", 0)

		indexerBuffer.PushStateChange(tx1, op1, sc1)
		indexerBuffer.PushStateChange(tx2, op2, sc2)
		indexerBuffer.PushStateChange(tx2, op3, sc3)               // This operation should be added
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc4) // Fee state changes don't have an operation
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc5) // Fee state changes don't have an operation

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3, sc4, sc5}, allStateChanges)

		// Assert transaction participants
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("someone", "alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("someone", "alice", "eve", "bob"), txParticipants[tx2.ToID])

		// Assert operation participants
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("someone", "alice"), opParticipants[int64(3)])
		assert.Equal(t, set.NewSet("someone", "alice"), opParticipants[int64(4)])
		assert.Equal(t, set.NewSet("eve"), opParticipants[int64(5)])
	})
}

func TestIndexerBuffer_GetNumberOfTransactions(t *testing.T) {
	t.Run("🟢 returns correct count", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		assert.Equal(t, 0, indexerBuffer.GetNumberOfTransactions())

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		indexerBuffer.PushTransaction("alice", tx1)
		assert.Equal(t, 1, indexerBuffer.GetNumberOfTransactions())

		indexerBuffer.PushTransaction("bob", tx2)
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Duplicate should not increase count
		indexerBuffer.PushTransaction("charlie", tx2)
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
	})
}

func TestIndexerBuffer_GetAllTransactions(t *testing.T) {
	t.Run("🟢 returns all unique transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1, LedgerNumber: 100}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2, LedgerNumber: 101}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("charlie", tx2) // duplicate

		allTxs := indexerBuffer.GetTransactions()
		require.Len(t, allTxs, 2)
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, allTxs)
	})
}

func TestIndexerBuffer_GetAllTransactionsParticipants(t *testing.T) {
	t.Run("🟢 returns correct participants mapping", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx1)
		indexerBuffer.PushTransaction("alice", tx2)

		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx2.ToID])
	})
}

func TestIndexerBuffer_GetAllOperations(t *testing.T) {
	t.Run("🟢 returns all unique operations", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op2, tx1)
		indexerBuffer.PushOperation("charlie", op2, tx1) // duplicate

		allOps := indexerBuffer.GetOperations()
		require.Len(t, allOps, 2)
		assert.ElementsMatch(t, []*types.Operation{&op1, &op2}, allOps)
	})
}

func TestIndexerBuffer_GetAllOperationsParticipants(t *testing.T) {
	t.Run("🟢 returns correct participants mapping", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op1, tx1)
		indexerBuffer.PushOperation("alice", op2, tx1)

		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(2)])
	})
}

func TestIndexerBuffer_GetAllStateChanges(t *testing.T) {
	t.Run("🟢 returns all state changes in order", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1, AccountID: "charlie"}

		indexerBuffer.PushStateChange(tx, op, sc1)
		indexerBuffer.PushStateChange(tx, op, sc2)
		indexerBuffer.PushStateChange(tx, op, sc3)

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})
}

func TestIndexerBuffer_Merge(t *testing.T) {
	t.Run("🟢 merge empty buffers", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		buffer1.Merge(buffer2)
		assert.Equal(t, 0, buffer1.GetNumberOfTransactions())
		assert.Len(t, buffer1.GetStateChanges(), 0)
	})

	t.Run("🟢 merge transactions only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		buffer1.PushTransaction("alice", tx1)
		buffer2.PushTransaction("bob", tx2)

		buffer1.Merge(buffer2)

		// Verify transactions
		allTxs := buffer1.GetTransactions()
		assert.Len(t, allTxs, 2)
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, allTxs)

		// Verify transaction participants
		txParticipants := buffer1.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.ToID])
	})

	t.Run("🟢 merge operations only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		buffer1.PushOperation("alice", op1, tx1)
		buffer2.PushOperation("bob", op2, tx1)

		buffer1.Merge(buffer2)

		// Verify operations
		allOps := buffer1.GetOperations()
		assert.Len(t, allOps, 2)
		assert.ElementsMatch(t, []*types.Operation{&op1, &op2}, allOps)

		// Verify operation participants
		opParticipants := buffer1.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob"), opParticipants[int64(2)])
	})

	t.Run("🟢 merge state changes only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}

		buffer1.PushStateChange(tx, op, sc1)
		buffer2.PushStateChange(tx, op, sc2)

		buffer1.Merge(buffer2)

		// Verify state changes
		allStateChanges := buffer1.GetStateChanges()
		assert.Len(t, allStateChanges, 2)
		assert.Equal(t, sc1, allStateChanges[0])
		assert.Equal(t, sc2, allStateChanges[1])
	})

	t.Run("🟢 merge with overlapping data", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 1}

		// Buffer1 has tx1 with alice
		buffer1.PushTransaction("alice", tx1)
		buffer1.PushOperation("alice", op1, tx1)

		// Buffer2 has tx1 with bob (overlapping tx) and tx2 with charlie
		buffer2.PushTransaction("bob", tx1)
		buffer2.PushTransaction("charlie", tx2)
		buffer2.PushOperation("bob", op1, tx1)

		buffer1.Merge(buffer2)

		// Verify transactions
		allTxs := buffer1.GetTransactions()
		assert.Len(t, allTxs, 2)

		// Verify tx1 has both alice and bob as participants
		txParticipants := buffer1.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("charlie"), txParticipants[tx2.ToID])

		// Verify operation participants merged
		opParticipants := buffer1.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), opParticipants[int64(1)])
	})

	t.Run("🟢 merge into empty buffer", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op1 := types.Operation{ID: 1}
		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}

		buffer2.PushTransaction("alice", tx1)
		buffer2.PushOperation("bob", op1, tx1)
		buffer2.PushStateChange(tx1, op1, sc1)

		buffer1.Merge(buffer2)

		assert.Equal(t, 1, buffer1.GetNumberOfTransactions())
		assert.Len(t, buffer1.GetOperations(), 1)
		assert.Len(t, buffer1.GetStateChanges(), 1)
	})

	t.Run("🟢 merge empty buffer into populated", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		buffer1.PushTransaction("alice", tx1)

		buffer1.Merge(buffer2)

		assert.Equal(t, 1, buffer1.GetNumberOfTransactions())
	})

	t.Run("🟢 concurrent merges", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()
		buffer3 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		tx3 := types.Transaction{Hash: "b76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48762", ToID: 3}

		buffer1.PushTransaction("alice", tx1)
		buffer2.PushTransaction("bob", tx2)
		buffer3.PushTransaction("charlie", tx3)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			buffer1.Merge(buffer2)
		}()

		go func() {
			defer wg.Done()
			buffer1.Merge(buffer3)
		}()

		wg.Wait()

		// Verify all data merged correctly
		assert.Equal(t, 3, buffer1.GetNumberOfTransactions())
	})

	t.Run("🟢 merge complete buffers with all data types", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}
		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice", OperationID: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob", OperationID: 2}

		// Buffer1
		buffer1.PushTransaction("alice", tx1)
		buffer1.PushOperation("alice", op1, tx1)
		buffer1.PushStateChange(tx1, op1, sc1)

		// Buffer2
		buffer2.PushTransaction("bob", tx2)
		buffer2.PushOperation("bob", op2, tx2)
		buffer2.PushStateChange(tx2, op2, sc2)

		buffer1.Merge(buffer2)

		// Verify transactions
		allTxs := buffer1.GetTransactions()
		assert.Len(t, allTxs, 2)

		// Verify operations
		allOps := buffer1.GetOperations()
		assert.Len(t, allOps, 2)

		// Verify state changes
		allStateChanges := buffer1.GetStateChanges()
		assert.Len(t, allStateChanges, 2)
		assert.Equal(t, sc1, allStateChanges[0])
		assert.Equal(t, sc2, allStateChanges[1])

		// Verify participants mappings
		txParticipants := buffer1.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.ToID])

		opParticipants := buffer1.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob"), opParticipants[int64(2)])
	})

}

func TestIndexerBuffer_PushSACBalanceChange(t *testing.T) {
	t.Run("🟢 stores SAC balance changes", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		change1 := types.SACBalanceChange{
			AccountID:   "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
			ContractID:  "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP",
			Balance:     "1000000",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 100,
		}

		buffer.PushSACBalanceChange(change1)

		changes := buffer.GetSACBalanceChanges()
		assert.Len(t, changes, 1)

		key := SACBalanceChangeKey{AccountID: change1.AccountID, ContractID: change1.ContractID}
		assert.Equal(t, change1, changes[key])
	})

	t.Run("🟢 keeps change with highest OperationID", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		change1 := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "1000000",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 100,
		}
		change2 := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "2000000",
			Operation:   types.SACBalanceOpUpdate,
			OperationID: 200, // Higher operation ID
		}
		change3 := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "500000",
			Operation:   types.SACBalanceOpUpdate,
			OperationID: 50, // Lower operation ID - should be ignored
		}

		buffer.PushSACBalanceChange(change1)
		buffer.PushSACBalanceChange(change2)
		buffer.PushSACBalanceChange(change3) // Should be ignored (lower opID)

		changes := buffer.GetSACBalanceChanges()
		assert.Len(t, changes, 1)

		key := SACBalanceChangeKey{AccountID: accountID, ContractID: contractID}
		assert.Equal(t, "2000000", changes[key].Balance)
		assert.Equal(t, int64(200), changes[key].OperationID)
	})

	t.Run("🟢 handles ADD→REMOVE no-op case", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		addChange := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "1000000",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 100,
		}
		removeChange := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "0",
			Operation:   types.SACBalanceOpRemove,
			OperationID: 200,
		}

		buffer.PushSACBalanceChange(addChange)
		buffer.PushSACBalanceChange(removeChange)

		// ADD→REMOVE in same batch is a no-op - entry should be removed
		changes := buffer.GetSACBalanceChanges()
		assert.Len(t, changes, 0)
	})

	t.Run("🟢 UPDATE→REMOVE is NOT a no-op", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		updateChange := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "1000000",
			Operation:   types.SACBalanceOpUpdate,
			OperationID: 100,
		}
		removeChange := types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "0",
			Operation:   types.SACBalanceOpRemove,
			OperationID: 200,
		}

		buffer.PushSACBalanceChange(updateChange)
		buffer.PushSACBalanceChange(removeChange)

		// UPDATE→REMOVE is NOT a no-op - the balance existed before and needs deletion
		changes := buffer.GetSACBalanceChanges()
		assert.Len(t, changes, 1)

		key := SACBalanceChangeKey{AccountID: accountID, ContractID: contractID}
		assert.Equal(t, types.SACBalanceOpRemove, changes[key].Operation)
	})

	t.Run("🟢 handles multiple accounts and contracts", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		account1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		account2 := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"
		contract1 := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"
		contract2 := "CBGTG7XFRY3L6OKAUTR6KGDKUXUQBX3YDJ3QFDYTGVMOM7VV4O7NCODG"

		changes := []types.SACBalanceChange{
			{AccountID: account1, ContractID: contract1, Balance: "100", Operation: types.SACBalanceOpAdd, OperationID: 1},
			{AccountID: account1, ContractID: contract2, Balance: "200", Operation: types.SACBalanceOpAdd, OperationID: 2},
			{AccountID: account2, ContractID: contract1, Balance: "300", Operation: types.SACBalanceOpAdd, OperationID: 3},
		}

		for _, change := range changes {
			buffer.PushSACBalanceChange(change)
		}

		result := buffer.GetSACBalanceChanges()
		assert.Len(t, result, 3)

		// Verify each unique (account, contract) pair is stored
		key1 := SACBalanceChangeKey{AccountID: account1, ContractID: contract1}
		key2 := SACBalanceChangeKey{AccountID: account1, ContractID: contract2}
		key3 := SACBalanceChangeKey{AccountID: account2, ContractID: contract1}

		assert.Equal(t, "100", result[key1].Balance)
		assert.Equal(t, "200", result[key2].Balance)
		assert.Equal(t, "300", result[key3].Balance)
	})
}

func TestIndexerBuffer_MergeSACBalanceChanges(t *testing.T) {
	t.Run("🟢 merge SAC balance changes from two buffers", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		account1 := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		account2 := "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM"
		contract1 := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		buffer1.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   account1,
			ContractID:  contract1,
			Balance:     "100",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 1,
		})

		buffer2.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   account2,
			ContractID:  contract1,
			Balance:     "200",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 2,
		})

		buffer1.Merge(buffer2)

		changes := buffer1.GetSACBalanceChanges()
		assert.Len(t, changes, 2)
	})

	t.Run("🟢 merge keeps higher OperationID during merge", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		buffer1.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "100",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 50,
		})

		buffer2.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "200",
			Operation:   types.SACBalanceOpUpdate,
			OperationID: 100, // Higher
		})

		buffer1.Merge(buffer2)

		changes := buffer1.GetSACBalanceChanges()
		assert.Len(t, changes, 1)

		key := SACBalanceChangeKey{AccountID: accountID, ContractID: contractID}
		assert.Equal(t, "200", changes[key].Balance)
		assert.Equal(t, int64(100), changes[key].OperationID)
	})

	t.Run("🟢 merge handles ADD→REMOVE no-op across buffers", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		// Buffer1 has ADD
		buffer1.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "100",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 50,
		})

		// Buffer2 has REMOVE (higher opID)
		buffer2.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "0",
			Operation:   types.SACBalanceOpRemove,
			OperationID: 100,
		})

		buffer1.Merge(buffer2)

		// ADD→REMOVE across merge is a no-op
		changes := buffer1.GetSACBalanceChanges()
		assert.Len(t, changes, 0)
	})

	t.Run("🟢 merge ignores lower OperationID from other buffer", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"

		// Buffer1 has higher opID
		buffer1.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "200",
			Operation:   types.SACBalanceOpUpdate,
			OperationID: 100,
		})

		// Buffer2 has lower opID - should be ignored
		buffer2.PushSACBalanceChange(types.SACBalanceChange{
			AccountID:   accountID,
			ContractID:  contractID,
			Balance:     "50",
			Operation:   types.SACBalanceOpAdd,
			OperationID: 50,
		})

		buffer1.Merge(buffer2)

		changes := buffer1.GetSACBalanceChanges()
		assert.Len(t, changes, 1)

		key := SACBalanceChangeKey{AccountID: accountID, ContractID: contractID}
		assert.Equal(t, "200", changes[key].Balance)
		assert.Equal(t, int64(100), changes[key].OperationID)
	})
}
