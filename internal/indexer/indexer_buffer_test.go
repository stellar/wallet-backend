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

func buildStateChange(toID int64, reason types.StateChangeReason, accountID string, txHash string, operationID int64) types.StateChange {
	return types.StateChange{
		ToID:                toID,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		AccountID:           accountID,
		TxHash:              txHash,
		OperationID:         operationID,
		SortKey:             fmt.Sprintf("%d:%s:%s", toID, types.StateChangeCategoryBalance, accountID),
	}
}

func TestIndexerBuffer_PushTransaction(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("alice", tx2)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("bob", tx2) // duplicate is a no-op

		// Assert participants by transaction
		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.Hash])

		// Assert participants
		assert.Equal(t, set.NewSet("alice", "bob"), indexerBuffer.participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

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
		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.Hash])

		// Assert participants
		assert.Equal(t, set.NewSet("alice", "bob"), indexerBuffer.participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
	})
}

func TestIndexerBuffer_PushOperation(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2) // duplicate operation ID is a no-op

		// Assert participants by operation
		opParticipants := indexerBuffer.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])

		// Assert participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.participants)

		// Assert transactions were also added
		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("bob", "chuck"), txParticipants[tx2.Hash])
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
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
		opParticipants := indexerBuffer.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])

		// Assert participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.participants)
	})
}

func TestIndexerBuffer_PushStateChange(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
		op := types.Operation{ID: 1, TxHash: tx.Hash}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1}

		indexerBuffer.PushStateChange(tx, op, sc1)
		indexerBuffer.PushStateChange(tx, op, sc2)
		indexerBuffer.PushStateChange(tx, op, sc3)

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢 concurrent pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
		op := types.Operation{ID: 1, TxHash: tx.Hash}

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

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Len(t, allStateChanges, 3)
		assert.ElementsMatch(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢 with operations and transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 2}
		op1 := types.Operation{ID: 3, TxHash: tx1.Hash}
		op2 := types.Operation{ID: 4, TxHash: tx2.Hash}
		op3 := types.Operation{ID: 5, TxHash: tx2.Hash}
		indexerBuffer.PushOperation("someone", op1, tx1)
		indexerBuffer.PushOperation("someone", op2, tx2)

		sc1 := buildStateChange(3, types.StateChangeReasonCredit, "alice", tx1.Hash, op1.ID)
		sc2 := buildStateChange(4, types.StateChangeReasonDebit, "alice", tx2.Hash, op2.ID)
		sc3 := buildStateChange(4, types.StateChangeReasonCredit, "eve", tx2.Hash, op3.ID)
		// These are fee state changes, so they don't have an operation ID.
		sc4 := buildStateChange(1, types.StateChangeReasonDebit, "bob", tx2.Hash, 0)
		sc5 := buildStateChange(2, types.StateChangeReasonDebit, "bob", tx2.Hash, 0)

		indexerBuffer.PushStateChange(tx1, op1, sc1)
		indexerBuffer.PushStateChange(tx2, op2, sc2)
		indexerBuffer.PushStateChange(tx2, op3, sc3)               // This operation should be added
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc4) // Fee state changes don't have an operation
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc5) // Fee state changes don't have an operation

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3, sc4, sc5}, allStateChanges)

		// Assert transaction participants
		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("someone", "alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("someone", "alice", "eve", "bob"), txParticipants[tx2.Hash])

		// Assert operation participants
		opParticipants := indexerBuffer.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("someone", "alice"), opParticipants[int64(3)])
		assert.Equal(t, set.NewSet("someone", "alice"), opParticipants[int64(4)])
		assert.Equal(t, set.NewSet("eve"), opParticipants[int64(5)])

		assert.Equal(t, set.NewSet("someone", "alice", "eve", "bob"), indexerBuffer.participants)
	})
}

func TestIndexerBuffer_GetNumberOfTransactions(t *testing.T) {
	t.Run("🟢 returns correct count", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		assert.Equal(t, 0, indexerBuffer.GetNumberOfTransactions())

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

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

		tx1 := types.Transaction{Hash: "tx_hash_1", LedgerNumber: 100}
		tx2 := types.Transaction{Hash: "tx_hash_2", LedgerNumber: 101}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("charlie", tx2) // duplicate

		allTxs := indexerBuffer.GetAllTransactions()
		require.Len(t, allTxs, 2)
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, allTxs)
	})
}

func TestIndexerBuffer_GetAllTransactionsParticipants(t *testing.T) {
	t.Run("🟢 returns correct participants mapping", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx1)
		indexerBuffer.PushTransaction("alice", tx2)

		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx2.Hash])
	})
}

func TestIndexerBuffer_GetAllOperations(t *testing.T) {
	t.Run("🟢 returns all unique operations", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}
		op2 := types.Operation{ID: 2, TxHash: tx1.Hash}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op2, tx1)
		indexerBuffer.PushOperation("charlie", op2, tx1) // duplicate

		allOps := indexerBuffer.GetAllOperations()
		require.Len(t, allOps, 2)
		assert.ElementsMatch(t, []types.Operation{op1, op2}, allOps)
	})
}

func TestIndexerBuffer_GetAllOperationsParticipants(t *testing.T) {
	t.Run("🟢 returns correct participants mapping", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}
		op2 := types.Operation{ID: 2, TxHash: tx1.Hash}

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op1, tx1)
		indexerBuffer.PushOperation("alice", op2, tx1)

		opParticipants := indexerBuffer.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(2)])
	})
}

func TestIndexerBuffer_GetAllStateChanges(t *testing.T) {
	t.Run("🟢 returns all state changes in order", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
		op := types.Operation{ID: 1, TxHash: tx.Hash}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1, AccountID: "charlie"}

		indexerBuffer.PushStateChange(tx, op, sc1)
		indexerBuffer.PushStateChange(tx, op, sc2)
		indexerBuffer.PushStateChange(tx, op, sc3)

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})
}

func TestIndexerBuffer_MergeBuffer(t *testing.T) {
	t.Run("🟢 merge empty buffers", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		buffer1.MergeBuffer(buffer2)

		assert.Equal(t, 0, buffer1.participants.Cardinality())
		assert.Equal(t, 0, buffer1.GetNumberOfTransactions())
		assert.Len(t, buffer1.GetAllStateChanges(), 0)
	})

	t.Run("🟢 merge transactions only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		buffer1.PushTransaction("alice", tx1)
		buffer2.PushTransaction("bob", tx2)

		buffer1.MergeBuffer(buffer2)

		// Verify participants
		assert.Equal(t, set.NewSet("alice", "bob"), buffer1.participants)

		// Verify transactions
		allTxs := buffer1.GetAllTransactions()
		assert.Len(t, allTxs, 2)
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, allTxs)

		// Verify transaction participants
		txParticipants := buffer1.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.Hash])
	})

	t.Run("🟢 merge operations only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}
		op2 := types.Operation{ID: 2, TxHash: tx1.Hash}

		buffer1.PushOperation("alice", op1, tx1)
		buffer2.PushOperation("bob", op2, tx1)

		buffer1.MergeBuffer(buffer2)

		// Verify participants
		assert.Equal(t, set.NewSet("alice", "bob"), buffer1.participants)

		// Verify operations
		allOps := buffer1.GetAllOperations()
		assert.Len(t, allOps, 2)
		assert.ElementsMatch(t, []types.Operation{op1, op2}, allOps)

		// Verify operation participants
		opParticipants := buffer1.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob"), opParticipants[int64(2)])
	})

	t.Run("🟢 merge state changes only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
		op := types.Operation{ID: 1, TxHash: tx.Hash}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}

		buffer1.PushStateChange(tx, op, sc1)
		buffer2.PushStateChange(tx, op, sc2)

		buffer1.MergeBuffer(buffer2)

		// Verify state changes
		allStateChanges := buffer1.GetAllStateChanges()
		assert.Len(t, allStateChanges, 2)
		assert.Equal(t, sc1, allStateChanges[0])
		assert.Equal(t, sc2, allStateChanges[1])
	})

	t.Run("🟢 merge with overlapping data", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}

		// Buffer1 has tx1 with alice
		buffer1.PushTransaction("alice", tx1)
		buffer1.PushOperation("alice", op1, tx1)

		// Buffer2 has tx1 with bob (overlapping tx) and tx2 with charlie
		buffer2.PushTransaction("bob", tx1)
		buffer2.PushTransaction("charlie", tx2)
		buffer2.PushOperation("bob", op1, tx1)

		buffer1.MergeBuffer(buffer2)

		// Verify all participants
		assert.Equal(t, set.NewSet("alice", "bob", "charlie"), buffer1.participants)

		// Verify transactions
		allTxs := buffer1.GetAllTransactions()
		assert.Len(t, allTxs, 2)

		// Verify tx1 has both alice and bob as participants
		txParticipants := buffer1.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("charlie"), txParticipants[tx2.Hash])

		// Verify operation participants merged
		opParticipants := buffer1.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), opParticipants[int64(1)])
	})

	t.Run("🟢 merge into empty buffer", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}
		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}

		buffer2.PushTransaction("alice", tx1)
		buffer2.PushOperation("bob", op1, tx1)
		buffer2.PushStateChange(tx1, op1, sc1)

		buffer1.MergeBuffer(buffer2)

		// Verify everything was copied
		assert.Equal(t, set.NewSet("alice", "bob"), buffer1.participants)
		assert.Equal(t, 1, buffer1.GetNumberOfTransactions())
		assert.Len(t, buffer1.GetAllOperations(), 1)
		assert.Len(t, buffer1.GetAllStateChanges(), 1)
	})

	t.Run("🟢 merge empty buffer into populated", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		buffer1.PushTransaction("alice", tx1)

		buffer1.MergeBuffer(buffer2)

		// Buffer1 should remain unchanged
		assert.Equal(t, set.NewSet("alice"), buffer1.participants)
		assert.Equal(t, 1, buffer1.GetNumberOfTransactions())
	})

	t.Run("🟢 concurrent merges", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()
		buffer3 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		tx3 := types.Transaction{Hash: "tx_hash_3"}

		buffer1.PushTransaction("alice", tx1)
		buffer2.PushTransaction("bob", tx2)
		buffer3.PushTransaction("charlie", tx3)

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			buffer1.MergeBuffer(buffer2)
		}()

		go func() {
			defer wg.Done()
			buffer1.MergeBuffer(buffer3)
		}()

		wg.Wait()

		// Verify all data merged correctly
		assert.Equal(t, set.NewSet("alice", "bob", "charlie"), buffer1.participants)
		assert.Equal(t, 3, buffer1.GetNumberOfTransactions())
	})

	t.Run("🟢 merge complete buffers with all data types", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 2}
		op1 := types.Operation{ID: 1, TxHash: tx1.Hash}
		op2 := types.Operation{ID: 2, TxHash: tx2.Hash}
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

		buffer1.MergeBuffer(buffer2)

		// Verify all participants
		assert.Equal(t, set.NewSet("alice", "bob"), buffer1.participants)

		// Verify transactions
		allTxs := buffer1.GetAllTransactions()
		assert.Len(t, allTxs, 2)

		// Verify operations
		allOps := buffer1.GetAllOperations()
		assert.Len(t, allOps, 2)

		// Verify state changes
		allStateChanges := buffer1.GetAllStateChanges()
		assert.Len(t, allStateChanges, 2)
		assert.Equal(t, sc1, allStateChanges[0])
		assert.Equal(t, sc2, allStateChanges[1])

		// Verify participants mappings
		txParticipants := buffer1.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.Hash])

		opParticipants := buffer1.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob"), opParticipants[int64(2)])
	})
}
