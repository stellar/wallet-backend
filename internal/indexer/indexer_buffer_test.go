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

func buildStateChange(toID int64, reason types.StateChangeReason, accountID string) types.StateChange {
	return types.StateChange{
		ToID:                toID,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   &reason,
		AccountID:           accountID,
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
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.Hash])

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, indexerBuffer.GetTransactions())
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
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx2.Hash])

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
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])

		// Assert transactions were also added
		txParticipants := indexerBuffer.GetTransactionsParticipants()
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
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])
	})
}

func TestIndexerBuffer_PushStateChange(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
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

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
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

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 2}
		op1 := types.Operation{ID: 3}
		op2 := types.Operation{ID: 4}
		op3 := types.Operation{ID: 5}
		indexerBuffer.PushOperation("someone", op1, tx1)
		indexerBuffer.PushOperation("someone", op2, tx2)

		sc1 := buildStateChange(3, types.StateChangeReasonCredit, "alice")
		sc2 := buildStateChange(4, types.StateChangeReasonDebit, "alice")
		sc3 := buildStateChange(4, types.StateChangeReasonCredit, "eve")
		// These are fee state changes, so they don't have an operation ID.
		sc4 := buildStateChange(1, types.StateChangeReasonDebit, "bob")
		sc5 := buildStateChange(2, types.StateChangeReasonDebit, "bob")

		indexerBuffer.PushStateChange(tx1, op1, sc1)
		indexerBuffer.PushStateChange(tx2, op2, sc2)
		indexerBuffer.PushStateChange(tx2, op3, sc3)               // This operation should be added
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc4) // Fee state changes don't have an operation
		indexerBuffer.PushStateChange(tx2, types.Operation{}, sc5) // Fee state changes don't have an operation

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3, sc4, sc5}, allStateChanges)

		// Assert transaction participants
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("someone", "alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("someone", "alice", "eve", "bob"), txParticipants[tx2.Hash])

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

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 100 << 32}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 101 << 32}

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

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx1)
		indexerBuffer.PushTransaction("alice", tx2)

		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx2.Hash])
	})
}

func TestIndexerBuffer_GetAllOperations(t *testing.T) {
	t.Run("🟢 returns all unique operations", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
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

		tx1 := types.Transaction{Hash: "tx_hash_1"}
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

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
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

func TestIndexerBuffer_GetAllParticipants(t *testing.T) {
	t.Run("🟢 returns empty for new buffer", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()
		participants := indexerBuffer.GetAllParticipants()
		assert.Empty(t, participants)
	})

	t.Run("🟢 collects participants from transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("alice", tx2) // duplicate participant

		participants := indexerBuffer.GetAllParticipants()
		assert.ElementsMatch(t, []string{"alice", "bob"}, participants)
	})

	t.Run("🟢 collects participants from operations", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "tx_hash_1"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", op1, tx)
		indexerBuffer.PushOperation("bob", op2, tx)
		indexerBuffer.PushOperation("charlie", op2, tx)

		participants := indexerBuffer.GetAllParticipants()
		assert.ElementsMatch(t, []string{"alice", "bob", "charlie"}, participants)
	})

	t.Run("🟢 collects participants from state changes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1, AccountID: "charlie"} // fee change

		indexerBuffer.PushStateChange(tx, op, sc1)
		indexerBuffer.PushStateChange(tx, op, sc2)
		indexerBuffer.PushStateChange(tx, types.Operation{}, sc3)

		participants := indexerBuffer.GetAllParticipants()
		assert.ElementsMatch(t, []string{"alice", "bob", "charlie"}, participants)
	})

	t.Run("🟢 collects unique participants from all sources", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		op := types.Operation{ID: 1}
		sc := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "dave"}

		// Add participants from different sources
		indexerBuffer.PushTransaction("alice", tx)
		indexerBuffer.PushOperation("bob", op, tx)
		indexerBuffer.PushStateChange(tx, op, sc)

		participants := indexerBuffer.GetAllParticipants()
		// alice (tx), bob (op which also adds to tx), dave (state change which also adds to tx and op)
		assert.ElementsMatch(t, []string{"alice", "bob", "dave"}, participants)
	})

	t.Run("🟢 ignores empty participants", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "tx_hash_1"}
		indexerBuffer.PushTransaction("", tx) // empty participant
		indexerBuffer.PushTransaction("alice", tx)

		participants := indexerBuffer.GetAllParticipants()
		assert.ElementsMatch(t, []string{"alice"}, participants)
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

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}

		buffer1.PushTransaction("alice", tx1)
		buffer2.PushTransaction("bob", tx2)

		buffer1.Merge(buffer2)

		// Verify transactions
		allTxs := buffer1.GetTransactions()
		assert.Len(t, allTxs, 2)
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, allTxs)

		// Verify transaction participants
		txParticipants := buffer1.GetTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.Hash])
	})

	t.Run("🟢 merge operations only", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
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

		tx := types.Transaction{Hash: "test_tx_hash", ToID: 1}
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

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
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
		assert.Equal(t, set.NewSet("alice", "bob"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("charlie"), txParticipants[tx2.Hash])

		// Verify operation participants merged
		opParticipants := buffer1.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice", "bob"), opParticipants[int64(1)])
	})

	t.Run("🟢 merge into empty buffer", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
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

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		buffer1.PushTransaction("alice", tx1)

		buffer1.Merge(buffer2)

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

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 2}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}
		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1, AccountID: "bob"}

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
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("bob"), txParticipants[tx2.Hash])

		opParticipants := buffer1.GetOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob"), opParticipants[int64(2)])
	})

	t.Run("🟢 merge all participants", func(t *testing.T) {
		buffer1 := NewIndexerBuffer()
		buffer2 := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1", ToID: 1}
		tx2 := types.Transaction{Hash: "tx_hash_2", ToID: 2}

		buffer1.PushTransaction("alice", tx1)
		buffer1.PushTransaction("bob", tx1)
		buffer2.PushTransaction("charlie", tx2)
		buffer2.PushTransaction("dave", tx2)

		buffer1.Merge(buffer2)

		// Verify all participants merged
		allParticipants := buffer1.GetAllParticipants()
		assert.ElementsMatch(t, []string{"alice", "bob", "charlie", "dave"}, allParticipants)
	})
}
