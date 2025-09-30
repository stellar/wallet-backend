package indexer

import (
	"fmt"
	"sync"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"

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

func Test_IndexerBuffer_PushParticipantTransaction_and_Getters(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushTransaction("alice", tx1)
		indexerBuffer.PushTransaction("alice", tx2)
		indexerBuffer.PushTransaction("bob", tx2)
		indexerBuffer.PushTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		indexerBuffer.PushOperation("alice", op1, tx1)
		indexerBuffer.PushOperation("bob", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2)
		indexerBuffer.PushOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally

		// Assert participants by transaction
		txParticipants := indexerBuffer.GetAllTransactionsParticipants()
		assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), txParticipants[tx2.Hash])

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())

		// Assert participants by operation
		opParticipants := indexerBuffer.GetAllOperationsParticipants()
		assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "tx_hash_1"}
		tx2 := types.Transaction{Hash: "tx_hash_2"}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		// Concurrent push operations
		wg := sync.WaitGroup{}
		wg.Add(8)
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
			indexerBuffer.PushTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally
			wg.Done()
		}()
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
			indexerBuffer.PushOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally
			wg.Done()
		}()
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(5)

		// Assert participants by transaction
		go func() {
			txParticipants := indexerBuffer.GetAllTransactionsParticipants()
			assert.Equal(t, set.NewSet("alice"), txParticipants[tx1.Hash])
			assert.Equal(t, set.NewSet("alice", "bob", "chuck"), txParticipants[tx2.Hash])
			wg.Done()
		}()

		// Assert participants by operation
		go func() {
			opParticipants := indexerBuffer.GetAllOperationsParticipants()
			assert.Equal(t, set.NewSet("alice"), opParticipants[int64(1)])
			assert.Equal(t, set.NewSet("bob", "chuck"), opParticipants[int64(2)])
			wg.Done()
		}()

		// Assert GetNumberOfTransactions
		go func() {
			assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
			wg.Done()
		}()

		// Assert Participants
		go func() {
			assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.participants)
			wg.Done()
		}()

		// Assert GetAllTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
			wg.Done()
		}()

		// Wait for all getter operations to complete
		wg.Wait()
	})
}

func Test_IndexerBuffer_StateChanges(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		// Create test transaction and operation for the new PushStateChange signature
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

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		// Create test transaction and operation for the new PushStateChange signature
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

	t.Run("🟢sequential_calls_with_ops_and_txs", func(t *testing.T) {
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
