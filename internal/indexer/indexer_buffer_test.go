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

		indexerBuffer.PushParticipantTransaction("alice", tx1)
		indexerBuffer.PushParticipantTransaction("alice", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2)
		indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally

		indexerBuffer.PushParticipantOperation("alice", op1, tx1)
		indexerBuffer.PushParticipantOperation("bob", op2, tx2)
		indexerBuffer.PushParticipantOperation("chuck", op2, tx2)
		indexerBuffer.PushParticipantOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally

		// Assert GetParticipantTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.GetParticipants())

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())

		// Assert GetParticipantOperations
		wantAliceOps := map[int64]types.Operation{
			1: op1,
		}
		assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
		wantBobOps := map[int64]types.Operation{
			2: op2,
		}
		assert.Equal(t, wantBobOps, indexerBuffer.GetParticipantOperations("bob"))
		wantChuckOps := map[int64]types.Operation{
			2: op2,
		}
		assert.Equal(t, wantChuckOps, indexerBuffer.GetParticipantOperations("chuck"))
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
			indexerBuffer.PushParticipantTransaction("alice", tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("alice", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("bob", tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantTransaction("bob", tx2) // <--- duplicate is a no-op because we use a Set internally
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("alice", op1, tx1)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("bob", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", op2, tx2)
			wg.Done()
		}()
		go func() {
			indexerBuffer.PushParticipantOperation("chuck", op2, tx2) // <--- duplicate operation ID is a no-op because we use a Set internally
			wg.Done()
		}()
		wg.Wait()

		// Concurrent getter operations
		wg = sync.WaitGroup{}
		wg.Add(9)

		// GetParticipantTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
			wg.Done()
		}()
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))
			wg.Done()
		}()

		// GetParticipantOperations
		go func() {
			wantAliceOps := map[int64]types.Operation{
				1: op1,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("alice"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]types.Operation{
				2: op2,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("bob"))
			wg.Done()
		}()
		go func() {
			wantAliceOps := map[int64]types.Operation{
				2: op2,
			}
			assert.Equal(t, wantAliceOps, indexerBuffer.GetParticipantOperations("chuck"))
			wg.Done()
		}()

		// Assert GetNumberOfTransactions
		go func() {
			assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
			wg.Done()
		}()

		// Assert Participants
		go func() {
			assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.GetParticipants())
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
		indexerBuffer.PushParticipantOperation("someone", op1, tx1)
		indexerBuffer.PushParticipantOperation("someone", op2, tx2)

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

		// Assert transactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("someone"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("eve"))
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))

		// Assert operations
		assert.Equal(t, map[int64]types.Operation{3: op1, 4: op2}, indexerBuffer.GetParticipantOperations("someone"))
		assert.Equal(t, map[int64]types.Operation{5: op3}, indexerBuffer.GetParticipantOperations("eve"))
		assert.Equal(t, map[int64]types.Operation{3: op1, 4: op2}, indexerBuffer.GetParticipantOperations("alice"))
		assert.Nil(t, indexerBuffer.GetParticipantOperations("bob"))

		assert.Equal(t, set.NewSet("someone", "alice", "eve", "bob"), indexerBuffer.GetParticipants())
	})
}
