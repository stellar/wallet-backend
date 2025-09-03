package indexer

import (
	"fmt"
	"sync"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func buildStateChange(toID int64, stateChangeCategory types.StateChangeCategory, reason types.StateChangeReason, accountID string, txHash string, operationID int64) types.StateChange {
	return types.StateChange{
		ToID:                toID,
		StateChangeCategory: stateChangeCategory,
		StateChangeReason:   &reason,
		AccountID:           accountID,
		TxHash:              txHash,
		OperationID:         operationID,
		SortKey:             fmt.Sprintf("%d:%s:%s", toID, stateChangeCategory, accountID),
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

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet("tx_hash_1", "tx_hash_2"),
			"bob":   set.NewSet("tx_hash_2"),
			"chuck": set.NewSet("tx_hash_2"),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert GetParticipantTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("chuck"))

		// Assert txByHash
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert Participants
		assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.Participants)

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())

		// Assert operations
		wantOpByID := map[int64]types.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)

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
			assert.Equal(t, set.NewSet("alice", "bob", "chuck"), indexerBuffer.Participants)
			wg.Done()
		}()

		// Assert GetAllTransactions
		go func() {
			assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetAllTransactions())
			wg.Done()
		}()

		// Wait for all getter operations to complete
		wg.Wait()

		// Assert txByHash
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)

		// Assert participants txHashes
		wantTxHashesByParticipant := map[string]set.Set[string]{
			"alice": set.NewSet("tx_hash_1", "tx_hash_2"),
			"bob":   set.NewSet("tx_hash_2"),
			"chuck": set.NewSet("tx_hash_2"),
		}
		assert.Equal(t, wantTxHashesByParticipant, indexerBuffer.txHashesByParticipant)

		// Assert operations
		wantOpByID := map[int64]types.Operation{
			1: op1,
			2: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)
	})
}

func Test_IndexerBuffer_StateChanges(t *testing.T) {
	t.Run("🟢sequential_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1}

		indexerBuffer.PushStateChanges([]types.StateChange{sc1})
		indexerBuffer.PushStateChanges([]types.StateChange{sc2, sc3})

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢concurrent_calls", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		sc1 := types.StateChange{ToID: 1, StateChangeOrder: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeOrder: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeOrder: 1}

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChanges([]types.StateChange{sc1})
		}()

		go func() {
			defer wg.Done()
			indexerBuffer.PushStateChanges([]types.StateChange{sc2, sc3})
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
		indexerBuffer.PushParticipantOperation("someone", op1, tx1)
		indexerBuffer.PushParticipantOperation("someone", op2, tx2)

		sc1 := buildStateChange(3, types.StateChangeCategoryBalance, types.StateChangeReasonCredit, "alice", tx1.Hash, op1.ID)
		sc2 := buildStateChange(4, types.StateChangeCategoryBalance, types.StateChangeReasonDebit, "alice", tx2.Hash, op2.ID)
		sc3 := buildStateChange(4, types.StateChangeCategoryBalance, types.StateChangeReasonCredit, "eve", tx2.Hash, op2.ID)
		// These are fee state changes, so they don't have an operation ID.
		sc4 := buildStateChange(1, types.StateChangeCategoryBalance, types.StateChangeReasonDebit, "bob", tx2.Hash, 0)
		sc5 := buildStateChange(2, types.StateChangeCategoryBalance, types.StateChangeReasonDebit, "bob", tx2.Hash, 0)

		indexerBuffer.PushStateChanges([]types.StateChange{sc1})
		indexerBuffer.PushStateChanges([]types.StateChange{sc2, sc3, sc4})
		indexerBuffer.PushStateChanges([]types.StateChange{sc5})

		allStateChanges := indexerBuffer.GetAllStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3, sc4, sc5}, allStateChanges)

		// Assert transactions
		wantTxByHash := map[string]types.Transaction{
			"tx_hash_1": tx1,
			"tx_hash_2": tx2,
		}
		assert.Equal(t, wantTxByHash, indexerBuffer.txByHash)
		assert.ElementsMatch(t, []types.Transaction{tx1, tx2}, indexerBuffer.GetParticipantTransactions("alice"))
		assert.ElementsMatch(t, []types.Transaction{tx2}, indexerBuffer.GetParticipantTransactions("bob"))

		// Assert operations
		wantOpByID := map[int64]types.Operation{
			3: op1,
			4: op2,
		}
		assert.Equal(t, wantOpByID, indexerBuffer.opByID)
		assert.Equal(t, map[int64]types.Operation{3: op1, 4: op2}, indexerBuffer.GetParticipantOperations("alice"))
		assert.Nil(t, indexerBuffer.GetParticipantOperations("bob"))

		// Assert participants
		assert.Equal(t, set.NewSet("alice", "bob", "eve", "someone"), indexerBuffer.Participants)

		// Assert state change order
		indexerBuffer.CalculateStateChangeOrder()
		allStateChanges = indexerBuffer.GetAllStateChanges()
		assert.Equal(t, int64(1), allStateChanges[0].StateChangeOrder)
		assert.Equal(t, int64(1), allStateChanges[1].StateChangeOrder)
		assert.Equal(t, int64(1), allStateChanges[2].StateChangeOrder)

		// For state changes with same operation ID, the order is calculated using the
		// sort key and the credit state change comes before the debit one.
		assert.Equal(t, int64(1), allStateChanges[3].StateChangeOrder)
		assert.Equal(t, types.StateChangeCategoryBalance, allStateChanges[3].StateChangeCategory)
		assert.Equal(t, int64(2), allStateChanges[4].StateChangeOrder)
		assert.Equal(t, types.StateChangeCategoryBalance, allStateChanges[4].StateChangeCategory)
	})
}
