package indexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func participantSet(addrs ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(addrs))
	for _, a := range addrs {
		m[a] = struct{}{}
	}
	return m
}

func buildStateChange(toID int64, reason types.StateChangeReason, accountID string, operationID int64) types.StateChange {
	return types.StateChange{
		ToID:                toID,
		StateChangeCategory: types.StateChangeCategoryBalance,
		StateChangeReason:   reason,
		AccountID:           types.AddressBytea(accountID),
		OperationID:         operationID,
	}
}

func TestIndexerBuffer_PushTransaction(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		indexerBuffer.PushTransaction("alice", &tx1)
		indexerBuffer.PushTransaction("alice", &tx2)
		indexerBuffer.PushTransaction("bob", &tx2)
		indexerBuffer.PushTransaction("bob", &tx2) // duplicate is a no-op

		// Assert participants by transaction
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, participantSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, participantSet("alice", "bob"), txParticipants[tx2.ToID])

		// Assert GetNumberOfTransactions
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Assert GetAllTransactions
		assert.ElementsMatch(t, []*types.Transaction{&tx1, &tx2}, indexerBuffer.GetTransactions())
	})
}

func TestIndexerBuffer_PushOperation(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", &op1, &tx1)
		indexerBuffer.PushOperation("bob", &op2, &tx2)
		indexerBuffer.PushOperation("chuck", &op2, &tx2)
		indexerBuffer.PushOperation("chuck", &op2, &tx2) // duplicate operation ID is a no-op

		// Assert participants by operation
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, participantSet("alice"), opParticipants[int64(1)])
		assert.Equal(t, participantSet("bob", "chuck"), opParticipants[int64(2)])

		// Assert transactions were also added
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, participantSet("alice"), txParticipants[tx1.ToID])
		assert.Equal(t, participantSet("bob", "chuck"), txParticipants[tx2.ToID])
	})
}

func TestIndexerBuffer_PushStateChange(t *testing.T) {
	t.Run("🟢 sequential pushes", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeID: 1}
		sc2 := types.StateChange{ToID: 2, StateChangeID: 1}
		sc3 := types.StateChange{ToID: 3, StateChangeID: 1}

		indexerBuffer.PushStateChange(&tx, &op, sc1)
		indexerBuffer.PushStateChange(&tx, &op, sc2)
		indexerBuffer.PushStateChange(&tx, &op, sc3)

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})

	t.Run("🟢 with operations and transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}
		op1 := types.Operation{ID: 3}
		op2 := types.Operation{ID: 4}
		op3 := types.Operation{ID: 5}
		indexerBuffer.PushOperation("someone", &op1, &tx1)
		indexerBuffer.PushOperation("someone", &op2, &tx2)

		sc1 := buildStateChange(3, types.StateChangeReasonCredit, "alice", op1.ID)
		sc2 := buildStateChange(4, types.StateChangeReasonDebit, "alice", op2.ID)
		sc3 := buildStateChange(4, types.StateChangeReasonCredit, "eve", op3.ID)
		// These are fee state changes, so they don't have an operation ID.
		sc4 := buildStateChange(1, types.StateChangeReasonDebit, "bob", 0)
		sc5 := buildStateChange(2, types.StateChangeReasonDebit, "bob", 0)

		indexerBuffer.PushStateChange(&tx1, &op1, sc1)
		indexerBuffer.PushStateChange(&tx2, &op2, sc2)
		indexerBuffer.PushStateChange(&tx2, &op3, sc3) // This operation should be added
		indexerBuffer.PushStateChange(&tx2, nil, sc4)  // Fee state changes don't have an operation
		indexerBuffer.PushStateChange(&tx2, nil, sc5)  // Fee state changes don't have an operation

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3, sc4, sc5}, allStateChanges)

		// Assert transaction participants
		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, participantSet("someone", "alice"), txParticipants[tx1.ToID])
		assert.Equal(t, participantSet("someone", "alice", "eve", "bob"), txParticipants[tx2.ToID])

		// Assert operation participants
		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, participantSet("someone", "alice"), opParticipants[int64(3)])
		assert.Equal(t, participantSet("someone", "alice"), opParticipants[int64(4)])
		assert.Equal(t, participantSet("eve"), opParticipants[int64(5)])
	})
}

func TestIndexerBuffer_GetNumberOfTransactions(t *testing.T) {
	t.Run("🟢 returns correct count", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		assert.Equal(t, 0, indexerBuffer.GetNumberOfTransactions())

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2}

		indexerBuffer.PushTransaction("alice", &tx1)
		assert.Equal(t, 1, indexerBuffer.GetNumberOfTransactions())

		indexerBuffer.PushTransaction("bob", &tx2)
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())

		// Duplicate should not increase count
		indexerBuffer.PushTransaction("charlie", &tx2)
		assert.Equal(t, 2, indexerBuffer.GetNumberOfTransactions())
	})
}

func TestIndexerBuffer_GetAllTransactions(t *testing.T) {
	t.Run("🟢 returns all unique transactions", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1, LedgerNumber: 100}
		tx2 := types.Transaction{Hash: "a76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48761", ToID: 2, LedgerNumber: 101}

		indexerBuffer.PushTransaction("alice", &tx1)
		indexerBuffer.PushTransaction("bob", &tx2)
		indexerBuffer.PushTransaction("charlie", &tx2) // duplicate

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

		indexerBuffer.PushTransaction("alice", &tx1)
		indexerBuffer.PushTransaction("bob", &tx1)
		indexerBuffer.PushTransaction("alice", &tx2)

		txParticipants := indexerBuffer.GetTransactionsParticipants()
		assert.Equal(t, participantSet("alice", "bob"), txParticipants[tx1.ToID])
		assert.Equal(t, participantSet("alice"), txParticipants[tx2.ToID])
	})
}

func TestIndexerBuffer_GetAllOperations(t *testing.T) {
	t.Run("🟢 returns all unique operations", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx1 := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op1 := types.Operation{ID: 1}
		op2 := types.Operation{ID: 2}

		indexerBuffer.PushOperation("alice", &op1, &tx1)
		indexerBuffer.PushOperation("bob", &op2, &tx1)
		indexerBuffer.PushOperation("charlie", &op2, &tx1) // duplicate

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

		indexerBuffer.PushOperation("alice", &op1, &tx1)
		indexerBuffer.PushOperation("bob", &op1, &tx1)
		indexerBuffer.PushOperation("alice", &op2, &tx1)

		opParticipants := indexerBuffer.GetOperationsParticipants()
		assert.Equal(t, participantSet("alice", "bob"), opParticipants[int64(1)])
		assert.Equal(t, participantSet("alice"), opParticipants[int64(2)])
	})
}

func TestIndexerBuffer_GetAllStateChanges(t *testing.T) {
	t.Run("🟢 returns all state changes in order", func(t *testing.T) {
		indexerBuffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "c76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48763", ToID: 1}
		op := types.Operation{ID: 1}

		sc1 := types.StateChange{ToID: 1, StateChangeID: 1, AccountID: "alice"}
		sc2 := types.StateChange{ToID: 2, StateChangeID: 1, AccountID: "bob"}
		sc3 := types.StateChange{ToID: 3, StateChangeID: 1, AccountID: "charlie"}

		indexerBuffer.PushStateChange(&tx, &op, sc1)
		indexerBuffer.PushStateChange(&tx, &op, sc2)
		indexerBuffer.PushStateChange(&tx, &op, sc3)

		allStateChanges := indexerBuffer.GetStateChanges()
		assert.Equal(t, []types.StateChange{sc1, sc2, sc3}, allStateChanges)
	})
}

func TestIndexerBuffer_IngestTransactionResult(t *testing.T) {
	t.Run("🟢 folds a single result into the buffer", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		tx := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}
		op := types.Operation{ID: 10}
		sc := buildStateChange(1, types.StateChangeReasonCredit, "alice", 10)

		result := &TransactionResult{
			Transaction:      &tx,
			TxParticipants:   []string{"alice"},
			Operations:       map[int64]*types.Operation{10: &op},
			OpParticipants:   map[int64][]string{10: {"alice", "bob"}},
			StateChanges:     []types.StateChange{sc},
			AccountChanges:   []types.AccountChange{{AccountID: "alice", SortKey: 1, Operation: types.AccountOpUpdate, Balance: 100}},
			LPShareChanges:   []types.LiquidityPoolShareChange{{AccountID: "alice", PoolID: "pool1", OperationID: 10, Operation: types.LiquidityPoolShareOpAdd, Shares: 5}},
			LPChanges:        []types.LiquidityPoolChange{{PoolID: "pool1", OperationID: 10, Operation: types.LiquidityPoolOpAdd, AssetA: "native", ReserveA: 1, AssetB: "USDC:GA", ReserveB: 2}},
			ParticipantCount: 2,
		}

		buffer.IngestTransactionResult(result)

		assert.Equal(t, 1, buffer.GetNumberOfTransactions())
		assert.Equal(t, 1, buffer.GetNumberOfOperations())
		assert.Equal(t, participantSet("alice", "bob"), buffer.GetOperationsParticipants()[10])
		assert.Equal(t, participantSet("alice", "bob"), buffer.GetTransactionsParticipants()[1])
		assert.Len(t, buffer.GetStateChanges(), 1)
		assert.Len(t, buffer.GetAccountChanges(), 1)
		assert.Len(t, buffer.GetLiquidityPoolShareChanges(), 1)
		assert.Len(t, buffer.GetLiquidityPoolChanges(), 1)
	})

	t.Run("🟢 LP ADD→REMOVE across folded results nets to nothing (tombstone)", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		tx := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}

		add := &TransactionResult{
			Transaction:    &tx,
			LPShareChanges: []types.LiquidityPoolShareChange{{AccountID: "alice", PoolID: "pool1", OperationID: 1, Operation: types.LiquidityPoolShareOpAdd, Shares: 5}},
			LPChanges:      []types.LiquidityPoolChange{{PoolID: "pool1", OperationID: 1, Operation: types.LiquidityPoolOpAdd}},
		}
		remove := &TransactionResult{
			Transaction:    &tx,
			LPShareChanges: []types.LiquidityPoolShareChange{{AccountID: "alice", PoolID: "pool1", OperationID: 2, Operation: types.LiquidityPoolShareOpRemove}},
			LPChanges:      []types.LiquidityPoolChange{{PoolID: "pool1", OperationID: 2, Operation: types.LiquidityPoolOpRemove}},
		}

		buffer.IngestTransactionResult(add)
		buffer.IngestTransactionResult(remove)

		assert.Len(t, buffer.GetLiquidityPoolShareChanges(), 0)
		assert.Len(t, buffer.GetLiquidityPoolChanges(), 0)
	})

	t.Run("🟢 dedups across multiple folded results (CREATE→REMOVE tombstone)", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		tx := types.Transaction{Hash: "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760", ToID: 1}

		// One result creates the account, a later result removes it within the same ledger. Folded
		// through the same buffer, the CREATE→REMOVE nets to nothing (see pushWithTombstone).
		create := &TransactionResult{
			Transaction:    &tx,
			AccountChanges: []types.AccountChange{{AccountID: "GACCT", SortKey: 1, Operation: types.AccountOpCreate, Balance: 100}},
		}
		remove := &TransactionResult{
			Transaction:    &tx,
			AccountChanges: []types.AccountChange{{AccountID: "GACCT", SortKey: 2, Operation: types.AccountOpRemove}},
		}

		buffer.IngestTransactionResult(create)
		buffer.IngestTransactionResult(remove)

		assert.Len(t, buffer.GetAccountChanges(), 0)
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

	t.Run("🟢 tombstone blocks lower-key resurrection after ADD→REMOVE", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "100", Operation: types.SACBalanceOpAdd, OperationID: 100})
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "0", Operation: types.SACBalanceOpRemove, OperationID: 200})
		// A lower-OperationID change afterward must NOT re-insert the removed balance.
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "50", Operation: types.SACBalanceOpUpdate, OperationID: 50})

		assert.Len(t, buffer.GetSACBalanceChanges(), 0)
	})

	t.Run("🟢 higher-key change re-adds a tombstoned SAC balance", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		accountID := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
		contractID := "CCWAMYJME4H5CKG7OLXGC2T4M6FL52XCZ3OQOAV6LL3GLA4RO4WH3ASP"
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "100", Operation: types.SACBalanceOpAdd, OperationID: 100})
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "0", Operation: types.SACBalanceOpRemove, OperationID: 200})
		// A genuine later re-add (higher OperationID) lifts the tombstone and wins.
		buffer.PushSACBalanceChange(types.SACBalanceChange{AccountID: accountID, ContractID: contractID, Balance: "700", Operation: types.SACBalanceOpAdd, OperationID: 300})

		changes := buffer.GetSACBalanceChanges()
		require.Len(t, changes, 1)
		key := SACBalanceChangeKey{AccountID: accountID, ContractID: contractID}
		assert.Equal(t, "700", changes[key].Balance)
	})
}

func TestIndexerBuffer_ProtocolWasms(t *testing.T) {
	t.Run("push and get protocol wasms with dedup", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		wasm1 := data.ProtocolWasms{WasmHash: "0100000000000000000000000000000000000000000000000000000000000000"}
		wasm2 := data.ProtocolWasms{WasmHash: "0200000000000000000000000000000000000000000000000000000000000000"}
		wasm1Dup := data.ProtocolWasms{WasmHash: "0100000000000000000000000000000000000000000000000000000000000000"} // duplicate

		buffer.PushProtocolWasm(wasm1)
		buffer.PushProtocolWasm(wasm2)
		buffer.PushProtocolWasm(wasm1Dup)

		wasms := buffer.GetProtocolWasms()
		assert.Len(t, wasms, 2)
		assert.Equal(t, wasm1, wasms[string(wasm1.WasmHash)])
		assert.Equal(t, wasm2, wasms[string(wasm2.WasmHash)])
	})

	t.Run("clear protocol wasms", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushProtocolWasm(data.ProtocolWasms{WasmHash: "0100000000000000000000000000000000000000000000000000000000000000"})

		buffer.Clear()

		wasms := buffer.GetProtocolWasms()
		assert.Len(t, wasms, 0)
	})
}

func TestIndexerBuffer_ProtocolContracts(t *testing.T) {
	t.Run("push and get protocol contracts with dedup", func(t *testing.T) {
		buffer := NewIndexerBuffer()

		c1 := data.ProtocolContracts{ContractID: "0100000000000000000000000000000000000000000000000000000000000000", WasmHash: "0a00000000000000000000000000000000000000000000000000000000000000"}
		c2 := data.ProtocolContracts{ContractID: "0200000000000000000000000000000000000000000000000000000000000000", WasmHash: "1400000000000000000000000000000000000000000000000000000000000000"}
		c1Dup := data.ProtocolContracts{ContractID: "0100000000000000000000000000000000000000000000000000000000000000", WasmHash: "0a00000000000000000000000000000000000000000000000000000000000000"}     // duplicate
		c1Upgrade := data.ProtocolContracts{ContractID: "0100000000000000000000000000000000000000000000000000000000000000", WasmHash: "ff00000000000000000000000000000000000000000000000000000000000000"} // upgrade with new wasm_hash

		buffer.PushProtocolContracts(c1)
		buffer.PushProtocolContracts(c2)
		buffer.PushProtocolContracts(c1Dup)
		buffer.PushProtocolContracts(c1Upgrade)

		contracts := buffer.GetProtocolContracts()
		assert.Len(t, contracts, 2)
		// Last-write-wins: c1Upgrade should overwrite c1
		assert.Equal(t, c1Upgrade, contracts[string(c1.ContractID)])
		assert.Equal(t, c2, contracts[string(c2.ContractID)])
	})

	t.Run("clear protocol contracts", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushProtocolContracts(data.ProtocolContracts{ContractID: "0100000000000000000000000000000000000000000000000000000000000000", WasmHash: "0a00000000000000000000000000000000000000000000000000000000000000"})

		buffer.Clear()

		contracts := buffer.GetProtocolContracts()
		assert.Len(t, contracts, 0)
	})
}

// accountChangeAddr is an arbitrary account address used as a map key in the
// account-change dedup tests below (the buffer keys on the string, not its validity).
const accountChangeAddr = "GBXGQJWVLWOYHFLVTKWV5FGHA3LNYY2JQKM7OAJAUEQFU6LPCSEFVXON"

// Dedup phases mirroring AccountsProcessor's canonical ordering: fee < operation < refund.
const (
	rankFee    int64 = 0
	rankOp     int64 = 1
	rankRefund int64 = 2
)

// accountRank builds an order-preserving dedup key (phase, then tx, then op) for these tests. It is
// deliberately NOT the production accountSortKey bit layout: the buffer only compares the int64, so
// any encoding that preserves (phase, tx, op) ordering exercises the dedup identically. The
// production encoding and its fee < operation < refund ordering are verified separately by
// TestAccountSortKey in the processors package. (Valid for the small tx/op values used here.)
func accountRank(phase, tx, op int64) int64 {
	return phase*1_000_000 + tx*1_000 + op
}

func TestIndexerBuffer_PushAccountChange(t *testing.T) {
	accountChange := func(sortKey, balance int64, op types.AccountOpType) types.AccountChange {
		return types.AccountChange{
			AccountID:    accountChangeAddr,
			SortKey:      sortKey,
			LedgerNumber: 12345,
			Operation:    op,
			Balance:      balance,
		}
	}

	t.Run("🟢 stores account change", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		change := accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpUpdate)
		buffer.PushAccountChange(change)

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, change, changes[accountChangeAddr])
	})

	t.Run("🟢 keeps change with highest sort key", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpUpdate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 2, 1), 200, types.AccountOpUpdate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 2), 50, types.AccountOpUpdate)) // lower than tx 2 → ignored

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, int64(200), changes[accountChangeAddr].Balance)
	})

	t.Run("🟢 handles CREATE→REMOVE no-op case", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpCreate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 2), 0, types.AccountOpRemove))

		assert.Len(t, buffer.GetAccountChanges(), 0)
	})

	t.Run("🟢 UPDATE→REMOVE is NOT a no-op", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpUpdate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 2), 0, types.AccountOpRemove))

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, types.AccountOpRemove, changes[accountChangeAddr].Operation)
	})

	t.Run("🟢 fee change captured when uncontested (issue #637)", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankFee, 1, 0), 999, types.AccountOpUpdate))

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, int64(999), changes[accountChangeAddr].Balance)
	})

	t.Run("🟢 operation beats fee regardless of push order", func(t *testing.T) {
		// Fee source in a late tx (9), op participant in an early tx (2): the op still wins
		// because phase dominates — every operation outranks every fee in the ledger.
		feeChange := accountChange(accountRank(rankFee, 9, 0), 999, types.AccountOpUpdate)
		opChange := accountChange(accountRank(rankOp, 2, 1), 500, types.AccountOpUpdate)

		feeFirst := NewIndexerBuffer()
		feeFirst.PushAccountChange(feeChange)
		feeFirst.PushAccountChange(opChange)
		assert.Equal(t, int64(500), feeFirst.GetAccountChanges()[accountChangeAddr].Balance)

		opFirst := NewIndexerBuffer()
		opFirst.PushAccountChange(opChange)
		opFirst.PushAccountChange(feeChange)
		assert.Equal(t, int64(500), opFirst.GetAccountChanges()[accountChangeAddr].Balance)
	})

	t.Run("🟢 refund beats operation regardless of push order", func(t *testing.T) {
		// Refund in an early tx (3) still outranks an op in a later tx (8): phase dominates,
		// matching the canonical order where all refunds apply after all operations.
		opChange := accountChange(accountRank(rankOp, 8, 1), 500, types.AccountOpUpdate)
		refundChange := accountChange(accountRank(rankRefund, 3, 0), 550, types.AccountOpUpdate)

		opFirst := NewIndexerBuffer()
		opFirst.PushAccountChange(opChange)
		opFirst.PushAccountChange(refundChange)
		assert.Equal(t, int64(550), opFirst.GetAccountChanges()[accountChangeAddr].Balance)

		refundFirst := NewIndexerBuffer()
		refundFirst.PushAccountChange(refundChange)
		refundFirst.PushAccountChange(opChange)
		assert.Equal(t, int64(550), refundFirst.GetAccountChanges()[accountChangeAddr].Balance)
	})

	t.Run("🟢 later-tx fee wins over earlier-tx fee regardless of push order", func(t *testing.T) {
		// Same account charged fees in tx 3 and tx 7 (e.g. a shared fee-bump source). The
		// later tx's cumulative balance wins by key — no longer reliant on push/merge order.
		early := accountChange(accountRank(rankFee, 3, 0), 100, types.AccountOpUpdate)
		late := accountChange(accountRank(rankFee, 7, 0), 200, types.AccountOpUpdate)

		earlyFirst := NewIndexerBuffer()
		earlyFirst.PushAccountChange(early)
		earlyFirst.PushAccountChange(late)
		assert.Equal(t, int64(200), earlyFirst.GetAccountChanges()[accountChangeAddr].Balance)

		lateFirst := NewIndexerBuffer()
		lateFirst.PushAccountChange(late)
		lateFirst.PushAccountChange(early)
		assert.Equal(t, int64(200), lateFirst.GetAccountChanges()[accountChangeAddr].Balance)
	})

	t.Run("🟢 later-tx refund wins over earlier-tx refund regardless of push order", func(t *testing.T) {
		early := accountChange(accountRank(rankRefund, 3, 0), 100, types.AccountOpUpdate)
		late := accountChange(accountRank(rankRefund, 7, 0), 200, types.AccountOpUpdate)

		earlyFirst := NewIndexerBuffer()
		earlyFirst.PushAccountChange(early)
		earlyFirst.PushAccountChange(late)
		assert.Equal(t, int64(200), earlyFirst.GetAccountChanges()[accountChangeAddr].Balance)

		lateFirst := NewIndexerBuffer()
		lateFirst.PushAccountChange(late)
		lateFirst.PushAccountChange(early)
		assert.Equal(t, int64(200), lateFirst.GetAccountChanges()[accountChangeAddr].Balance)
	})

	t.Run("🟢 refund wins over fee and operation together", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankFee, 2, 0), 100, types.AccountOpUpdate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 5, 1), 200, types.AccountOpUpdate)) // later tx than the refund
		buffer.PushAccountChange(accountChange(accountRank(rankRefund, 2, 0), 300, types.AccountOpUpdate))

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, int64(300), changes[accountChangeAddr].Balance)
	})

	t.Run("🟢 tombstone blocks lower-key resurrection after CREATE→REMOVE", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		// Account created then merged within the ledger's operations → nets to nothing.
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpCreate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 2), 0, types.AccountOpRemove))
		// A lower-key change arriving afterward must NOT re-insert the removed account.
		buffer.PushAccountChange(accountChange(accountRank(rankFee, 1, 0), 999, types.AccountOpUpdate))

		assert.Len(t, buffer.GetAccountChanges(), 0)
	})

	t.Run("🟢 higher-key change re-creates a tombstoned account", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 1), 100, types.AccountOpCreate))
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 1, 2), 0, types.AccountOpRemove))
		// A genuine later re-creation (higher key) lifts the tombstone and wins.
		buffer.PushAccountChange(accountChange(accountRank(rankOp, 5, 1), 700, types.AccountOpCreate))

		changes := buffer.GetAccountChanges()
		require.Len(t, changes, 1)
		assert.Equal(t, int64(700), changes[accountChangeAddr].Balance)
		assert.Equal(t, types.AccountOpCreate, changes[accountChangeAddr].Operation)
	})
}

func TestIndexerBuffer_TrustlineTombstone(t *testing.T) {
	const asset = "USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	trustline := func(opID, balance int64, op types.TrustlineOpType) types.TrustlineChange {
		return types.TrustlineChange{
			AccountID:   accountChangeAddr,
			Asset:       asset,
			OperationID: opID,
			Operation:   op,
			Balance:     balance,
		}
	}

	t.Run("🟢 tombstone blocks lower-key resurrection after ADD→REMOVE", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushTrustlineChange(trustline(100, 1000, types.TrustlineOpAdd))
		buffer.PushTrustlineChange(trustline(200, 0, types.TrustlineOpRemove))
		// A lower-OperationID change afterward must NOT re-insert the removed trustline.
		buffer.PushTrustlineChange(trustline(50, 500, types.TrustlineOpUpdate))

		assert.Len(t, buffer.GetTrustlineChanges(), 0)
	})

	t.Run("🟢 higher-key change re-adds a tombstoned trustline", func(t *testing.T) {
		buffer := NewIndexerBuffer()
		buffer.PushTrustlineChange(trustline(100, 1000, types.TrustlineOpAdd))
		buffer.PushTrustlineChange(trustline(200, 0, types.TrustlineOpRemove))
		// A genuine later re-add (higher OperationID) lifts the tombstone and wins.
		buffer.PushTrustlineChange(trustline(300, 700, types.TrustlineOpAdd))

		changes := buffer.GetTrustlineChanges()
		require.Len(t, changes, 1)
		for _, c := range changes {
			assert.Equal(t, int64(700), c.Balance)
		}
	})
}
