package services

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/entities"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// classicFixture bundles the RPC mock plumbing for one classic simulation case:
// the given ledger entries are returned for any GetLedgerEntries call, and the
// service is constructed with no models (protocol processors are exercised by
// the SEP-41 tests, not here).
func classicFixture(t *testing.T, entries ...entities.LedgerEntryResult) *transactionSimulationService {
	t.Helper()
	rpcMock := &RPCServiceMock{}
	rpcMock.On("GetLedgerEntries", mock.Anything).
		Return(entities.RPCGetLedgerEntriesResult{LatestLedger: 5_000_000, Entries: entries}, nil).Maybe()
	svc, err := NewTransactionSimulationService(rpcMock, nil, network.TestNetworkPassphrase)
	require.NoError(t, err)
	return svc
}

func ledgerEntryResult(t *testing.T, key xdr.LedgerKey, data xdr.LedgerEntryData) entities.LedgerEntryResult {
	t.Helper()
	keyB64, err := xdr.MarshalBase64(key)
	require.NoError(t, err)
	dataB64, err := xdr.MarshalBase64(data)
	require.NoError(t, err)
	return entities.LedgerEntryResult{KeyXDR: keyB64, DataXDR: dataB64, LastModifiedLedger: 4_999_000}
}

func accountEntryResult(t *testing.T, address string, balanceStroops int64) entities.LedgerEntryResult {
	t.Helper()
	id := xdr.MustAddress(address)
	return ledgerEntryResult(t,
		accountLedgerKey(id),
		xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.AccountEntry{
			AccountId:  id,
			Balance:    xdr.Int64(balanceStroops),
			SeqNum:     1,
			Thresholds: xdr.Thresholds{1, 0, 0, 0},
		}},
	)
}

func trustlineEntryResult(t *testing.T, address string, asset xdr.Asset, balance, limit int64) entities.LedgerEntryResult {
	t.Helper()
	id := xdr.MustAddress(address)
	return ledgerEntryResult(t,
		trustlineLedgerKey(id, asset),
		xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeTrustline, TrustLine: &xdr.TrustLineEntry{
			AccountId: id,
			Asset:     asset.ToTrustLineAsset(),
			Balance:   xdr.Int64(balance),
			Limit:     xdr.Int64(limit),
			Flags:     xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag),
		}},
	)
}

// balanceChangesByReasonAndOp separates the transaction-fee debit (OperationID 0)
// from operation-level balance rows, since fee and payment debits share
// (BALANCE, DEBIT).
func balanceChangesByReasonAndOp(stateChanges []types.StateChange) (feeDebit *types.StateChange, opByReason map[types.StateChangeReason]types.StateChange) {
	opByReason = map[types.StateChangeReason]types.StateChange{}
	for i, sc := range stateChanges {
		if sc.StateChangeCategory != types.StateChangeCategoryBalance {
			continue
		}
		if sc.OperationID == 0 && sc.StateChangeReason == types.StateChangeReasonDebit {
			feeDebit = &stateChanges[i]
			continue
		}
		opByReason[sc.StateChangeReason] = sc
	}
	return feeDebit, opByReason
}

func TestTransactionSimulationService_classicPayment(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	dst := keypair.MustRandom().Address()

	t.Run("🟢 native payment produces debit, credit, and fee rows", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000), // 100 XLM
			accountEntryResult(t, dst, 50_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: dst, Amount: "1", Asset: txnbuild.NativeAsset{},
		}))
		require.NoError(t, err)
		assert.Equal(t, uint32(5_000_000), result.LatestLedger)

		feeDebit, opByReason := balanceChangesByReasonAndOp(result.StateChanges)
		require.NotNil(t, feeDebit, "expected a transaction-fee debit row")
		assert.Equal(t, src, string(feeDebit.AccountID))
		assert.Equal(t, "100", feeDebit.Amount.String)

		debit, ok := opByReason[types.StateChangeReasonDebit]
		require.True(t, ok, "expected a payment DEBIT")
		assert.Equal(t, src, string(debit.AccountID))
		assert.Equal(t, "10000000", debit.Amount.String)

		credit, ok := opByReason[types.StateChangeReasonCredit]
		require.True(t, ok, "expected a payment CREDIT")
		assert.Equal(t, dst, string(credit.AccountID))
		assert.Equal(t, "10000000", credit.Amount.String)
	})

	t.Run("🔴 insufficient balance is a would-fail, not a phantom preview", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 1_0000000+2*baseReserveStroops), // 1 XLM above reserve
			accountEntryResult(t, dst, 50_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: dst, Amount: "5", Asset: txnbuild.NativeAsset{},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 missing destination is a would-fail", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: dst, Amount: "1", Asset: txnbuild.NativeAsset{},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🟢 credit-asset payment moves trustline balances", func(t *testing.T) {
		issuer := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("USDC", issuer)
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, dst, 50_0000000),
			trustlineEntryResult(t, src, asset, 20_0000000, 100_0000000),
			trustlineEntryResult(t, dst, asset, 0, 100_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: dst, Amount: "2",
			Asset: txnbuild.CreditAsset{Code: "USDC", Issuer: issuer},
		}))
		require.NoError(t, err)

		_, opByReason := balanceChangesByReasonAndOp(result.StateChanges)
		debit, ok := opByReason[types.StateChangeReasonDebit]
		require.True(t, ok)
		assert.Equal(t, src, string(debit.AccountID))
		assert.Equal(t, "20000000", debit.Amount.String)
		credit, ok := opByReason[types.StateChangeReasonCredit]
		require.True(t, ok)
		assert.Equal(t, dst, string(credit.AccountID))
	})
}

func TestTransactionSimulationService_classicCreateAccount(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	newAccount := keypair.MustRandom().Address()

	t.Run("🟢 createAccount produces ACCOUNT CREATE and funding movement", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.CreateAccount{
			Destination: newAccount, Amount: "10",
		}))
		require.NoError(t, err)

		var created *types.StateChange
		for i, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryAccount && sc.StateChangeReason == types.StateChangeReasonCreate {
				created = &result.StateChanges[i]
			}
		}
		require.NotNil(t, created, "expected an (ACCOUNT, CREATE) state change")
		assert.Equal(t, newAccount, string(created.AccountID))
		assert.Equal(t, src, created.CreatorAccountID.String())
	})

	t.Run("🔴 destination that already exists is a would-fail", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, newAccount, 1_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.CreateAccount{
			Destination: newAccount, Amount: "10",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})
}

func TestTransactionSimulationService_classicChangeTrust(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	issuer := keypair.MustRandom().Address()
	asset := xdr.MustNewCreditAsset("USDC", issuer)
	line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}

	t.Run("🟢 adding a trustline produces (TRUSTLINE, ADD)", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "1000",
		}))
		require.NoError(t, err)

		var added *types.StateChange
		for i, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryTrustline && sc.StateChangeReason == types.StateChangeReasonAdd {
				added = &result.StateChanges[i]
			}
		}
		require.NotNil(t, added, "expected a (TRUSTLINE, ADD) state change")
		assert.Equal(t, src, string(added.AccountID))
		// Effects-derived rows carry decimal amount strings, matching history.
		assert.Equal(t, "1000.0000000", added.TrustlineLimitNew.String)
	})

	t.Run("🟢 raising an existing limit produces (TRUSTLINE, UPDATE)", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
			trustlineEntryResult(t, src, asset, 5_0000000, 10_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "50",
		}))
		require.NoError(t, err)

		var updated *types.StateChange
		for i, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryTrustline && sc.StateChangeReason == types.StateChangeReasonUpdate {
				updated = &result.StateChanges[i]
			}
		}
		require.NotNil(t, updated, "expected a (TRUSTLINE, UPDATE) state change")
		assert.Equal(t, "50.0000000", updated.TrustlineLimitNew.String)
	})

	t.Run("🟢 removing an empty trustline produces (TRUSTLINE, REMOVE)", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
			trustlineEntryResult(t, src, asset, 0, 100_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "0",
		}))
		require.NoError(t, err)

		found := false
		for _, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryTrustline && sc.StateChangeReason == types.StateChangeReasonRemove {
				found = true
			}
		}
		assert.True(t, found, "expected a (TRUSTLINE, REMOVE) state change")
	})

	t.Run("🔴 removing a trustline with balance is a would-fail", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
			trustlineEntryResult(t, src, asset, 5_0000000, 100_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "0",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})
}

func TestTransactionSimulationService_classicSetOptions(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	signer := keypair.MustRandom().Address()
	homeDomain := "example.com"

	svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
	result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.SetOptions{
		HomeDomain: &homeDomain,
		Signer:     &txnbuild.Signer{Address: signer, Weight: 5},
	}))
	require.NoError(t, err)

	byCategory := map[types.StateChangeCategory]types.StateChange{}
	for _, sc := range result.StateChanges {
		byCategory[sc.StateChangeCategory] = sc
	}

	domain, ok := byCategory[types.StateChangeCategoryHomeDomain]
	require.True(t, ok, "expected a HOME_DOMAIN state change")
	assert.Equal(t, types.StateChangeReasonSet, domain.StateChangeReason)
	assert.Equal(t, src, string(domain.AccountID))
	assert.Equal(t, homeDomain, domain.KeyValue["new"])

	signerChange, ok := byCategory[types.StateChangeCategorySigner]
	require.True(t, ok, "expected a SIGNER state change")
	assert.Equal(t, types.StateChangeReasonAdd, signerChange.StateChangeReason)
	assert.Equal(t, signer, signerChange.SignerAccountID.String())
}

func TestTransactionSimulationService_classicManageData(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()

	svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
	result, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ManageData{
		Name: "config", Value: []byte("v1"),
	}))
	require.NoError(t, err)

	var added *types.StateChange
	for i, sc := range result.StateChanges {
		if sc.StateChangeCategory == types.StateChangeCategoryDataEntry && sc.StateChangeReason == types.StateChangeReasonAdd {
			added = &result.StateChanges[i]
		}
	}
	require.NotNil(t, added, "expected a (DATA_ENTRY, ADD) state change")
	assert.Equal(t, src, string(added.AccountID))
	assert.Equal(t, "config", added.DataEntryName.String)
}

func TestTransactionSimulationService_classicUnsupported(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	svc := classicFixture(t)

	t.Run("🔴 order-book operations stay unsupported", func(t *testing.T) {
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ManageSellOffer{
			Selling: txnbuild.NativeAsset{},
			Buying:  txnbuild.CreditAsset{Code: "USDC", Issuer: keypair.MustRandom().Address()},
			Amount:  "1", Price: xdr.Price{N: 1, D: 1},
		}))
		assert.ErrorIs(t, err, ErrUnsupportedTransaction)
	})

	t.Run("🔴 multi-operation transactions stay unsupported until phase 3", func(t *testing.T) {
		dst := keypair.MustRandom().Address()
		payment := &txnbuild.Payment{Destination: dst, Amount: "1", Asset: txnbuild.NativeAsset{}}
		second := &txnbuild.Payment{Destination: dst, Amount: "2", Asset: txnbuild.NativeAsset{}}
		srcAccount := txnbuild.SimpleAccount{AccountID: src, Sequence: 1}
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &srcAccount,
			Operations:           []txnbuild.Operation{payment, second},
			BaseFee:              txnbuild.MinBaseFee,
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
			IncrementSequenceNum: true,
		})
		require.NoError(t, err)
		txXDR, err := tx.Base64()
		require.NoError(t, err)

		_, err = svc.SimulateStateChanges(ctx, txXDR)
		assert.ErrorIs(t, err, ErrUnsupportedTransaction)
	})
}
