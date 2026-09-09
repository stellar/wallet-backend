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

	t.Run("🔴 balance covering amount and reserve but not the fee is a would-fail", func(t *testing.T) {
		// Exactly the reserve floor plus the 1 XLM being sent: the network
		// charges the fee first, so this transaction would fail on-chain.
		svc := classicFixture(t,
			accountEntryResult(t, src, 2*baseReserveStroops+1_0000000),
			accountEntryResult(t, dst, 50_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: dst, Amount: "1", Asset: txnbuild.NativeAsset{},
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
		// Limits are stroops strings, the same unit as every other amount column.
		assert.Equal(t, "10000000000", added.TrustlineLimitNew.String)
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
		assert.Equal(t, "500000000", updated.TrustlineLimitNew.String)
		assert.Equal(t, "100000000", updated.TrustlineLimitOld.String)
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

	t.Run("🔴 issuer trusting its own asset is a would-fail", func(t *testing.T) {
		// The current protocol rejects self-trust as CHANGE_TRUST_MALFORMED.
		svc := classicFixture(t, accountEntryResult(t, issuer, 100_0000000))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, issuer, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "1000",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 absent issuer is a would-fail for changeTrust", func(t *testing.T) {
		// Unlike payments, changeTrust still returns CHANGE_TRUST_NO_ISSUER.
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "1000",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
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

func TestTransactionSimulationService_classicReviewFixes(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	issuer := keypair.MustRandom().Address()
	asset := xdr.MustNewCreditAsset("USDC", issuer)
	line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}

	t.Run("🔴 fee payer that cannot cover the fee is a would-fail", func(t *testing.T) {
		// Exactly the reserve floor: zero spendable balance, so even the
		// 100-stroop fee is unaffordable, and the operation itself (setOptions)
		// spends nothing.
		homeDomain := "example.com"
		svc := classicFixture(t, accountEntryResult(t, src, 2*baseReserveStroops))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.SetOptions{
			HomeDomain: &homeDomain,
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 adding a trustline without reserve headroom is a would-fail", func(t *testing.T) {
		// Spendable after fee is one stroop short of the base reserve the new
		// subentry locks.
		svc := classicFixture(t,
			accountEntryResult(t, src, 3*baseReserveStroops+99),
			accountEntryResult(t, issuer, 100_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "1000",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 adding a signer without reserve headroom is a would-fail", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 3*baseReserveStroops+99))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.SetOptions{
			Signer: &txnbuild.Signer{Address: keypair.MustRandom().Address(), Weight: 5},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 removing a trustline with open offer liabilities is a would-fail", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
			trustlineEntryResultWithLiabilities(t, src, asset, 0, 100_0000000, 5_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.ChangeTrust{
			Line: line.MustToChangeTrustAsset(), Limit: "0",
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 inflation destination that does not exist is a would-fail", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		missing := keypair.MustRandom().Address()
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.SetOptions{
			InflationDestination: &missing,
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 negative payment amount is a would-fail", func(t *testing.T) {
		// txnbuild refuses to build this, so the malformed operation is
		// assembled as raw XDR, the way a hostile client would submit it.
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		dst := xdr.MustAddress(keypair.MustRandom().Address())
		_, err := svc.SimulateStateChanges(ctx, buildRawTxXDRFrom(t, src, xdr.OperationBody{
			Type: xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{
				Destination: dst.ToMuxedAccount(),
				Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
				Amount:      -5_0000000,
			},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 threshold above 255 is a would-fail", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		high := xdr.Uint32(300)
		_, err := svc.SimulateStateChanges(ctx, buildRawTxXDRFrom(t, src, xdr.OperationBody{
			Type:         xdr.OperationTypeSetOptions,
			SetOptionsOp: &xdr.SetOptionsOp{HighThreshold: &high},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})
}

// TestComputeChangeTrustChanges_entryFidelity checks details of the synthesized
// entries that state-change assertions cannot see: the clawback flag inherited
// from the issuer and the source account's subentry counter.
func TestComputeChangeTrustChanges_entryFidelity(t *testing.T) {
	src := xdr.MustAddress(keypair.MustRandom().Address())
	issuer := keypair.MustRandom().Address()
	asset := xdr.MustNewCreditAsset("USDC", issuer)

	before := map[string]xdr.LedgerEntry{}
	put := func(key xdr.LedgerKey, data xdr.LedgerEntryData) {
		b64, err := xdr.MarshalBase64(key)
		require.NoError(t, err)
		before[b64] = xdr.LedgerEntry{Data: data}
	}
	put(accountLedgerKey(src), xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.AccountEntry{
		AccountId: src, Balance: 100_0000000, NumSubEntries: 3,
	}})
	issuerID := xdr.MustAddress(issuer)
	put(accountLedgerKey(issuerID), xdr.LedgerEntryData{Type: xdr.LedgerEntryTypeAccount, Account: &xdr.AccountEntry{
		AccountId: issuerID,
		Flags:     xdr.Uint32(xdr.AccountFlagsAuthClawbackEnabledFlag),
	}})

	changes, err := computeChangeTrustChanges(xdr.ChangeTrustOp{
		Line:  asset.ToChangeTrustAsset(),
		Limit: 10_0000000,
	}, src, before)
	require.NoError(t, err)
	require.Len(t, changes, 3, "expected trustline creation plus the source account pair")

	created := changes[0].Created.Data.MustTrustLine()
	assert.NotZero(t, created.Flags&xdr.Uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag),
		"a clawback-enabled issuer must stamp new trustlines with the clawback flag")
	assert.NotZero(t, created.Flags&xdr.Uint32(xdr.TrustLineFlagsAuthorizedFlag),
		"issuer without AUTH_REQUIRED means the trustline starts authorized")

	srcAfter := changes[2].Updated.Data.MustAccount()
	assert.Equal(t, xdr.Uint32(4), srcAfter.NumSubEntries, "new trustline must bump the subentry counter")
}

// buildRawTxXDRFrom assembles a single-operation envelope directly as XDR,
// bypassing txnbuild's client-side validation, so tests can feed the service
// operations a hostile client could submit.
func buildRawTxXDRFrom(t *testing.T, source string, body xdr.OperationBody) string {
	t.Helper()
	aid := xdr.MustAddress(source)
	env := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: aid.ToMuxedAccount(),
				Fee:           100,
				SeqNum:        1,
				Cond:          xdr.Preconditions{Type: xdr.PreconditionTypePrecondNone},
				Operations:    []xdr.Operation{{Body: body}},
			},
		},
	}
	b64, err := xdr.MarshalBase64(env)
	require.NoError(t, err)
	return b64
}

func trustlineEntryResultWithLiabilities(t *testing.T, address string, asset xdr.Asset, balance, limit, buyingLiabilities int64) entities.LedgerEntryResult {
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
			Ext: xdr.TrustLineEntryExt{V: 1, V1: &xdr.TrustLineEntryV1{
				Liabilities: xdr.Liabilities{Buying: xdr.Int64(buyingLiabilities)},
			}},
		}},
	)
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

	t.Run("🔴 one unsupported operation rejects the whole multi-op transaction", func(t *testing.T) {
		dst := keypair.MustRandom().Address()
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.Payment{Destination: dst, Amount: "1", Asset: txnbuild.NativeAsset{}},
			&txnbuild.ManageSellOffer{
				Selling: txnbuild.NativeAsset{},
				Buying:  txnbuild.CreditAsset{Code: "USDC", Issuer: keypair.MustRandom().Address()},
				Amount:  "1", Price: xdr.Price{N: 1, D: 1},
			},
		))
		assert.ErrorIs(t, err, ErrUnsupportedTransaction)
	})
}

// TestFetchLedgerEntries_batches verifies footprints larger than the RPC
// getLedgerEntries key limit are fetched in multiple calls.
func TestFetchLedgerEntries_batches(t *testing.T) {
	rpcMock := &RPCServiceMock{}
	rpcMock.On("GetLedgerEntries", mock.MatchedBy(func(keys []string) bool {
		return len(keys) > 0 && len(keys) <= rpcLedgerEntryBatchSize
	})).Return(entities.RPCGetLedgerEntriesResult{LatestLedger: 5_000_000}, nil)
	svc, err := NewTransactionSimulationService(rpcMock, nil, network.TestNetworkPassphrase)
	require.NoError(t, err)

	keys := make([]xdr.LedgerKey, 0, rpcLedgerEntryBatchSize+50)
	for range rpcLedgerEntryBatchSize + 50 {
		keys = append(keys, accountLedgerKey(xdr.MustAddress(keypair.MustRandom().Address())))
	}
	_, latestLedger, err := svc.fetchLedgerEntries(keys)
	require.NoError(t, err)
	assert.Equal(t, uint32(5_000_000), latestLedger)
	rpcMock.AssertNumberOfCalls(t, "GetLedgerEntries", 2)
}

// buildMultiOpTxXDRFrom builds an unsigned transaction envelope carrying the
// given operations in order.
func buildMultiOpTxXDRFrom(t *testing.T, sourceAccount string, ops ...txnbuild.Operation) string {
	t.Helper()
	src := txnbuild.SimpleAccount{AccountID: sourceAccount, Sequence: 1}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &src,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	})
	require.NoError(t, err)
	txXDR, err := tx.Base64()
	require.NoError(t, err)
	return txXDR
}

func TestTransactionSimulationService_classicMultiOp(t *testing.T) {
	ctx := context.Background()
	src := keypair.MustRandom().Address()
	dst := keypair.MustRandom().Address()

	t.Run("🟢 two payments apply sequentially with per-operation attribution", func(t *testing.T) {
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, dst, 50_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
		))
		require.NoError(t, err)

		var debits, credits []types.StateChange
		operationIDs := map[int64]struct{}{}
		for _, sc := range result.StateChanges {
			if sc.StateChangeCategory != types.StateChangeCategoryBalance || sc.OperationID == 0 {
				continue
			}
			operationIDs[sc.OperationID] = struct{}{}
			switch sc.StateChangeReason {
			case types.StateChangeReasonDebit:
				debits = append(debits, sc)
			case types.StateChangeReasonCredit:
				credits = append(credits, sc)
			default:
			}
		}
		assert.Len(t, debits, 2, "one DEBIT per payment operation")
		assert.Len(t, credits, 2, "one CREDIT per payment operation")
		assert.Len(t, operationIDs, 2, "the two operations' rows must carry distinct operation IDs")
	})

	t.Run("🔴 all-or-nothing: a later failing operation aborts the whole preview", func(t *testing.T) {
		// Spendable after fee is 40 XLM: the first 30 XLM payment succeeds, the
		// second must see the reduced balance and fail, taking the whole
		// transaction with it.
		svc := classicFixture(t,
			accountEntryResult(t, src, 2*baseReserveStroops+200+40_0000000),
			accountEntryResult(t, dst, 50_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
		))
		require.ErrorIs(t, err, ErrSimulationFailed)
		assert.ErrorContains(t, err, "operation 2", "the error should attribute the failure to the second operation")
	})

	t.Run("🟢 an account created by operation 1 can act in operation 2", func(t *testing.T) {
		newAccount := keypair.MustRandom().Address()
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		result, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.CreateAccount{Destination: newAccount, Amount: "20"},
			&txnbuild.Payment{
				SourceAccount: newAccount,
				Destination:   src, Amount: "5", Asset: txnbuild.NativeAsset{},
			},
		))
		require.NoError(t, err)

		var newAccountDebited bool
		for _, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryBalance &&
				sc.StateChangeReason == types.StateChangeReasonDebit &&
				string(sc.AccountID) == newAccount {
				newAccountDebited = true
			}
		}
		assert.True(t, newAccountDebited, "the account created by operation 1 must be able to pay in operation 2")
	})

	t.Run("🟢 the fee is charged once, not per operation", func(t *testing.T) {
		// Exactly reserve + one fee (200 for two operations) + the two amounts:
		// double-charging the fee would make this fail.
		svc := classicFixture(t,
			accountEntryResult(t, src, 2*baseReserveStroops+200+60_0000000),
			accountEntryResult(t, dst, 50_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
			&txnbuild.Payment{Destination: dst, Amount: "30", Asset: txnbuild.NativeAsset{}},
		))
		require.NoError(t, err)

		var feeRows []types.StateChange
		for _, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryBalance &&
				sc.StateChangeReason == types.StateChangeReasonDebit && sc.OperationID == 0 {
				feeRows = append(feeRows, sc)
			}
		}
		require.Len(t, feeRows, 1, "expected exactly one fee row for the whole transaction")
		assert.Equal(t, "200", feeRows[0].Amount.String)
	})

	t.Run("🔴 an entry removed by operation 1 is gone for operation 2", func(t *testing.T) {
		issuer := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("USDC", issuer)
		line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
			trustlineEntryResult(t, src, asset, 0, 100_0000000),
		)
		// Operation 1 deletes src's empty trustline; operation 2 tries to pay
		// the asset into it and must fail on the missing trustline.
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.ChangeTrust{Line: line.MustToChangeTrustAsset(), Limit: "0"},
			&txnbuild.Payment{
				SourceAccount: issuer,
				Destination:   src, Amount: "5", Asset: line,
			},
		))
		require.ErrorIs(t, err, ErrSimulationFailed)
		assert.ErrorContains(t, err, "operation 2")
	})

	t.Run("🔴 a reserve locked by operation 1 constrains operation 2", func(t *testing.T) {
		issuer := keypair.MustRandom().Address()
		line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}
		// One stroop short: after the fee and the base reserve the new
		// trustline locks, the 10 XLM payment no longer fits.
		svc := classicFixture(t,
			accountEntryResult(t, src, 3*baseReserveStroops+200+10_0000000-1),
			accountEntryResult(t, dst, 50_0000000),
			accountEntryResult(t, issuer, 100_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.ChangeTrust{Line: line.MustToChangeTrustAsset(), Limit: "1000"},
			&txnbuild.Payment{Destination: dst, Amount: "10", Asset: txnbuild.NativeAsset{}},
		))
		require.ErrorIs(t, err, ErrSimulationFailed)
		assert.ErrorContains(t, err, "operation 2")
	})

	t.Run("🔴 a credit self-payment must not inflate the working balance", func(t *testing.T) {
		// The self-payment emits no entry changes; if it wrongly applied a
		// credit built from the same snapshot as the debit, the working balance
		// would grow to 25 and the second payment of 22 would pass.
		issuer := keypair.MustRandom().Address()
		asset := xdr.MustNewCreditAsset("USDC", issuer)
		line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, dst, 50_0000000),
			trustlineEntryResult(t, src, asset, 20_0000000, 100_0000000),
			trustlineEntryResult(t, dst, asset, 0, 100_0000000),
		)
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.Payment{Destination: src, Amount: "5", Asset: line},
			&txnbuild.Payment{Destination: dst, Amount: "22", Asset: line},
		))
		require.ErrorIs(t, err, ErrSimulationFailed)
		assert.ErrorContains(t, err, "operation 2")
	})

	t.Run("🔴 a self-payment above the available balance is a would-fail", func(t *testing.T) {
		svc := classicFixture(t, accountEntryResult(t, src, 2*baseReserveStroops+1_0000000))
		_, err := svc.SimulateStateChanges(ctx, buildTxXDRFrom(t, src, &txnbuild.Payment{
			Destination: src, Amount: "5", Asset: txnbuild.NativeAsset{},
		}))
		assert.ErrorIs(t, err, ErrSimulationFailed)
	})

	t.Run("🔴 an account created by operation 1 cannot be created again", func(t *testing.T) {
		newAccount := keypair.MustRandom().Address()
		svc := classicFixture(t, accountEntryResult(t, src, 100_0000000))
		_, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.CreateAccount{Destination: newAccount, Amount: "10"},
			&txnbuild.CreateAccount{Destination: newAccount, Amount: "10"},
		))
		require.ErrorIs(t, err, ErrSimulationFailed)
		assert.ErrorContains(t, err, "operation 2")
	})

	t.Run("🟢 a trustline created by operation 1 can receive in operation 2", func(t *testing.T) {
		issuer := keypair.MustRandom().Address()
		line := txnbuild.CreditAsset{Code: "USDC", Issuer: issuer}
		svc := classicFixture(t,
			accountEntryResult(t, src, 100_0000000),
			accountEntryResult(t, issuer, 100_0000000),
		)
		result, err := svc.SimulateStateChanges(ctx, buildMultiOpTxXDRFrom(t, src,
			&txnbuild.ChangeTrust{Line: line.MustToChangeTrustAsset(), Limit: "1000"},
			&txnbuild.Payment{
				SourceAccount: issuer,
				Destination:   src, Amount: "5", Asset: line,
			},
		))
		require.NoError(t, err)

		var credited bool
		for _, sc := range result.StateChanges {
			if sc.StateChangeCategory == types.StateChangeCategoryBalance &&
				sc.StateChangeReason == types.StateChangeReasonCredit &&
				string(sc.AccountID) == src {
				credited = true
			}
		}
		assert.True(t, credited, "the trustline created by operation 1 must be able to receive in operation 2")
	})
}
