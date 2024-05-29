package services

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/db"
	"github.com/stellar/wallet-backend/internal/db/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessLedger(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	dbConnectionPool, err := db.OpenDBConnectionPool(dbt.DSN)
	require.NoError(t, err)
	defer dbConnectionPool.Close()

	m := &IngestManager{
		PaymentModel: &data.PaymentModel{
			DB: dbConnectionPool,
		},
		NetworkPassphrase: network.TestNetworkPassphrase,
		LedgerCursorName:  "last_synced_ledger",
		LedgerBackend:     nil,
	}

	ctx := context.Background()

	// Insert destination account into subscribed addresses
	destinationAccount := "GBLI2OE4H3HAW7Z2GXLYZQNQ57XLHJ5OILFPVL33EPA4GDAIQ5F33JGA"
	_, err = dbConnectionPool.ExecContext(ctx, "INSERT INTO accounts (stellar_address) VALUES ($1)", destinationAccount)
	require.NoError(t, err)

	ledgerMeta := xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: 123,
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(time.Date(2024, 5, 28, 11, 0, 0, 0, time.UTC).Unix()),
					},
					LedgerVersion: 10,
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{
						{
							V0Components: &[]xdr.TxSetComponent{
								{
									TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
										Txs: []xdr.TransactionEnvelope{
											{
												Type: xdr.EnvelopeTypeEnvelopeTypeTx,
												V1: &xdr.TransactionV1Envelope{
													Tx: xdr.Transaction{
														SourceAccount: xdr.MustMuxedAddress("GB3H2CRRTO7W5WF54K53A3MRAFEUISHZ7Y5YGRVGRGHUZESLV5VYYWXI"),
														SeqNum:        321,
														Memo:          xdr.MemoText("memo_test"),
														Operations: []xdr.Operation{
															{
																SourceAccount: nil,
																Body: xdr.OperationBody{
																	Type: xdr.OperationTypePayment,
																	PaymentOp: &xdr.PaymentOp{
																		Destination: xdr.MustMuxedAddress(destinationAccount),
																		Asset: xdr.Asset{
																			Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
																			AlphaNum4: &xdr.AlphaNum4{
																				AssetCode: xdr.AssetCode4([]byte("USDC")),
																				Issuer:    xdr.MustMuxedAddress("GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5").ToAccountId(),
																			},
																		},
																		Amount: xdr.Int64(50),
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			TxProcessing: []xdr.TransactionResultMeta{
				{
					Result: xdr.TransactionResultPair{
						TransactionHash: xdr.Hash{},
					},
					TxApplyProcessing: xdr.TransactionMeta{
						V: 3,
					},
				},
			},
		},
	}

	// Compute transaction hash and inject into ledger meta
	components := ledgerMeta.V1.TxSet.V1TxSet.Phases[0].V0Components
	xdrHash, err := network.HashTransactionInEnvelope((*components)[0].TxsMaybeDiscountedFee.Txs[0], m.NetworkPassphrase)
	require.NoError(t, err)
	ledgerMeta.V1.TxProcessing[0].Result.TransactionHash = xdrHash

	// Run ledger ingestion
	err = m.processLedger(ctx, 1, ledgerMeta)
	require.NoError(t, err)

	// Assert payment properly persisted to database
	var payment data.Payment
	query := `SELECT operation_id, operation_type, transaction_id, transaction_hash, from_address, to_address, src_asset_code, src_asset_issuer, src_amount, dest_asset_code, dest_asset_issuer, dest_amount, created_at, memo FROM ingest_payments`
	err = dbConnectionPool.GetContext(ctx, &payment, query)
	require.NoError(t, err)

	expectedMemo := "memo_test"
	assert.Equal(t, data.Payment{
		OperationID:     528280981505,
		OperationType:   "OperationTypePayment",
		TransactionID:   528280981504,
		TransactionHash: "c20936e363c85799b31fd321b67aa49ecd88f04fc41297959387e445245080db",
		From:            "GB3H2CRRTO7W5WF54K53A3MRAFEUISHZ7Y5YGRVGRGHUZESLV5VYYWXI",
		To:              "GBLI2OE4H3HAW7Z2GXLYZQNQ57XLHJ5OILFPVL33EPA4GDAIQ5F33JGA",
		SrcAssetCode:    "USDC",
		SrcAssetIssuer:  "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		SrcAmount:       50,
		DestAssetCode:   "USDC",
		DestAssetIssuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		DestAmount:      50,
		CreatedAt:       time.Date(2024, 5, 28, 11, 0, 0, 0, time.UTC),
		Memo:            &expectedMemo,
	}, payment)
}
