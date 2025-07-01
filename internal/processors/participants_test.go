package processors

import (
	"fmt"
	"testing"

	set "github.com/deckarep/golang-set/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	stellarAddress1 = "GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"
	stellarAddress2 = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	stellarAddress3 = "GAXI33UCLQTCKM2NMRBS7XYBR535LLEVAHL5YBN4FTCB4HZHT7ZA5CVK"
	stellarAddress4 = "GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2"
)

// createTestAccount creates a test account with the given address
func createTestAccount(address string) xdr.AccountEntry {
	aid := xdr.MustAddress(address)
	return xdr.AccountEntry{
		AccountId:  aid,
		Balance:    1000000000,
		SeqNum:     xdr.SequenceNumber(1),
		Thresholds: xdr.NewThreshold(1, 1, 1, 1),
		Flags:      0,
		Ext:        xdr.AccountEntryExt{V: 0},
	}
}

// createTestLedgerEntry creates a test ledger entry for an account
func createTestLedgerEntry(account xdr.AccountEntry) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &account,
		},
		Ext: xdr.LedgerEntryExt{V: 0},
	}
}

// createTestLedgerKey creates a test ledger key for an account
func createTestLedgerKey(address string) xdr.LedgerKey {
	aid := xdr.MustAddress(address)
	return xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: aid,
		},
	}
}

// createTestLedgerEntryChange creates a test ledger entry change for an account
func createTestLedgerEntryChange(t *testing.T, letType xdr.LedgerEntryChangeType, accountAddress string) xdr.LedgerEntryChange {
	t.Helper()

	var entryValue any
	switch letType {
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		entryValue = createTestLedgerKey(accountAddress)
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, xdr.LedgerEntryChangeTypeLedgerEntryState:
		entryValue = createTestLedgerEntry(createTestAccount(accountAddress))
	default:
		require.FailNow(t, "Unknown change type %s", letType)
	}

	ledgerEntryType, err := xdr.NewLedgerEntryChange(letType, entryValue)
	require.NoError(t, err)

	return ledgerEntryType
}

func Test_participantsForChanges(t *testing.T) {
	testCases := []struct {
		name                 string
		getChanges           func(t *testing.T) xdr.LedgerEntryChanges
		expectedParticipants []string
		wantErrContains      string
	}{
		{
			name:                 "🟢empty_changes",
			getChanges:           func(t *testing.T) xdr.LedgerEntryChanges { return xdr.LedgerEntryChanges{} },
			expectedParticipants: []string{},
		},
		{
			name: "🟢single_account_created",
			getChanges: func(t *testing.T) xdr.LedgerEntryChanges {
				return xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stellarAddress1),
				}
			},
			expectedParticipants: []string{stellarAddress1},
		},
		{
			name: "🟢multiple_changes_with_accounts",
			getChanges: func(t *testing.T) xdr.LedgerEntryChanges {
				return xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stellarAddress1),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, stellarAddress2),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stellarAddress3),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stellarAddress4),
				}
			},
			expectedParticipants: []string{stellarAddress1, stellarAddress2, stellarAddress3, stellarAddress4},
		},
		{
			name: "🟢non_account_ledger_entry_should_be_ignored",
			getChanges: func(t *testing.T) xdr.LedgerEntryChanges {
				return xdr.LedgerEntryChanges{
					{
						Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
						Created: &xdr.LedgerEntry{
							Data: xdr.LedgerEntryData{
								Type: xdr.LedgerEntryTypeTrustline,
								TrustLine: &xdr.TrustLineEntry{
									AccountId: xdr.MustAddress(stellarAddress1),
									Asset: xdr.TrustLineAsset{
										Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
										AlphaNum4: &xdr.AlphaNum4{
											AssetCode: [4]byte{'T', 'E', 'S', 'T'},
											Issuer:    xdr.MustAddress(stellarAddress1),
										},
									},
								},
							},
						},
					},
				}
			},
			expectedParticipants: []string{},
		},
		{
			name: "🔴unknown_change_type",
			getChanges: func(t *testing.T) xdr.LedgerEntryChanges {
				return xdr.LedgerEntryChanges{{Type: xdr.LedgerEntryChangeType(999)}}
			},
			expectedParticipants: nil,
			wantErrContains:      "unknown ledger entrychange type 999",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForChanges(tc.getChanges(t))

			if tc.wantErrContains != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Empty(t, participants)
			} else {
				assert.NoError(t, err)
				assert.Len(t, participants, len(tc.expectedParticipants))

				addresses := make([]string, len(participants))
				for i, participant := range participants {
					addresses[i] = participant.Address()
				}
				assert.ElementsMatch(t, tc.expectedParticipants, addresses)
			}
		})
	}
}

func Test_participantsForLedgerEntry(t *testing.T) {
	testCases := []struct {
		name                string
		entry               xdr.LedgerEntry
		expectedParticipant *xdr.AccountId
	}{
		{
			name:                "🟢account_ledger_entry",
			entry:               createTestLedgerEntry(createTestAccount("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY")),
			expectedParticipant: utils.PointOf(xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY")),
		},
		{
			name: "🟡offer_ledger_entry_should_return_nil",
			entry: xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
					Offer: &xdr.OfferEntry{
						SellerId: xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
						OfferId:  1,
						Selling:  xdr.MustNewNativeAsset(),
						Buying:   xdr.MustNewCreditAsset("TEST", "GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
						Amount:   1000,
						Price:    xdr.Price{N: 1, D: 1},
						Flags:    0,
						Ext:      xdr.OfferEntryExt{V: 0},
					},
				},
			},
			expectedParticipant: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participant := participantsForLedgerEntry(tc.entry)

			if tc.expectedParticipant == nil {
				require.Nil(t, participant)
			} else {
				require.NotNil(t, participant)
				assert.Equal(t, tc.expectedParticipant, participant)
				assert.Equal(t, tc.expectedParticipant.Address(), participant.Address())
			}
		})
	}
}

func Test_participantsForLedgerKey(t *testing.T) {
	testCases := []struct {
		name                string
		key                 xdr.LedgerKey
		expectedParticipant *xdr.AccountId
	}{
		{
			name:                "🟢account_ledger_key",
			key:                 createTestLedgerKey("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
			expectedParticipant: utils.PointOf(xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY")),
		},
		{
			name: "🟡offer_ledger_key_should_return_nil",
			key: xdr.LedgerKey{
				Type: xdr.LedgerEntryTypeOffer,
				Offer: &xdr.LedgerKeyOffer{
					SellerId: xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
					OfferId:  1,
				},
			},
			expectedParticipant: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participant := participantsForLedgerKey(tc.key)

			if tc.expectedParticipant == nil {
				assert.Nil(t, participant)
			} else {
				require.NotNil(t, participant)
				assert.Equal(t, tc.expectedParticipant, participant)
				assert.Equal(t, tc.expectedParticipant.Address(), participant.Address())
			}
		})
	}
}

func Test_participantsForMeta(t *testing.T) {
	testCases := []struct {
		name            string
		getMeta         func(t *testing.T) xdr.TransactionMeta
		expected        []string
		wantErrContains string
	}{
		{
			name:     "🟢nil_operations",
			getMeta:  func(t *testing.T) xdr.TransactionMeta { return xdr.TransactionMeta{} },
			expected: []string{},
		},
		{
			name:     "🟢empty_operations",
			getMeta:  func(t *testing.T) xdr.TransactionMeta { return xdr.TransactionMeta{Operations: &[]xdr.OperationMeta{}} },
			expected: []string{},
		},
		{
			name: "🟢single_operation_with_single_account_change",
			getMeta: func(t *testing.T) xdr.TransactionMeta {
				return xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stellarAddress1),
							},
						},
					},
				}
			},
			expected: []string{stellarAddress1},
		},
		{
			name: "🟢multiple_operations_with_multiple_account_changes",
			getMeta: func(t *testing.T) xdr.TransactionMeta {
				return xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stellarAddress1),
							},
						},
						{
							Changes: xdr.LedgerEntryChanges{
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, stellarAddress2),
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stellarAddress3),
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stellarAddress4),
							},
						},
					},
				}
			},
			expected: []string{stellarAddress1, stellarAddress2, stellarAddress3, stellarAddress4},
		},
		{
			name: "🟢operations_with_non_account_changes_should_be_ignored",
			getMeta: func(t *testing.T) xdr.TransactionMeta {
				return xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								{
									Type: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
									Created: &xdr.LedgerEntry{
										Data: xdr.LedgerEntryData{
											Type: xdr.LedgerEntryTypeTrustline,
											TrustLine: &xdr.TrustLineEntry{
												AccountId: xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
												Asset: xdr.TrustLineAsset{
													Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
													AlphaNum4: &xdr.AlphaNum4{
														AssetCode: [4]byte{'T', 'E', 'S', 'T'},
														Issuer:    xdr.MustAddress("GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"),
													},
												},
												Balance: 1000,
												Limit:   10000,
												Flags:   1,
												Ext:     xdr.TrustLineEntryExt{V: 0},
											},
										},
									},
								},
							},
						},
					},
				}
			},
			expected: []string{},
		},
		{
			name: "🔴operation_with_invalid_change_type",
			getMeta: func(t *testing.T) xdr.TransactionMeta {
				return xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								{Type: xdr.LedgerEntryChangeType(999)}, // Invalid type
							},
						},
					},
				}
			},
			expected:        []string{},
			wantErrContains: "unknown ledger entrychange type 999",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			participants, err := participantsForMeta(tc.getMeta(t))

			if tc.wantErrContains != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Empty(t, participants)
			} else {
				assert.NoError(t, err)
				assert.Len(t, participants, len(tc.expected))

				addresses := make([]string, len(participants))
				for i, participant := range participants {
					addresses[i] = participant.Address()
				}
				assert.ElementsMatch(t, tc.expected, addresses)
			}
		})
	}
}

func TestParticipantsProcessor_GetTransactionParticipants(t *testing.T) {
	const innerTxSourceAccount = "GAE5OQ6ICDENTKOAGTMAC6MMCQYJ22TCZ7OZXGQ34QUATIAESDOXRTIA"
	const feeBumpSourceAccount = "GCR4GZM7T6EKITZ4GQ2A5UZMMQYZYR6DZRAVUIPN2HJXJ5MXM3DJDXV3"
	const destinationAccount = "GDYHYQXJU47ULPGEYFILGJD53ZPNOBS5CSD435TFBBQPCL7SORZPETVM"

	// Helper function to create a comprehensive test transaction
	createTestTransaction := func(t *testing.T, isSuccessful, isFeeBump bool) ingest.LedgerTransaction {
		t.Helper()

		destinationAccountID := xdr.MustAddress(destinationAccount)
		ops := []xdr.Operation{{
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: destinationAccountID.ToMuxedAccount(),
					Asset:       xdr.Asset{Type: xdr.AssetTypeAssetTypeNative},
					Amount:      1000000000,
				},
			},
		}}

		innerTx := xdr.Transaction{
			SourceAccount: utils.PointOf(xdr.MustAddress(innerTxSourceAccount)).ToMuxedAccount(),
			Fee:           100,
			SeqNum:        1,
			Operations:    ops,
		}
		envelope, err := xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTx, xdr.TransactionV1Envelope{Tx: innerTx})
		require.NoError(t, err)
		feeAddress := innerTxSourceAccount

		if isFeeBump {
			feeAddress = feeBumpSourceAccount
			envelope, err = xdr.NewTransactionEnvelope(xdr.EnvelopeTypeEnvelopeTypeTxFeeBump, xdr.FeeBumpTransactionEnvelope{
				Tx: xdr.FeeBumpTransaction{
					FeeSource: utils.PointOf(xdr.MustAddress(feeBumpSourceAccount)).ToMuxedAccount(),
					Fee:       200,
					InnerTx: xdr.FeeBumpTransactionInnerTx{
						Type: xdr.EnvelopeTypeEnvelopeTypeTx,
						V1:   &xdr.TransactionV1Envelope{Tx: innerTx},
					},
				},
			})
			require.NoError(t, err)
		}

		paymentResultCode := xdr.PaymentResultCodePaymentSuccess
		txResultCode := xdr.TransactionResultCodeTxSuccess
		if !isSuccessful {
			paymentResultCode = xdr.PaymentResultCodePaymentUnderfunded
			txResultCode = xdr.TransactionResultCodeTxFailed
		}

		result := xdr.TransactionResult{
			FeeCharged: 100,
			Result: xdr.TransactionResultResult{
				Code: txResultCode,
				Results: &[]xdr.OperationResult{{
					Code: xdr.OperationResultCodeOpInner,
					Tr: &xdr.OperationResultTr{
						Type:          xdr.OperationTypePayment,
						PaymentResult: &xdr.PaymentResult{Code: paymentResultCode},
					},
				}},
			},
		}

		unsafeMeta := xdr.TransactionMeta{
			Operations: &[]xdr.OperationMeta{
				{Changes: xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stellarAddress1),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stellarAddress2),
				}},
				{Changes: xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stellarAddress3),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stellarAddress4),
				}},
			},
		}

		feeChanges := xdr.LedgerEntryChanges{
			createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, feeAddress),
		}

		ledgerCloseMeta := xdr.LedgerCloseMeta{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: 12345,
						ScpValue:  xdr.StellarValue{CloseTime: 1000000000},
					},
				},
			},
		}

		return ingest.LedgerTransaction{
			Index:         200,
			LedgerVersion: 23,
			Hash:          xdr.Hash{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			Envelope:      envelope,
			Result:        xdr.TransactionResultPair{Result: result},
			UnsafeMeta:    unsafeMeta,
			FeeChanges:    feeChanges,
			Ledger:        ledgerCloseMeta,
		}
	}

	testCases := []struct {
		name                 string
		getTransaction       func(t *testing.T) ingest.LedgerTransaction
		expectedParticipants []string
		wantErrContains      string
	}{
		{
			name: "🟢successful_inner_transaction",
			getTransaction: func(t *testing.T) ingest.LedgerTransaction {
				return createTestTransaction(t, true, false)
			},
			expectedParticipants: []string{innerTxSourceAccount, stellarAddress1, stellarAddress2, stellarAddress3, stellarAddress4},
		},
		{
			name: "🟡failed_inner_transaction_only_considers_fee_account",
			getTransaction: func(t *testing.T) ingest.LedgerTransaction {
				return createTestTransaction(t, false, false)
			},
			expectedParticipants: []string{innerTxSourceAccount},
		},
		{
			name: "🟢successful_fee_bump_transaction",
			getTransaction: func(t *testing.T) ingest.LedgerTransaction {
				return createTestTransaction(t, true, true)
			},
			expectedParticipants: []string{feeBumpSourceAccount, innerTxSourceAccount, stellarAddress1, stellarAddress2, stellarAddress3, stellarAddress4},
		},
		{
			name: "🟡failed_fee_bump_transaction_only_considers_fee_account",
			getTransaction: func(t *testing.T) ingest.LedgerTransaction {
				return createTestTransaction(t, false, true)
			},
			expectedParticipants: []string{feeBumpSourceAccount, innerTxSourceAccount},
		},
		{
			name: "🔴failed_inner_transaction_with_invalid_meta_changes",
			getTransaction: func(t *testing.T) ingest.LedgerTransaction {
				tx := createTestTransaction(t, true, false)
				// Create invalid meta with unknown change type
				tx.UnsafeMeta = xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								{Type: xdr.LedgerEntryChangeType(999)}, // Invalid type
							},
						},
					},
				}
				return tx
			},
			wantErrContains: "identifying participants for meta",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			processor := NewParticipantsProcessor(network.TestNetworkPassphrase)
			transaction := tc.getTransaction(t)

			participants, err := processor.GetTransactionParticipants(transaction)

			if tc.wantErrContains != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, participants)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(tc.expectedParticipants), participants.Cardinality())
				for _, expectedParticipant := range tc.expectedParticipants {
					assert.True(t, participants.Contains(expectedParticipant))
				}
			}
		})
	}
}

func TestParticipantsProcessor_GetOperationsParticipants(t *testing.T) {
	testCases := []struct {
		name               string
		opXDRStr           string
		ledgerCloseMetaXDR string
		wantParticipantsFn func(t *testing.T) map[int64]OperationParticipants
		wantErrContains    string
	}{
		{
			name:               "🟢INVOKE_HOST_FUNCTION",
			ledgerCloseMetaXDR: "AAAAAQAAAADwuDvdXAqJsJTYyKvtpXcHylD9Y/kXEFndJ9V0eGP1bQAAABYJYL5X6XR61vHA1O/89Wna1bbnEHB4uFvaJ5nYdjfDp7Xun+JIFzQ4kDeLSBRgQ6DvKbClCs0DENQ/uYPKWFQNAAAAAGhTU8QAAAAAAAAAAQAAAACoJM0YvJ11Bk0pmltbrKQ7w6ovMmk4FT2ML5u1y23wMwAAAEBx539oYuMATaS/VJmPQ3OWGuWgmk+v0ztVLkg8hURfFgJl77HybMgk0RXW88oMZf0bCqyjqxKNIqbqtjogT9QNJ6K9PhEyv59XnPhcIURs4l4oT24o5SXn+XBg79w0+m/77M+Et4/QHz4tkapE1KMe74aMXbR8pT/V1sRFfW/vCwAAEwkN4Lazp2QAAAAAAAM7RHqXAAAAAAAAAAAAAAAOAAAAZABMS0AAAADI8RijRYewto4PdEb3c25/NuxDVc01YaS4mbBdqAM68SEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEJYL5X6XR61vHA1O/89Wna1bbnEHB4uFvaJ5nYdjfDpwAAAAIAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAEAAAAAAAAAZAAAAAEAAAAFAAAAAC1ecYfUjZ5bo883Bn1bKJXB0AnGyqpZIeAvaBGi0c8WAAAAAABRJPYAAAACAAAAAGwD/A4OcAwuf/Z+yAxxikQiRjWMhJ4YBDV1Ltuf5lZHAFEjNgAAEuUAAAADAAAAAQAAAAAAAAAAAAAAAGhTU94AAAAAAAAAAQAAAAAAAAAYAAAAAwAAAAAAAAAAAAAAAGwD/A4OcAwuf/Z+yAxxikQiRjWMhJ4YBDV1Ltuf5lZHw7M8g9MZoIMtl8sLC3Sm6DNcs0zcb2D2SETDf8ITk/wAAAAAWTxTx2pOgJz4ri6fbF6VfiksElZZQR/lZC/dy12NhKgAAAABAAAAEAAAAAEAAAAFAAAADwAAAAdFZDI1NTE5AAAAAA0AAAAgsepGzM0n9s6GKA1hGgQDFTjoOAvaskIzvkZLX+PMm/IAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAA8AAAAKUGVyc2lzdGVudAAAAAAAAQAAAAAAAAACAAAAAAAAAAAAAAAAbAP8Dg5wDC5/9n7IDHGKRCJGNYyEnhgENXUu25/mVkfDszyD0xmggy2XywsLdKboM1yzTNxvYPZIRMN/whOT/AAAAABZPFPHak6AnPiuLp9sXpV+KSwSVllBH+VkL93LXY2EqAAAAAEAAAAQAAAAAQAAAAUAAAAPAAAAB0VkMjU1MTkAAAAADQAAACCx6kbMzSf2zoYoDWEaBAMVOOg4C9qyQjO+Rktf48yb8gAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAADwAAAApQZXJzaXN0ZW50AAAAAAAAAAAAAQAAAAAAAAACAAAABgAAAAEblSaEyzWhu1daX8BHIElN8yahQS6fz4XRgIBXg+RZdgAAABAAAAABAAAAAgAAAA8AAAAHRWQyNTUxOQAAAAANAAAAILHqRszNJ/bOhigNYRoEAxU46DgL2rJCM75GS1/jzJvyAAAAAAAAAAdZPFPHak6AnPiuLp9sXpV+KSwSVllBH+VkL93LXY2EqAAAAAIAAAAGAAAAARuVJoTLNaG7V1pfwEcgSU3zJqFBLp/PhdGAgFeD5Fl2AAAAEAAAAAEAAAACAAAADwAAAAdFZDI1NTE5AAAAAA0AAAAgsepGzM0n9s6GKA1hGgQDFTjoOAvaskIzvkZLX+PMm/IAAAABAAAABgAAAAEblSaEyzWhu1daX8BHIElN8yahQS6fz4XRgIBXg+RZdgAAABQAAAABAE15iwAAW1gAAAE4AAAAAABRItIAAAABn+ZWRwAAAED9GZvaj8uSTke8bhFFsMGwuWhRJMBjzP6p4MtoHGSH+fvoLsT2tvM7g6NMVUqs1dnnwDmSgEhMI1ZUOb6SvLIKAAAAAAAAAAGi0c8WAAAAQCGdWutCgCuaHhHIWr8/A4p1+aJQvbQ9zLZGU7j2NyLsbZnsRTXafqseZOqnM6K8g/Dx5av2uHHmFvIbjbmxTQAAAAABZOuUrMUO78MjzqgDh/3O78MUZsw6aeuNKzEuC1w8YvAAAAAAAEYgzgAAAAGvrvihtletXSNgzAAesxt2O/00MMuiAnPUn/RL4qIVLgAAAAAARiBqAAAAAAAAAAEAAAAAAAAAGAAAAAAqWM8LTwCLmRwC4kGslynnDaqkRvAPdSBL6P02IT7CRgAAAAAAAAAAAAAAAgAAAAMAABL+AAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2tKASAAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAABMJAAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2Y3x4AAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAABAAAAAMAABMJAAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2Y3x4AAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAABMJAAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2Y3x4AAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAMAABL+AAAAAAAAAABsA/wODnAMLn/2fsgMcYpEIkY1jISeGAQ1dS7bn+ZWRwAAABdIdugAAAAS5QAAAAIAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAAEv4AAAAAaFNTjQAAAAAAAAABAAATCQAAAAAAAAAAbAP8Dg5wDC5/9n7IDHGKRCJGNYyEnhgENXUu25/mVkcAAAAXSHboAAAAEuUAAAADAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAABMJAAAAAGhTU8QAAAAAAAAAAQAAAAQAAAAAAAATCQAAAAlBNG49F8YWIlAlO4+h5IDdJIhckyboBlgA32CdKmsECwAviQgAAAAAAAAAAAAAEwkAAAAJsbCmYaveA4YkxKjb0ojMCJOA/FWz0y/hcY4DbPAEojgAL4kIAAAAAAAAAAAAABMJAAAABgAAAAAAAAABG5UmhMs1obtXWl/ARyBJTfMmoUEun8+F0YCAV4PkWXYAAAAUAAAAAQAAABMAAAAAWTxTx2pOgJz4ri6fbF6VfiksElZZQR/lZC/dy12NhKgAAAABAAAAAQAAAA8AAAAEaW5pdAAAAAAAAAABAAAAAAAAAAAAABMJAAAABgAAAAAAAAABG5UmhMs1obtXWl/ARyBJTfMmoUEun8+F0YCAV4PkWXYAAAAQAAAAAQAAAAIAAAAPAAAAB0VkMjU1MTkAAAAADQAAACCx6kbMzSf2zoYoDWEaBAMVOOg4C9qyQjO+Rktf48yb8gAAAAEAAAAQAAAAAQAAAAMAAAAPAAAAB0VkMjU1MTkAAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAAEAAAAAAAAAAgAAAAMAABMJAAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2Y3x4AAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAABMJAAAAAAAAAAAtXnGH1I2eW6PPNwZ9WyiVwdAJxsqqWSHgL2gRotHPFgAAABc2bn9EAAAFIQAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAeTHAAAAAABEOz8AAAAAAEQwGwAAAAEAAAAAAAAAARuVJoTLNaG7V1pfwEcgSU3zJqFBLp/PhdGAgFeD5Fl2AAAAAQAAAAAAAAADAAAADwAAAAhzcF9zd192MQAAAA8AAAADYWRkAAAAABAAAAABAAAAAgAAAA8AAAAHRWQyNTUxOQAAAAANAAAAILHqRszNJ/bOhigNYRoEAxU46DgL2rJCM75GS1/jzJvyAAAAEAAAAAEAAAACAAAAEAAAAAEAAAADAAAADwAAAAdFZDI1NTE5AAAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAADwAAAApQZXJzaXN0ZW50AAAAAAASAAAAARuVJoTLNaG7V1pfwEcgSU3zJqFBLp/PhdGAgFeD5Fl2AAAAFgAAAAEAAAAAAAAAAAAAAAIAAAAAAAAAAwAAAA8AAAAHZm5fY2FsbAAAAAANAAAAIBuVJoTLNaG7V1pfwEcgSU3zJqFBLp/PhdGAgFeD5Fl2AAAADwAAAA1fX2NvbnN0cnVjdG9yAAAAAAAAEAAAAAEAAAAFAAAADwAAAAdFZDI1NTE5AAAAAA0AAAAgsepGzM0n9s6GKA1hGgQDFTjoOAvaskIzvkZLX+PMm/IAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAA8AAAAKUGVyc2lzdGVudAAAAAAAAQAAAAAAAAABG5UmhMs1obtXWl/ARyBJTfMmoUEun8+F0YCAV4PkWXYAAAABAAAAAAAAAAMAAAAPAAAACHNwX3N3X3YxAAAADwAAAANhZGQAAAAAEAAAAAEAAAACAAAADwAAAAdFZDI1NTE5AAAAAA0AAAAgsepGzM0n9s6GKA1hGgQDFTjoOAvaskIzvkZLX+PMm/IAAAAQAAAAAQAAAAIAAAAQAAAAAQAAAAMAAAAPAAAAB0VkMjU1MTkAAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAAPAAAAClBlcnNpc3RlbnQAAAAAAAEAAAAAAAAAARuVJoTLNaG7V1pfwEcgSU3zJqFBLp/PhdGAgFeD5Fl2AAAAAgAAAAAAAAACAAAADwAAAAlmbl9yZXR1cm4AAAAAAAAPAAAADV9fY29uc3RydWN0b3IAAAAAAAABAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAACnJlYWRfZW50cnkAAAAAAAUAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAt3cml0ZV9lbnRyeQAAAAAFAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAQbGVkZ2VyX3JlYWRfYnl0ZQAAAAUAAAAAAABbWAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABFsZWRnZXJfd3JpdGVfYnl0ZQAAAAAAAAUAAAAAAAABOAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA1yZWFkX2tleV9ieXRlAAAAAAAABQAAAAAAAAE0AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAADndyaXRlX2tleV9ieXRlAAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAOcmVhZF9kYXRhX2J5dGUAAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA93cml0ZV9kYXRhX2J5dGUAAAAABQAAAAAAAAE4AAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAADnJlYWRfY29kZV9ieXRlAAAAAAAFAAAAAAAAW1gAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAPd3JpdGVfY29kZV9ieXRlAAAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAplbWl0X2V2ZW50AAAAAAAFAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAPZW1pdF9ldmVudF9ieXRlAAAAAAUAAAAAAAAA/AAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAhjcHVfaW5zbgAAAAUAAAAAAEoF/AAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAAhtZW1fYnl0ZQAAAAUAAAAAADGiPwAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABFpbnZva2VfdGltZV9uc2VjcwAAAAAAAAUAAAAAABF5LwAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAAA9tYXhfcndfa2V5X2J5dGUAAAAABQAAAAAAAABwAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAACAAAADwAAAAxjb3JlX21ldHJpY3MAAAAPAAAAEG1heF9yd19kYXRhX2J5dGUAAAAFAAAAAAAAALgAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAPAAAADGNvcmVfbWV0cmljcwAAAA8AAAAQbWF4X3J3X2NvZGVfYnl0ZQAAAAUAAAAAAABbWAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAgAAAA8AAAAMY29yZV9tZXRyaWNzAAAADwAAABNtYXhfZW1pdF9ldmVudF9ieXRlAAAAAAUAAAAAAAAA/AAAAAAAAAAAAAAAAABDQ0MAAAAAAAAAAA==",
			opXDRStr:           "AAAAAAAAABgAAAADAAAAAAAAAAAAAAAAbAP8Dg5wDC5/9n7IDHGKRCJGNYyEnhgENXUu25/mVkfDszyD0xmggy2XywsLdKboM1yzTNxvYPZIRMN/whOT/AAAAABZPFPHak6AnPiuLp9sXpV+KSwSVllBH+VkL93LXY2EqAAAAAEAAAAQAAAAAQAAAAUAAAAPAAAAB0VkMjU1MTkAAAAADQAAACCx6kbMzSf2zoYoDWEaBAMVOOg4C9qyQjO+Rktf48yb8gAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAABAAAAEAAAAAEAAAABAAAADwAAAApQZXJzaXN0ZW50AAAAAAABAAAAAAAAAAIAAAAAAAAAAAAAAABsA/wODnAMLn/2fsgMcYpEIkY1jISeGAQ1dS7bn+ZWR8OzPIPTGaCDLZfLCwt0pugzXLNM3G9g9khEw3/CE5P8AAAAAFk8U8dqToCc+K4un2xelX4pLBJWWUEf5WQv3ctdjYSoAAAAAQAAABAAAAABAAAABQAAAA8AAAAHRWQyNTUxOQAAAAANAAAAILHqRszNJ/bOhigNYRoEAxU46DgL2rJCM75GS1/jzJvyAAAAEAAAAAEAAAABAAAAAQAAABAAAAABAAAAAQAAAAEAAAAQAAAAAQAAAAEAAAAPAAAAClBlcnNpc3RlbnQAAAAAAAA=",
			wantParticipantsFn: func(t *testing.T) map[int64]OperationParticipants {
				var op xdr.Operation
				err := xdr.SafeUnmarshalBase64(opXDRStr, &op)
				require.NoError(t, err)

				return map[int64]OperationParticipants{
					20929375637505: {
						Operation:    op,
						Participants: set.NewSet("GBWAH7AOBZYAYLT76Z7MQDDRRJCCERRVRSCJ4GAEGV2S5W474ZLEOH4U"),
					},
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var lcm xdr.LedgerCloseMeta
			err := xdr.SafeUnmarshalBase64(ledgerCloseMetaXDR, &lcm)
			require.NoError(t, err)

			ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.TestNetworkPassphrase, lcm)
			require.NoError(t, err)
			ingestTx, err := ledgerTxReader.Read()
			require.NoError(t, err)

			processor := NewParticipantsProcessor(network.TestNetworkPassphrase)
			gotParticipants, err := processor.GetOperationsParticipants(ingestTx)
			require.NoError(t, err)

			assert.Equal(t, tc.wantParticipantsFn(t), gotParticipants)
		})
	}
}

func Test_metaXDR(t *testing.T) {
	// transfer(from: GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P, to: CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR, asset: CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC)/ hash=45593823f5229546618668ed22c8c4e8d3f74a6138f609fa9aa317ad217e64e3
	const ledgerCloseMetaXDR = "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"

	var lcm xdr.LedgerCloseMeta
	err := xdr.SafeUnmarshalBase64(ledgerCloseMetaXDR, &lcm)
	require.NoError(t, err)

	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.TestNetworkPassphrase, lcm)
	require.NoError(t, err)
	ingestTx, err := ledgerTxReader.Read()
	require.NoError(t, err)

	processor := NewParticipantsProcessor(network.TestNetworkPassphrase)
	gotParticipants, err := processor.GetOperationsParticipants(ingestTx)
	require.NoError(t, err)
	for opID, opParticipants := range gotParticipants {
		for _, participant := range opParticipants.Participants.ToSlice() {
			opType := opParticipants.Operation.Body.Type
			t.Logf("opID: %d, opType: %s, participant: %s", opID, opType.String(), participant)
		}
		contractOpParticipants, err := GetContractOpParticipants(opParticipants.Operation, ingestTx)
		require.NoError(t, err)
		t.Logf("contractOpParticipants: %s", contractOpParticipants)
		wantParticipants := []string{"GDYH62HW5R57ZFCJE77Q32YVUANQPK2A4663BWFVKAIMINNWVV6QEI5P", "CDSMYK7ADPT32KBXPXWSOWMBANDDFG76IVB4HWHOE2SA3DPAKXA4C6ZR", "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"}
		require.ElementsMatch(t, wantParticipants, contractOpParticipants)
	}
}

func GetContractOpParticipants(op xdr.Operation, tx ingest.LedgerTransaction) ([]string, error) {
	// 1. Source Account
	participants := set.NewSet[string]()
	if op.SourceAccount != nil {
		participants.Add(op.SourceAccount.Address())
	} else {
		participants.Add(tx.Envelope.SourceAccount().ToAccountId().Address())
	}

	invokeHostFunctionOp := op.Body.MustInvokeHostFunctionOp()

	// 2. ContractID
	contractID := invokeHostFunctionOp.HostFunction.InvokeContract.ContractAddress.ContractId
	contractIDStr := strkey.MustEncode(strkey.VersionByteContract, contractID[:])
	participants.Add(contractIDStr)

	// 3. Args
	var argsScVec xdr.ScVec = invokeHostFunctionOp.HostFunction.InvokeContract.Args
	argsScVal, err := xdr.NewScVal(xdr.ScValTypeScvVec, &argsScVec)
	if err != nil {
		return nil, fmt.Errorf("creating NewScVal for the args vector: %w", err)
	}
	argParticipants, err := GetScValParticipants(argsScVal)
	if err != nil {
		return nil, fmt.Errorf("getting scVal participants: %w", err)
	}
	participants = participants.Union(argParticipants)

	// 4. AuthEntries
	authEntriesParticipants, err := GetAuthEntryParticipants(invokeHostFunctionOp.Auth)
	if err != nil {
		return nil, fmt.Errorf("getting authEntry participants: %w", err)
	}
	participants = participants.Union(authEntriesParticipants)

	return participants.ToSlice(), nil
}

func GetAuthEntryParticipants(authEntries []xdr.SorobanAuthorizationEntry) (set.Set[string], error) {
	participants := set.NewSet[string]()
	for _, authEntry := range authEntries {
		switch authEntry.Credentials.Type {
		case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
			participant, err := authEntry.Credentials.MustAddress().Address.String()
			if err != nil {
				return nil, fmt.Errorf("converting ScAddress to string: %w", err)
			}
			participants.Add(participant)
		default:
			continue
		}
	}

	return participants, nil
}

func GetScValParticipants(scVal xdr.ScVal) (set.Set[string], error) {
	scvAddresses := GetScValScvAddresses(scVal)
	participants := set.NewSet[string]()

	for scvAddress := range scvAddresses.Iterator().C {
		scAddressStr, err := scvAddress.String()
		if err != nil {
			return nil, fmt.Errorf("converting ScAddress to string: %w", err)
		}
		participants.Add(scAddressStr)
	}

	return participants, nil
}

func GetScValScvAddresses(scVal xdr.ScVal) set.Set[xdr.ScAddress] {
	scAddresses := set.NewSet[xdr.ScAddress]()
	switch scVal.Type {
	case xdr.ScValTypeScvAddress:
		scAddresses.Add(scVal.MustAddress())

	case xdr.ScValTypeScvVec:
		for _, innerVal := range *scVal.MustVec() {
			scAddresses = scAddresses.Union(GetScValScvAddresses(innerVal))
		}

	case xdr.ScValTypeScvMap:
		for _, mapEntry := range *scVal.MustMap() {
			scAddresses = scAddresses.Union(GetScValScvAddresses(mapEntry.Key))
			scAddresses = scAddresses.Union(GetScValScvAddresses(mapEntry.Val))
		}

	default:
		break
	}

	return scAddresses
}
