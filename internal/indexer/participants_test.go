package indexer

import (
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/utils"
)

const (
	stelarAddress1 = "GAUJETIZVEP2NRYLUESJ3LS66NVCEGMON4UDCBCSBEVPIID773P2W6AY"
	stelarAddress2 = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
	stelarAddress3 = "GAXI33UCLQTCKM2NMRBS7XYBR535LLEVAHL5YBN4FTCB4HZHT7ZA5CVK"
	stelarAddress4 = "GA5WBPYA5Y4WAEHXWR2UKO2UO4BUGHUQ74EUPKON2QHV4WRHOIRNKKH2"
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
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stelarAddress1),
				}
			},
			expectedParticipants: []string{stelarAddress1},
		},
		{
			name: "🟢multiple_changes_with_accounts",
			getChanges: func(t *testing.T) xdr.LedgerEntryChanges {
				return xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stelarAddress1),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, stelarAddress2),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stelarAddress3),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stelarAddress4),
				}
			},
			expectedParticipants: []string{stelarAddress1, stelarAddress2, stelarAddress3, stelarAddress4},
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
									AccountId: xdr.MustAddress(stelarAddress1),
									Asset: xdr.TrustLineAsset{
										Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
										AlphaNum4: &xdr.AlphaNum4{
											AssetCode: [4]byte{'T', 'E', 'S', 'T'},
											Issuer:    xdr.MustAddress(stelarAddress1),
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
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stelarAddress1),
							},
						},
					},
				}
			},
			expected: []string{stelarAddress1},
		},
		{
			name: "🟢multiple_operations_with_multiple_account_changes",
			getMeta: func(t *testing.T) xdr.TransactionMeta {
				return xdr.TransactionMeta{
					Operations: &[]xdr.OperationMeta{
						{
							Changes: xdr.LedgerEntryChanges{
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stelarAddress1),
							},
						},
						{
							Changes: xdr.LedgerEntryChanges{
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryRemoved, stelarAddress2),
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stelarAddress3),
								createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stelarAddress4),
							},
						},
					},
				}
			},
			expected: []string{stelarAddress1, stelarAddress2, stelarAddress3, stelarAddress4},
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

func TestParticipantsProcessor_addTransactionParticipants(t *testing.T) {
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
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryCreated, stelarAddress1),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryUpdated, stelarAddress2),
				}},
				{Changes: xdr.LedgerEntryChanges{
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stelarAddress3),
					createTestLedgerEntryChange(t, xdr.LedgerEntryChangeTypeLedgerEntryState, stelarAddress4),
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
			expectedParticipants: []string{innerTxSourceAccount, stelarAddress1, stelarAddress2, stelarAddress3, stelarAddress4},
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
			expectedParticipants: []string{feeBumpSourceAccount, innerTxSourceAccount, stelarAddress1, stelarAddress2, stelarAddress3, stelarAddress4},
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
			processor := NewParticipantsProcessor()
			transaction := tc.getTransaction(t)

			err := processor.addTransactionParticipants(transaction)

			if tc.wantErrContains != "" {
				assert.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErrContains)
				// When there's an error, the ingestion buffer should be empty
				assert.Empty(t, processor.IngestionBuffer.Participants.ToSlice())
			} else {
				assert.NoError(t, err)

				// Verify participants
				participants := processor.IngestionBuffer.Participants.ToSlice()
				assert.Len(t, participants, len(tc.expectedParticipants))
				for _, expectedParticipant := range tc.expectedParticipants {
					assert.Contains(t, participants, expectedParticipant)
				}

				// Verify transactions
				transactionHash := transaction.Hash.HexString()
				storedTx := processor.IngestionBuffer.GetTransaction(transactionHash)
				assert.Equal(t, transactionHash, storedTx.Hash)
				assert.Equal(t, uint32(12345), storedTx.LedgerNumber)

				// Verify participant-transaction mappings
				for _, participant := range tc.expectedParticipants {
					txHashes := processor.IngestionBuffer.GetParticipantTransactionHashes(participant)
					assert.True(t, txHashes.Contains(transactionHash))
				}
			}
		})
	}
}
