package processors

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestEffects_ProcessTransaction(t *testing.T) {
	t.Run("SetOption", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAFAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMADd8YAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduWoAAgYegAAAAYAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEADeINAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduVEAAgYegAAAAYAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)

		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 8)

		for _, change := range changes {
			assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), change.OperationID)
			assert.Equal(t, uint32(12345), change.LedgerNumber)
			assert.Equal(t, time.Unix(12345*100, 0), change.LedgerCreatedAt)

			//exhaustive:ignore
			switch change.StateChangeCategory {
			case types.StateChangeCategoryMetadata:
				assert.Equal(t, types.StateChangeReasonHomeDomain, change.StateChangeReason)
				assert.Equal(t, "https://www.home.org/", change.KeyValue["home_domain"])
			case types.StateChangeCategorySignatureThreshold:
				//exhaustive:ignore
				switch change.StateChangeReason {
				case types.StateChangeReasonLow:
					assert.Equal(t, int16(0), change.ThresholdOld.Int16)
					assert.Equal(t, int16(1), change.ThresholdNew.Int16)
				case types.StateChangeReasonMedium:
					assert.Equal(t, int16(0), change.ThresholdOld.Int16)
					assert.Equal(t, int16(2), change.ThresholdNew.Int16)
				case types.StateChangeReasonHigh:
					assert.Equal(t, int16(0), change.ThresholdOld.Int16)
					assert.Equal(t, int16(3), change.ThresholdNew.Int16)
				}
			case types.StateChangeCategoryFlags:
				//exhaustive:ignore
				switch change.StateChangeReason {
				case types.StateChangeReasonSet:
					assert.Equal(t, sql.NullInt16{Int16: types.FlagBitAuthRequired, Valid: true}, change.Flags)
				case types.StateChangeReasonClear:
					assert.Equal(t, sql.NullInt16{Int16: types.FlagBitAuthRevocable, Valid: true}, change.Flags)
				}
			case types.StateChangeCategorySigner:
				//exhaustive:ignore
				switch change.StateChangeReason {
				case types.StateChangeReasonUpdate:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH", change.SignerAccountID.String())
					assert.Equal(t, int16(1), change.SignerWeightOld.Int16)
					assert.Equal(t, int16(3), change.SignerWeightNew.Int16)
				case types.StateChangeReasonAdd:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GAQHWQYBBW272OOXNQMMLCA5WY2XAZPODGB7Q3S5OKKIXVESKO55ZQ7C", change.SignerAccountID.String())
					assert.False(t, change.SignerWeightOld.Valid) // New signer has no old weight
					assert.Equal(t, int16(2), change.SignerWeightNew.Int16)
				}
			}
		}
	})

	t.Run("SetTrustlineFlags - generates balance authorization state changes", func(t *testing.T) {
		setTrustlineFlagsOp := setTrustlineFlagsOp()
		transaction := createTx(setTrustlineFlagsOp, nil, nil, false)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 2)

		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[0].OperationID)
		assert.Equal(t, uint32(12345), changes[0].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[0].LedgerCreatedAt)
		assert.Equal(t, types.StateChangeCategoryBalanceAuthorization, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSet, changes[0].StateChangeReason)
		assert.Equal(t, sql.NullInt16{Int16: types.FlagBitAuthorizedToMaintainLiabilities, Valid: true}, changes[0].Flags)

		assert.Equal(t, types.StateChangeCategoryBalanceAuthorization, changes[1].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonClear, changes[1].StateChangeReason)
		// Bitmask for authorized (1) | clawback_enabled (32) = 33
		assert.Equal(t, sql.NullInt16{Int16: types.FlagBitAuthorized | types.FlagBitClawbackEnabled, Valid: true}, changes[1].Flags)
	})

	t.Run("ManageData - data created", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+OcAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+M4AAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "e4609180751e7702466a8845857df43e4d154ec84b6bad62ce507fe12f1daf99"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)

		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 1)

		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[0].OperationID)
		assert.Equal(t, uint32(12345), changes[0].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[0].LedgerCreatedAt)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"name2": map[string]any{"new": "NTY3OA=="}}, changes[0].KeyValue)
	})
	t.Run("ManageData - data updated", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAEQqbAAAAAAAAAACjucNTqfcIo+aDBbQk1KBjvWIHsFCkMniMNYPy9JjD/gAAABdIYtW4AAAttgAAM2IAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAEQrbAAAAAAAAAACjucNTqfcIo+aDBbQk1KBjvWIHsFCkMniMNYPy9JjD/gAAABdIYtVUAAAttgAAM2IAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "c60b74a14b628d06d3683db8b36ce81344967ac13bc433124bcef44115fbb257"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)

		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 1)

		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[0].OperationID)
		assert.Equal(t, uint32(12345), changes[0].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[0].LedgerCreatedAt)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{
			"GCR3TQ2TVH3QRI7GQMC3IJGUUBR32YQHWBIKIMTYRQ2YH4XUTDB75UKE": map[string]any{
				"new": "MTU3ODUyMTIwNF8yOTMyOTAyNzg=",
				"old": "MTU3ODUyMDg1OF8yNTIzOTE3Njg=",
			},
		}, changes[0].KeyValue)
	})
	t.Run("ManageData - data removed", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAET3LAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduR8AAgYegAAAAkAAAACAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAEAAAAVaHR0cHM6Ly93d3cuaG9tZS5vcmcvAAAAAwECAwAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAACAAAAAAAAAAAAAAABABE92wAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHbkGAAIGHoAAAAJAAAAAgAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAABAAAAFWh0dHBzOi8vd3d3LmhvbWUub3JnLwAAAAMBAgMAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAgAAAAAAAAAA"
		hash := "397b208adb3d484d14ddd3237422baae0b6bd1e8feb3c970147bc6bcc493d112"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)

		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 1)

		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[0].OperationID)
		assert.Equal(t, uint32(12345), changes[0].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[0].LedgerCreatedAt)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"hello": map[string]any{"old": ""}}, changes[0].KeyValue)
	})
	t.Run("Sponsorship - reserves sponsorship created/updated/revoked", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAAAA3AAAAAAAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wsatlj11nHQAAAAAAAAABkAAAAAAAAAAQAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAA5AAAAAAAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wsatlj11nFsAAAAAAAAABkAAAAAAAAAAQAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "0e5bd332291e3098e49886df2cdb9b5369a5f9e0a9973f0d9e1a9489c6581ba2"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 9) // Updated: CreateAccount now generates SIGNER/ADD effect

		// CreateAccount generates SIGNER/ADD effect at index 0
		assert.Equal(t, types.StateChangeCategorySigner, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonAdd, changes[0].StateChangeReason)

		// Sponsorship revoked creates two state changes - one for the sponsor and one for the target account
		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[1].OperationID)
		assert.Equal(t, uint32(12345), changes[1].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[1].LedgerCreatedAt)
		assert.Equal(t, types.StateChangeCategoryReserves, changes[1].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUnsponsor, changes[1].StateChangeReason)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[1].AccountID.String())
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[1].SponsoredAccountID.String())

		assert.Equal(t, types.StateChangeCategoryReserves, changes[2].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUnsponsor, changes[2].StateChangeReason)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[2].AccountID.String())
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[2].SponsorAccountID.String())

		// Updating sponsorship creates 4 state changes - one for the new sponsor, one for the former sponsor, and two for the target account
		assert.Equal(t, types.StateChangeCategoryReserves, changes[3].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSponsor, changes[3].StateChangeReason)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[3].AccountID.String())
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[3].SponsoredAccountID.String())

		assert.Equal(t, types.StateChangeCategoryReserves, changes[4].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUnsponsor, changes[4].StateChangeReason)
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[4].AccountID.String())
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[4].SponsoredAccountID.String())

		assert.Equal(t, types.StateChangeCategoryReserves, changes[5].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSponsor, changes[5].StateChangeReason)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[5].AccountID.String())
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[5].SponsorAccountID.String())

		assert.Equal(t, types.StateChangeCategoryReserves, changes[6].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUnsponsor, changes[6].StateChangeReason)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[6].AccountID.String())
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[6].SponsorAccountID.String())

		// Sponsorship created creates two state changes - one for the sponsor and one for the target account
		assert.Equal(t, types.StateChangeCategoryReserves, changes[7].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSponsor, changes[7].StateChangeReason)
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[7].AccountID.String())
		assert.Equal(t, "GCQZP3IU7XU6EJ63JZXKCQOYT2RNXN3HB5CNHENNUEUHSMA4VUJJJSEN", changes[7].SponsoredAccountID.String())

		assert.Equal(t, types.StateChangeCategoryReserves, changes[8].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSponsor, changes[8].StateChangeReason)
		assert.Equal(t, "GCQZP3IU7XU6EJ63JZXKCQOYT2RNXN3HB5CNHENNUEUHSMA4VUJJJSEN", changes[8].AccountID.String())
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[8].SponsorAccountID.String())
	})
	t.Run("ChangeTrust - trustline created", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAB19pAAAAAAAAAAAf1miSBZ7jc0TxIHULMUqdj+dibtkh1JEEwITVtQ05ZgAAABcM3B04AAdXiwAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAHX2kAAAAAaMMDfAAAAAAAAAABAAdfhwAAAAAAAAAAH9ZokgWe43NE8SB1CzFKnY/nYm7ZIdSRBMCE1bUNOWYAAAAXDNwc1AAHV4sAAAACAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAB19pAAAAAGjDA3wAAAAA"
		hash := "c7bee372d573009ac63ad7476e310ad29b1f7399a6941b57d84527d4015dba57"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		// Only expect 1 change because the trustline in metadata is for a different asset (USD vs TEST)
		// so balance authorization generation will fail and be skipped
		require.Len(t, changes, 1)

		// Should only get the trustline creation
		assert.Equal(t, types.StateChangeCategoryTrustline, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonAdd, changes[0].StateChangeReason)
		assert.False(t, changes[0].TrustlineLimitOld.Valid) // New trustline has no old limit
		assert.Equal(t, "922337203685.4775807", changes[0].TrustlineLimitNew.String)
		asset := xdr.MustNewCreditAsset("TEST", "GBNOOJYISY7Y5IKJFDOGDTVQMPO6DZ46SCS64O2IB4NSCAMXGCKOLORN")
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		assert.Equal(t, strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]), changes[0].TokenID.String())
	})
	t.Run("ChangeTrust - trustline updated", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAETp/AAAAAAAAAABx2xIVYqcmdpOG8sgmZEtQXb6Y+32gQobmKPEv0bfPVAAAAAA7mseoABC4kgAAAAYAAAACAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAETqPAAAAAAAAAABx2xIVYqcmdpOG8sgmZEtQXb6Y+32gQobmKPEv0bfPVAAAAAA7msdEABC4kgAAAAYAAAACAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "dc8d4714d7db3d0e27ae07f629bc72f1605fc24a2d178af04edbb602592791aa"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		assert.Equal(t, types.StateChangeCategoryTrustline, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUpdate, changes[0].StateChangeReason)
		asset := xdr.MustNewCreditAsset("TESTASSET", "GA5SKSJEB7VWACRNWFGVZBDSZYLGK44A2JPPBWUK3GB7NYEFOOQJAC2B")
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		assert.Equal(t, strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]), changes[0].TokenID.String())
		assert.Equal(t, "1000000000", changes[0].TrustlineLimitOld.String)
		assert.Equal(t, "100.0000000", changes[0].TrustlineLimitNew.String)
	})
	t.Run("ChangeTrust - trustline removed", func(t *testing.T) {
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAETPeAAAAAAAAAAAcA0n7S501QB6SlBmD2X0ya+qyJuWeSKgx1QRbkyN7mwAAABdIduc4ABEz2QAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAETPfAAAAAAAAAAAcA0n7S501QB6SlBmD2X0ya+qyJuWeSKgx1QRbkyN7mwAAABdIdubUABEz2QAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "0f1e93ed9a83edb01ad8ccab67fd59dc7a513c413a8d5a580c5eb7a9c44f2844"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				ResultXDR:     resultXDR,
				FeeChangesXDR: feeChangesXDR,
				Hash:          hash,
			},
		)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase, nil)
		opWrapper := &TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		assert.Equal(t, types.StateChangeCategoryTrustline, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonRemove, changes[0].StateChangeReason)
		asset := xdr.MustNewCreditAsset("OCIToken", "GBE4L76HUCHCQ2B7IIWBXRAJDBDPIY6MGWX7VZHUZD2N5RO7XI4J6GTJ")
		assetContractID, err := asset.ContractID(networkPassphrase)
		require.NoError(t, err)
		assert.Equal(t, strkey.MustEncode(strkey.VersionByteContract, assetContractID[:]), changes[0].TokenID.String())
	})
}
