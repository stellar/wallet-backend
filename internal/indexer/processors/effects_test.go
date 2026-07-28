package processors

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
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
		envelopeXDR := "AAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAAZAAIGHoAAAAHAAAAAQAAAAAAAAAAAAAAAF4FFtcAAAAAAAAAAQAAAAAAAAAFAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAEAAAACAAAAAQAAAAEAAAABAAAAAwAAAAEAAAABAAAAAQAAAAIAAAABAAAAAwAAAAEAAAAVaHR0cHM6Ly93d3cuaG9tZS5vcmcvAAAAAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAIAAAAAAAAAAaJsK1MAAABAiQjCxE53GjInjJtvNr6gdhztRi0GWOZKlUS2KZBLjX3n2N/y7RRNt7B1ZuFcZAxrnxWHD/fF2XcrEwFAuf4TDA=="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAFAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADAA3iDQAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHblRAAIGHoAAAAGAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAA3iDQAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHblRAAIGHoAAAAHAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAgAAAAMADeINAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduVEAAgYegAAAAcAAAAAAAAAAAAAAAAAAAAPb2xkLmV4YW1wbGUub3JnAAEAAAAAAAAAAAAAAAAAAAAAAAABAA3iDQAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHblRAAIGHoAAAAHAAAAAQAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAABAAAAFWh0dHBzOi8vd3d3LmhvbWUub3JnLwAAAAMBAgMAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAgAAAAAAAAAA"
		feeChangesXDR := "AAAAAgAAAAMADd8YAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduWoAAgYegAAAAYAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEADeINAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduVEAAgYegAAAAYAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "e76b7b0133690fbfb2de8fa9ca2273cb4f2e29447e0cf0e14a5f82d0daa48760"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
			case types.StateChangeCategoryHomeDomain:
				// Both the pre-image and the new value are non-empty in this fixture.
				assert.Equal(t, types.StateChangeReasonUpdate, change.StateChangeReason)
				assert.Equal(t, "old.example.org", change.KeyValue["old"])
				assert.Equal(t, "https://www.home.org/", change.KeyValue["new"])
			case types.StateChangeCategorySignatureThreshold:
				assert.Equal(t, types.StateChangeReasonUpdate, change.StateChangeReason)
				require.True(t, change.Threshold.Valid, "threshold level must identify which threshold changed")
				switch types.ThresholdLevel(change.Threshold.String) {
				case types.ThresholdLevelLow:
					assert.Equal(t, int16(0), change.ThresholdOld.Int16)
					assert.Equal(t, int16(1), change.ThresholdNew.Int16)
				case types.ThresholdLevelMedium:
					assert.Equal(t, int16(0), change.ThresholdOld.Int16)
					assert.Equal(t, int16(2), change.ThresholdNew.Int16)
				case types.ThresholdLevelHigh:
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
		envelopeXDR := "AAAAADEhMVDHiYXdz5z8l73XGyrQ2RN85ZRW1uLsCNQumfsZAAAAZAAAADAAAAACAAAAAAAAAAAAAAABAAAAAAAAAAoAAAAFbmFtZTIAAAAAAAABAAAABDU2NzgAAAAAAAAAAS6Z+xkAAABAjxgnTRBCa0n1efZocxpEjXeITQ5sEYTVd9fowuto2kPw5eFwgVnz6OrKJwCRt5L8ylmWiATXVI3Zyfi3yTKqBA=="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADAAAAMQAAAAAAAAAAMSExUMeJhd3PnPyXvdcbKtDZE3zllFbW4uwI1C6Z+xkAAAACVAvi1AAAADAAAAABAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAMQAAAAAAAAAAMSExUMeJhd3PnPyXvdcbKtDZE3zllFbW4uwI1C6Z+xkAAAACVAvi1AAAADAAAAACAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAwAAAAMAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+LUAAAAMAAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+LUAAAAMAAAAAIAAAACAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAxAAAAAwAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAVuYW1lMgAAAAAAAAQ1Njc4AAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+OcAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAAxAAAAAAAAAAAxITFQx4mF3c+c/Je91xsq0NkTfOWUVtbi7AjULpn7GQAAAAJUC+M4AAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "e4609180751e7702466a8845857df43e4d154ec84b6bad62ce507fe12f1daf99"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
		assert.Equal(t, types.StateChangeCategoryDataEntry, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonAdd, changes[0].StateChangeReason)
		assert.Equal(t, sql.NullString{String: "name2", Valid: true}, changes[0].DataEntryName)
		assert.Equal(t, types.NullableJSONB{"new": "NTY3OA=="}, changes[0].KeyValue)
	})
	t.Run("ManageData - data updated", func(t *testing.T) {
		envelopeXDR := "AAAAAKO5w1Op9wij5oMFtCTUoGO9YgewUKQyeIw1g/L0mMP+AAAAZAAALbYAADNjAAAAAQAAAAAAAAAAAAAAAF4WVfgAAAAAAAAAAQAAAAEAAAAAOO6NdKTWKbGao6zsPag+izHxq3eUPLiwjREobLhQAmQAAAAKAAAAOEdDUjNUUTJUVkgzUVJJN0dRTUMzSUpHVVVCUjMyWVFIV0JJS0lNVFlSUTJZSDRYVVREQjc1VUtFAAAAAQAAABQxNTc4NTIxMjA0XzI5MzI5MDI3OAAAAAAAAAAC0oPafQAAAEAcsS0iq/t8i+p85xwLsRy8JpRNEeqobEC5yuhO9ouVf3PE0VjLqv8sDd0St4qbtXU5fqlHd49R9CR+z7tiRLEB9JjD/gAAAEBmaa9sGxQhEhrakzXcSNpMbR4nox/Ha0p/1sI4tabNEzjgYLwKMn1U9tIdVvKKDwE22jg+CI2FlPJ3+FJPmKUA"
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADABEK2wAAAAAAAAAAo7nDU6n3CKPmgwW0JNSgY71iB7BQpDJ4jDWD8vSYw/4AAAAXSGLVVAAALbYAADNiAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABABEK2wAAAAAAAAAAo7nDU6n3CKPmgwW0JNSgY71iB7BQpDJ4jDWD8vSYw/4AAAAXSGLVVAAALbYAADNjAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAgAAAAMAEQqbAAAAAwAAAAA47o10pNYpsZqjrOw9qD6LMfGrd5Q8uLCNEShsuFACZAAAADhHQ1IzVFEyVFZIM1FSSTdHUU1DM0lKR1VVQlIzMllRSFdCSUtJTVRZUlEyWUg0WFVUREI3NVVLRQAAABQxNTc4NTIwODU4XzI1MjM5MTc2OAAAAAAAAAAAAAAAAQARCtsAAAADAAAAADjujXSk1imxmqOs7D2oPosx8at3lDy4sI0RKGy4UAJkAAAAOEdDUjNUUTJUVkgzUVJJN0dRTUMzSUpHVVVCUjMyWVFIV0JJS0lNVFlSUTJZSDRYVVREQjc1VUtFAAAAFDE1Nzg1MjEyMDRfMjkzMjkwMjc4AAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAEQqbAAAAAAAAAACjucNTqfcIo+aDBbQk1KBjvWIHsFCkMniMNYPy9JjD/gAAABdIYtW4AAAttgAAM2IAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAEQrbAAAAAAAAAACjucNTqfcIo+aDBbQk1KBjvWIHsFCkMniMNYPy9JjD/gAAABdIYtVUAAAttgAAM2IAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "c60b74a14b628d06d3683db8b36ce81344967ac13bc433124bcef44115fbb257"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
		assert.Equal(t, types.StateChangeCategoryDataEntry, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUpdate, changes[0].StateChangeReason)
		assert.Equal(t, sql.NullString{String: "GCR3TQ2TVH3QRI7GQMC3IJGUUBR32YQHWBIKIMTYRQ2YH4XUTDB75UKE", Valid: true}, changes[0].DataEntryName)
		assert.Equal(t, types.NullableJSONB{
			"new": "MTU3ODUyMTIwNF8yOTMyOTAyNzg=",
			"old": "MTU3ODUyMDg1OF8yNTIzOTE3Njg=",
		}, changes[0].KeyValue)
	})
	t.Run("ManageData - data removed", func(t *testing.T) {
		envelopeXDR := "AAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAAZAAIGHoAAAAKAAAAAQAAAAAAAAAAAAAAAF4XaMIAAAAAAAAAAQAAAAAAAAAKAAAABWhlbGxvAAAAAAAAAAAAAAAAAAABomwrUwAAAEDyu3HI9bdkzNBs4UgTjVmYt3LQ0CC/6a8yWBmz8OiKeY/RJ9wJvV9/m0JWGtFWbPOXWBg/Pj3ttgKMiHh9TKoF"
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAKAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADABE92wAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHbkGAAIGHoAAAAJAAAAAgAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAABAAAAFWh0dHBzOi8vd3d3LmhvbWUub3JnLwAAAAMBAgMAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAgAAAAAAAAAAAAAAAQARPdsAAAAAAAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAAF0h25BgACBh6AAAACgAAAAIAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAQAAABVodHRwczovL3d3dy5ob21lLm9yZy8AAAADAQIDAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAIAAAAAAAAAAAAAAAEAAAAEAAAAAwARPcsAAAADAAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAABWhlbGxvAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAMAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAFaGVsbG8AAAAAAAADABE92wAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHbkGAAIGHoAAAAKAAAAAgAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAABAAAAFWh0dHBzOi8vd3d3LmhvbWUub3JnLwAAAAMBAgMAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAgAAAAAAAAAAAAAAAQARPdsAAAAAAAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAAF0h25BgACBh6AAAACgAAAAEAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAQAAABVodHRwczovL3d3dy5ob21lLm9yZy8AAAADAQIDAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAIAAAAAAAAAAA=="
		feeChangesXDR := "AAAAAgAAAAMAET3LAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduR8AAgYegAAAAkAAAACAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAEAAAAVaHR0cHM6Ly93d3cuaG9tZS5vcmcvAAAAAwECAwAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAACAAAAAAAAAAAAAAABABE92wAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHbkGAAIGHoAAAAJAAAAAgAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAABAAAAFWh0dHBzOi8vd3d3LmhvbWUub3JnLwAAAAMBAgMAAAABAAAAACB7QwENtf0512wYxYgdtjVwZe4Zg/huXXKUi9SSU7vcAAAAAgAAAAAAAAAA"
		hash := "397b208adb3d484d14ddd3237422baae0b6bd1e8feb3c970147bc6bcc493d112"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
		assert.Equal(t, types.StateChangeCategoryDataEntry, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonRemove, changes[0].StateChangeReason)
		assert.Equal(t, sql.NullString{String: "hello", Valid: true}, changes[0].DataEntryName)
		assert.Equal(t, types.NullableJSONB{"old": ""}, changes[0].KeyValue)
	})
	t.Run("ChangeTrust - trustline created", func(t *testing.T) {
		envelopeXDR := "AAAAAgAAAAAf1miSBZ7jc0TxIHULMUqdj+dibtkh1JEEwITVtQ05ZgAAAGQAB1eLAAAAAwAAAAEAAAAAAAAAAAAAAABowwQqAAAAAAAAAAEAAAAAAAAABgAAAAFURVNUAAAAAFrnJwiWP46hSSjcYc6wY93h556Qpe47SA8bIQGXMJTlf/////////8AAAAAAAAAAbUNOWYAAABAzWelNCrF4Q+iSKX30xHrBm76FMa2h89pPauijrWAVlcj/swEyYZqjU94SYU+8XEWUuvg2rpjCIHGPHHyzSXlAw=="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADAAAAKAAAAAAAAAAAq26sUclf95G3mAzqohcAxtpe+UiaovKwDpCv20t6bF8AAAACVAvjOAAAACYAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAKAAAAAAAAAAAq26sUclf95G3mAzqohcAxtpe+UiaovKwDpCv20t6bF8AAAACVAvjOAAAACYAAAABAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAwAAAAMAAAAoAAAAAAAAAACrbqxRyV/3kbeYDOqiFwDG2l75SJqi8rAOkK/bS3psXwAAAAJUC+M4AAAAJgAAAAEAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAAoAAAAAAAAAACrbqxRyV/3kbeYDOqiFwDG2l75SJqi8rAOkK/bS3psXwAAAAJUC+M4AAAAJgAAAAEAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAoAAAAAQAAAACrbqxRyV/3kbeYDOqiFwDG2l75SJqi8rAOkK/bS3psXwAAAAFVU0QAAAAAAPkmOJur5F/mOxTJDb+0bMLCJGDRl3meP2MBEDVKSPP4AAAAAAAAAAB//////////wAAAAAAAAAAAAAAAA=="
		feeChangesXDR := "AAAAAgAAAAMAB19pAAAAAAAAAAAf1miSBZ7jc0TxIHULMUqdj+dibtkh1JEEwITVtQ05ZgAAABcM3B04AAdXiwAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAADAAAAAAAHX2kAAAAAaMMDfAAAAAAAAAABAAdfhwAAAAAAAAAAH9ZokgWe43NE8SB1CzFKnY/nYm7ZIdSRBMCE1bUNOWYAAAAXDNwc1AAHV4sAAAACAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAwAAAAAAB19pAAAAAGjDA3wAAAAA"
		hash := "c7bee372d573009ac63ad7476e310ad29b1f7399a6941b57d84527d4015dba57"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
		envelopeXDR := "AAAAAHHbEhVipyZ2k4byyCZkS1Bdvpj7faBChuYo8S/Rt89UAAAAZAAQuJIAAAAHAAAAAQAAAAAAAAAAAAAAAF4XVskAAAAAAAAAAQAAAAAAAAAGAAAAAlRFU1RBU1NFVAAAAAAAAAA7JUkkD+tgCi2xTVyEcs4WZXOA0l7w2orZg/bghXOgkAAAAAA7msoAAAAAAAAAAAHRt89UAAAAQOCi2ylqRvvRzZaCFjGkLYFk7DCjJA5uZ1nXo8FaPCRl2LZczoMbc46sZIlHh0ENzk7fKjFnRPMo8XAirrrf2go="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADABE6jwAAAAAAAAAAcdsSFWKnJnaThvLIJmRLUF2+mPt9oEKG5ijxL9G3z1QAAAAAO5rHRAAQuJIAAAAGAAAAAgAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABABE6jwAAAAAAAAAAcdsSFWKnJnaThvLIJmRLUF2+mPt9oEKG5ijxL9G3z1QAAAAAO5rHRAAQuJIAAAAHAAAAAgAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAgAAAAMAETqAAAAAAQAAAABx2xIVYqcmdpOG8sgmZEtQXb6Y+32gQobmKPEv0bfPVAAAAAJURVNUQVNTRVQAAAAAAAAAOyVJJA/rYAotsU1chHLOFmVzgNJe8NqK2YP24IVzoJAAAAAAO5rKAAAAAAA7msoAAAAAAQAAAAAAAAAAAAAAAQAROo8AAAABAAAAAHHbEhVipyZ2k4byyCZkS1Bdvpj7faBChuYo8S/Rt89UAAAAAlRFU1RBU1NFVAAAAAAAAAA7JUkkD+tgCi2xTVyEcs4WZXOA0l7w2orZg/bghXOgkAAAAAA7msoAAAAAADuaygAAAAABAAAAAAAAAAA="
		feeChangesXDR := "AAAAAgAAAAMAETp/AAAAAAAAAABx2xIVYqcmdpOG8sgmZEtQXb6Y+32gQobmKPEv0bfPVAAAAAA7mseoABC4kgAAAAYAAAACAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAETqPAAAAAAAAAABx2xIVYqcmdpOG8sgmZEtQXb6Y+32gQobmKPEv0bfPVAAAAAA7msdEABC4kgAAAAYAAAACAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "dc8d4714d7db3d0e27ae07f629bc72f1605fc24a2d178af04edbb602592791aa"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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
		envelopeXDR := "AAAAABwDSftLnTVAHpKUGYPZfTJr6rIm5Z5IqDHVBFuTI3ubAAAAZAARM9kAAAADAAAAAQAAAAAAAAAAAAAAAF4XMm8AAAAAAAAAAQAAAAAAAAAGAAAAAk9DSVRva2VuAAAAAAAAAABJxf/HoI4oaD9CLBvECRhG9GPMNa/65PTI9N7F37o4nwAAAAAAAAAAAAAAAAAAAAGTI3ubAAAAQMHTFPeyHA+W2EYHVDut4dQ18zvF+47SsTPaePwZUaCgw/A3tKDx7sO7R8xlI3GwKQl91Ljmm1dbvAONU9nk/AQ="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAGAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADABEz3wAAAAAAAAAAHANJ+0udNUAekpQZg9l9MmvqsiblnkioMdUEW5Mje5sAAAAXSHbm1AARM9kAAAACAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABABEz3wAAAAAAAAAAHANJ+0udNUAekpQZg9l9MmvqsiblnkioMdUEW5Mje5sAAAAXSHbm1AARM9kAAAADAAAAAQAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAABAAAAAMAETPeAAAAAQAAAAAcA0n7S501QB6SlBmD2X0ya+qyJuWeSKgx1QRbkyN7mwAAAAJPQ0lUb2tlbgAAAAAAAAAAScX/x6COKGg/QiwbxAkYRvRjzDWv+uT0yPTexd+6OJ8AAAAAAAAAAH//////////AAAAAQAAAAAAAAAAAAAAAgAAAAEAAAAAHANJ+0udNUAekpQZg9l9MmvqsiblnkioMdUEW5Mje5sAAAACT0NJVG9rZW4AAAAAAAAAAEnF/8egjihoP0IsG8QJGEb0Y8w1r/rk9Mj03sXfujifAAAAAwARM98AAAAAAAAAABwDSftLnTVAHpKUGYPZfTJr6rIm5Z5IqDHVBFuTI3ubAAAAF0h25tQAETPZAAAAAwAAAAEAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAQARM98AAAAAAAAAABwDSftLnTVAHpKUGYPZfTJr6rIm5Z5IqDHVBFuTI3ubAAAAF0h25tQAETPZAAAAAwAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAA"
		feeChangesXDR := "AAAAAgAAAAMAETPeAAAAAAAAAAAcA0n7S501QB6SlBmD2X0ya+qyJuWeSKgx1QRbkyN7mwAAABdIduc4ABEz2QAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAETPfAAAAAAAAAAAcA0n7S501QB6SlBmD2X0ya+qyJuWeSKgx1QRbkyN7mwAAABdIdubUABEz2QAAAAIAAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "0f1e93ed9a83edb01ad8ccab67fd59dc7a513c413a8d5a580c5eb7a9c44f2844"
		transaction := buildTransactionFromXDR(
			t,
			testTransaction{
				Index:         1,
				EnvelopeXDR:   envelopeXDR,
				ResultXDR:     resultXDR,
				MetaXDR:       metaXDR,
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

// TestEffects_ParseThresholds_DeterministicOrder pins the emission order of threshold state
// changes: when a single SetOptions effect updates low, medium, and high thresholds together,
// the resulting state changes must always come back in low -> medium -> high order so that
// ordinals assigned within a (to_id, operation_id) group are reproducible across re-ingests.
// It also covers the required old values, which come from the account pre-image.
func TestEffects_ParseThresholds_DeterministicOrder(t *testing.T) {
	const address = "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH"
	processor := NewEffectsProcessor(networkPassphrase, nil)
	changeBuilder := NewStateChangeBuilder(12345, 12345*100, toid.New(12345, 1, 1).ToInt64(), nil).
		WithAccount(address).
		WithCategory(types.StateChangeCategorySignatureThreshold)
	effect := &EffectOutput{
		Address: address,
		Details: map[string]interface{}{
			"low_threshold":  xdr.Uint32(1),
			"med_threshold":  xdr.Uint32(2),
			"high_threshold": xdr.Uint32(3),
		},
	}
	changes := []ingest.Change{{
		Type: xdr.LedgerEntryTypeAccount,
		Pre: &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress(address),
				// Thresholds are [master, low, medium, high].
				Thresholds: xdr.Thresholds{1, 5, 6, 7},
			},
		}},
	}}

	thresholdChanges, err := processor.parseThresholds(changeBuilder, effect, changes)
	require.NoError(t, err)

	require.Len(t, thresholdChanges, 3)
	for i, change := range thresholdChanges {
		assert.Equal(t, types.StateChangeReasonUpdate, change.StateChangeReason, "change %d", i)
	}
	assert.Equal(t, types.ThresholdLevelLow, types.ThresholdLevel(thresholdChanges[0].Threshold.String))
	assert.Equal(t, types.ThresholdLevelMedium, types.ThresholdLevel(thresholdChanges[1].Threshold.String))
	assert.Equal(t, types.ThresholdLevelHigh, types.ThresholdLevel(thresholdChanges[2].Threshold.String))
	assert.Equal(t, int16(5), thresholdChanges[0].ThresholdOld.Int16)
	assert.Equal(t, int16(6), thresholdChanges[1].ThresholdOld.Int16)
	assert.Equal(t, int16(7), thresholdChanges[2].ThresholdOld.Int16)

	t.Run("missing account pre-image is an error", func(t *testing.T) {
		_, err := processor.parseThresholds(changeBuilder, effect, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no previous account state")
	})
}

// TestEffects_HomeDomainChange covers the derivation of the HOME_DOMAIN reason and the
// trimmed key-value payload from a domain transition, including the no-op transition that
// emits no state change at all.
func TestEffects_HomeDomainChange(t *testing.T) {
	testCases := []struct {
		name         string
		oldDomain    string
		newDomain    string
		wantReason   types.StateChangeReason
		wantKeyValue map[string]any
		wantChanged  bool
	}{
		{
			name:         "empty old domain means the domain was set for the first time",
			oldDomain:    "",
			newDomain:    "home.org",
			wantReason:   types.StateChangeReasonSet,
			wantKeyValue: map[string]any{"new": "home.org"},
			wantChanged:  true,
		},
		{
			name:         "empty new domain means the domain was cleared",
			oldDomain:    "home.org",
			newDomain:    "",
			wantReason:   types.StateChangeReasonClear,
			wantKeyValue: map[string]any{"old": "home.org"},
			wantChanged:  true,
		},
		{
			name:         "two distinct non-empty domains mean the domain was updated",
			oldDomain:    "old.example.org",
			newDomain:    "home.org",
			wantReason:   types.StateChangeReasonUpdate,
			wantKeyValue: map[string]any{"old": "old.example.org", "new": "home.org"},
			wantChanged:  true,
		},
		{
			name:        "rewriting the same domain changes nothing",
			oldDomain:   "same.org",
			newDomain:   "same.org",
			wantChanged: false,
		},
		{
			name:        "an absent domain left absent changes nothing",
			oldDomain:   "",
			newDomain:   "",
			wantChanged: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reason, keyValue, changed := homeDomainChange(tc.oldDomain, tc.newDomain)
			assert.Equal(t, tc.wantChanged, changed)
			assert.Equal(t, tc.wantReason, reason)
			assert.Equal(t, tc.wantKeyValue, keyValue)
		})
	}
}

// TestEffects_GetPrevLedgerEntryState_MatchesEntity pins the pre-image lookup to the
// effect's entity rather than the first entry of the requested type: multi-account
// operations such as merges carry several account pre-images in one change set, and an
// operation can touch several of one account's trustlines. Serving another entity's
// pre-image would fabricate old values.
func TestEffects_GetPrevLedgerEntryState_MatchesEntity(t *testing.T) {
	p := NewEffectsProcessor(networkPassphrase, nil)
	const target = "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH"
	const other = "GAQHWQYBBW272OOXNQMMLCA5WY2XAZPODGB7Q3S5OKKIXVESKO55ZQ7C"

	accountEntry := func(addr string, lowThreshold byte) *xdr.LedgerEntry {
		return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:  xdr.MustAddress(addr),
				Thresholds: xdr.Thresholds{1, lowThreshold, 0, 0},
			},
		}}
	}
	effect := &EffectOutput{Address: target, Details: map[string]interface{}{}}

	t.Run("account pre-image is matched by address, not position", func(t *testing.T) {
		changes := []ingest.Change{
			{Type: xdr.LedgerEntryTypeAccount, Pre: accountEntry(other, 9)},
			{Type: xdr.LedgerEntryTypeAccount, Pre: accountEntry(target, 5)},
		}
		pre := p.getPrevLedgerEntryState(effect, xdr.LedgerEntryTypeAccount, changes)
		require.NotNil(t, pre)
		account := pre.Data.MustAccount()
		assert.Equal(t, target, account.AccountId.Address())
		assert.Equal(t, xdr.Thresholds{1, 5, 0, 0}, account.Thresholds)
	})

	t.Run("no matching account yields nil", func(t *testing.T) {
		changes := []ingest.Change{{Type: xdr.LedgerEntryTypeAccount, Pre: accountEntry(other, 9)}}
		assert.Nil(t, p.getPrevLedgerEntryState(effect, xdr.LedgerEntryTypeAccount, changes))
	})

	t.Run("trustline pre-image is matched by trustor and asset", func(t *testing.T) {
		trustlineEntry := func(addr, code string, limit int64) *xdr.LedgerEntry {
			asset := xdr.MustNewCreditAsset(code, other)
			return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: xdr.MustAddress(addr),
					Asset:     asset.ToTrustLineAsset(),
					Limit:     xdr.Int64(limit),
				},
			}}
		}
		changes := []ingest.Change{
			{Type: xdr.LedgerEntryTypeTrustline, Pre: trustlineEntry(other, "USDC", 111)},
			{Type: xdr.LedgerEntryTypeTrustline, Pre: trustlineEntry(target, "EURC", 222)},
			{Type: xdr.LedgerEntryTypeTrustline, Pre: trustlineEntry(target, "USDC", 333)},
		}
		tlEffect := &EffectOutput{Address: target, Details: map[string]interface{}{
			"asset_type": "credit_alphanum4", "asset_code": "USDC", "asset_issuer": other,
		}}
		pre := p.getPrevLedgerEntryState(tlEffect, xdr.LedgerEntryTypeTrustline, changes)
		require.NotNil(t, pre)
		assert.Equal(t, xdr.Int64(333), pre.Data.MustTrustLine().Limit)
	})

	t.Run("data pre-image is matched by owner and entry name", func(t *testing.T) {
		dataEntry := func(addr, name, value string) *xdr.LedgerEntry {
			return &xdr.LedgerEntry{Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeData,
				Data: &xdr.DataEntry{
					AccountId: xdr.MustAddress(addr),
					DataName:  xdr.String64(name),
					DataValue: xdr.DataValue(value),
				},
			}}
		}
		changes := []ingest.Change{
			{Type: xdr.LedgerEntryTypeData, Pre: dataEntry(target, "config_a", "v1")},
			{Type: xdr.LedgerEntryTypeData, Pre: dataEntry(target, "config_b", "v2")},
		}
		dataEffect := &EffectOutput{Address: target, Details: map[string]interface{}{
			"name": xdr.String64("config_b"),
		}}
		pre := p.getPrevLedgerEntryState(dataEffect, xdr.LedgerEntryTypeData, changes)
		require.NotNil(t, pre)
		assert.Equal(t, xdr.DataValue("v2"), pre.Data.MustData().DataValue)
	})
}
