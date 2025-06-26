package processors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func TestEffects_ProcessTransaction(t *testing.T) {
	t.Run("SetOption", func(t *testing.T) {
		envelopeXDR := "AAAAALly/iTceP/82O3aZAmd8hyqUjYAANfc5RfN0/iibCtTAAAAZAAIGHoAAAAHAAAAAQAAAAAAAAAAAAAAAF4FFtcAAAAAAAAAAQAAAAAAAAAFAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAEAAAACAAAAAQAAAAEAAAABAAAAAwAAAAEAAAABAAAAAQAAAAIAAAABAAAAAwAAAAEAAAAVaHR0cHM6Ly93d3cuaG9tZS5vcmcvAAAAAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAIAAAAAAAAAAaJsK1MAAABAiQjCxE53GjInjJtvNr6gdhztRi0GWOZKlUS2KZBLjX3n2N/y7RRNt7B1ZuFcZAxrnxWHD/fF2XcrEwFAuf4TDA=="
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAAFAAAAAAAAAAA="
		metaXDR := "AAAAAQAAAAIAAAADAA3iDQAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHblRAAIGHoAAAAGAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAA3iDQAAAAAAAAAAuXL+JNx4//zY7dpkCZ3yHKpSNgAA19zlF83T+KJsK1MAAAAXSHblRAAIGHoAAAAHAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAABAAAAAgAAAAMADeINAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduVEAAgYegAAAAcAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEADeINAAAAAAAAAAC5cv4k3Hj//Njt2mQJnfIcqlI2AADX3OUXzdP4omwrUwAAABdIduVEAAgYegAAAAcAAAABAAAAAQAAAAAge0MBDbX9OddsGMWIHbY1cGXuGYP4bl1ylIvUklO73AAAAAEAAAAVaHR0cHM6Ly93d3cuaG9tZS5vcmcvAAAAAwECAwAAAAEAAAAAIHtDAQ21/TnXbBjFiB22NXBl7hmD+G5dcpSL1JJTu9wAAAACAAAAAAAAAAA="
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
		processor := NewEffectsProcessor(networkPassphrase)
		changes, err := processor.ProcessTransaction(context.Background(), transaction, op, 0)
		require.NoError(t, err)
		require.Len(t, changes, 8)

		for _, change := range changes {
			assert.Equal(t, "0", change.OperationID)
			assert.Equal(t, int64(12345), change.LedgerNumber)
			assert.Equal(t, time.Unix(12345*100, 0), change.LedgerCreatedAt)
			assert.Equal(t, hash, change.TxHash)

			//exhaustive:ignore
			switch change.StateChangeCategory {
			case types.StateChangeCategoryMetadata:
				assert.Equal(t, types.StateChangeReasonHomeDomain, *change.StateChangeReason)
				assert.Equal(t, "https://www.home.org/", change.KeyValue["home_domain"])
			case types.StateChangeCategorySignatureThreshold:
				//exhaustive:ignore
				switch *change.StateChangeReason {
				case types.StateChangeReasonLow:
					assert.Equal(t, types.NullableJSONB{"low_threshold": "1"}, change.Thresholds)
				case types.StateChangeReasonMedium:
					assert.Equal(t, types.NullableJSONB{"med_threshold": "2"}, change.Thresholds)
				case types.StateChangeReasonHigh:
					assert.Equal(t, types.NullableJSONB{"high_threshold": "3"}, change.Thresholds)
				}
			case types.StateChangeCategoryFlags:
				//exhaustive:ignore
				switch *change.StateChangeReason {
				case types.StateChangeReasonSet:
					assert.Equal(t, types.NullableJSONB{"auth_required_flag": true}, change.Flags)
				case types.StateChangeReasonClear:
					assert.Equal(t, types.NullableJSONB{"auth_revocable_flag": false}, change.Flags)
				}
			case types.StateChangeCategorySigner:
				//exhaustive:ignore
				switch *change.StateChangeReason {
				case types.StateChangeReasonUpdate:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH", change.SignerAccountID.String)
					assert.True(t, change.SignerWeight.Valid)
					assert.Equal(t, int64(3), change.SignerWeight.Int64)
				case types.StateChangeReasonAdd:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GAQHWQYBBW272OOXNQMMLCA5WY2XAZPODGB7Q3S5OKKIXVESKO55ZQ7C", change.SignerAccountID.String)
					assert.True(t, change.SignerWeight.Valid)
					assert.Equal(t, int64(2), change.SignerWeight.Int64)
				}
			}
		}
	})

	t.Run("SetTrustlineFlags", func(t *testing.T) {
		setTrustlineFlagsOp := setTrustlineFlagsOp()
		transaction := createTx(setTrustlineFlagsOp, nil, nil, false)
		op, found := transaction.GetOperation(0)
		require.True(t, found)
		processor := NewEffectsProcessor(networkPassphrase)
		changes, err := processor.ProcessTransaction(context.Background(), transaction, op, 0)
		require.NoError(t, err)
		require.Len(t, changes, 2)

		assert.Equal(t, "0", changes[0].OperationID)
		assert.Equal(t, int64(12345), changes[0].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[0].LedgerCreatedAt)
		assert.Equal(t, transaction.Result.TransactionHash.HexString(), changes[0].TxHash)
		assert.Equal(t, types.StateChangeCategoryTrustlineFlags, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSet, *changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"authorized_to_maintain_liabilites": true}, changes[0].Flags)

		assert.Equal(t, types.StateChangeCategoryTrustlineFlags, changes[1].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonClear, *changes[1].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"clawback_enabled_flag": false, "authorized_flag": false}, changes[1].Flags)
	})
}
