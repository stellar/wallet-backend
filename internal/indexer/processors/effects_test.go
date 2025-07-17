package processors

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go/network"
	operation_processor "github.com/stellar/go/processors/operation"
	"github.com/stellar/go/toid"
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
		opWrapper := operation_processor.TransactionOperationWrapper{
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
					assert.Equal(t, "0", change.Thresholds["old"])
					assert.Equal(t, "1", change.Thresholds["new"])
				case types.StateChangeReasonMedium:
					assert.Equal(t, "0", change.Thresholds["old"])
					assert.Equal(t, "2", change.Thresholds["new"])
				case types.StateChangeReasonHigh:
					assert.Equal(t, "0", change.Thresholds["old"])
					assert.Equal(t, "3", change.Thresholds["new"])
				}
			case types.StateChangeCategoryFlags:
				//exhaustive:ignore
				switch *change.StateChangeReason {
				case types.StateChangeReasonSet:
					assert.Equal(t, types.NullableJSON{"auth_required_flag"}, change.Flags)
				case types.StateChangeReasonClear:
					assert.Equal(t, types.NullableJSON{"auth_revocable_flag"}, change.Flags)
				}
			case types.StateChangeCategorySigner:
				//exhaustive:ignore
				switch *change.StateChangeReason {
				case types.StateChangeReasonUpdate:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GC4XF7RE3R4P77GY5XNGICM56IOKUURWAAANPXHFC7G5H6FCNQVVH3OH", change.SignerAccountID.String)
					assert.Equal(t, types.NullableJSONB{"new": int32(3), "old": int32(1)}, change.SignerWeights)
				case types.StateChangeReasonAdd:
					assert.True(t, change.SignerAccountID.Valid)
					assert.Equal(t, "GAQHWQYBBW272OOXNQMMLCA5WY2XAZPODGB7Q3S5OKKIXVESKO55ZQ7C", change.SignerAccountID.String)
					assert.Equal(t, types.NullableJSONB{"new": int32(2)}, change.SignerWeights)
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
		opWrapper := operation_processor.TransactionOperationWrapper{
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
		assert.Equal(t, types.StateChangeCategoryTrustlineFlags, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSet, *changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSON{"authorized_to_maintain_liabilites"}, changes[0].Flags)

		assert.Equal(t, types.StateChangeCategoryTrustlineFlags, changes[1].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonClear, *changes[1].StateChangeReason)
		assert.Equal(t, types.NullableJSON{"authorized_flag", "clawback_enabled_flag"}, changes[1].Flags)
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
		processor := NewEffectsProcessor(networkPassphrase)
		opWrapper := operation_processor.TransactionOperationWrapper{
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
		assert.Equal(t, hash, changes[0].TxHash)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, *changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"name2": map[string]any{"new": "NTY3OA=="}}, changes[0].KeyValue)
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
		processor := NewEffectsProcessor(networkPassphrase)
		opWrapper := operation_processor.TransactionOperationWrapper{
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
		assert.Equal(t, hash, changes[0].TxHash)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, *changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{
			"GCR3TQ2TVH3QRI7GQMC3IJGUUBR32YQHWBIKIMTYRQ2YH4XUTDB75UKE": map[string]any{
				"new": "MTU3ODUyMTIwNF8yOTMyOTAyNzg=",
				"old": "MTU3ODUyMDg1OF8yNTIzOTE3Njg=",
			},
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
		processor := NewEffectsProcessor(networkPassphrase)
		opWrapper := operation_processor.TransactionOperationWrapper{
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
		assert.Equal(t, hash, changes[0].TxHash)
		assert.Equal(t, types.StateChangeCategoryMetadata, changes[0].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonDataEntry, *changes[0].StateChangeReason)
		assert.Equal(t, types.NullableJSONB{"hello": map[string]any{"old": ""}}, changes[0].KeyValue)
	})

	t.Run("Sponsorship - account sponsorship created/updated/revoked", func(t *testing.T) {
		envelopeXDR := "AAAAAGL8HQvQkbK2HA3WVjRrKmjX00fG8sLI7m0ERwJW/AX3AAAAZAAAAAAAAAAaAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAoZftFP3p4ifbTm6hQdieotu3Zw9E05GtoSh5MBytEpQAAAACVAvkAAAAAAAAAAABVvwF9wAAAEDHU95E9wxgETD8TqxUrkgC0/7XHyNDts6Q5huRHfDRyRcoHdv7aMp/sPvC3RPkXjOMjgbKJUX7SgExUeYB5f8F"
		resultXDR := "AAAAAAAAAGQAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAA="
		metaXDR := createAccountMetaB64
		feeChangesXDR := "AAAAAgAAAAMAAAA3AAAAAAAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wsatlj11nHQAAAAAAAAABkAAAAAAAAAAQAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAEAAAA5AAAAAAAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wsatlj11nFsAAAAAAAAABkAAAAAAAAAAQAAAABi/B0L0JGythwN1lY0aypo19NHxvLCyO5tBEcCVvwF9wAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAA=="
		hash := "0e5bd332291e3098e49886df2cdb9b5369a5f9e0a9973f0d9e1a9489c6581ba2"
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
		opWrapper := operation_processor.TransactionOperationWrapper{
			Index:          0,
			Operation:      op,
			Network:        network.TestNetworkPassphrase,
			Transaction:    transaction,
			LedgerSequence: 12345,
		}
		changes, err := processor.ProcessOperation(context.Background(), opWrapper)
		require.NoError(t, err)
		require.Len(t, changes, 8)

		// Sponsorship revoked creates two state changes - one for the sponsor and one for the target account
		assert.Equal(t, toid.New(12345, 1, 1).ToInt64(), changes[1].OperationID)
		assert.Equal(t, uint32(12345), changes[1].LedgerNumber)
		assert.Equal(t, time.Unix(12345*100, 0), changes[1].LedgerCreatedAt)
		assert.Equal(t, hash, changes[1].TxHash)
		assert.Equal(t, types.StateChangeCategorySponsorship, changes[1].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonRemove, *changes[1].StateChangeReason)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[1].AccountID)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[1].SponsoredAccountID.String)

		assert.Equal(t, types.StateChangeCategorySponsorship, changes[2].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonRemove, *changes[2].StateChangeReason)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[2].AccountID)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[2].SponsorAccountID.String)

		// Updating sponsorship creates 3 state changes - one for the new sponsor, one for the former sponsor, and one for the target account
		assert.Equal(t, types.StateChangeCategorySponsorship, changes[3].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonSet, *changes[3].StateChangeReason)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[3].AccountID)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[3].SponsoredAccountID.String)

		assert.Equal(t, types.StateChangeCategorySponsorship, changes[4].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonRemove, *changes[4].StateChangeReason)
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[4].AccountID)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[4].SponsoredAccountID.String)

		assert.Equal(t, types.StateChangeCategorySponsorship, changes[5].StateChangeCategory)
		assert.Equal(t, types.StateChangeReasonUpdate, *changes[5].StateChangeReason)
		assert.Equal(t, "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H", changes[5].AccountID)
		assert.Equal(t, "GACMZD5VJXTRLKVET72CETCYKELPNCOTTBDC6DHFEUPLG5DHEK534JQX", changes[5].SponsorAccountID.String)
		assert.Equal(t, "GAHK7EEG2WWHVKDNT4CEQFZGKF2LGDSW2IVM4S5DP42RBW3K6BTODB4A", changes[5].KeyValue["former_sponsor"])
	})
}
