package cmd

import (
	"errors"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

func Test_ChannelAccountsCommand_EnsureCommand(t *testing.T) {
	dbt := dbtest.Open(t)
	defer dbt.Close()

	distributionKP := keypair.MustRandom()

	mChAccService := NewMockChAccCmdServiceInterface(t)
	caCommand := (&channelAccountCmd{}).Command(mChAccService)
	rootCmd.AddCommand(caCommand)
	rootCmd.SetArgs([]string{
		"channel-account",
		"ensure", "2",
		"--channel-account-encryption-passphrase", keypair.MustRandom().Seed(),
		"--database-url", dbt.DSN,
		"--distribution-account-private-key", distributionKP.Seed(),
		"--distribution-account-public-key", distributionKP.Address(),
		"--distribution-account-signature-provider", "ENV",
	})

	t.Run("🟢executes_successfully", func(t *testing.T) {
		mChAccService.EXPECT().
			EnsureChannelAccounts(mock.AnythingOfType("context.backgroundCtx"), mock.AnythingOfType("*services.channelAccountService"), int64(2)).
			Return(nil).
			Once()
		err := rootCmd.Execute()
		require.NoError(t, err)
	})

	t.Run("🔴fails_if_ChannelAccountsService_fails", func(t *testing.T) {
		mChAccService.EXPECT().
			EnsureChannelAccounts(mock.AnythingOfType("context.backgroundCtx"), mock.AnythingOfType("*services.channelAccountService"), int64(2)).
			Return(errors.New("foo bar baz")).
			Once()
		err := rootCmd.Execute()
		require.Error(t, err)
		assert.ErrorContains(t, err, "ensuring the number of channel accounts is created")
		assert.ErrorContains(t, err, "foo bar baz")
	})
}
