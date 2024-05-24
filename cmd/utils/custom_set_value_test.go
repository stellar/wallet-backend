package utils

import (
	"go/types"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/wallet-backend/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// customSetterTestCase is a test case to test a custom_set_value function.
type customSetterTestCase[T any] struct {
	name            string
	args            []string
	envValue        string
	wantErrContains string
	wantResult      T
}

// customSetterTester tests a custom_set_value function, according with the customSetterTestCase provided.
func customSetterTester[T any](t *testing.T, tc customSetterTestCase[T], co config.ConfigOption) {
	t.Helper()
	ClearTestEnvironment(t)
	if tc.envValue != "" {
		envName := strings.ToUpper(co.Name)
		envName = strings.ReplaceAll(envName, "-", "_")
		t.Setenv(envName, tc.envValue)
	}

	// start the CLI command
	testCmd := cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			co.Require()
			return co.SetValue()
		},
	}
	// mock the command line output
	buf := new(strings.Builder)
	testCmd.SetOut(buf)

	// Initialize the command for the given option
	err := co.Init(&testCmd)
	require.NoError(t, err)

	// execute command line
	if len(tc.args) > 0 {
		testCmd.SetArgs(tc.args)
	}
	err = testCmd.Execute()

	// check the result
	if tc.wantErrContains != "" {
		assert.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantErrContains)
	} else {
		assert.NoError(t, err)
	}

	if !utils.IsEmpty(tc.wantResult) {
		destPointer := utils.UnwrapInterfaceToPointer[T](co.ConfigKey)
		assert.Equal(t, tc.wantResult, *destPointer)
	}
}

// clearTestEnvironment removes all envs from the test environment. It's useful
// to make tests independent from the localhost environment variables.
func ClearTestEnvironment(t *testing.T) {
	t.Helper()

	// remove all envs from tghe test environment
	for _, env := range os.Environ() {
		key := env[:strings.Index(env, "=")]
		t.Setenv(key, "")
	}
}

func TestSetConfigOptionStellarPublicKey(t *testing.T) {
	opts := struct{ sep10SigningPublicKey string }{}

	co := config.ConfigOption{
		Name:           "wallet-signing-key",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionStellarPublicKey,
		ConfigKey:      &opts.sep10SigningPublicKey,
	}
	expectedPublicKey := "GAX46JJZ3NPUM2EUBTTGFM6ITDF7IGAFNBSVWDONPYZJREHFPP2I5U7S"

	testCases := []customSetterTestCase[string]{
		{
			name:            "returns an error if the public key is empty",
			wantErrContains: "validating public key in wallet-signing-key: strkey is 0 bytes long; minimum valid length is 5",
		},
		{
			name:            "returns an error if the public key is invalid",
			args:            []string{"--wallet-signing-key", "invalid_public_key"},
			wantErrContains: "validating public key in wallet-signing-key: base32 decode failed: illegal base32 data at input byte 18",
		},
		{
			name:            "returns an error if the public key is invalid (private key instead)",
			args:            []string{"--wallet-signing-key", "SDISQRUPIHAO5WIIGY4QRDCINZSA44TX3OIIUK3C63NUKN5DABKEQ276"},
			wantErrContains: "validating public key in wallet-signing-key: invalid version byte",
		},
		{
			name:       "handles Stellar public key through the CLI flag",
			args:       []string{"--wallet-signing-key", "GAX46JJZ3NPUM2EUBTTGFM6ITDF7IGAFNBSVWDONPYZJREHFPP2I5U7S"},
			wantResult: expectedPublicKey,
		},
		{
			name:       "handles Stellar public key through the ENV vars",
			envValue:   "GAX46JJZ3NPUM2EUBTTGFM6ITDF7IGAFNBSVWDONPYZJREHFPP2I5U7S",
			wantResult: expectedPublicKey,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts.sep10SigningPublicKey = ""
			customSetterTester(t, tc, co)
		})
	}
}
