package utils

import (
	"go/types"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/support/config"
	"github.com/stellar/wallet-backend/internal/entities"
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

func TestSetConfigOptionStellarPrivateKey(t *testing.T) {
	opts := struct{ distributionPrivateKey string }{}

	co := config.ConfigOption{
		Name:           "distribution-private-key",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionStellarPrivateKey,
		ConfigKey:      &opts.distributionPrivateKey,
	}
	expectedPrivateKey := "SBUSPEKAZKLZSWHRSJ2HWDZUK6I3IVDUWA7JJZSGBLZ2WZIUJI7FPNB5"

	testCases := []customSetterTestCase[string]{
		{
			name:            "returns an error if the private key is invalid",
			args:            []string{"--distribution-private-key", "invalid_private_key"},
			wantErrContains: `invalid private key provided in distribution-private-key`,
		},
		{
			name:            "returns an error if the private key is invalid (public key instead)",
			args:            []string{"--distribution-private-key", "GAX46JJZ3NPUM2EUBTTGFM6ITDF7IGAFNBSVWDONPYZJREHFPP2I5U7S"},
			wantErrContains: `invalid private key provided in distribution-private-key`,
		},
		{
			name:       "handles Stellar private key through the CLI flag",
			args:       []string{"--distribution-private-key", "SBUSPEKAZKLZSWHRSJ2HWDZUK6I3IVDUWA7JJZSGBLZ2WZIUJI7FPNB5"},
			wantResult: expectedPrivateKey,
		},
		{
			name:       "handles Stellar private key through the ENV flag",
			envValue:   "SBUSPEKAZKLZSWHRSJ2HWDZUK6I3IVDUWA7JJZSGBLZ2WZIUJI7FPNB5",
			wantResult: expectedPrivateKey,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts.distributionPrivateKey = ""
			customSetterTester[string](t, tc, co)
		})
	}
}

func Test_SetConfigOptionLogLevel(t *testing.T) {
	opts := struct{ logrusLevel logrus.Level }{}

	co := config.ConfigOption{
		Name:           "log-level",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionLogLevel,
		ConfigKey:      &opts.logrusLevel,
	}

	testCases := []customSetterTestCase[logrus.Level]{
		{
			name:            "returns an error if the log level is empty",
			args:            []string{},
			wantErrContains: `couldn't parse log level in log-level: not a valid logrus Level: ""`,
		},
		{
			name:            "returns an error if the log level is invalid",
			args:            []string{"--log-level", "test"},
			wantErrContains: `couldn't parse log level in log-level: not a valid logrus Level: "test"`,
		},
		{
			name:       "handles messenger type TRACE (through CLI args)",
			args:       []string{"--log-level", "TRACE"},
			wantResult: logrus.TraceLevel,
		},
		{
			name:       "handles messenger type TRACE (through ENV vars)",
			envValue:   "TRACE",
			wantResult: logrus.TraceLevel,
		},
		{
			name:       "handles messenger type INFO (through CLI args)",
			args:       []string{"--log-level", "iNfO"},
			wantResult: logrus.InfoLevel,
		},
		{
			name:       "handles messenger type INFO (through ENV vars)",
			envValue:   "INFO",
			wantResult: logrus.InfoLevel,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts.logrusLevel = 0
			customSetterTester[logrus.Level](t, tc, co)
		})
	}
}

func TestSetConfigOptionAssets(t *testing.T) {
	opts := struct{ assets []entities.Asset }{}

	co := config.ConfigOption{
		Name:           "assets",
		OptType:        types.String,
		CustomSetValue: SetConfigOptionAssets,
		ConfigKey:      &opts.assets,
	}
	expectedAssets := []entities.Asset{
		{
			Code:   "USDC",
			Issuer: "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		},
		{
			Code:   "ARST",
			Issuer: "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO",
		},
	}

	testCases := []customSetterTestCase[[]entities.Asset]{
		{
			name:            "returns an error if asset is empty",
			wantErrContains: "assets cannot be empty",
		},
		{
			name:            "returns an error if assets JSON is invalid",
			args:            []string{"--assets", "invalid"},
			wantErrContains: "decoding assets JSON: invalid character 'i' looking for beginning of value",
		},
		{
			name:       "handles assets JSON through the CLI flag",
			args:       []string{"--assets", `[{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}, {"code": "ARST", "issuer": "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO"}]`},
			wantResult: expectedAssets,
		},
		{
			name:       "handles assets JSON through the ENV vars",
			envValue:   `[{"code": "USDC", "issuer": "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5"}, {"code": "ARST", "issuer": "GB7TAYRUZGE6TVT7NHP5SMIZRNQA6PLM423EYISAOAP3MKYIQMVYP2JO"}]`,
			wantResult: expectedAssets,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts.assets = []entities.Asset{}
			customSetterTester(t, tc, co)
		})
	}
}
