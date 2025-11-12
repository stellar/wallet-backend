package integrationtests

import (
	"context"

	"github.com/stretchr/testify/suite"

	"github.com/stellar/go/keypair"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type AccountBalancesTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *AccountBalancesTestSuite) TestAccountBalancesForClassicTrustlines() {
	for _, account := range []*keypair.Full{suite.testEnv.BalanceTestAccount1KP, suite.testEnv.BalanceTestAccount2KP} {
		balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(context.Background(), account.Address())
		suite.Require().NoError(err)
		suite.Require().NotNil(balances)
		suite.Require().NotEmpty(balances)

		suite.Require().Len(balances, 2)

		nativeBalance, ok := balances[0].(*types.NativeBalance)
		suite.Require().True(ok)
		suite.Require().Equal("10000.0000000", nativeBalance.GetBalance())
		suite.Require().Equal(types.TokenTypeNative, nativeBalance.GetTokenType())

		usdcBalance, ok := balances[1].(*types.TrustlineBalance)
		suite.Require().True(ok)
		suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
		suite.Require().Equal(suite.testEnv.USDCContractAddress, usdcBalance.GetTokenID())
		suite.Require().Equal(types.TokenTypeClassic, usdcBalance.GetTokenType())
	}
}
