package integrationtests

import (
	"context"

	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type AccountBalancesTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *AccountBalancesTestSuite) TestAccountBalancesForClassicTrustlines() {
	balances, err := suite.testEnv.WBClient.GetBalancesByAccountAddress(context.Background(), suite.testEnv.PrimaryAccountKP.Address())
	suite.Require().NoError(err)
	suite.Require().NotNil(balances)
	suite.Require().NotEmpty(balances)

	suite.Require().Len(balances, 2)

	nativeBalance, ok := balances[0].(*types.NativeBalance)
	suite.Require().True(ok)
	suite.Require().Equal("10000.0000000", nativeBalance.GetBalance())
	suite.Require().Equal("NATIVE", nativeBalance.TokenType)

	usdcBalance, ok := balances[1].(*types.TrustlineBalance)
	suite.Require().True(ok)
	suite.Require().Equal("100.0000000", usdcBalance.GetBalance())
	suite.Require().Equal("USDC", usdcBalance.TokenID)
	suite.Require().Equal("CLASSIC", usdcBalance.TokenType)
}
