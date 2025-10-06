package integrationtests

import (
	"context"

	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

type AccountRegisterTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *AccountRegisterTestSuite) TestAccountRegistration() {
	ctx := context.Background()

	client := suite.testEnv.WBClient
	addresses := []string{suite.testEnv.PrimaryAccountKP.Address(), suite.testEnv.SecondaryAccountKP.Address()}
	for _, address := range addresses {
		account, err := client.RegisterAccount(ctx, address)
		suite.Require().NoError(err)
		suite.Require().True(account.Success)
		suite.Require().Equal(address, account.Account.Address)

		// Fetch the address to make sure it was registered
		fetchedAccount, err := client.GetAccountByAddress(ctx, address)
		suite.Require().NoError(err)
		suite.Require().Equal(address, fetchedAccount.Address)
	}
}

func (suite *AccountRegisterTestSuite) TestDuplicateAccountRegistration() {
	ctx := context.Background()

	client := suite.testEnv.WBClient
	address := suite.testEnv.PrimaryAccountKP.Address()
	_, err := client.RegisterAccount(ctx, address)
	suite.Require().Error(err)
	suite.Require().ErrorContains(err, "Account is already registered")
}
