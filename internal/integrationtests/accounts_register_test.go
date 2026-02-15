package integrationtests

import (
	"context"

	"github.com/stretchr/testify/suite"

	"github.com/stellar/go-stellar-sdk/keypair"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
	"github.com/stellar/wallet-backend/pkg/wbclient/types"
)

type AccountRegisterTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}

func (suite *AccountRegisterTestSuite) TestParticipantFiltering() {
	ctx := context.Background()
	client := suite.testEnv.WBClient

	// 1. Enable participant filtering
	err := suite.testEnv.RestartIngestContainer(ctx, map[string]string{
		"ENABLE_PARTICIPANT_FILTERING": "true",
	})
	suite.Require().NoError(err)

	// 2. Create a new random account (unregistered)
	unregisteredKP := keypair.MustRandom()

	// Fund the account (this creates an operation on the network)
	suite.testEnv.Containers.CreateAndFundAccounts(ctx, suite.T(), []*keypair.Full{unregisteredKP})

	// Wait for the ledger to be ingested
	suite.testEnv.WaitForLedgers(ctx, 2)

	// 3. Verify operations are NOT stored for unregistered account
	limit := int32(10)
	ops, err := client.GetAccountOperations(ctx, unregisteredKP.Address(), nil, nil, &limit, nil, nil, nil)
	suite.Require().NoError(err)
	suite.Require().Empty(ops.Edges, "Expected no operations for unregistered account")

	// 4. Register the account
	suite.registerAndVerify(ctx, unregisteredKP.Address())

	// Check for duplicate registration
	suite.verifyDuplicateRegistration(ctx, unregisteredKP.Address())

	// 5. Perform a payment operation from the registered account to another account
	// Execute payment from the registered account (now it should be tracked)
	suite.testEnv.Containers.SubmitPaymentOp(ctx, suite.T(), unregisteredKP.Address(), "100")

	// Wait for ledger
	suite.testEnv.WaitForLedgers(ctx, 2)

	// 6. Verify operations ARE stored now for the registered account
	ops, err = client.GetAccountOperations(ctx, unregisteredKP.Address(), nil, nil, &limit, nil, nil, nil)
	suite.Require().NoError(err)
	suite.Require().NotEmpty(ops.Edges, "Expected operations for registered account")

	// Verify the payment operation was ingested correctly
	suite.Require().Len(ops.Edges, 1, "Expected exactly one operation")
	paymentOp := ops.Edges[0].Node
	suite.Require().NotNil(paymentOp, "Operation node should not be nil")
	suite.Require().Equal(types.OperationTypePayment, paymentOp.OperationType, "Expected PAYMENT operation type")
}

func (suite *AccountRegisterTestSuite) registerAndVerify(ctx context.Context, address string) {
	client := suite.testEnv.WBClient
	account, err := client.RegisterAccount(ctx, address)
	suite.Require().NoError(err)
	suite.Require().True(account.Success)
	suite.Require().Equal(address, account.Account.Address)

	// Fetch the address to make sure it was registered
	fetchedAccount, err := client.GetAccountByAddress(ctx, address)
	suite.Require().NoError(err)
	suite.Require().Equal(address, fetchedAccount.Address)
}

func (suite *AccountRegisterTestSuite) verifyDuplicateRegistration(ctx context.Context, address string) {
	client := suite.testEnv.WBClient
	_, err := client.RegisterAccount(ctx, address)
	suite.Require().Error(err)
	suite.Require().ErrorContains(err, "Account is already registered")
}
