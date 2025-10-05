package integrationtests

import (
	"github.com/stretchr/testify/suite"

	"github.com/stellar/wallet-backend/internal/integrationtests/infrastructure"
)

type AccountRegisterTestSuite struct {
	suite.Suite
	testEnv *infrastructure.TestEnvironment
}
