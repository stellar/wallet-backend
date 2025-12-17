package infrastructure

import (
	"context"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/require"
)

// CreateAndFundAccounts creates and funds multiple accounts in a single transaction using the master account
func (s *SharedContainers) CreateAndFundAccounts(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
	// Build CreateAccount operations for all accounts
	ops := make([]txnbuild.Operation, len(accounts))
	for i, kp := range accounts {
		ops[i] = &txnbuild.CreateAccount{
			Destination:   kp.Address(),
			Amount:        DefaultFundingAmount, // Fund each with 10,000 XLM
			SourceAccount: s.masterKeyPair.Address(),
		}
	}

	// Execute the account creation operations
	_, err := executeClassicOperation(ctx, t, s, ops, []*keypair.Full{s.masterKeyPair})
	require.NoError(t, err, "failed to create and fund accounts")

	// Log funded accounts
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("💰 Funded account: %s", kp.Address())
	}
}

// createUSDCTrustlines creates test trustlines for balance test accounts
func (s *SharedContainers) createUSDCTrustlines(ctx context.Context, t *testing.T, accounts []*keypair.Full) {
	// Create test asset issued by master account
	testAsset := txnbuild.CreditAsset{
		Code:   "USDC",
		Issuer: s.masterKeyPair.Address(),
	}
	// Convert to XDR Asset to get contract ID
	xdrAsset, err := testAsset.ToXDR()
	require.NoError(t, err)
	contractID, err := xdrAsset.ContractID(networkPassphrase)
	require.NoError(t, err)
	s.usdcContractAddress = strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Deploy the SAC for the USDC credit asset
	s.deployCreditAssetSAC(ctx, t, "USDC", s.masterKeyPair.Address())
	log.Ctx(ctx).Infof("✅ Deployed USDC SAC at address: %s", s.usdcContractAddress)

	// Build ChangeTrust operations for all accounts
	ops := make([]txnbuild.Operation, 0)
	for _, kp := range accounts {
		ops = append(ops, &txnbuild.ChangeTrust{
			Line: txnbuild.ChangeTrustAssetWrapper{
				Asset: testAsset,
			},
			Limit:         DefaultTrustlineLimit, // 1 million units
			SourceAccount: kp.Address(),
		}, &txnbuild.Payment{
			Destination:   kp.Address(),
			Amount:        TestUSDCPaymentAmount,
			Asset:         testAsset,
			SourceAccount: s.masterKeyPair.Address(),
		})
	}

	// Build signers list: master key + all account keys
	signers := make([]*keypair.Full, 0, len(accounts)+1)
	signers = append(signers, s.masterKeyPair)
	signers = append(signers, accounts...)

	// Execute the trustline creation operations
	_, err = executeClassicOperation(ctx, t, s, ops, signers)
	require.NoError(t, err, "failed to create USDC trustlines")

	// Log created trustlines
	for _, kp := range accounts {
		log.Ctx(ctx).Infof("🔗 Created balance test trustline for account %s: %s:%s", kp.Address(), testAsset.Code, testAsset.Issuer)
	}
}

// createEURCTrustlines creates EURC trustline for balance test account 1 only
func (s *SharedContainers) createEURCTrustlines(ctx context.Context, t *testing.T) {
	// Create EURC asset issued by master account
	eurcAsset := txnbuild.CreditAsset{
		Code:   "EURC",
		Issuer: s.masterKeyPair.Address(),
	}
	// Convert to XDR Asset to get contract ID
	xdrAsset, err := eurcAsset.ToXDR()
	require.NoError(t, err)
	contractID, err := xdrAsset.ContractID(networkPassphrase)
	require.NoError(t, err)
	s.eurcContractAddress = strkey.MustEncode(strkey.VersionByteContract, contractID[:])

	// Deploy the SAC for the EURC credit asset
	s.deployCreditAssetSAC(ctx, t, "EURC", s.masterKeyPair.Address())
	log.Ctx(ctx).Infof("✅ Deployed EURC SAC at address: %s", s.eurcContractAddress)

	// Build ChangeTrust operation for balance test account 1 only
	ops := []txnbuild.Operation{
		&txnbuild.ChangeTrust{
			Line: txnbuild.ChangeTrustAssetWrapper{
				Asset: eurcAsset,
			},
			Limit:         DefaultTrustlineLimit, // 1 million units
			SourceAccount: s.balanceTestAccount1KeyPair.Address(),
		},
		&txnbuild.Payment{
			Destination:   s.balanceTestAccount1KeyPair.Address(),
			Amount:        TestUSDCPaymentAmount,
			Asset:         eurcAsset,
			SourceAccount: s.masterKeyPair.Address(),
		},
	}

	// Execute the trustline creation operations
	_, err = executeClassicOperation(ctx, t, s, ops, []*keypair.Full{s.masterKeyPair, s.balanceTestAccount1KeyPair})
	require.NoError(t, err, "failed to create EURC trustline")

	log.Ctx(ctx).Infof("🔗 Created EURC trustline for balance test account 1 %s: %s:%s", s.balanceTestAccount1KeyPair.Address(), eurcAsset.Code, eurcAsset.Issuer)
}

// SubmitPaymentOp executes a native XLM payment from the master account to a destination
func (s *SharedContainers) SubmitPaymentOp(ctx context.Context, t *testing.T, to string, amount string) {
	ops := []txnbuild.Operation{
		&txnbuild.Payment{
			Destination:   to,
			Amount:        amount,
			Asset:         txnbuild.NativeAsset{},
			SourceAccount: s.masterKeyPair.Address(),
		},
	}

	_, err := executeClassicOperation(ctx, t, s, ops, []*keypair.Full{s.masterKeyPair})
	require.NoError(t, err, "failed to execute payment")

	log.Ctx(ctx).Infof("💸 Payment of %s XLM from %s to %s", amount, s.masterKeyPair.Address(), to)
}
