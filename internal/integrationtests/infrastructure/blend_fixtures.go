// Package infrastructure provides Soroban transaction helpers for integration tests.
//
// This file drives a deployed BlendStack (blend_setup.go) through the pool/backstop/emitter
// operations exercised by the protocol-migrate and live-ingestion integration test suites.
package infrastructure

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// Blend fixture amounts, named so test suites (a later task) can assert exact values rather than
// re-deriving them from the ops below. All reserve-token amounts are in stroops (7 decimals).
const (
	// Phase 1 — protocol-migrate coverage (history + current-state migration).
	BlendSupplierSupplyUSDC           = 10_000_000_000 // Supplier PoolSubmit: Supply USDC (1,000 USDC)
	BlendSupplierSupplyXLM            = 10_000_000_000 // Supplier PoolSubmit: Supply XLM, native (1,000 XLM; borrow liquidity)
	BlendSupplierSupplyCollateralUSDC = 5_000_000_000  // Supplier PoolSubmit: SupplyCollateral USDC (500 USDC)
	BlendBorrowerSupplyCollateralUSDC = 20_000_000_000 // Borrower PoolSubmit: SupplyCollateral USDC (2,000 USDC)
	BlendBorrowerBorrowXLM            = 3_000_000_000  // Borrower PoolSubmit: Borrow XLM (300 XLM)
	BlendBorrowerRepayXLM             = 1_000_000_000  // Borrower PoolSubmit: Repay XLM (100 XLM)

	// Phase 2 — live-ingestion coverage (withdrawals, backstop Q4W, emissions, liquidation).
	BlendSupplierWithdrawUSDC           = 2_000_000_000  // Supplier PoolSubmit: Withdraw USDC (200 USDC)
	BlendSupplierWithdrawCollateralUSDC = 1_000_000_000  // Supplier PoolSubmit: WithdrawCollateral USDC (100 USDC)
	BlendWhaleQ4WAmount                 = 50_000_000_000 // Whale BackstopQueueWithdrawal / BackstopDequeueWithdrawal (5,000 backstop shares)
	BlendFillerSupplyCollateralUSDC     = 30_000_000_000 // Filler PoolSubmit: SupplyCollateral USDC (3,000 USDC; auction headroom)
	BlendAuctionFillPercent             = 50             // Filler PoolSubmit: FillUserLiquidationAuction percent
	BlendAuctionRepayXLM                = 1_500_000_000  // Filler PoolSubmit: Repay XLM (150 XLM; absorbed debt)

	// blendAuctionCrashedUSDCPrice is USDC's oracle price ($0.005, 7 decimals) set right before
	// PoolNewAuction, tanking the borrower's USDC collateral value enough to become liquidatable.
	// The borrower must have liability_base > collateral_base: its liability is 200 XLM
	// ($0.10 * 200 / l_factor 0.9 = $22.2 base) against 2,000 USDC collateral, so USDC must fall
	// below ~$0.0117 ($22.2 / (2,000 * c_factor 0.95)); $0.005 leaves solid margin ($9.5 base)
	// while keeping the filler healthy after the fill (3,000 USDC * $0.005 * 0.95 = $14.25
	// effective collateral vs the $11.1 absorbed debt, which the Repay in the same submit clears).
	// XLM and BLND prices are left unchanged from SetupBlendStack's OracleSetPriceStable call.
	blendAuctionCrashedUSDCPrice = 50_000
	blendXLMPrice                = 1_000_000
	blendBLNDPrice               = 500_000
)

// SubmitBlendPhase1Ops executes the Blend operations covered by the protocol-migrate
// (history + current-state) integration path: a supplier depositing liquidity and collateral,
// and a borrower posting collateral, borrowing, and partially repaying.
func (s *SharedContainers) SubmitBlendPhase1Ops(ctx context.Context, t *testing.T, stack *BlendStack) {
	t.Helper()

	s.PoolSubmit(ctx, t, stack.PoolID, stack.Supplier, []BlendRequest{
		{RequestType: 0, Address: stack.USDCTokenID, Amount: big.NewInt(BlendSupplierSupplyUSDC)},
		{RequestType: 0, Address: stack.XLMTokenID, Amount: big.NewInt(BlendSupplierSupplyXLM)},
		{RequestType: 2, Address: stack.USDCTokenID, Amount: big.NewInt(BlendSupplierSupplyCollateralUSDC)},
	})
	log.Ctx(ctx).Info("✅ Blend phase 1: supplier supplied USDC/XLM and posted USDC collateral")

	s.PoolSubmit(ctx, t, stack.PoolID, stack.Borrower, []BlendRequest{
		{RequestType: 2, Address: stack.USDCTokenID, Amount: big.NewInt(BlendBorrowerSupplyCollateralUSDC)},
		{RequestType: 4, Address: stack.XLMTokenID, Amount: big.NewInt(BlendBorrowerBorrowXLM)},
		{RequestType: 5, Address: stack.XLMTokenID, Amount: big.NewInt(BlendBorrowerRepayXLM)},
	})
	log.Ctx(ctx).Info("✅ Blend phase 1: borrower posted USDC collateral, borrowed XLM, and partially repaid")
}

// SubmitBlendPhase2Ops executes the Blend operations covered by live ingestion: withdrawals,
// backstop queue/dequeue, emissions claims, and a liquidation. betweenQ4W, if non-nil, is called
// after the whale's backstop withdrawal is queued but before it is dequeued, so a caller can
// observe the queued (Q4W) state — e.g. via GraphQL — while it is still pending.
func (s *SharedContainers) SubmitBlendPhase2Ops(ctx context.Context, t *testing.T, stack *BlendStack, betweenQ4W func()) {
	t.Helper()

	s.PoolSubmit(ctx, t, stack.PoolID, stack.Supplier, []BlendRequest{
		{RequestType: 1, Address: stack.USDCTokenID, Amount: big.NewInt(BlendSupplierWithdrawUSDC)},
		{RequestType: 3, Address: stack.USDCTokenID, Amount: big.NewInt(BlendSupplierWithdrawCollateralUSDC)},
	})
	log.Ctx(ctx).Info("✅ Blend phase 2: supplier withdrew USDC and USDC collateral")

	s.BackstopQueueWithdrawal(ctx, t, stack.BackstopID, stack.Whale, stack.PoolID, big.NewInt(BlendWhaleQ4WAmount))
	log.Ctx(ctx).Info("✅ Blend phase 2: whale queued a backstop withdrawal")
	if betweenQ4W != nil {
		betweenQ4W()
	}
	s.BackstopDequeueWithdrawal(ctx, t, stack.BackstopID, stack.Whale, stack.PoolID, big.NewInt(BlendWhaleQ4WAmount))
	log.Ctx(ctx).Info("✅ Blend phase 2: whale dequeued the backstop withdrawal")

	s.submitBlendEmissions(ctx, t, stack)
	s.submitBlendLiquidation(ctx, t, stack)
}

// submitBlendEmissions distributes BLND emissions from the emitter to the backstop, gulps them
// into the pool, and lets both the supplier (pool emissions, via a bToken supply stream) and the
// whale (backstop emissions) claim their accrued share.
//
// The backstop's own emissions accounting (gulp_emissions on the backstop) is pool-authorized and
// invoked internally by the pool's gulp_emissions call — there is no separate backstop-side gulp
// wrapper to call here; PoolGulpEmissions is the only gulp the test needs to drive.
func (s *SharedContainers) submitBlendEmissions(ctx context.Context, t *testing.T, stack *BlendStack) {
	t.Helper()

	s.EmitterDistribute(ctx, t, stack.EmitterID, s.masterKeyPair)
	s.BackstopDistribute(ctx, t, stack.BackstopID, s.masterKeyPair) // first-ever call: initializes, returns 0

	time.Sleep(6 * time.Second)

	s.EmitterDistribute(ctx, t, stack.EmitterID, s.masterKeyPair)
	s.BackstopDistribute(ctx, t, stack.BackstopID, s.masterKeyPair) // now accrues since the first call

	s.PoolGulpEmissions(ctx, t, stack.PoolID, s.masterKeyPair)
	log.Ctx(ctx).Info("✅ Blend phase 2: distributed and gulped emissions")

	// Let per-user emissions accrue before claiming.
	time.Sleep(10 * time.Second)

	// Reserve token id = reserve_index*2 (+1 for the bToken/supply side): id 1 is USDC's (index
	// 0) bToken supply stream, which the supplier holds via its Supply + SupplyCollateral USDC
	// positions from phase 1.
	s.PoolClaim(ctx, t, stack.PoolID, stack.Supplier, []uint32{1})
	log.Ctx(ctx).Info("✅ Blend phase 2: supplier claimed pool emissions")

	s.BackstopClaim(ctx, t, stack.BackstopID, stack.Whale, []string{stack.PoolID}, big.NewInt(0))
	log.Ctx(ctx).Info("✅ Blend phase 2: whale claimed backstop emissions")
}

// submitBlendLiquidation crashes the USDC oracle price to make the borrower's phase-1 position
// liquidatable, opens a liquidation auction against it, waits for the auction's price to decay,
// and has the filler partially fill it (50%) while repaying part of the absorbed XLM debt.
func (s *SharedContainers) submitBlendLiquidation(ctx context.Context, t *testing.T, stack *BlendStack) {
	t.Helper()

	s.PoolSubmit(ctx, t, stack.PoolID, stack.Filler, []BlendRequest{
		{RequestType: 2, Address: stack.USDCTokenID, Amount: big.NewInt(BlendFillerSupplyCollateralUSDC)},
	})
	log.Ctx(ctx).Info("✅ Blend phase 2: filler posted USDC collateral (auction headroom)")

	s.OracleSetPriceStable(ctx, t, stack.OracleID, s.masterKeyPair, []*big.Int{
		big.NewInt(blendAuctionCrashedUSDCPrice), // USDC $1.00 -> $0.005
		big.NewInt(blendXLMPrice),                // XLM unchanged: $0.10
		big.NewInt(blendBLNDPrice),               // BLND unchanged: $0.05
	})
	log.Ctx(ctx).Info("✅ Blend phase 2: crashed USDC oracle price to make the borrower liquidatable")

	s.PoolNewAuction(ctx, t, stack.PoolID, stack.Filler, 0, stack.Borrower.Address(),
		[]string{stack.XLMTokenID}, []string{stack.USDCTokenID}, 100)
	log.Ctx(ctx).Info("✅ Blend phase 2: filler opened a liquidation auction against the borrower")

	// Let the auction's price decay for a few ledgers (~1s/ledger on the standalone network).
	time.Sleep(6 * time.Second)

	s.PoolSubmit(ctx, t, stack.PoolID, stack.Filler, []BlendRequest{
		{RequestType: 6, Address: stack.Borrower.Address(), Amount: big.NewInt(BlendAuctionFillPercent)},
		{RequestType: 5, Address: stack.XLMTokenID, Amount: big.NewInt(BlendAuctionRepayXLM)},
	})
	log.Ctx(ctx).Info("✅ Blend phase 2: filler filled 50% of the liquidation auction and repaid absorbed XLM debt")
}
