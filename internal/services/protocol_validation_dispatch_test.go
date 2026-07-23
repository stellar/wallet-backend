package services

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// recordingValidator captures the inputs each phase call observed and
// returns a configurable set of matches. Its Apply method signature has no
// RPCService parameter at all — the same compile-level guarantee production
// validators get from the real services.ProtocolValidator interface.
type recordingValidator struct {
	mu            sync.Mutex
	id            string
	matches       map[types.HashBytea]struct{}
	prefetchErr   error
	applyErr      error
	matchCalls    int
	prefetchCalls int
	applyCalls    int
	lastContracts []ContractCandidate
}

func newRecordingValidator(id string, matches ...types.HashBytea) *recordingValidator {
	c := &recordingValidator{id: id, matches: map[types.HashBytea]struct{}{}}
	for _, h := range matches {
		c.matches[h] = struct{}{}
	}
	return c
}

func (r *recordingValidator) ProtocolID() string { return r.id }

func (r *recordingValidator) Match(candidates []WasmCandidate) map[types.HashBytea]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.matchCalls++
	out := map[types.HashBytea]struct{}{}
	for _, c := range candidates {
		if _, ok := r.matches[c.Hash]; ok {
			out[c.Hash] = struct{}{}
		}
	}
	return out
}

func (r *recordingValidator) Prefetch(_ context.Context, _ RPCService, _ []WasmCandidate, _ map[types.HashBytea]struct{}, contracts []ContractCandidate) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prefetchCalls++
	r.lastContracts = contracts
	if r.prefetchErr != nil {
		return nil, r.prefetchErr
	}
	return nil, nil
}

func (r *recordingValidator) Apply(_ context.Context, _ pgx.Tx, _ map[types.HashBytea]struct{}, _ []ContractCandidate, _ any, _ *data.Models) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyCalls++
	return r.applyErr
}

func TestPrepareClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("nil extractor returns error", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		c := newRecordingValidator("A")

		plan, err := PrepareClassification(
			ctx, nil,
			[]ProtocolValidator{c},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.Error(t, err)
		assert.Nil(t, plan)
		assert.Equal(t, 0, c.matchCalls)
	})

	t.Run("no candidates and no contracts returns an empty plan", func(t *testing.T) {
		c := newRecordingValidator("A")
		extractor := NewWasmSpecExtractorMock(t)

		plan, err := PrepareClassification(ctx, extractor, []ProtocolValidator{c}, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Empty(t, plan.Matches)
		assert.Equal(t, 0, c.matchCalls)
	})

	t.Run("first match wins", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cA := newRecordingValidator("A", hash)
		cB := newRecordingValidator("B", hash)

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cA, cB},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "A", plan.Matches[hash])

		// B saw filtered candidates (none, since A claimed the only one) and,
		// with nothing left to do, is never called.
		assert.Equal(t, 1, cA.matchCalls)
		assert.Equal(t, 1, cA.prefetchCalls)
		assert.Equal(t, 0, cB.matchCalls)
		assert.Equal(t, 0, cB.prefetchCalls)
	})

	t.Run("no match leaves empty map", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		c := newRecordingValidator("A") // claims nothing

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{c},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)
		assert.Empty(t, plan.Matches)
		assert.Equal(t, 1, c.matchCalls)
		assert.Equal(t, 1, c.prefetchCalls)
	})

	t.Run("spec extraction failure drops candidate", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		c := newRecordingValidator("A")

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return(nil, errors.New("compile fail")).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{c},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)
		assert.Empty(t, plan.Matches)
		// A spec-less candidate cannot match any signature-based validator, so it is
		// dropped from the candidate set and never reaches the validator. The caller
		// persists the underlying wasm with protocol_id = NULL (absent from Matches).
		assert.Equal(t, 0, c.matchCalls)
	})

	t.Run("validator prefetch error aborts dispatch", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cBoom := newRecordingValidator("A", hash)
		cBoom.prefetchErr = errors.New("boom")
		cOK := newRecordingValidator("B")

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cBoom, cOK},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.Error(t, err)
		assert.Nil(t, plan)
		assert.Equal(t, 0, cOK.matchCalls, "later validators must not run after one aborts")
	})

	t.Run("known protocol annotation reaches validator and seeds Matches", func(t *testing.T) {
		wasmHash := types.HashBytea("ccdd")
		contractID := types.HashBytea("11")
		cA := newRecordingValidator("A")
		extractor := NewWasmSpecExtractorMock(t)

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cA},
			nil, // no in-batch candidates
			[]data.ProtocolContracts{{ContractID: contractID, WasmHash: wasmHash}},
			nil,
			map[types.HashBytea]string{wasmHash: "A"},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, "A", plan.Matches[wasmHash], "known classification is preserved in the plan")
		require.Equal(t, 1, cA.matchCalls)
		require.Len(t, cA.lastContracts, 1)
		assert.Equal(t, "A", cA.lastContracts[0].KnownProtocolID)
	})
}

func newFailureCounterForTest() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_wasm_classification_failures_total"},
		[]string{"protocol_id", "reason"},
	)
}

func TestPrepareClassification_FailureCounter(t *testing.T) {
	ctx := context.Background()

	t.Run("spec extraction failure increments counter", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		c := newRecordingValidator("A")

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return(nil, errors.New("compile fail")).Once()

		counter := newFailureCounterForTest()

		_, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{c},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil,
			counter,
		)
		require.NoError(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(counter.WithLabelValues("unknown", "spec_extraction_error")))
		assert.Equal(t, 0.0, testutil.ToFloat64(counter.WithLabelValues("A", "validate_error")))
	})

	t.Run("validator prefetch error increments counter", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cBoom := newRecordingValidator("sep41", hash)
		cBoom.prefetchErr = errors.New("boom")

		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		counter := newFailureCounterForTest()

		_, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cBoom},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil,
			counter,
		)
		require.Error(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(counter.WithLabelValues("sep41", "validate_error")))
		assert.Equal(t, 0.0, testutil.ToFloat64(counter.WithLabelValues("unknown", "spec_extraction_error")))
	})
}

// wantApplyClassificationPlanSignature pins the exact signature
// ApplyClassificationPlan must have; assigning the real function to this
// named type below is what makes TestApplyClassificationPlan_NoRPCService a
// compile-level check rather than an inferred-type no-op.
type wantApplyClassificationPlanSignature func(ctx context.Context, dbTx pgx.Tx, models *data.Models, plan *ClassificationPlan, failureCounter *prometheus.CounterVec) error

// TestApplyClassificationPlan_NoRPCService is the compile-level guarantee the
// RPC-out-of-transaction hardening rests on: ApplyClassificationPlan's
// signature has no RPCService (or anything RPC-capable) parameter, so it is
// impossible — not just unlikely — for a validator's Apply call reached from
// here to make a network call while dbTx is open. If this test compiles, the
// guarantee holds; nothing at runtime needs to prove it.
func TestApplyClassificationPlan_NoRPCService(t *testing.T) {
	var _ wantApplyClassificationPlanSignature = ApplyClassificationPlan
}

func TestApplyClassificationPlan(t *testing.T) {
	ctx := context.Background()

	t.Run("nil plan is a no-op", func(t *testing.T) {
		require.NoError(t, ApplyClassificationPlan(ctx, nil, nil, nil, nil))
	})

	t.Run("applies every validator in the plan using only prefetched data", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cA := newRecordingValidator("A", hash)
		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cA},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)

		require.NoError(t, ApplyClassificationPlan(ctx, nil, nil, plan, nil))
		assert.Equal(t, 1, cA.applyCalls)
	})

	t.Run("apply error is counted and propagated", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cBoom := newRecordingValidator("sep41", hash)
		cBoom.applyErr = errors.New("boom")
		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cBoom},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)

		counter := newFailureCounterForTest()
		err = ApplyClassificationPlan(ctx, nil, nil, plan, counter)
		require.Error(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(counter.WithLabelValues("sep41", "validate_error")))
	})

	t.Run("retrying Apply with the same plan does not re-invoke Prefetch", func(t *testing.T) {
		hash := types.HashBytea("aabb")
		cA := newRecordingValidator("A", hash)
		extractor := NewWasmSpecExtractorMock(t)
		extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

		plan, err := PrepareClassification(
			ctx, extractor,
			[]ProtocolValidator{cA},
			map[types.HashBytea][]byte{hash: {1, 2, 3}},
			nil, nil, nil, nil,
		)
		require.NoError(t, err)
		require.Equal(t, 1, cA.prefetchCalls)

		// Simulate ingestProcessedDataWithRetry's retry loop: the same plan is
		// applied across multiple attempts.
		for i := 0; i < 3; i++ {
			require.NoError(t, ApplyClassificationPlan(ctx, nil, nil, plan, nil))
		}
		assert.Equal(t, 3, cA.applyCalls)
		assert.Equal(t, 1, cA.prefetchCalls, "prefetch must not be re-issued on retry")
	})
}
