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

// recordingValidator captures the inputs each Validate call observed and
// returns a configurable set of matches.
type recordingValidator struct {
	mu        sync.Mutex
	id        string
	matches   map[types.HashBytea]struct{}
	err       error
	calls     int
	lastInput ValidationInput
}

func newRecordingValidator(id string, matches ...types.HashBytea) *recordingValidator {
	c := &recordingValidator{id: id, matches: map[types.HashBytea]struct{}{}}
	for _, h := range matches {
		c.matches[h] = struct{}{}
	}
	return c
}

func (r *recordingValidator) ProtocolID() string { return r.id }

func (r *recordingValidator) Validate(_ context.Context, _ pgx.Tx, input ValidationInput) (ValidationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastInput = input
	if r.err != nil {
		return ValidationResult{}, r.err
	}
	out := ValidationResult{}
	for _, c := range input.Candidates {
		if _, ok := r.matches[c.Hash]; ok {
			out.MatchedWasms = append(out.MatchedWasms, c.Hash)
		}
	}
	return out, nil
}

func TestDispatchClassification_FirstMatchWins(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	cA := newRecordingValidator("A", hash)
	cB := newRecordingValidator("B", hash)

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

	matches, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{cA, cB},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "A", matches[hash])

	// B saw filtered candidates (none, since A claimed the only one).
	assert.Equal(t, 1, cA.calls)
	// B is not called when there are no remaining candidates AND no contracts.
	assert.Equal(t, 0, cB.calls)
}

func TestDispatchClassification_NoMatchLeavesEmptyMap(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	c := newRecordingValidator("A") // claims nothing

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

	matches, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{c},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Empty(t, matches)
	assert.Equal(t, 1, c.calls)
}

func TestDispatchClassification_SpecExtractionFailureKeepsRow(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	c := newRecordingValidator("A")

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return(nil, errors.New("compile fail")).Once()

	matches, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{c},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	assert.Empty(t, matches)
	// Candidate is still passed to the validator (with empty SpecEntries) so it
	// can decide; the SEP-41 validator short-circuits on len(SpecEntries)==0.
	assert.Equal(t, 1, c.calls)
}

func TestDispatchClassification_ValidatorErrorAbortsDispatch(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	cBoom := newRecordingValidator("A")
	cBoom.err = errors.New("boom")
	cOK := newRecordingValidator("B", hash)

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

	matches, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{cBoom, cOK},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil, nil,
	)
	require.Error(t, err)
	assert.Nil(t, matches)
	assert.Equal(t, 0, cOK.calls, "later validators must not run after one aborts")
}

func TestDispatchClassification_NoCandidatesNoContractsReturnsNil(t *testing.T) {
	ctx := context.Background()
	c := newRecordingValidator("A")
	matches, err := DispatchClassification(ctx, nil, nil, []ProtocolValidator{c}, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, matches)
	assert.Equal(t, 0, c.calls)
}

func TestDispatchClassification_KnownProtocolAnnotationReachesValidator(t *testing.T) {
	ctx := context.Background()
	wasmHash := types.HashBytea("ccdd")
	contractID := types.HashBytea("11")
	cA := newRecordingValidator("A")

	matches, err := DispatchClassification(
		ctx, nil, nil,
		[]ProtocolValidator{cA},
		nil, // no in-batch candidates
		[]data.ProtocolContracts{{ContractID: contractID, WasmHash: wasmHash}},
		nil, nil,
		map[types.HashBytea]string{wasmHash: "A"},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, matches)
	require.Equal(t, 1, cA.calls)
	require.Len(t, cA.lastInput.Contracts, 1)
	assert.Equal(t, "A", cA.lastInput.Contracts[0].KnownProtocolID)
}

func newFailureCounterForTest() *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_wasm_classification_failures_total"},
		[]string{"protocol_id", "reason"},
	)
}

func TestDispatchClassification_SpecExtractionFailureIncrementsCounter(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	c := newRecordingValidator("A")

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return(nil, errors.New("compile fail")).Once()

	counter := newFailureCounterForTest()

	_, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{c},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil,
		counter,
	)
	require.NoError(t, err)
	assert.Equal(t, 1.0, testutil.ToFloat64(counter.WithLabelValues("unknown", "spec_extraction_error")))
	assert.Equal(t, 0.0, testutil.ToFloat64(counter.WithLabelValues("A", "validate_error")))
}

func TestDispatchClassification_ValidatorErrorIncrementsCounter(t *testing.T) {
	ctx := context.Background()
	hash := types.HashBytea("aabb")
	cBoom := newRecordingValidator("sep41")
	cBoom.err = errors.New("boom")

	extractor := NewWasmSpecExtractorMock(t)
	extractor.On("ExtractSpec", mock.Anything, mock.Anything).Return([]xdr.ScSpecEntry{{}}, nil).Once()

	counter := newFailureCounterForTest()

	_, err := DispatchClassification(
		ctx, nil, extractor,
		[]ProtocolValidator{cBoom},
		map[types.HashBytea][]byte{hash: {1, 2, 3}},
		nil, nil, nil, nil,
		counter,
	)
	require.Error(t, err)
	assert.Equal(t, 1.0, testutil.ToFloat64(counter.WithLabelValues("sep41", "validate_error")))
	assert.Equal(t, 0.0, testutil.ToFloat64(counter.WithLabelValues("unknown", "spec_extraction_error")))
}
