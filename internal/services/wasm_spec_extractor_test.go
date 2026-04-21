package services

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wasmTestdataDir returns the absolute path to the shared WASM testdata directory.
func wasmTestdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "integrationtests", "infrastructure", "testdata")
}

// loadTestWasm reads a WASM file from testdata and returns its bytes.
func loadTestWasm(t *testing.T, filename string) []byte {
	t.Helper()
	wasmBytes, err := os.ReadFile(filepath.Join(wasmTestdataDir(), filename))
	require.NoError(t, err, "reading test WASM file %s", filename)
	return wasmBytes
}

func TestWasmSpecExtractor_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	t.Run("token contract", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_token_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.NotEmpty(t, specs)

		hasFunctionEntry := false
		for _, spec := range specs {
			if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
				hasFunctionEntry = true
				break
			}
		}
		assert.True(t, hasFunctionEntry, "expected at least one ScSpecEntryFunctionV0 entry")
	})

	t.Run("increment contract", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)
		assert.NotEmpty(t, specs)

		hasFunctionEntry := false
		for _, spec := range specs {
			if spec.Kind == xdr.ScSpecEntryKindScSpecEntryFunctionV0 && spec.FunctionV0 != nil {
				hasFunctionEntry = true
				break
			}
		}
		assert.True(t, hasFunctionEntry, "expected at least one ScSpecEntryFunctionV0 entry")
	})
}

func TestWasmSpecExtractor_RejectsOversizedWasm(t *testing.T) {
	ctx := context.Background()
	extractor := NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	oversized := make([]byte, maxWasmBytes+1)
	specs, err := extractor.ExtractSpec(ctx, oversized)
	require.Error(t, err)
	assert.Nil(t, specs)
	assert.Contains(t, err.Error(), "too large")
}

func TestWasmSpecExtractor_HonorsCallerContextCancellation(t *testing.T) {
	extractor := NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(context.Background())) }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Any input under the size cap is fine; the extractor should short-circuit
	// because the caller's context is already done once we derive a child.
	specs, err := extractor.ExtractSpec(ctx, []byte("not a valid wasm"))
	require.Error(t, err)
	assert.Nil(t, specs)
}

func TestWasmSpecExtractor_RespectsWasmCompileTimeoutConstant(t *testing.T) {
	// Sanity check: the timeout constant should be small enough that tests
	// would notice regressions but large enough to compile real contracts on
	// slow CI machines.
	assert.Greater(t, wasmCompileTimeout, time.Second)
	assert.LessOrEqual(t, wasmCompileTimeout, 30*time.Second)
}

func TestSEP41ProtocolValidator_RealWasm(t *testing.T) {
	ctx := context.Background()
	extractor := NewWasmSpecExtractor()
	defer func() { require.NoError(t, extractor.Close(ctx)) }()

	validator := NewSEP41ProtocolValidator()

	t.Run("token contract validates as SEP-41", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_token_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.True(t, validator.Validate(specs), "token contract should validate as SEP-41")
		assert.Equal(t, "SEP41", validator.ProtocolID())
	})

	t.Run("increment contract does not validate as SEP-41", func(t *testing.T) {
		wasmBytes := loadTestWasm(t, "soroban_increment_contract.wasm")
		specs, err := extractor.ExtractSpec(ctx, wasmBytes)
		require.NoError(t, err)

		assert.False(t, validator.Validate(specs), "increment contract should not validate as SEP-41")
	})
}
