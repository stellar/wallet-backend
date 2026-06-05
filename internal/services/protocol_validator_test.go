package services

import (
	"bytes"
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wasmHeader is a minimal valid WebAssembly module: magic bytes + version 1.
var wasmHeader = []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00}

// encodeUvarint encodes a uint32 as LEB128 as used by the WebAssembly binary format.
func encodeUvarint(v uint32) []byte {
	out := make([]byte, 0, 5)
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v == 0 {
			out = append(out, b)
			return out
		}
		out = append(out, b|0x80)
	}
}

// buildWasmWithCustomSection returns a valid WASM module containing a single
// custom section with the given name and raw payload.
func buildWasmWithCustomSection(name string, payload []byte) []byte {
	nameLen := encodeUvarint(uint32(len(name)))
	content := append(append(nameLen, []byte(name)...), payload...)
	sectionSize := encodeUvarint(uint32(len(content)))

	mod := append([]byte{}, wasmHeader...)
	mod = append(mod, 0x00) // custom section id
	mod = append(mod, sectionSize...)
	mod = append(mod, content...)
	return mod
}

// marshalSpecEntries marshals entries to their concatenated XDR byte form.
func marshalSpecEntries(t *testing.T, entries []xdr.ScSpecEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	for i := range entries {
		_, err := xdr.Marshal(&buf, &entries[i])
		require.NoError(t, err)
	}
	return buf.Bytes()
}

// These tests cover the hand-crafted WASM paths that the real-contract tests
// in wasm_spec_extractor_test.go can't exercise (missing section, section
// exceeding maxSpecEntries, invalid XDR, invalid WASM header).
func TestWasmSpecExtractor_ExtractSpec(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects module without contractspecv0 section", func(t *testing.T) {
		ex := NewWasmSpecExtractor()
		t.Cleanup(func() { _ = ex.Close(ctx) })

		// Valid WASM with an unrelated custom section.
		mod := buildWasmWithCustomSection("unrelated", []byte{0x01, 0x02, 0x03})
		_, err := ex.ExtractSpec(ctx, mod)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "contractspecv0 section not found")
	})

	t.Run("rejects invalid WASM bytes", func(t *testing.T) {
		ex := NewWasmSpecExtractor()
		t.Cleanup(func() { _ = ex.Close(ctx) })

		_, err := ex.ExtractSpec(ctx, []byte{0xDE, 0xAD, 0xBE, 0xEF})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "compiling WASM module")
	})

	t.Run("extracts valid spec entries", func(t *testing.T) {
		ex := NewWasmSpecExtractor()
		t.Cleanup(func() { _ = ex.Close(ctx) })

		specs := []xdr.ScSpecEntry{
			{
				Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
				FunctionV0: &xdr.ScSpecFunctionV0{Name: xdr.ScSymbol("balance")},
			},
			{
				Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
				FunctionV0: &xdr.ScSpecFunctionV0{Name: xdr.ScSymbol("transfer")},
			},
		}
		payload := marshalSpecEntries(t, specs)
		mod := buildWasmWithCustomSection(contractSpecV0SectionName, payload)

		out, err := ex.ExtractSpec(ctx, mod)
		require.NoError(t, err)
		assert.Equal(t, len(specs), len(out))
	})

	t.Run("rejects sections with more than maxSpecEntries entries", func(t *testing.T) {
		ex := NewWasmSpecExtractor()
		t.Cleanup(func() { _ = ex.Close(ctx) })

		// Marshal a minimal entry once, then concatenate maxSpecEntries+1 copies.
		// Each repeated block is itself valid XDR for a FunctionV0 entry so the
		// unmarshal loop runs until the cap check trips.
		single := xdr.ScSpecEntry{
			Kind:       xdr.ScSpecEntryKindScSpecEntryFunctionV0,
			FunctionV0: &xdr.ScSpecFunctionV0{Name: xdr.ScSymbol("f")},
		}
		oneEntry := marshalSpecEntries(t, []xdr.ScSpecEntry{single})
		require.NotEmpty(t, oneEntry)

		payload := bytes.Repeat(oneEntry, maxSpecEntries+1)
		mod := buildWasmWithCustomSection(contractSpecV0SectionName, payload)
		require.LessOrEqual(t, len(mod), maxWasmBytes, "test payload must fit within maxWasmBytes")

		_, err := ex.ExtractSpec(ctx, mod)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "more than")
	})

	t.Run("rejects invalid XDR in contractspecv0 section", func(t *testing.T) {
		ex := NewWasmSpecExtractor()
		t.Cleanup(func() { _ = ex.Close(ctx) })

		// Kind discriminator 0xFFFFFFFF is not a defined ScSpecEntryKind,
		// so xdr.Unmarshal fails on the first entry.
		mod := buildWasmWithCustomSection(contractSpecV0SectionName, []byte{0xFF, 0xFF, 0xFF, 0xFF})
		_, err := ex.ExtractSpec(ctx, mod)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshaling spec entry")
	})
}
