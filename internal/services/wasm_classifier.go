package services

import (
	"context"
	"fmt"
	"sort"

	"github.com/stellar/wallet-backend/internal/indexer/processors"
)

// NewLiveWasmClassifier returns a processors.WasmClassifier backed by the given
// validators + extractor. Pass services.GetAllValidators() and
// services.NewWasmSpecExtractor() at indexer startup. Callers own the
// extractor's lifecycle (Close it on shutdown).
//
// Validators are tried in protocol-id order for deterministic match results.
// The first matching validator wins; if no validator matches, an empty
// protocol id is returned (the processor records the WASM with protocol_id =
// NULL, which is the same outcome a future protocol-setup run will see).
func NewLiveWasmClassifier(validators []ProtocolValidator, extractor WasmSpecExtractor) processors.WasmClassifier {
	sorted := append([]ProtocolValidator(nil), validators...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ProtocolID() < sorted[j].ProtocolID() })
	return &liveWasmClassifier{validators: sorted, extractor: extractor}
}

type liveWasmClassifier struct {
	validators []ProtocolValidator
	extractor  WasmSpecExtractor
}

func (c *liveWasmClassifier) Classify(ctx context.Context, wasmCode []byte) (string, error) {
	if c.extractor == nil || len(c.validators) == 0 {
		return "", nil
	}
	specs, err := c.extractor.ExtractSpec(ctx, wasmCode)
	if err != nil {
		return "", fmt.Errorf("extracting spec: %w", err)
	}
	for _, v := range c.validators {
		if v.Validate(specs) {
			return v.ProtocolID(), nil
		}
	}
	return "", nil
}
