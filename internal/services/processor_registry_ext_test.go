package services_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
	"github.com/stellar/wallet-backend/internal/services"

	// Blank-import every protocol package so its init() registers with the
	// framework. This test lives in the external services_test package to avoid
	// the import cycle a protocol package (which imports services) would create
	// with an in-package test. Add new protocol packages here as they land so
	// BuildProcessors validates their state_change_id bases in CI automatically.
	_ "github.com/stellar/wallet-backend/internal/services/sep41"
)

// TestBuildProcessorsAllRegistered builds the full registered set with
// zero-value deps and asserts BuildProcessors accepts it: every base is a
// positive multiple of the namespace width and no two collide. Any future
// protocol whose factory tolerates zero-value deps is checked here for free.
func TestBuildProcessorsAllRegistered(t *testing.T) {
	ids := services.GetAllProcessorIDs()
	require.NotEmpty(t, ids, "expected at least one registered protocol processor")

	procs, err := services.BuildProcessors(services.ProtocolDeps{}, ids)
	require.NoError(t, err)
	assert.Len(t, procs, len(ids))

	seen := make(map[int64]string, len(procs))
	for _, p := range procs {
		base := p.StateChangeOrdinalBase()
		assert.Positive(t, base, "protocol %q base must be positive", p.ProtocolID())
		assert.Zero(t, base%types.StateChangeOrdinalNamespaceWidth,
			"protocol %q base %d must be a multiple of %d", p.ProtocolID(), base, types.StateChangeOrdinalNamespaceWidth)
		if other, dup := seen[base]; dup {
			t.Errorf("protocols %q and %q share base %d", other, p.ProtocolID(), base)
		}
		seen[base] = p.ProtocolID()
	}
}
