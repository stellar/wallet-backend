package services

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// stubProcessor is a minimal ProtocolProcessor for BuildProcessors validation
// tests: it carries an ID and a state_change_id base and no-ops everything else.
type stubProcessor struct {
	ProtocolProcessor // embed the interface; only the methods below are exercised
	id                string
	base              int64
}

func (s stubProcessor) ProtocolID() string            { return s.id }
func (s stubProcessor) StateChangeOrdinalBase() int64 { return s.base }

func registerStub(id string, base int64) {
	RegisterProcessor(id, func(ProtocolDeps) ProtocolProcessor {
		return stubProcessor{id: id, base: base}
	})
}

func withCleanProcessorRegistry(t *testing.T) {
	t.Helper()
	original := processorRegistry
	processorRegistry = map[string]func(ProtocolDeps) ProtocolProcessor{}
	t.Cleanup(func() { processorRegistry = original })
}

func TestRegisterProcessor(t *testing.T) {
	t.Run("register and retrieve", func(t *testing.T) {
		withCleanProcessorRegistry(t)

		called := false
		RegisterProcessor("TEST", func(ProtocolDeps) ProtocolProcessor {
			called = true
			return nil
		})

		factory, ok := GetProcessor("TEST")
		require.True(t, ok)
		factory(ProtocolDeps{})
		assert.True(t, called)
	})

	t.Run("unknown protocol returns false", func(t *testing.T) {
		withCleanProcessorRegistry(t)

		factory, ok := GetProcessor("NONEXISTENT")
		assert.False(t, ok)
		assert.Nil(t, factory)
	})

	t.Run("re-register overwrites previous factory", func(t *testing.T) {
		withCleanProcessorRegistry(t)

		RegisterProcessor("DUP", func(ProtocolDeps) ProtocolProcessor { return nil })

		mock := NewProtocolProcessorMock(t)
		mock.On("ProtocolID").Return("DUP")
		RegisterProcessor("DUP", func(ProtocolDeps) ProtocolProcessor { return mock })

		factory, ok := GetProcessor("DUP")
		require.True(t, ok)
		p := factory(ProtocolDeps{})
		assert.Equal(t, "DUP", p.ProtocolID())
	})

	t.Run("GetAllProcessorIDs returns sorted protocol IDs", func(t *testing.T) {
		withCleanProcessorRegistry(t)

		RegisterProcessor("B", func(ProtocolDeps) ProtocolProcessor { return nil })
		RegisterProcessor("A", func(ProtocolDeps) ProtocolProcessor { return nil })

		ids := GetAllProcessorIDs()
		assert.Equal(t, []string{"A", "B"}, ids)
	})
}

func TestBuildProcessorsBaseValidation(t *testing.T) {
	w := types.StateChangeOrdinalNamespaceWidth

	t.Run("distinct valid bases succeed", func(t *testing.T) {
		withCleanProcessorRegistry(t)
		registerStub("A", 1*w)
		registerStub("B", 2*w)

		procs, err := BuildProcessors(ProtocolDeps{}, []string{"A", "B"})
		require.NoError(t, err)
		assert.Len(t, procs, 2)
	})

	t.Run("duplicate bases error names both protocols", func(t *testing.T) {
		withCleanProcessorRegistry(t)
		registerStub("A", 3*w)
		registerStub("B", 3*w)

		_, err := BuildProcessors(ProtocolDeps{}, []string{"A", "B"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "A")
		assert.Contains(t, err.Error(), "B")
	})

	t.Run("zero base errors", func(t *testing.T) {
		withCleanProcessorRegistry(t)
		registerStub("A", 0)

		_, err := BuildProcessors(ProtocolDeps{}, []string{"A"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "A")
	})

	t.Run("non-multiple base errors", func(t *testing.T) {
		withCleanProcessorRegistry(t)
		registerStub("A", w+1)

		_, err := BuildProcessors(ProtocolDeps{}, []string{"A"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "A")
	})
}
