package services

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withCleanProcessorRegistry(t *testing.T) {
	t.Helper()
	processorRegistryMu.RLock()
	original := processorRegistry
	processorRegistryMu.RUnlock()
	resetProcessorRegistry(map[string]func(ProtocolDeps) ProtocolProcessor{})
	t.Cleanup(func() { resetProcessorRegistry(original) })
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

	t.Run("concurrent register and get does not race", func(t *testing.T) {
		withCleanProcessorRegistry(t)

		const n = 50
		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := range n {
			id := fmt.Sprintf("PROTO_%d", i)
			go func() {
				defer wg.Done()
				RegisterProcessor(id, func(ProtocolDeps) ProtocolProcessor { return nil })
			}()
			go func() {
				defer wg.Done()
				GetProcessor(id)
			}()
		}

		wg.Wait()
	})
}
