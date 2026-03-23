package services

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withCleanRegistry(t *testing.T) {
	t.Helper()
	registryMu.RLock()
	original := validatorRegistry
	registryMu.RUnlock()
	resetRegistry(map[string]ProtocolValidator{})
	t.Cleanup(func() { resetRegistry(original) })
}

func TestRegisterValidator(t *testing.T) {
	t.Run("register and retrieve", func(t *testing.T) {
		withCleanRegistry(t)

		mock := NewProtocolValidatorMock(t)
		mock.On("ProtocolID").Return("TEST")
		RegisterValidator("TEST", mock)

		v, ok := GetValidator("TEST")
		require.True(t, ok)
		assert.Equal(t, "TEST", v.ProtocolID())
	})

	t.Run("unknown protocol returns false", func(t *testing.T) {
		withCleanRegistry(t)

		v, ok := GetValidator("NONEXISTENT")
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("re-register overwrites previous instance", func(t *testing.T) {
		withCleanRegistry(t)

		first := NewProtocolValidatorMock(t)
		RegisterValidator("DUP", first)

		second := NewProtocolValidatorMock(t)
		second.On("ProtocolID").Return("DUP")
		RegisterValidator("DUP", second)

		v, ok := GetValidator("DUP")
		require.True(t, ok)
		assert.Equal(t, "DUP", v.ProtocolID())
	})

	t.Run("concurrent register and get does not race", func(t *testing.T) {
		withCleanRegistry(t)

		const n = 50
		var wg sync.WaitGroup
		wg.Add(n * 2)

		for i := range n {
			id := fmt.Sprintf("PROTO_%d", i)
			go func() {
				defer wg.Done()
				mock := NewProtocolValidatorMock(t)
				RegisterValidator(id, mock)
			}()
			go func() {
				defer wg.Done()
				GetValidator(id)
			}()
		}

		wg.Wait()
	})
}
