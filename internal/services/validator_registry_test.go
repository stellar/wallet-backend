package services

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withCleanRegistry(t *testing.T) {
	t.Helper()
	original := validatorRegistry
	validatorRegistry = map[string]func() ProtocolValidator{}
	t.Cleanup(func() { validatorRegistry = original })
}

func TestRegisterValidator(t *testing.T) {
	t.Run("register and retrieve", func(t *testing.T) {
		withCleanRegistry(t)

		called := false
		RegisterValidator("TEST", func() ProtocolValidator {
			called = true
			return nil
		})

		factory, ok := GetValidator("TEST")
		require.True(t, ok)
		factory()
		assert.True(t, called)
	})

	t.Run("unknown protocol returns false", func(t *testing.T) {
		withCleanRegistry(t)

		factory, ok := GetValidator("NONEXISTENT")
		assert.False(t, ok)
		assert.Nil(t, factory)
	})

	t.Run("re-register overwrites previous factory", func(t *testing.T) {
		withCleanRegistry(t)

		RegisterValidator("DUP", func() ProtocolValidator { return nil })

		mock := NewProtocolValidatorMock(t)
		mock.On("ProtocolID").Return("DUP")
		RegisterValidator("DUP", func() ProtocolValidator { return mock })

		factory, ok := GetValidator("DUP")
		require.True(t, ok)
		v := factory()
		assert.Equal(t, "DUP", v.ProtocolID())
	})
}
