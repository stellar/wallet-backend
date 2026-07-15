package services

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/data"
	"github.com/stellar/wallet-backend/internal/indexer/types"
)

func withCleanValidatorRegistry(t *testing.T) {
	t.Helper()
	original := validatorRegistry
	validatorRegistry = map[string]func(ProtocolDeps) ProtocolValidator{}
	t.Cleanup(func() { validatorRegistry = original })
}

// idValidator is a minimal ProtocolValidator that reports a configurable
// ProtocolID, used to distinguish factories; the package has no generated
// ProtocolValidatorMock.
type idValidator struct{ id string }

func (v idValidator) ProtocolID() string                                 { return v.id }
func (v idValidator) Match([]WasmCandidate) map[types.HashBytea]struct{} { return nil }
func (v idValidator) Prefetch(context.Context, RPCService, []WasmCandidate, map[types.HashBytea]struct{}, []ContractCandidate) (any, error) {
	return nil, nil
}

func (v idValidator) Apply(context.Context, pgx.Tx, map[types.HashBytea]struct{}, []ContractCandidate, any, *data.Models) error {
	return nil
}

func TestRegisterValidator(t *testing.T) {
	t.Run("register and retrieve", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		called := false
		RegisterValidator("TEST", func(ProtocolDeps) ProtocolValidator {
			called = true
			return nil
		})

		factory, ok := GetValidator("TEST")
		require.True(t, ok)
		factory(ProtocolDeps{})
		assert.True(t, called)
	})

	t.Run("unknown protocol returns false", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		factory, ok := GetValidator("NONEXISTENT")
		assert.False(t, ok)
		assert.Nil(t, factory)
	})

	t.Run("re-register overwrites previous factory", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		RegisterValidator("DUP", func(ProtocolDeps) ProtocolValidator { return nil })
		RegisterValidator("DUP", func(ProtocolDeps) ProtocolValidator { return idValidator{id: "DUP"} })

		factory, ok := GetValidator("DUP")
		require.True(t, ok)
		v := factory(ProtocolDeps{})
		assert.Equal(t, "DUP", v.ProtocolID())
	})

	t.Run("GetAllValidatorIDs returns sorted protocol IDs", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		RegisterValidator("B", func(ProtocolDeps) ProtocolValidator { return nil })
		RegisterValidator("A", func(ProtocolDeps) ProtocolValidator { return nil })

		ids := GetAllValidatorIDs()
		assert.Equal(t, []string{"A", "B"}, ids)
	})
}

func TestBuildValidators(t *testing.T) {
	t.Run("happy path materializes validators in input order", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		RegisterValidator("A", func(ProtocolDeps) ProtocolValidator { return idValidator{id: "A"} })
		RegisterValidator("B", func(ProtocolDeps) ProtocolValidator { return idValidator{id: "B"} })

		validators, err := BuildValidators(ProtocolDeps{}, []string{"B", "A"})
		require.NoError(t, err)
		require.Len(t, validators, 2)
		assert.Equal(t, "B", validators[0].ProtocolID())
		assert.Equal(t, "A", validators[1].ProtocolID())
	})

	t.Run("error when a requested ID has no registered factory", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		RegisterValidator("A", func(ProtocolDeps) ProtocolValidator { return idValidator{id: "A"} })

		validators, err := BuildValidators(ProtocolDeps{}, []string{"A", "MISSING"})
		require.Error(t, err)
		assert.Nil(t, validators)
	})

	t.Run("error when a factory returns nil", func(t *testing.T) {
		withCleanValidatorRegistry(t)

		RegisterValidator("NIL", func(ProtocolDeps) ProtocolValidator { return nil })

		validators, err := BuildValidators(ProtocolDeps{}, []string{"NIL"})
		require.Error(t, err)
		assert.Nil(t, validators)
	})
}
