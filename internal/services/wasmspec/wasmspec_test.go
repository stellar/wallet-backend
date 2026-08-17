package wasmspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateFunctionInputsAndOutputs(t *testing.T) {
	t.Run("returns true for exact match", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})

	t.Run("returns false for too few inputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many inputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
			{Name: "amount", TypeName: "i128"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter name", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "sender", TypeName: "Address"},
			{Name: "receiver", TypeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong input parameter type", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "u32"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for reordered inputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "to", TypeName: "Address"},
			{Name: "from", TypeName: "Address"},
		}
		outputs := []string{"i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too few outputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		outputs := []string{}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for too many outputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		outputs := []string{"i128", "u32"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for wrong output type", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		outputs := []string{"u32"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns false for duplicate outputs", func(t *testing.T) {
		inputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		outputs := []string{"i128", "i128"}

		expectedInputs := []InputSpec{
			{Name: "from", TypeName: "Address"},
			{Name: "to", TypeName: "Address"},
		}
		expectedOutputs := []string{"i128"}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.False(t, result)
	})

	t.Run("returns true for empty inputs and outputs", func(t *testing.T) {
		inputs := []InputSpec{}
		outputs := []string{}

		expectedInputs := []InputSpec{}
		expectedOutputs := []string{}

		result := validateFunctionInputsAndOutputs(inputs, outputs, expectedInputs, expectedOutputs)
		assert.True(t, result)
	})
}
