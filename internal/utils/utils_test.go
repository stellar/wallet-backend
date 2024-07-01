package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeUTF8(t *testing.T) {
	t.Run("non_utf8", func(t *testing.T) {
		result := SanitizeUTF8("test\xF5")
		assert.Equal(t, "test?", result)
	})

	t.Run("zero_byte", func(t *testing.T) {
		result := SanitizeUTF8("test\x00\x00")
		assert.Equal(t, "test", result)
	})
}

func TestUnwrapInterfaceToPointer(t *testing.T) {
	// Test with a string
	strValue := "test"
	strValuePtr := &strValue
	i := interface{}(strValuePtr)

	unwrappedValue := UnwrapInterfaceToPointer[string](i)
	assert.Equal(t, "test", *unwrappedValue)

	// Test with a struct
	type testStruct struct {
		Name string
	}
	testStructValue := testStruct{Name: "test"}
	testStructValuePtr := &testStructValue
	i = interface{}(testStructValuePtr)
	assert.Equal(t, testStruct{Name: "test"}, *UnwrapInterfaceToPointer[testStruct](i))
}

func TestPointOf(t *testing.T) {
	// Test with a string
	strPointer := PointOf("test")
	assert.Equal(t, "test", *strPointer)

	// Test with a struct
	type testStruct struct {
		Name string
	}
	structPointer := PointOf(testStruct{Name: "test"})
	assert.Equal(t, testStruct{Name: "test"}, *structPointer)
}

func TestIsEmpty(t *testing.T) {
	type testCase struct {
		name      string
		isEmptyFn func() bool
		expected  bool
	}

	// testStruct is used just for testing empty and non empty structs.
	type testStruct struct{ Name string }

	// Define test cases
	testCases := []testCase{
		// String
		{name: "String empty", isEmptyFn: func() bool { return IsEmpty[string]("") }, expected: true},
		{name: "String non-empty", isEmptyFn: func() bool { return IsEmpty[string]("not empty") }, expected: false},
		// Int
		{name: "Int zero", isEmptyFn: func() bool { return IsEmpty[int](0) }, expected: true},
		{name: "Int non-zero", isEmptyFn: func() bool { return IsEmpty[int](1) }, expected: false},
		// Slice:
		{name: "Slice nil", isEmptyFn: func() bool { return IsEmpty[[]string](nil) }, expected: true},
		{name: "Slice empty", isEmptyFn: func() bool { return IsEmpty[[]string]([]string{}) }, expected: false},
		{name: "Slice non-empty", isEmptyFn: func() bool { return IsEmpty[[]string]([]string{"not empty"}) }, expected: false},
		// Struct:
		{name: "Struct zero", isEmptyFn: func() bool { return IsEmpty[testStruct](testStruct{}) }, expected: true},
		{name: "Struct non-zero", isEmptyFn: func() bool { return IsEmpty[testStruct](testStruct{Name: "not empty"}) }, expected: false},
		// Pointer:
		{name: "Pointer nil", isEmptyFn: func() bool { return IsEmpty[*string](nil) }, expected: true},
		{name: "Pointer non-nil", isEmptyFn: func() bool { return IsEmpty[*string](new(string)) }, expected: false},
		// Function:
		{name: "Function nil", isEmptyFn: func() bool { return IsEmpty[func() string](nil) }, expected: true},
		{name: "Function non-nil", isEmptyFn: func() bool { return IsEmpty[func() string](func() string { return "not empty" }) }, expected: false},
		// Interface:
		{name: "Interface nil", isEmptyFn: func() bool { return IsEmpty[interface{}](nil) }, expected: true},
		{name: "Interface non-nil", isEmptyFn: func() bool { return IsEmpty[interface{}](new(string)) }, expected: false},
		// Map:
		{name: "Map nil", isEmptyFn: func() bool { return IsEmpty[map[string]string](nil) }, expected: true},
		{name: "Map empty", isEmptyFn: func() bool { return IsEmpty[map[string]string](map[string]string{}) }, expected: false},
		{name: "Map non-empty", isEmptyFn: func() bool { return IsEmpty[map[string]string](map[string]string{"not empty": "not empty"}) }, expected: false},
		// Channel:
		{name: "Channel nil", isEmptyFn: func() bool { return IsEmpty[chan string](nil) }, expected: true},
		{name: "Channel non-nil", isEmptyFn: func() bool { return IsEmpty[chan string](make(chan string)) }, expected: false},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.isEmptyFn())
		})
	}
}
