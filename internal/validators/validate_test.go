package validators

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseValidationError(t *testing.T) {
	type testStructNested struct {
		NestedRequiredField string `validate:"required"`
		NestedEnumField     string `validate:"oneof=foo bar"`
	}

	type testStruct struct {
		RequiredField      string             `validate:"required"`
		RequiredArrayField []string           `validate:"required,gt=0,dive,not_empty"`
		EnumField          string             `validate:"oneof=foo bar"`
		PublicKeyField     string             `validate:"public_key"`
		UnknownTagField    int                `validate:"lte=1"`
		NestedField        []testStructNested `validate:"dive"`
	}

	testCases := []struct {
		stc                 *testStruct
		expectedFieldErrors map[string]interface{}
	}{
		{
			stc: &testStruct{
				RequiredField:      "",
				RequiredArrayField: []string{},
				EnumField:          "invalid",
				PublicKeyField:     "invalid",
				UnknownTagField:    1,
			},
			expectedFieldErrors: map[string]interface{}{
				"requiredField":      "This field is required",
				"requiredArrayField": "Should have at least 1 element",
				"enumField":          `Unexpected value "invalid". Expected one of the following values: foo, bar`,
				"publicKeyField":     "Invalid public key provided",
			},
		},
		{
			stc: &testStruct{
				RequiredField:      "foo",
				RequiredArrayField: []string{"bar", ""},
				EnumField:          "bar",
				PublicKeyField:     keypair.MustRandom().Address(),
				UnknownTagField:    1,
			},
			expectedFieldErrors: map[string]interface{}{
				"requiredArrayField[1]": "This field cannot be empty e",
			},
		},
		{
			stc: &testStruct{
				RequiredField:      "foo",
				RequiredArrayField: []string{"bar"},
				EnumField:          "bar",
				PublicKeyField:     keypair.MustRandom().Address(),
				UnknownTagField:    2,
			},
			expectedFieldErrors: map[string]interface{}{
				"unknownTagField": "Invalid value",
			},
		},
		{
			stc: &testStruct{
				RequiredField:      "foo",
				RequiredArrayField: []string{"bar"},
				EnumField:          "bar",
				PublicKeyField:     keypair.MustRandom().Address(),
				UnknownTagField:    1,
				NestedField: []testStructNested{
					{
						NestedRequiredField: "",
						NestedEnumField:     "invalid",
					},
				},
			},
			expectedFieldErrors: map[string]interface{}{
				"nestedField[0].nestedRequiredField": "This field is required",
				"nestedField[0].nestedEnumField":     `Unexpected value "invalid". Expected one of the following values: foo, bar`,
			},
		},
	}

	val := NewValidator()
	for _, tc := range testCases {
		err := val.Struct(tc.stc)
		require.Error(t, err)
		vErrs, ok := err.(validator.ValidationErrors)
		require.True(t, ok)
		fieldErrors := ParseValidationError(vErrs)
		assert.Equal(t, tc.expectedFieldErrors, fieldErrors)
	}
}

func TestGetFieldName(t *testing.T) {
	type testStructNested struct {
		Name     string             `validate:"not_empty"`
		Children []testStructNested `validate:"dive"`
	}

	type testStruct struct {
		PublicKey   string             `validate:"public_key"`
		NestedField []testStructNested `validate:"required,dive"`
	}

	stc := &testStruct{
		PublicKey: "",
		NestedField: []testStructNested{
			{
				Name: "first",
				Children: []testStructNested{
					{
						Name: "second",
						Children: []testStructNested{
							{
								Name:     "children1",
								Children: []testStructNested{},
							},
							{
								Name: "children2",
								Children: []testStructNested{
									{
										Name:     "",
										Children: []testStructNested{},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	val := NewValidator()
	err := val.Struct(stc)
	require.Error(t, err)

	vErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok)
	require.Len(t, vErrs, 2)

	assert.Equal(t, "publicKey", getFieldName(vErrs[0]))
	assert.Equal(t, "children[0].name", getFieldName(vErrs[1]))
}

func TestLCFist(t *testing.T) {
	got := lcFirst("Address")
	assert.Equal(t, "address", got)
	got = lcFirst("PublicKey")
	assert.Equal(t, "publicKey", got)
	got = lcFirst("A")
	assert.Equal(t, "a", got)
	got = lcFirst("")
	assert.Equal(t, "", got)
}
