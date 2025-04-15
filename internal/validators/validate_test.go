package validators

import (
	"errors"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseValidationError(t *testing.T) {
	t.Run("general_tests", func(t *testing.T) {
		type testStructNested struct {
			NestedRequiredField string `validate:"required"`
			NestedEnumField     string `validate:"oneof=foo bar"`
		}

		type testStruct struct {
			RequiredField      string             `validate:"required"`
			RequiredArrayField []string           `validate:"required,gt=0,dive,not_empty"`
			EnumField          string             `validate:"oneof=foo bar"`
			PublicKeyField     string             `validate:"public_key"`
			UnknownTagField    int                `validate:"ne=1"`
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
					UnknownTagField:    2,
				},
				expectedFieldErrors: map[string]interface{}{
					"requiredField":      "This field is required",
					"requiredArrayField": "Should have at least 1 element(s)",
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
					UnknownTagField:    2,
				},
				expectedFieldErrors: map[string]interface{}{
					"requiredArrayField[1]": "This field cannot be empty",
				},
			},
			{
				stc: &testStruct{
					RequiredField:      "foo",
					RequiredArrayField: []string{"bar"},
					EnumField:          "bar",
					PublicKeyField:     keypair.MustRandom().Address(),
					UnknownTagField:    1,
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
					UnknownTagField:    2,
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
			var vErrs validator.ValidationErrors
			ok := errors.As(err, &vErrs)
			require.True(t, ok)
			fieldErrors := ParseValidationError(vErrs)
			assert.Equal(t, tc.expectedFieldErrors, fieldErrors)
		}
	})

	t.Run("gt_and_gte", func(t *testing.T) {
		type testStruct struct {
			AmountGT  int64    `validate:"gt=10"`
			AmountGTE int64    `validate:"gte=11"`
			SliceGT   []string `validate:"gt=2"`
			SliceGTE  []string `validate:"gte=2"`
		}

		stc := testStruct{
			AmountGT:  10,
			AmountGTE: 10,
			SliceGT:   []string{"a", "b"},
			SliceGTE:  []string{"a"},
		}
		val := NewValidator()
		err := val.Struct(stc)
		require.Error(t, err)

		var vErrs validator.ValidationErrors
		ok := errors.As(err, &vErrs)
		require.True(t, ok)
		fieldErrors := ParseValidationError(vErrs)
		assert.Equal(t, map[string]interface{}{
			"amountGT":  "Should be greater than 10",
			"amountGTE": "Should be greater than or equal to 11",
			"sliceGT":   "Should have at least 3 element(s)",
			"sliceGTE":  "Should have at least 2 element(s)",
		}, fieldErrors)

		type testStructInvalid struct {
			InvalidGT  int64 `validate:"gt=a"`
			InvalidGTE int64 `validate:"gte=a"`
		}
		stcInvalid := testStructInvalid{
			InvalidGT:  0,
			InvalidGTE: 0,
		}
		assert.Panics(t, func() {
			err := val.Struct(stcInvalid)
			require.Error(t, err)
		})
	})

	t.Run("lt_and_lte", func(t *testing.T) {
		type testStruct struct {
			AmountLT  int64    `validate:"lt=10"`
			AmountLTE int64    `validate:"lte=10"`
			SliceLT   []string `validate:"lt=2"`
			SliceLTE  []string `validate:"lte=2"`
		}

		stc := testStruct{
			AmountLT:  10,
			AmountLTE: 11,
			SliceLT:   []string{"a", "b"},
			SliceLTE:  []string{"a", "b", "c"},
		}
		val := NewValidator()
		err := val.Struct(stc)
		require.Error(t, err)
		var vErrs validator.ValidationErrors
		ok := errors.As(err, &vErrs)
		require.True(t, ok)
		fieldErrors := ParseValidationError(vErrs)
		assert.Equal(t, map[string]interface{}{
			"amountLT":  "Should be less than 10",
			"amountLTE": "Should be less than or equal to 10",
			"sliceLT":   "Should have at most 1 element(s)",
			"sliceLTE":  "Should have at most 2 element(s)",
		}, fieldErrors)

		type testStructInvalid struct {
			InvalidLT  int64 `validate:"lt=a"`
			InvalidLTE int64 `validate:"lte=a"`
		}
		stcInvalid := testStructInvalid{
			InvalidLT:  0,
			InvalidLTE: 0,
		}
		assert.Panics(t, func() {
			err := val.Struct(stcInvalid)
			require.Error(t, err)
		})
	})
}

func TestGetFieldName(t *testing.T) {
	type testStructNested struct {
		Name     string             `validate:"not_empty"`
		Children []testStructNested `validate:"dive"`
	}

	type testStruct struct {
		PublicKey   string             `validate:"required,public_key"`
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

	var vErrs validator.ValidationErrors
	ok := errors.As(err, &vErrs)
	require.True(t, ok)
	require.Len(t, vErrs, 2)

	assert.Equal(t, "publicKey", getFieldName(vErrs[0]))
	assert.Equal(t, "nestedField[0].children[0].children[1].children[0].name", getFieldName(vErrs[1]))
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
