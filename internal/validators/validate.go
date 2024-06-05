package validators

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/strkey"
)

func NewValidator() *validator.Validate {
	validate := validator.New()
	_ = validate.RegisterValidation("public_key", publicKeyValidation)
	validate.RegisterAlias("not_empty", "required")
	return validate
}

func publicKeyValidation(fl validator.FieldLevel) bool {
	addr := fl.Field().String()
	return strkey.IsValidEd25519PublicKey(addr) || strkey.IsValidMuxedAccountEd25519PublicKey(addr)
}

func ParseValidationError(errors validator.ValidationErrors) map[string]interface{} {
	fieldErrors := make(map[string]interface{})
	for _, err := range errors {
		fieldErrors[getFieldName(err)] = msgForFieldError(err)
	}
	return fieldErrors
}

// msgForFieldError gets the message for the given validation error (tag).
func msgForFieldError(fieldError validator.FieldError) string {
	switch fieldError.Tag() {
	case "required":
		return "This field is required"
	case "not_empty":
		return "This field cannot be empty"
	case "public_key":
		return "Invalid public key provided"
	case "oneof":
		params := strings.Join(strings.Split(fieldError.Param(), " "), ", ")
		return fmt.Sprintf("Unexpected value %q. Expected one of the following values: %s", fieldError.Value(), params)
	case "gt":
		if fieldError.Kind() == reflect.Slice || fieldError.Kind() == reflect.Array {
			return "Should have at least 1 element"
		}
		return fmt.Sprintf("Should be greater than %s", fieldError.Param())
	case "gte":
		return fmt.Sprintf("Should be greater than or equal %s", fieldError.Param())
	default:
		return "Invalid value"
	}
}

func getFieldName(fieldError validator.FieldError) string {
	// Ex.: structName.FieldName, structName.nestedStructName.nestedStructFieldName, structName.nestedStructName.nestedStructName....
	namespace := strings.Split(fieldError.StructNamespace(), ".")
	length := len(namespace)
	if length == 2 {
		return lcFirst(namespace[1])
	}

	if length > 2 {
		return fmt.Sprintf("%s.%s", lcFirst(namespace[length-2]), lcFirst(namespace[length-1]))
	}

	return lcFirst(namespace[0])
}

// lcFirst lowers the case of the first letter of the given string.
//
//	Example: Address -> address
func lcFirst(str string) string {
	for index, letter := range str {
		return string(unicode.ToLower(letter)) + str[index+1:]
	}
	return ""
}
