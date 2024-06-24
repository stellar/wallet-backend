package validators

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/go-playground/validator/v10"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
)

func NewValidator() *validator.Validate {
	validate := validator.New()
	_ = validate.RegisterValidation("public_key", publicKeyValidation)
	validate.RegisterAlias("not_empty", "required")
	return validate
}

func publicKeyValidation(fl validator.FieldLevel) bool {
	addr := fl.Field().String()
	return addr == "" || strkey.IsValidEd25519PublicKey(addr) || strkey.IsValidMuxedAccountEd25519PublicKey(addr)
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
			v, err := strconv.Atoi(fieldError.Param())
			if err != nil {
				log.Errorf(`Error parsing "gt" param %q to integer: %s`, fieldError.Param(), err.Error())
				return "Should have at least 1 element" // Fallback to this error message
			}
			return fmt.Sprintf("Should have at least %d element", v+1) // For instance, if "gt" is 0 (zero) then it should have at least 1 element
		}
		return fmt.Sprintf("Should be greater than %s", fieldError.Param())
	case "gte":
		if fieldError.Kind() == reflect.Slice || fieldError.Kind() == reflect.Array {
			v, err := strconv.Atoi(fieldError.Param())
			if err != nil {
				log.Errorf(`Error parsing "gte" param %q to integer: %s`, fieldError.Param(), err.Error())
				return "Should have at least 1 element" // Fallback to this error message
			}
			return fmt.Sprintf("Should have at least %d element", v)
		}
		return fmt.Sprintf("Should be greater than or equal to %s", fieldError.Param())
	default:
		return "Invalid value"
	}
}

func getFieldName(fieldError validator.FieldError) string {
	// Ex.: structName.FieldName, structName.nestedStructName.nestedStructFieldName, structName.nestedStructName.nestedStructName....
	namespace := strings.Split(fieldError.StructNamespace(), ".")

	// we remove the root struct name
	relevantNamespace := namespace[1:]
	for i := range relevantNamespace {
		relevantNamespace[i] = lcFirst(relevantNamespace[i])
	}
	return strings.Join(relevantNamespace, ".")
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
