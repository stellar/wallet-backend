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

var customValidators = map[string]validator.Func{
	"public_key":   publicKeyValidation,
	"asset_issuer": assetIssuerValidation,
	"asset_code":   assetCodeValidation,
}

func NewValidator() (*validator.Validate, error) {
	validate := validator.New(validator.WithRequiredStructEnabled())

	for tag, fn := range customValidators {
		if err := validate.RegisterValidation(tag, fn); err != nil {
			return nil, fmt.Errorf("registering %s validation: %w", tag, err)
		}
	}

	return validate, nil
}

// publicKeyValidation validates that the public_key is a valid public key.
func publicKeyValidation(fl validator.FieldLevel) bool {
	addr := strings.TrimSpace(fl.Field().String())
	return addr == "" ||
		strkey.IsValidEd25519PublicKey(addr) ||
		strkey.IsValidMuxedAccountEd25519PublicKey(addr) ||
		strkey.IsValidContractAddress(addr)
}

// assetIssuerValidation validates that the asset_issuer is a valid public key.
func assetIssuerValidation(fl validator.FieldLevel) bool {
	addr := strings.TrimSpace(fl.Field().String())
	return addr == "" || strkey.IsValidEd25519PublicKey(addr)
}

// assetCodeValidation validates that the asset_code is a valid asset code.
func assetCodeValidation(fl validator.FieldLevel) bool {
	code := strings.TrimSpace(fl.Field().String())
	return code == "" || len(code) <= 12
}

func ParseValidationError(errors validator.ValidationErrors) map[string]any {
	fieldErrors := make(map[string]any)
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
	case "public_key":
		return "Invalid public key provided"
	case "asset_issuer":
		return "Invalid asset issuer provided"
	case "asset_code":
		return "Invalid asset code provided"
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
			return fmt.Sprintf("Should have at least %d element(s)", v+1) // For instance, if "gt" is 0 (zero) then it should have at least 1 element
		}
		return fmt.Sprintf("Should be greater than %s", fieldError.Param())
	case "gte":
		if fieldError.Kind() == reflect.Slice || fieldError.Kind() == reflect.Array {
			v, err := strconv.Atoi(fieldError.Param())
			if err != nil {
				log.Errorf(`Error parsing "gte" param %q to integer: %s`, fieldError.Param(), err.Error())
				return "Should have at least 1 element" // Fallback to this error message
			}
			return fmt.Sprintf("Should have at least %d element(s)", v)
		}
		return fmt.Sprintf("Should be greater than or equal to %s", fieldError.Param())
	case "lt":
		if fieldError.Kind() == reflect.Slice || fieldError.Kind() == reflect.Array {
			v, err := strconv.Atoi(fieldError.Param())
			if err != nil {
				log.Errorf(`Error parsing "lt" param %q to integer: %s`, fieldError.Param(), err.Error())
				return "Should have at most 1 element" // Fallback to this error message
			}
			return fmt.Sprintf("Should have at most %d element(s)", v-1)
		}
		return fmt.Sprintf("Should be less than %s", fieldError.Param())
	case "lte":
		if fieldError.Kind() == reflect.Slice || fieldError.Kind() == reflect.Array {
			v, err := strconv.Atoi(fieldError.Param())
			if err != nil {
				log.Errorf(`Error parsing "lte" param %q to integer: %s`, fieldError.Param(), err.Error())
				return "Should have at most 1 element" // Fallback to this error message
			}
			return fmt.Sprintf("Should have at most %d element(s)", v)
		}
		return fmt.Sprintf("Should be less than or equal to %s", fieldError.Param())
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
