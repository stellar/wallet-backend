package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

// SanitizeUTF8 sanitizes a string to comply to the UTF-8 character set and Postgres' code zero byte constraint
func SanitizeUTF8(input string) string {
	// Postgres does not allow code zero bytes on the "text" type and will throw "invalid byte sequence" when encountering one
	// https://www.postgresql.org/docs/13/datatype-character.html
	bs := bytes.ReplaceAll([]byte(input), []byte{0}, []byte{})
	return strings.ToValidUTF8(string(bs), "?")
}

// IsEmpty checks if a value is empty.
func IsEmpty[T any](v T) bool {
	return reflect.ValueOf(&v).Elem().IsZero()
}

// UnwrapInterfaceToPointer unwraps an interface to a pointer of the given type.
func UnwrapInterfaceToPointer[T any](i interface{}) *T {
	t, ok := i.(*T)
	if ok {
		return t
	}
	return nil
}

// PointOf returns a pointer to the value
func PointOf[T any](value T) *T {
	return &value
}

func GetAccountLedgerKey(address string) (string, error) {
	decoded, err := strkey.Decode(strkey.VersionByteAccountID, address)
	if err != nil {
		return "", fmt.Errorf("decoding address %q: %w", address, err)
	}
	var key xdr.Uint256
	copy(key[:], decoded)
	keyXdr, err := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.AccountId(xdr.PublicKey{
				Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
				Ed25519: &key,
			}),
		},
	}.MarshalBinaryBase64()
	if err != nil {
		return "", fmt.Errorf("marshalling ledger key: %w", err)
	}
	return keyXdr, nil
}

// DeferredClose is a function that closes an `io.Closer` resource and logs an error if it fails.
func DeferredClose(ctx context.Context, closer io.Closer, errMsg string) {
	if err := closer.Close(); err != nil {
		if errMsg == "" {
			errMsg = "closing resource"
		}
		log.Ctx(ctx).Errorf("%s: %v", errMsg, err)
	}
}
