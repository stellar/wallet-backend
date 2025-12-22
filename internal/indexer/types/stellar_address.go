// Package types provides custom types for Stellar address handling with BYTEA storage.
// StellarAddress stores addresses as 33-byte BYTEA: [version_byte][32-byte_public_key].

package types

import (
	"database/sql/driver"
	"fmt"

	"github.com/stellar/go/strkey"
)

// Ensure types implement the required interfaces.
var (
	_ driver.Valuer = StellarAddress("")
	_ driver.Valuer = NullableStellarAddress{}
)

// StellarAddress is a string type that stores as 33-byte BYTEA in PostgreSQL.
// In Go code it behaves like a regular string (G... or C... format).
// In the database it's stored as: [version_byte][32-byte_public_key]
type StellarAddress string

// Scan implements sql.Scanner - reads 33-byte BYTEA from DB, converts to G.../C... string.
func (s *StellarAddress) Scan(value interface{}) error {
	if value == nil {
		*s = ""
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}

	if len(bytes) != 33 {
		return fmt.Errorf("invalid StellarAddress length: got %d, want 33", len(bytes))
	}

	versionByte := strkey.VersionByte(bytes[0])
	encoded, err := strkey.Encode(versionByte, bytes[1:])
	if err != nil {
		return fmt.Errorf("encoding address: %w", err)
	}

	*s = StellarAddress(encoded)
	return nil
}

// Value implements driver.Valuer - converts G.../C... string to 33-byte BYTEA for DB.
func (s StellarAddress) Value() (driver.Value, error) {
	if s == "" {
		return nil, nil
	}

	address := string(s)

	// Try account (G...) first
	if decoded, err := strkey.Decode(strkey.VersionByteAccountID, address); err == nil {
		result := make([]byte, 33)
		result[0] = byte(strkey.VersionByteAccountID)
		copy(result[1:], decoded)
		return result, nil
	}

	// Try contract (C...)
	if decoded, err := strkey.Decode(strkey.VersionByteContract, address); err == nil {
		result := make([]byte, 33)
		result[0] = byte(strkey.VersionByteContract)
		copy(result[1:], decoded)
		return result, nil
	}

	return nil, fmt.Errorf("invalid stellar address: %s", address)
}

// NullableStellarAddress is like sql.NullString but stores as 33-byte BYTEA.
// Used for optional account/contract ID fields.
type NullableStellarAddress struct {
	String string
	Valid  bool
}

// Scan implements sql.Scanner - reads nullable 33-byte BYTEA from DB.
func (n *NullableStellarAddress) Scan(value interface{}) error {
	if value == nil {
		n.String = ""
		n.Valid = false
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}

	if len(bytes) != 33 {
		return fmt.Errorf("invalid NullableStellarAddress length: got %d, want 33", len(bytes))
	}

	versionByte := strkey.VersionByte(bytes[0])
	encoded, err := strkey.Encode(versionByte, bytes[1:])
	if err != nil {
		return fmt.Errorf("encoding address: %w", err)
	}

	n.String = encoded
	n.Valid = true
	return nil
}

// Value implements driver.Valuer - converts to 33-byte BYTEA for DB.
func (n NullableStellarAddress) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return StellarAddress(n.String).Value()
}

// NewNullableStellarAddress creates a NullableStellarAddress from a string.
// If the string is empty, returns an invalid NullableStellarAddress.
func NewNullableStellarAddress(address string) NullableStellarAddress {
	if address == "" {
		return NullableStellarAddress{Valid: false}
	}
	return NullableStellarAddress{String: address, Valid: true}
}
