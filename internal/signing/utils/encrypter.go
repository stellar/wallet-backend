package utils

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

const keyBytes = 16

type PrivateKeyEncrypter interface {
	Encrypt(ctx context.Context, message, passphrase string) (string, error)
	Decrypt(ctx context.Context, encryptedMessage, passphrase string) (string, error)
}

type DefaultPrivateKeyEncrypter struct{}

var _ PrivateKeyEncrypter = (*DefaultPrivateKeyEncrypter)(nil)

func (e *DefaultPrivateKeyEncrypter) Encrypt(ctx context.Context, message, passphrase string) (string, error) {
	passHash := sha256.New()
	passHash.Write([]byte(passphrase))

	key := make([]byte, keyBytes)
	copy(key, passHash.Sum(nil))

	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}

	gcmCipher, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcmCipher.NonceSize())
	lenRead, err := rand.Read(nonce)
	if err != nil {
		return "", fmt.Errorf("error while generating random nonce: %w", err)
	}
	if lenRead != gcmCipher.NonceSize() {
		return "", fmt.Errorf("length of generated nonce %d different from expected length %d", lenRead, gcmCipher.NonceSize())
	}

	cipheredText := gcmCipher.Seal(nonce, nonce, []byte(message), nil)
	return base64.StdEncoding.EncodeToString(cipheredText), nil
}

func (e *DefaultPrivateKeyEncrypter) Decrypt(ctx context.Context, encryptedMessage, passphrase string) (string, error) {
	passHash := sha256.New()
	passHash.Write([]byte(passphrase))

	key := make([]byte, keyBytes)
	copy(key, passHash.Sum(nil))

	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}

	gcmCipher, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	decodedMsg, err := base64.StdEncoding.DecodeString(encryptedMessage)
	if err != nil {
		return "", err
	}

	nonceSize := gcmCipher.NonceSize()
	nonce, cipheredText := decodedMsg[:nonceSize], decodedMsg[nonceSize:]

	plainText, err := gcmCipher.Open(nil, nonce, cipheredText, nil)
	if err != nil {
		return "", err
	}

	return string(plainText), nil
}
