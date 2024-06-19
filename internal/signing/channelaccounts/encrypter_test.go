package channelaccounts

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultPrivateKeyEncrypterEncrypt(t *testing.T) {
	ctx := context.Background()
	e := DefaultPrivateKeyEncrypter{}

	encryptedMsg, err := e.Encrypt(ctx, "private key", "test")
	require.NoError(t, err)
	assert.NotEqual(t, "private key", encryptedMsg)

	decryptedMsg, err := e.Decrypt(ctx, encryptedMsg, "test")
	require.NoError(t, err)
	assert.Equal(t, "private key", decryptedMsg)
}
