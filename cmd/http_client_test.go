package cmd

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeepAlivesDisabledHTTPClient(t *testing.T) {
	client := keepAlivesDisabledHTTPClient(30 * time.Second)

	require.NotNil(t, client)
	assert.Equal(t, 30*time.Second, client.Timeout)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "transport should be *http.Transport")
	assert.True(t, transport.DisableKeepAlives, "keep-alives should be disabled")
	// Cloning the default transport preserves proxy support so HTTP_PROXY/HTTPS_PROXY still apply.
	assert.NotNil(t, transport.Proxy, "proxy resolver from the default transport must be preserved")
}
