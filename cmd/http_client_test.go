package cmd

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeepAlivesDisabledHTTPClient(t *testing.T) {
	client := keepAlivesDisabledHTTPClient()

	require.NotNil(t, client)
	assert.Equal(t, 30*time.Second, client.Timeout)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "transport should be *http.Transport")
	assert.True(t, transport.DisableKeepAlives, "keep-alives should be disabled")
	// Cloning the default transport preserves proxy support so HTTP_PROXY/HTTPS_PROXY still apply.
	assert.NotNil(t, transport.Proxy, "proxy resolver from the default transport must be preserved")
}

func TestKeepAliveHTTPClient(t *testing.T) {
	client := keepAliveHTTPClient(16)

	require.NotNil(t, client)
	assert.Equal(t, 30*time.Second, client.Timeout)

	transport, ok := client.Transport.(*http.Transport)
	require.True(t, ok, "transport should be *http.Transport")
	assert.False(t, transport.DisableKeepAlives, "keep-alives should be enabled")
	// All traffic targets one RPC host, so the idle pool must fit the fan-out or
	// connections beyond the default per-host cap (2) churn on every response.
	assert.Equal(t, 16, transport.MaxIdleConnsPerHost)
	assert.GreaterOrEqual(t, transport.MaxIdleConns, 16)
	assert.NotNil(t, transport.Proxy, "proxy resolver from the default transport must be preserved")

	// A fan-out at or below the effective default (0 field value means
	// http.DefaultMaxIdleConnsPerHost) must not shrink it.
	small := keepAliveHTTPClient(1).Transport.(*http.Transport)
	assert.Zero(t, small.MaxIdleConnsPerHost, "small fan-out keeps the default per-host idle pool")
}
