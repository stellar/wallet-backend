package cmd

import (
	"net/http"
	"time"
)

// httpClientTimeout bounds every RPC request the one-shot commands make.
const httpClientTimeout = 30 * time.Second

// keepAlivesDisabledHTTPClient returns an HTTP client whose transport is a clone of
// http.DefaultTransport (preserving ProxyFromEnvironment and the other defaults) with
// keep-alives disabled. A fresh connection per request sidesteps stale-connection EOFs
// behind intermediaries that don't support HTTP connection reuse (e.g. kubectl
// port-forward), which is negligible at the one-shot protocol commands' RPC volume.
//
// Cloning rather than constructing a bare &http.Transport{DisableKeepAlives: true} keeps
// HTTP_PROXY/HTTPS_PROXY support intact for operators who reach the RPC endpoint via a proxy.
func keepAlivesDisabledHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	return &http.Client{
		Timeout:   httpClientTimeout,
		Transport: transport,
	}
}

// keepAliveHTTPClient returns an HTTP client that reuses connections, sized for
// fanning maxConcurrent parallel requests at a single host. The default
// transport keeps only 2 idle connections per host, so a higher fan-out would
// churn through fresh TCP+TLS setups despite keep-alives — raising
// MaxIdleConnsPerHost to the fan-out keeps every worker's connection warm.
// Cloning the default transport preserves HTTP_PROXY/HTTPS_PROXY support.
func keepAliveHTTPClient(maxConcurrent int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The cloned field is 0, which means http.DefaultMaxIdleConnsPerHost (2) —
	// only raise it, never shrink below that effective default.
	if maxConcurrent > http.DefaultMaxIdleConnsPerHost {
		transport.MaxIdleConnsPerHost = maxConcurrent
	}
	if maxConcurrent > transport.MaxIdleConns {
		transport.MaxIdleConns = maxConcurrent
	}
	return &http.Client{
		Timeout:   httpClientTimeout,
		Transport: transport,
	}
}
