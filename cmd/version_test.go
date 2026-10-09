package cmd

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionCommand(t *testing.T) {
	testCases := []struct {
		name      string
		gitCommit string
		want      string
	}{
		{name: "injected commit", gitCommit: "abc123", want: "wallet-backend v1.2.3 (commit abc123, " + runtime.Version() + ")\n"},
		{name: "missing commit", gitCommit: "", want: "wallet-backend v1.2.3 (commit unknown, " + runtime.Version() + ")\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := versionCommand(RootConfig{Version: "v1.2.3", GitCommit: tc.gitCommit})
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetArgs([]string{})

			require.NoError(t, cmd.Execute())
			assert.Equal(t, tc.want, out.String())
		})
	}
}
