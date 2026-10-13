package prices

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveTasks_NoAnchorOracleReturnsNoTasks(t *testing.T) {
	tasks, err := LiveTasks(context.Background(), LiveTasksConfig{NetworkPassphrase: network.TestNetworkPassphrase})
	require.NoError(t, err)
	assert.Empty(t, tasks)
}
