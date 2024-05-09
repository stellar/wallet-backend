package dbtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	db := Open(t)
	defer db.Close()
	conn := db.Open()
	defer conn.Close()

	count := 0
	err := conn.Get(&count, `SELECT COUNT(*) FROM gorp_migrations`)
	require.NoError(t, err)
	assert.Greater(t, count, 0)
}
