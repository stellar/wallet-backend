package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDataMigrateWorkers(t *testing.T) {
	testCases := []struct {
		name    string
		workers int
		wantErr string
	}{
		{name: "zero", workers: 0, wantErr: "--data-migration-workers must be at least 1, got 0"},
		{name: "one", workers: 1},
		{name: "as_many_as_the_pool_serves", workers: 10},
		{name: "more_than_the_pool_serves", workers: 11, wantErr: "--data-migration-workers must be at most 10: the run's database pool has 12 connections, one per worker plus 2 the run holds, got 11"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDataMigrateWorkers(tc.workers)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}
