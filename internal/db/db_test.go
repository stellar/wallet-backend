package db

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/db/dbtest"
)

type queryHelperPerson struct {
	Name string `db:"name"`
	Age  int32  `db:"age"`
}

func openTestPool(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()

	ctx := context.Background()
	dbt := dbtest.OpenWithoutMigrations(t)
	t.Cleanup(dbt.Close)

	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool, ctx
}

func TestOpenDBConnectionPool(t *testing.T) {
	ctx := context.Background()
	dbt := dbtest.OpenWithoutMigrations(t)
	defer dbt.Close()

	pool, err := OpenDBConnectionPool(ctx, dbt.DSN)
	require.NoError(t, err)
	defer pool.Close()

	assert.NotNil(t, pool)

	err = pool.Ping(ctx)
	require.NoError(t, err)
}

func TestQueryOne(t *testing.T) {
	pool, ctx := openTestPool(t)

	t.Run("struct", func(t *testing.T) {
		person, err := QueryOne[queryHelperPerson](ctx, pool, `
			SELECT *
			FROM (
				VALUES ('alice', 30)
			) AS people(name, age)
		`)
		require.NoError(t, err)
		assert.Equal(t, "alice", person.Name)
		assert.EqualValues(t, 30, person.Age)
	})

	t.Run("scalar", func(t *testing.T) {
		answer, err := QueryOne[int32](ctx, pool, `SELECT 42::int4`)
		require.NoError(t, err)
		assert.EqualValues(t, 42, answer)
	})

	t.Run("lax struct mapping", func(t *testing.T) {
		person, err := QueryOne[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name`)
		require.NoError(t, err)
		assert.Equal(t, "alice", person.Name)
		assert.EqualValues(t, 0, person.Age)
	})

	t.Run("no rows returns pgx err no rows", func(t *testing.T) {
		person, err := QueryOne[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name WHERE false`)
		require.Error(t, err)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
		assert.Equal(t, queryHelperPerson{}, person)
	})

	t.Run("query error is wrapped", func(t *testing.T) {
		person, err := QueryOne[queryHelperPerson](ctx, pool, `SELECT * FROM missing_table`)
		require.Error(t, err)
		assert.ErrorContains(t, err, "executing query")
		assert.Equal(t, queryHelperPerson{}, person)
	})

	t.Run("scan error is returned unwrapped", func(t *testing.T) {
		value, err := QueryOne[int32](ctx, pool, `SELECT 'alice'::text`)
		require.Error(t, err)
		assert.False(t, errors.Is(err, pgx.ErrNoRows))
		assert.NotContains(t, err.Error(), "executing query")
		assert.EqualValues(t, 0, value)
	})
}

func TestQueryMany(t *testing.T) {
	pool, ctx := openTestPool(t)

	t.Run("struct", func(t *testing.T) {
		people, err := QueryMany[queryHelperPerson](ctx, pool, `
			SELECT *
			FROM (
				VALUES
					('alice', 30),
					('bob', 40)
			) AS people(name, age)
			ORDER BY age
		`)
		require.NoError(t, err)
		require.Len(t, people, 2)
		assert.Equal(t, "alice", people[0].Name)
		assert.EqualValues(t, 30, people[0].Age)
		assert.Equal(t, "bob", people[1].Name)
		assert.EqualValues(t, 40, people[1].Age)
	})

	t.Run("scalar", func(t *testing.T) {
		values, err := QueryMany[int32](ctx, pool, `
			SELECT *
			FROM (
				VALUES (1), (2), (3)
			) AS numbers(value)
		`)
		require.NoError(t, err)
		assert.Equal(t, []int32{1, 2, 3}, values)
	})

	t.Run("empty result returns non nil slice", func(t *testing.T) {
		people, err := QueryMany[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name WHERE false`)
		require.NoError(t, err)
		require.NotNil(t, people)
		assert.Empty(t, people)
	})

	t.Run("lax struct mapping", func(t *testing.T) {
		people, err := QueryMany[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name`)
		require.NoError(t, err)
		require.Len(t, people, 1)
		assert.Equal(t, "alice", people[0].Name)
		assert.EqualValues(t, 0, people[0].Age)
	})

	t.Run("query error is wrapped", func(t *testing.T) {
		people, err := QueryMany[queryHelperPerson](ctx, pool, `SELECT * FROM missing_table`)
		require.Error(t, err)
		assert.Nil(t, people)
		assert.ErrorContains(t, err, "executing query")
	})

	t.Run("scan error is wrapped", func(t *testing.T) {
		values, err := QueryMany[int32](ctx, pool, `SELECT 'alice'::text`)
		require.Error(t, err)
		assert.Nil(t, values)
		assert.ErrorContains(t, err, "collecting rows")
	})
}

func TestQueryManyPtrs(t *testing.T) {
	pool, ctx := openTestPool(t)

	t.Run("struct", func(t *testing.T) {
		people, err := QueryManyPtrs[queryHelperPerson](ctx, pool, `
			SELECT *
			FROM (
				VALUES
					('alice', 30),
					('bob', 40)
			) AS people(name, age)
			ORDER BY age
		`)
		require.NoError(t, err)
		require.Len(t, people, 2)
		require.NotNil(t, people[0])
		require.NotNil(t, people[1])
		assert.Equal(t, "alice", people[0].Name)
		assert.EqualValues(t, 30, people[0].Age)
		assert.Equal(t, "bob", people[1].Name)
		assert.EqualValues(t, 40, people[1].Age)
		assert.NotSame(t, people[0], people[1])
	})

	t.Run("scalar", func(t *testing.T) {
		values, err := QueryManyPtrs[int32](ctx, pool, `
			SELECT *
			FROM (
				VALUES (1), (2), (3)
			) AS numbers(value)
		`)
		require.NoError(t, err)
		require.Len(t, values, 3)
		assert.EqualValues(t, 1, *values[0])
		assert.EqualValues(t, 2, *values[1])
		assert.EqualValues(t, 3, *values[2])
		assert.NotSame(t, values[0], values[1])
	})

	t.Run("empty result returns non nil slice", func(t *testing.T) {
		people, err := QueryManyPtrs[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name WHERE false`)
		require.NoError(t, err)
		require.NotNil(t, people)
		assert.Empty(t, people)
	})

	t.Run("lax struct mapping", func(t *testing.T) {
		people, err := QueryManyPtrs[queryHelperPerson](ctx, pool, `SELECT 'alice'::text AS name`)
		require.NoError(t, err)
		require.Len(t, people, 1)
		require.NotNil(t, people[0])
		assert.Equal(t, "alice", people[0].Name)
		assert.EqualValues(t, 0, people[0].Age)
	})

	t.Run("query error is wrapped", func(t *testing.T) {
		people, err := QueryManyPtrs[queryHelperPerson](ctx, pool, `SELECT * FROM missing_table`)
		require.Error(t, err)
		assert.Nil(t, people)
		assert.ErrorContains(t, err, "executing query")
	})

	t.Run("scan error is wrapped", func(t *testing.T) {
		values, err := QueryManyPtrs[int32](ctx, pool, `SELECT 'alice'::text`)
		require.Error(t, err)
		assert.Nil(t, values)
		assert.ErrorContains(t, err, "collecting rows")
	})
}
