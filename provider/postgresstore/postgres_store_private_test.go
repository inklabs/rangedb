package postgresstore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/rangedbtest"
)

func TestPrivate(t *testing.T) {
	t.Run("lockStream errors when obtaining advisory lock", func(t *testing.T) {
		// Given
		config := configFromEnvironment(t)
		store, err := New(config)
		require.NoError(t, err)
		const aggregateID = "9a483cf6fc1c45b2a126c146498f519b"
		failingDB := newFailingDBExecAble()
		ctx := rangedbtest.TimeoutContext(t)

		// When
		err = store.lockStream(ctx, failingDB, aggregateID)

		// Then
		assert.EqualError(t, err, "unable to obtain lock: failingDBExecAble.ExecContext")
	})

	t.Run("batchInsert errors from closed context", func(t *testing.T) {
		config := configFromEnvironment(t)
		store, err := New(config)
		require.NoError(t, err)
		failingDB := newFailingDBQueryable()
		ctx := rangedbtest.TimeoutContext(t)

		// When
		ints, lastStreamSequenceNumber, err := store.batchInsert(ctx, failingDB, nil)

		// Then
		assert.EqualError(t, err, "unable to insert: failingDBQueryable.QueryContext")
		assert.Empty(t, ints)
		assert.Equal(t, uint64(0), lastStreamSequenceNumber)
	})

}

type failingDBQueryable struct{}

func newFailingDBQueryable() *failingDBQueryable {
	return &failingDBQueryable{}
}

func (f failingDBQueryable) QueryContext(_ context.Context, _ string, _ ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("failingDBQueryable.QueryContext")
}

type failingDBExecAble struct{}

func newFailingDBExecAble() *failingDBExecAble {
	return &failingDBExecAble{}
}

func (f failingDBExecAble) ExecContext(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	return nil, fmt.Errorf("failingDBExecAble.ExecContext")
}

func configFromEnvironment(t *testing.T) *Config {
	config, err := NewConfigFromEnvironment()
	if err != nil {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	return config
}
