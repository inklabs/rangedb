package recordsubscriber_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func TestConfig(t *testing.T) {
	// Given
	ctx := context.Background()
	store := inmemorystore.New()
	broadcaster := broadcast.New(1, time.Nanosecond)
	bufferLength := 1
	consumeRecord := func(record *rangedb.Record) error {
		return nil
	}
	var aggregateTypes []string

	// When
	allEventsConfig := recordsubscriber.AllEventsConfig(ctx, store, broadcaster, bufferLength, consumeRecord)
	aggregateTypesConfig := recordsubscriber.AggregateTypesConfig(ctx, store, broadcaster, bufferLength, aggregateTypes, consumeRecord)

	// Then
	assert.IsType(t, recordsubscriber.Config{}, allEventsConfig)
	assert.IsType(t, recordsubscriber.Config{}, aggregateTypesConfig)
}
