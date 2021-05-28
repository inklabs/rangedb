package encryptedstore_test

import (
	"testing"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/eventencryptor"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/encryptedstore"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_EncryptedStore_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		inMemoryStore := inmemorystore.New(
			inmemorystore.WithClock(clock),
			inmemorystore.WithUUIDGenerator(uuidGenerator),
		)
		encryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, encryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		rangedbtest.BindEvents(encryptedStore)

		return encryptedStore
	})
}

func TestEncryptedStore(t *testing.T) {
	t.Run("OptimisticSave method encrypts, and Events method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.OptimisticSave(ctx, 0, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		ctx = rangedbtest.TimeoutContext(t)
		rawRecords := recordIteratorToSlice(t, inMemoryStore.Events(ctx, 0))
		records := recordIteratorToSlice(t, encryptedStore.Events(ctx, 0))
		require.Len(t, rawRecords, 1)
		require.Len(t, records, 1)
		assert.NotEqual(t, email, rawRecords[0].(*cryptotest.CustomerSignedUp).Email)
		assert.Equal(t, email, records[0].(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("save method encrypts, and Events method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		ctx = rangedbtest.TimeoutContext(t)
		rawRecords := recordIteratorToSlice(t, inMemoryStore.Events(ctx, 0))
		records := recordIteratorToSlice(t, encryptedStore.Events(ctx, 0))
		require.Len(t, rawRecords, 1)
		require.Len(t, records, 1)
		assert.NotEqual(t, email, rawRecords[0].(*cryptotest.CustomerSignedUp).Email)
		assert.Equal(t, email, records[0].(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("save method encrypts, and EventsByAggregateTypes method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		ctx = rangedbtest.TimeoutContext(t)
		rawRecords := recordIteratorToSlice(t, inMemoryStore.EventsByAggregateTypes(ctx, 0, event.AggregateType()))
		records := recordIteratorToSlice(t, encryptedStore.EventsByAggregateTypes(ctx, 0, event.AggregateType()))
		require.Len(t, rawRecords, 1)
		require.Len(t, records, 1)
		assert.NotEqual(t, email, rawRecords[0].(*cryptotest.CustomerSignedUp).Email)
		assert.Equal(t, email, records[0].(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("save method encrypts, and EventsByStream method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		ctx = rangedbtest.TimeoutContext(t)
		rawRecords := recordIteratorToSlice(t, inMemoryStore.EventsByStream(ctx, 0, rangedb.GetEventStream(event)))
		records := recordIteratorToSlice(t, encryptedStore.EventsByStream(ctx, 0, rangedb.GetEventStream(event)))
		require.Len(t, rawRecords, 1)
		require.Len(t, records, 1)
		assert.NotEqual(t, email, rawRecords[0].(*cryptotest.CustomerSignedUp).Email)
		assert.Equal(t, email, records[0].(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("save method encrypts, and AllEventsSubscription method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		records := make(chan *rangedb.Record)
		defer close(records)
		subscriber := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
			records <- record
		})
		ctx := rangedbtest.TimeoutContext(t)
		subscription := encryptedStore.AllEventsSubscription(ctx, 0, subscriber)
		require.NoError(t, subscription.Start())
		ctx = rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		actualRecord := <-records
		subscription.Stop()
		assert.Equal(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("save method encrypts, and AggregateTypesSubscription method decrypts", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
		encryptedStore.Bind(&cryptotest.CustomerSignedUp{})
		const email = "john@example.com"
		event := &cryptotest.CustomerSignedUp{
			ID:     "753b3e92f49b48979962fc185e644f89",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		records := make(chan *rangedb.Record)
		defer close(records)
		subscriber := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
			records <- record
		})
		ctx := rangedbtest.TimeoutContext(t)
		subscription := encryptedStore.AggregateTypesSubscription(ctx, 0, subscriber, event.AggregateType())
		require.NoError(t, subscription.Start())
		ctx = rangedbtest.TimeoutContext(t)

		// When
		_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})

		// Then
		require.NoError(t, err)
		actualRecord := <-records
		subscription.Stop()
		assert.Equal(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
	})
}

func recordIteratorToSlice(t *testing.T, recordIterator rangedb.RecordIterator) []rangedb.Event {
	t.Helper()
	var events []rangedb.Event

	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			require.NoError(t, recordIterator.Err())
			return nil
		}

		value := recordIterator.Record().Data.(rangedb.Event)
		events = append(events, value)
	}

	return events
}
