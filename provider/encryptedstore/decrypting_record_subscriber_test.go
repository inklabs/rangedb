package encryptedstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/eventencryptor"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/provider/encryptedstore"
)

func TestNewDecryptingRecordSubscriber(t *testing.T) {
	t.Run("accept succeeds", func(t *testing.T) {
		// Given
		const email = "john@example.com"
		encryptedEvent := &cryptotest.CustomerSignedUp{
			ID:     "fa14d796bab84c9f9c2026a5324d6a34",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		require.NoError(t, eventEncryptor.Encrypt(encryptedEvent))
		encryptedRecord := &rangedb.Record{
			StreamName:           "customer-fa14d796bab84c9f9c2026a5324d6a34",
			AggregateType:        encryptedEvent.AggregateType(),
			AggregateID:          encryptedEvent.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            encryptedEvent.EventType(),
			InsertTimestamp:      0,
			Data:                 encryptedEvent,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 1)
		defer close(records)
		parent := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
			records <- record
		})
		subscriber := encryptedstore.NewDecryptingRecordSubscriber(parent, eventEncryptor)

		// When
		subscriber.Accept(encryptedRecord)

		// Then
		actualRecord := <-records
		assert.Equal(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("accept ignores decryption errors", func(t *testing.T) {
		// Given
		const email = "john@example.com"
		encryptedEvent := &cryptotest.CustomerSignedUp{
			ID:     "fa14d796bab84c9f9c2026a5324d6a34",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		require.NoError(t, eventEncryptor.Encrypt(encryptedEvent))

		failingEventEncryptor := cryptotest.NewFailingEventEncryptor()
		encryptedRecord := &rangedb.Record{
			StreamName:           "customer-fa14d796bab84c9f9c2026a5324d6a34",
			AggregateType:        encryptedEvent.AggregateType(),
			AggregateID:          encryptedEvent.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            encryptedEvent.EventType(),
			InsertTimestamp:      0,
			Data:                 encryptedEvent,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 1)
		defer close(records)
		parent := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
			records <- record
		})
		subscriber := encryptedstore.NewDecryptingRecordSubscriber(parent, failingEventEncryptor)

		// When
		subscriber.Accept(encryptedRecord)

		// Then
		actualRecord := <-records
		assert.NotEqual(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
	})

	t.Run("does not fail decrypting twice", func(t *testing.T) {
		// Given
		records := make(chan *rangedb.Record, 2)
		defer close(records)
		parent := rangedb.RecordSubscriberFunc(func(record *rangedb.Record) {
			records <- record
		})
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		decryptingRecordSubscriber := encryptedstore.NewDecryptingRecordSubscriber(parent, eventEncryptor)
		event := &cryptotest.CustomerSignedUp{
			ID:     "fa14d796bab84c9f9c2026a5324d6a34",
			Name:   "test",
			Email:  "john@example.com",
			Status: "active",
		}
		require.NoError(t, eventEncryptor.Encrypt(event))
		record := &rangedb.Record{
			StreamName:           "customer-fa14d796bab84c9f9c2026a5324d6a34",
			AggregateType:        event.AggregateType(),
			AggregateID:          event.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            event.EventType(),
			InsertTimestamp:      0,
			Data:                 event,
			Metadata:             nil,
		}

		// When
		decryptingRecordSubscriber.Accept(record)
		decryptingRecordSubscriber.Accept(record)

		// Then
		assert.Equal(t, event.Name, (<-records).Data.(*cryptotest.CustomerSignedUp).Name)
		assert.Equal(t, event.Name, (<-records).Data.(*cryptotest.CustomerSignedUp).Name)
	})
}
