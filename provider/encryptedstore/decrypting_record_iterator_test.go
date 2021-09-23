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
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestNewDecryptingRecordIterator(t *testing.T) {
	t.Run("decrypts one record: succeeds", func(t *testing.T) {
		// Given
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		const email = "john@example.com"
		encryptedEvent := &cryptotest.CustomerSignedUp{
			ID:     "fa14d796bab84c9f9c2026a5324d6a34",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		require.NoError(t, eventEncryptor.Encrypt(encryptedEvent))
		record := &rangedb.Record{
			AggregateType:        encryptedEvent.AggregateType(),
			AggregateID:          encryptedEvent.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            encryptedEvent.EventType(),
			InsertTimestamp:      0,
			Data:                 encryptedEvent,
			Metadata:             nil,
		}

		resultRecords := make(chan rangedb.ResultRecord, 1)
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)
		decryptingIterator := encryptedstore.NewDecryptingRecordIterator(recordIterator, eventEncryptor)

		// When
		assert.True(t, decryptingIterator.Next())

		// Then
		require.NoError(t, decryptingIterator.Err())
		actualRecord := decryptingIterator.Record()
		assert.Equal(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
		assert.False(t, decryptingIterator.Next())
	})

	t.Run("decrypts one record using NextContext: succeeds", func(t *testing.T) {
		// Given
		aesEncryptor := aes.NewGCM()
		keyStore := inmemorykeystore.New()
		eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
		const email = "john@example.com"
		encryptedEvent := &cryptotest.CustomerSignedUp{
			ID:     "fa14d796bab84c9f9c2026a5324d6a34",
			Name:   "John Doe",
			Email:  email,
			Status: "active",
		}
		require.NoError(t, eventEncryptor.Encrypt(encryptedEvent))
		record := &rangedb.Record{
			AggregateType:        encryptedEvent.AggregateType(),
			AggregateID:          encryptedEvent.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            encryptedEvent.EventType(),
			InsertTimestamp:      0,
			Data:                 encryptedEvent,
			Metadata:             nil,
		}

		resultRecords := make(chan rangedb.ResultRecord, 1)
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)
		decryptingIterator := encryptedstore.NewDecryptingRecordIterator(recordIterator, eventEncryptor)
		ctx := rangedbtest.TimeoutContext(t)

		// When
		assert.True(t, decryptingIterator.NextContext(ctx))

		// Then
		require.NoError(t, decryptingIterator.Err())
		actualRecord := decryptingIterator.Record()
		assert.Equal(t, email, actualRecord.Data.(*cryptotest.CustomerSignedUp).Email)
		assert.False(t, decryptingIterator.NextContext(ctx))
	})

	t.Run("decrypt two records: fails on first record", func(t *testing.T) {
		// Given
		failingEventEncryptor := cryptotest.NewFailingEventEncryptor()
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
		record := &rangedb.Record{
			AggregateType:        encryptedEvent.AggregateType(),
			AggregateID:          encryptedEvent.AggregateID(),
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            encryptedEvent.EventType(),
			InsertTimestamp:      0,
			Data:                 encryptedEvent,
			Metadata:             nil,
		}

		resultRecords := make(chan rangedb.ResultRecord, 2)
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)
		decryptingIterator := encryptedstore.NewDecryptingRecordIterator(recordIterator, failingEventEncryptor)

		// When
		assert.True(t, decryptingIterator.Next())

		// Then
		assert.Nil(t, decryptingIterator.Record())
		require.EqualError(t, decryptingIterator.Err(), "failingEventEncryptor:Decrypt")
		assert.False(t, decryptingIterator.Next())
	})
}
