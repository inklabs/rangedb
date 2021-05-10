package encryptedstore

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto"
)

type encryptedStore struct {
	parent         rangedb.Store
	eventEncryptor crypto.EventEncryptor
}

// New constructs an Encrypted Store that automatically encrypts/decrypts events for a decorated parent rangedb.Store.
func New(parent rangedb.Store, eventEncryptor crypto.EventEncryptor) *encryptedStore {
	return &encryptedStore{
		parent:         parent,
		eventEncryptor: eventEncryptor,
	}
}

func (e *encryptedStore) Bind(events ...rangedb.Event) {
	e.parent.Bind(events...)
}

func (e *encryptedStore) Events(ctx context.Context, globalSequenceNumber uint64) rangedb.RecordIterator {
	return NewDecryptingRecordIterator(e.parent.Events(ctx, globalSequenceNumber), e.eventEncryptor)
}

func (e *encryptedStore) EventsByAggregateTypes(ctx context.Context, globalSequenceNumber uint64, aggregateTypes ...string) rangedb.RecordIterator {
	return NewDecryptingRecordIterator(e.parent.EventsByAggregateTypes(ctx, globalSequenceNumber, aggregateTypes...), e.eventEncryptor)
}

func (e *encryptedStore) EventsByStream(ctx context.Context, streamSequenceNumber uint64, streamName string) rangedb.RecordIterator {
	return NewDecryptingRecordIterator(e.parent.EventsByStream(ctx, streamSequenceNumber, streamName), e.eventEncryptor)
}

func (e *encryptedStore) OptimisticSave(ctx context.Context, expectedStreamSequenceNumber uint64, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	for _, record := range eventRecords {
		err := e.eventEncryptor.Encrypt(record.Event)
		if err != nil {
			return 0, err
		}
	}

	return e.parent.OptimisticSave(ctx, expectedStreamSequenceNumber, eventRecords...)
}

func (e *encryptedStore) Save(ctx context.Context, eventRecords ...*rangedb.EventRecord) (uint64, error) {
	for _, record := range eventRecords {
		err := e.eventEncryptor.Encrypt(record.Event)
		if err != nil {
			return 0, err
		}
	}

	return e.parent.Save(ctx, eventRecords...)
}

func (e *encryptedStore) AllEventsSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber) rangedb.RecordSubscription {
	return e.parent.AllEventsSubscription(ctx, bufferSize, NewDecryptingRecordSubscriber(subscriber, e.eventEncryptor))
}

func (e *encryptedStore) AggregateTypesSubscription(ctx context.Context, bufferSize int, subscriber rangedb.RecordSubscriber, aggregateTypes ...string) rangedb.RecordSubscription {
	return e.parent.AggregateTypesSubscription(ctx, bufferSize, NewDecryptingRecordSubscriber(subscriber, e.eventEncryptor), aggregateTypes...)
}

func (e *encryptedStore) TotalEventsInStream(ctx context.Context, streamName string) (uint64, error) {
	return e.parent.TotalEventsInStream(ctx, streamName)
}
