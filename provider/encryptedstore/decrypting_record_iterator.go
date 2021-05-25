package encryptedstore

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto"
)

type decryptingRecordIterator struct {
	parent         rangedb.RecordIterator
	eventEncryptor crypto.EventEncryptor
	currentErr     error
}

// NewDecryptingRecordIterator constructs a new rangedb.Record iterator that decrypts events
func NewDecryptingRecordIterator(parent rangedb.RecordIterator, eventEncryptor crypto.EventEncryptor) *decryptingRecordIterator {
	return &decryptingRecordIterator{
		parent:         parent,
		eventEncryptor: eventEncryptor,
	}
}

func (i *decryptingRecordIterator) Next() bool {
	if i.currentErr != nil {
		return false
	}

	return i.parent.Next()
}

func (i *decryptingRecordIterator) Record() *rangedb.Record {
	record := i.parent.Record()

	if record == nil {
		return nil
	}

	if rangedbEvent, ok := record.Data.(rangedb.Event); ok {
		err := i.eventEncryptor.Decrypt(rangedbEvent)
		if err != nil {
			i.currentErr = err
			return nil
		}
	}

	return record
}

func (i *decryptingRecordIterator) Err() error {
	if i.currentErr != nil {
		return i.currentErr
	}

	return i.parent.Err()
}
