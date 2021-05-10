package encryptedstore

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto"
)

type decryptingRecordSubscriber struct {
	parent         rangedb.RecordSubscriber
	eventEncryptor crypto.EventEncryptor
}

// NewDecryptingRecordSubscriber decrypts records on Accept
func NewDecryptingRecordSubscriber(parent rangedb.RecordSubscriber, eventEncryptor crypto.EventEncryptor) *decryptingRecordSubscriber {
	return &decryptingRecordSubscriber{
		parent:         parent,
		eventEncryptor: eventEncryptor,
	}
}

func (d *decryptingRecordSubscriber) Accept(record *rangedb.Record) {
	if rangedbEvent, ok := record.Data.(rangedb.Event); ok {
		_ = d.eventEncryptor.Decrypt(rangedbEvent)
	}

	d.parent.Accept(record)
}
