package crypto

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

type eventEncryptor struct {
	engine *engine
}

func NewEventEncryptor(store KeyStore, encryptor Encryptor) *eventEncryptor {
	return &eventEncryptor{
		engine: newEngine(store, encryptor),
	}
}

func (e *eventEncryptor) Encrypt(event rangedb.Event) error {
	if event, ok := event.(SelfEncryptor); ok {
		return event.Encrypt(e.engine)
	}

	return nil
}

func (e *eventEncryptor) Decrypt(event rangedb.Event) error {
	if event, ok := event.(SelfEncryptor); ok {
		return event.Decrypt(e.engine)
	}

	return nil
}

type engine struct {
	keyStore  KeyStore
	encryptor Encryptor
}

func newEngine(store KeyStore, encryptor Encryptor) *engine {
	return &engine{
		keyStore:  store,
		encryptor: encryptor,
	}
}

func (e *engine) Encrypt(subjectID, data string) (string, error) {
	key, err := e.keyStore.Get(subjectID)
	if err == ErrKeyNotFound {
		key = shortuuid.New().String()
		err = e.keyStore.Set(subjectID, key)
	}

	if err != nil {
		return "", err
	}

	return e.encryptor.Encrypt(key, data)
}

func (e *engine) Decrypt(subjectID, encryptedData string) (string, error) {
	key, err := e.keyStore.Get(subjectID)
	if err != nil {
		return "", err
	}

	return e.encryptor.Decrypt(key, encryptedData)
}
