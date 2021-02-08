package eventencryptor

import (
	cryptoRand "crypto/rand"
	"encoding/base64"
	"io"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto"
)

type eventEncryptor struct {
	engine *engine
}

func New(store crypto.KeyStore, encryptor crypto.Encryptor) *eventEncryptor {
	return &eventEncryptor{
		engine: newEngine(store, encryptor),
	}
}

func (e *eventEncryptor) Encrypt(event rangedb.Event) error {
	if event, ok := event.(crypto.SelfEncryptor); ok {
		return event.Encrypt(e.engine)
	}

	return nil
}

func (e *eventEncryptor) Decrypt(event rangedb.Event) error {
	if event, ok := event.(crypto.SelfEncryptor); ok {
		return event.Decrypt(e.engine)
	}

	return nil
}

func (e *eventEncryptor) SetRandReader(randReader io.Reader) {
	e.engine.setRandReader(randReader)
}

type engine struct {
	keyStore   crypto.KeyStore
	encryptor  crypto.Encryptor
	randReader io.Reader
}

func newEngine(store crypto.KeyStore, encryptor crypto.Encryptor) *engine {
	return &engine{
		keyStore:   store,
		encryptor:  encryptor,
		randReader: cryptoRand.Reader,
	}
}

func (e *engine) Encrypt(subjectID, data string) (string, error) {
	base64Key, err := e.keyStore.Get(subjectID)
	if err == crypto.ErrKeyNotFound {
		base64Key, err = e.newBase64AES256Key()
		if err != nil {
			return "", err
		}

		err = e.keyStore.Set(subjectID, base64Key)
	}

	if err != nil {
		return "", err
	}

	return e.encryptor.Encrypt(base64Key, data)
}

func (e *engine) Decrypt(subjectID, cipherText string) (string, error) {
	base64Key, err := e.keyStore.Get(subjectID)
	if err != nil {
		return "", err
	}

	return e.encryptor.Decrypt(base64Key, cipherText)
}

func (e *engine) newBase64AES256Key() (string, error) {
	const aes256ByteLength = 32
	encryptionKey := make([]byte, aes256ByteLength)
	if _, err := io.ReadFull(e.randReader, encryptionKey); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(encryptionKey), nil
}

func (e *engine) setRandReader(randReader io.Reader) {
	e.randReader = randReader
}
