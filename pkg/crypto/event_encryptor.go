package crypto

import (
	"github.com/inklabs/rangedb"
)

type eventEncryptor struct {
	engine Encryptor
}

func NewEventEncryptor(engine Engine) *eventEncryptor {
	return &eventEncryptor{
		engine: engine,
	}
}

func (e eventEncryptor) Encrypt(event rangedb.Event) error {
	if event, ok := event.(SelfEncryptor); ok {
		return event.Encrypt(e.engine)
	}

	return nil
}

func (e eventEncryptor) Decrypt(event rangedb.Event) error {
	if event, ok := event.(SelfEncryptor); ok {
		return event.Decrypt(e.engine)
	}

	return nil
}
