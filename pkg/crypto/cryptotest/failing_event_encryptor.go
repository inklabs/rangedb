package cryptotest

import (
	"fmt"

	"github.com/inklabs/rangedb"
)

type failingEventEncryptor struct{}

func NewFailingEventEncryptor() *failingEventEncryptor {
	return &failingEventEncryptor{}
}

func (f *failingEventEncryptor) Encrypt(_ rangedb.Event) error {
	return fmt.Errorf("failingEventEncryptor:Encrypt")
}

func (f *failingEventEncryptor) Decrypt(_ rangedb.Event) error {
	return fmt.Errorf("failingEventEncryptor:Decrypt")
}
