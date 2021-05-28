package shortuuid

import (
	"encoding/hex"

	"github.com/google/uuid"
)

// ShortUUID is a type that can stringify a UUID without hyphens.
type ShortUUID uuid.UUID

// New constructs a ShortUUID object.
func New() ShortUUID {
	return ShortUUID(uuid.New())
}

func (u ShortUUID) String() string {
	buf := make([]byte, 32)
	hex.Encode(buf[:], u[:])
	return string(buf)
}

type Generator interface {
	// New returns a new ShortUUID string
	New() string
}

type uuidGenerator struct{}

func NewUUIDGenerator() *uuidGenerator {
	return &uuidGenerator{}
}

func (g *uuidGenerator) New() string {
	return New().String()
}
