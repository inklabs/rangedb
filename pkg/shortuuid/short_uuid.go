package shortuuid

import (
	"encoding/hex"
	"math/rand"

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

// SetRand sets the random number generator to seed.
func SetRand(seed int64) {
	uuid.SetRand(rand.New(rand.NewSource(seed)))
}
