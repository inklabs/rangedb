package shortuuid

import (
	"encoding/hex"
	"math/rand"

	"github.com/google/uuid"
)

type ShortUUID uuid.UUID

func New() ShortUUID {
	return ShortUUID(uuid.New())
}

func (u ShortUUID) String() string {
	buf := make([]byte, 32)
	hex.Encode(buf[:], u[:])
	return string(buf)
}

func SetRand(seed int64) {
	uuid.SetRand(rand.New(rand.NewSource(seed)))
}
