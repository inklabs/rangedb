package rangedbtest

import (
	"math/rand"
	"sync"

	"github.com/google/uuid"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

type seededUUIDGenerator struct {
	mux            sync.RWMutex
	generatedUUIDs []string
}

func NewSeededUUIDGenerator() *seededUUIDGenerator {
	return &seededUUIDGenerator{}
}

func (g *seededUUIDGenerator) New() string {
	newUUID := shortuuid.New().String()
	g.generatedUUIDs = append(g.generatedUUIDs, newUUID)
	return newUUID
}

func (g *seededUUIDGenerator) Get(index int) string {
	g.mux.RLock()
	defer g.mux.RUnlock()

	return g.generatedUUIDs[index-1]
}

func SetRand(seed int64) {
	uuid.SetRand(rand.New(rand.NewSource(seed)))
}
