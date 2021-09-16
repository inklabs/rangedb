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

	g.mux.Lock()
	g.generatedUUIDs = append(g.generatedUUIDs, newUUID)
	g.mux.Unlock()

	return newUUID
}

func (g *seededUUIDGenerator) Get(index int) string {
	g.mux.RLock()
	defer g.mux.RUnlock()

	if len(g.generatedUUIDs) < index {
		return ""
	}

	return g.generatedUUIDs[index-1]
}

func SetRand(seed int64) {
	uuid.SetRand(rand.New(rand.NewSource(seed)))
}
