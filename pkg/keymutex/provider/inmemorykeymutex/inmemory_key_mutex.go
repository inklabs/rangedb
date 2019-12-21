package inmemorykeymutex

import (
	"hash/fnv"
	"sync"
)

type inMemoryKeyMutex struct {
	maxMutexes uint16

	mutex      sync.RWMutex
	mutexByKey map[uint16]*sync.Mutex
}

func New(maxMutexes uint16) *inMemoryKeyMutex {
	return &inMemoryKeyMutex{
		maxMutexes: maxMutexes,
		mutexByKey: make(map[uint16]*sync.Mutex, maxMutexes),
	}
}

func (s *inMemoryKeyMutex) Get(stringKey string) sync.Locker {
	key := s.getKey(stringKey)

	s.mutex.RLock()
	_, ok := s.mutexByKey[key]
	s.mutex.RUnlock()

	if !ok {
		s.mutex.Lock()
		s.mutexByKey[key] = &sync.Mutex{}
		s.mutex.Unlock()
	}

	return s.mutexByKey[key]
}

func (s *inMemoryKeyMutex) getKey(key string) uint16 {
	return uint16(hash(key) % uint32(s.maxMutexes))
}

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
