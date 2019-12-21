package keymutex

import (
	"sync"
)

type KeyMutex interface {
	Get(stringKey string) sync.Locker
}
