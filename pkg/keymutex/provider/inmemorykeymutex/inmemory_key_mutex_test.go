package inmemorykeymutex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/keymutex/provider/inmemorykeymutex"
)

func Test_InMemoryKeyMutex_WithOneMutex(t *testing.T) {
	// Given
	keyMutex := inmemorykeymutex.New(1)
	lockerA := keyMutex.Get("A")
	lockerB := keyMutex.Get("B")

	// When
	lockerA.Lock()

	// Then
	assert.Equal(t, lockerA, lockerB)
}

func Test_InMemoryKeyMutex_WithTwoMutexes(t *testing.T) {
	// Given
	keyMutex := inmemorykeymutex.New(2)
	lockerA := keyMutex.Get("A")
	lockerB := keyMutex.Get("B")

	// When
	lockerA.Lock()

	// Then
	assert.NotEqual(t, lockerA, lockerB)
}
