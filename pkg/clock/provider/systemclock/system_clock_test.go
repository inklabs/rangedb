package systemclock_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/clock/provider/systemclock"
)

func Test_SystemClock(t *testing.T) {
	// Given
	clock := systemclock.New()

	// When
	actualTime := clock.Now()

	// Then
	assert.True(t, actualTime.Unix() > 0)
}
