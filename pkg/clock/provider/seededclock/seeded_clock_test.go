package seededclock_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/clock/provider/seededclock"
)

func Test_SeededClock_EmptyConstructor_ReturnsZero(t *testing.T) {
	// Given
	clock := seededclock.New()

	// When
	actualTime := clock.Now()

	// Then
	assert.Equal(t, 0, int(actualTime.Unix()))
}

func Test_EmptyConstructor_SeedsWithZero(t *testing.T) {
	// Given
	clock := seededclock.New()

	// Then
	assert.Equal(t, 0, int(clock.Now().Unix()))
	assert.Equal(t, 0, int(clock.Now().Unix()))
}

func Test_WithSeed_RepeatsPattern(t *testing.T) {
	// Given
	clock := seededclock.New(
		time.Unix(1576828800, 0),
		time.Unix(1579507200, 0),
	)

	// Then
	assert.Equal(t, 1576828800, int(clock.Now().Unix()))
	assert.Equal(t, 1579507200, int(clock.Now().Unix()))
	assert.Equal(t, 1576828800, int(clock.Now().Unix()))
	assert.Equal(t, 1579507200, int(clock.Now().Unix()))
}

func Example_WithSeed() {
	// Given
	clock := seededclock.New(
		time.Unix(1576828800, 0),
		time.Unix(1579507200, 0),
	)

	// When
	fmt.Println(clock.Now().Unix())
	fmt.Println(clock.Now().Unix())

	// Output:
	// 1576828800
	// 1579507200
}
