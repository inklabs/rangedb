package sequentialclock_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
)

func Test_SequentialClock(t *testing.T) {
	// Given
	clock := sequentialclock.New()

	// When
	actualTime := clock.Now()

	// Then
	assert.Equal(t, 0, int(actualTime.Unix()))
}

func ExampleOutput() {
	// Given
	clock := sequentialclock.New()

	// When
	fmt.Println(clock.Now().Unix())
	fmt.Println(clock.Now().Unix())
	fmt.Println(clock.Now().Unix())

	// Output:
	// 0
	// 1
	// 2
}
