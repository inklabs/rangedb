package shortuuid_test

import (
	"fmt"
	"testing"

	"github.com/inklabs/rangedb/rangedbtest"
	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func Test_NewToString(t *testing.T) {
	// Given
	rangedbtest.SetRand(100)
	u := shortuuid.New()

	// When
	output := u.String()

	// Then
	assert.Equal(t, "d2ba8e70072943388203c438d4e94bf3", output)
}

func ExampleShortUUID_String() {
	// Given
	rangedbtest.SetRand(100)
	u := shortuuid.New()

	// When
	fmt.Println(u.String())

	// Output:
	// b03b2442c5de4e58b6c62e8094847f3d
}
