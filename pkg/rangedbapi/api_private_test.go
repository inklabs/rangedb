package rangedbapi

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_InvalidInput(t *testing.T) {
	// Given
	err := fmt.Errorf("EOF")

	// When
	invalidErr := newInvalidInput(err)

	// Then
	assert.Equal(t, "invalid input: EOF", invalidErr.Error())
}
