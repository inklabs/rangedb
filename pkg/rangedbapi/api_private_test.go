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

func Test_stringToSequenceNumber(t *testing.T) {
	// Then
	assert.Nil(t, stringToSequenceNumber(""))
	assert.Equal(t, uint64(0), *stringToSequenceNumber("0"))
	assert.Equal(t, uint64(1), *stringToSequenceNumber("1"))
	assert.Nil(t, stringToSequenceNumber("-1"))
	assert.Equal(t, uint64(9999), *stringToSequenceNumber("9999"))
}
