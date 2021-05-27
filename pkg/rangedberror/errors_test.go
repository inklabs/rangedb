package rangedberror_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/rangedberror"
)

func TestUnexpectedSequenceNumber_NewFromString(t *testing.T) {
	t.Run("normal error", func(t *testing.T) {
		// Given
		input := "unable to save to store: unexpected sequence number: 1, actual: 0"

		// When
		actual := rangedberror.NewUnexpectedSequenceNumberFromString(input)

		// Then
		require.NotNil(t, actual)
		assert.Equal(t, uint64(1), actual.Expected)
		assert.Equal(t, uint64(0), actual.ActualSequenceNumber)
		assert.Equal(t, "unexpected sequence number: 1, actual: 0", actual.Error())
	})

	t.Run("exotic error", func(t *testing.T) {
		// Given
		input := "some rpc error: unable to save to store: unexpected sequence number: 1, actual: 0"

		// When
		actual := rangedberror.NewUnexpectedSequenceNumberFromString(input)

		// Then
		require.NotNil(t, actual)
		assert.Equal(t, uint64(1), actual.Expected)
		assert.Equal(t, uint64(0), actual.ActualSequenceNumber)
	})

	t.Run("unable to parse", func(t *testing.T) {
		// Given
		input := "invalid input"

		// When
		actual := rangedberror.NewUnexpectedSequenceNumberFromString(input)

		// Then
		require.NotNil(t, actual)
		assert.Equal(t, uint64(0), actual.Expected)
		assert.Equal(t, uint64(0), actual.ActualSequenceNumber)
	})

	t.Run("unable to scan", func(t *testing.T) {
		// Given
		input := "unable to save to store: unexpected sequence number: xyz, actual: !@#"

		// When
		actual := rangedberror.NewUnexpectedSequenceNumberFromString(input)

		// Then
		require.NotNil(t, actual)
		assert.Equal(t, uint64(0), actual.Expected)
		assert.Equal(t, uint64(0), actual.ActualSequenceNumber)
	})
}
