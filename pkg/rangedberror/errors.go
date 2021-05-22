package rangedberror

import (
	"fmt"
	"strings"
)

// UnexpectedSequenceNumber is an error containing expected and actual sequence numbers.
type UnexpectedSequenceNumber struct {
	Expected             uint64
	ActualSequenceNumber uint64
}

// NewUnexpectedSequenceNumberFromString constructs an UnexpectedSequenceNumber error.
func NewUnexpectedSequenceNumberFromString(input string) *UnexpectedSequenceNumber {
	pieces := strings.Split(input, "unexpected sequence number:")
	if len(pieces) < 2 {
		return &UnexpectedSequenceNumber{}
	}

	var expected, actual uint64
	_, err := fmt.Sscanf(pieces[1], "%d, actual: %d", &expected, &actual)
	if err != nil {
		return &UnexpectedSequenceNumber{}
	}

	return &UnexpectedSequenceNumber{
		Expected:             expected,
		ActualSequenceNumber: actual,
	}
}

func (e UnexpectedSequenceNumber) Error() string {
	return fmt.Sprintf("unexpected sequence number: %d, actual: %d",
		e.Expected,
		e.ActualSequenceNumber,
	)
}
