package errors

import (
	"fmt"
)

type UnexpectedSequenceNumber struct {
	Expected           uint64
	NextSequenceNumber uint64
}

func (e UnexpectedSequenceNumber) Error() string {
	return fmt.Sprintf("unexpected sequence number: %d, next: %d",
		e.Expected,
		e.NextSequenceNumber,
	)
}
