package rangedberror

import (
	"fmt"
	"log"
	"strings"
)

type UnexpectedSequenceNumber struct {
	Expected           uint64
	NextSequenceNumber uint64
}

func NewUnexpectedSequenceNumberFromString(input string) *UnexpectedSequenceNumber {
	pieces := strings.Split(input, "unexpected sequence number:")
	var expected, next uint64
	_, err := fmt.Sscanf(pieces[1], "%d, next: %d", &expected, &next)
	if err != nil {
		log.Print(err)
		return nil
	}

	return &UnexpectedSequenceNumber{
		Expected:           expected,
		NextSequenceNumber: next,
	}
}

func (e UnexpectedSequenceNumber) Error() string {
	return fmt.Sprintf("unexpected sequence number: %d, next: %d",
		e.Expected,
		e.NextSequenceNumber,
	)
}
