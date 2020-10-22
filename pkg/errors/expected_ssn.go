package errors

type ErrUnexpectedVersionError struct {
	ExpectedVersion uint64
	EventVersion    uint64
}

func (e ErrUnexpectedVersionError) Error() string {
	return "wrong expected version"
}
