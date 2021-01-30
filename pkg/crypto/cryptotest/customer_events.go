package cryptotest

//go:generate go run ../../../gen/eventgenerator/main.go -id ID -aggregateType customer

// CustomerSignedUp is an event used for testing PII encryption of string fields.
type CustomerSignedUp struct {
	ID     string `encrypt:"subject-id"`
	Name   string `encrypt:"personal-data"`
	Email  string `encrypt:"personal-data"`
	Status string
}

// CustomerAddedBirth is an event used for testing PII encryption of numeric fields.
type CustomerAddedBirth struct {
	ID                  string `encrypt:"subject-id"`
	BirthMonth          int    `encrypt:"personal-data" serialized:"BirthMonthEncrypted"`
	BirthYear           int    `encrypt:"personal-data" serialized:"BirthYearEncrypted"`
	BirthMonthEncrypted string
	BirthYearEncrypted  string
}
