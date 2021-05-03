package dynamodbkeystore

import (
	"fmt"
	"os"
)

type Config struct {
	TableName       string
	AWSRegion       string
	AccessKeyID     string
	SecretAccessKey string
}

// NewConfigFromEnvironment loads a DynamoDB config from environment variables.
func NewConfigFromEnvironment() (*Config, error) {
	awsRegion := os.Getenv("DYDB_AWS_REGION")
	tableName := os.Getenv("DYDB_TABLE_NAME")
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if awsRegion == "" || tableName == "" || accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("DynamoDB has not been configured via environment variables")
	}

	return &Config{
		TableName:       tableName,
		AWSRegion:       awsRegion,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}, nil
}
