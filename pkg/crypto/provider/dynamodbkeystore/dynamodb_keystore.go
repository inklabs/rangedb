package dynamodbkeystore

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type dynamoDBKeyStore struct {
	config   *Config
	dynamoDB dynamodbiface.DynamoDBAPI
}

func New(config *Config) (*dynamoDBKeyStore, error) {
	sess := awsSessionFromConfig(config)

	dynamoDB := dynamodb.New(sess)

	keyStore := &dynamoDBKeyStore{
		config:   config,
		dynamoDB: dynamoDB,
	}

	return keyStore, nil
}

func (d *dynamoDBKeyStore) Get(subjectID string) (string, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"SubjectID": {
				S: aws.String(subjectID),
			},
		},
		TableName: aws.String(d.config.TableName),
		AttributesToGet: []*string{
			aws.String("EncryptionKey"),
			aws.String("DeletedAtTimestamp"),
		},
	}
	output, err := d.dynamoDB.GetItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeResourceNotFoundException:
				return "", crypto.ErrKeyNotFound
			}
		}

		return "", err
	}

	var value string
	if _, ok := output.Item["EncryptionKey"]; !ok {
		return "", crypto.ErrKeyNotFound
	}

	err = dynamodbattribute.Unmarshal(output.Item["EncryptionKey"], &value)
	if err != nil {
		return "", err
	}

	var deletedAtTimestamp int64
	err = dynamodbattribute.Unmarshal(output.Item["DeletedAtTimestamp"], &deletedAtTimestamp)
	if err != nil {
		return "", err
	}

	if deletedAtTimestamp > 0 {
		return "", crypto.ErrKeyWasDeleted
	}

	return value, nil
}

func (d *dynamoDBKeyStore) Set(subjectID, encryptionKey string) error {
	if encryptionKey == "" {
		return crypto.ErrInvalidKey
	}

	input := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"SubjectID": {
				S: aws.String(subjectID),
			},
			"EncryptionKey": {
				S: aws.String(encryptionKey),
			},
		},
		TableName:           aws.String(d.config.TableName),
		ConditionExpression: aws.String("attribute_not_exists(SubjectID)"),
	}

	_, err := d.dynamoDB.PutItem(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return crypto.ErrKeyExistsForSubjectID
			}
		}
		return err
	}

	return nil
}

func (d *dynamoDBKeyStore) Delete(subjectID string) error {

	timestamp := time.Now().Unix()
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#EK":  aws.String("EncryptionKey"),
			"#DAT": aws.String("DeletedAtTimestamp"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":k": {
				S: aws.String(""),
			},
			":t": {
				N: aws.String(fmt.Sprintf("%d", timestamp)),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"SubjectID": {
				S: aws.String(subjectID),
			},
		},
		TableName:        aws.String(d.config.TableName),
		UpdateExpression: aws.String("SET #EK = :k, #DAT = :t"),
	}

	_, err := d.dynamoDB.UpdateItem(input)
	if err != nil {
		return err
	}

	return nil
}

func awsSessionFromConfig(config *Config) *session.Session {
	awsCredentials := credentials.NewStaticCredentialsFromCreds(
		credentials.Value{
			AccessKeyID:     config.AccessKeyID,
			SecretAccessKey: config.SecretAccessKey,
		},
	)
	awsConfig := &aws.Config{
		Credentials: awsCredentials,
		Region:      aws.String(config.AWSRegion),
	}
	sess := session.Must(session.NewSession(awsConfig))
	return sess
}
