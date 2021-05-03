# DynamoDB KeyStore

## Testing

### Environment Variables

```
DYDB_AWS_REGION=us-east-1
DYDB_TABLE_NAME=test_encryption_keys
AWS_ACCESS_KEY_ID=<insert key>
AWS_SECRET_ACCESS_KEY=<insert secret>
```

### Create Table

```
aws dynamodb create-table \
  --table-name test_encryption_keys \
  --attribute-definitions AttributeName=SubjectID,AttributeType=S \
  --key-schema AttributeName=SubjectID,KeyType=HASH \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
```

### List Tables

```
aws dynamodb list-tables
```

### Scan Items

```
aws dynamodb scan --table-name test_encryption_keys
```
