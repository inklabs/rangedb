package rangedb

import (
	"context"
	"time"
)

const timeoutDuration = time.Second * 1

func PublishRecordOrCancel(ctx context.Context, resultRecords chan ResultRecord, record *Record) bool {
	select {
	case <-ctx.Done():
		timeout := time.After(timeoutDuration)
		select {
		case <-timeout:
		case resultRecords <- ResultRecord{Err: ctx.Err()}:
		}
		return false

	default:
	}

	select {
	case <-ctx.Done():
		timeout := time.After(timeoutDuration)
		select {
		case <-timeout:
		case resultRecords <- ResultRecord{Err: ctx.Err()}:
		}
		return false

	case resultRecords <- ResultRecord{Record: record}:
	}

	return true
}
