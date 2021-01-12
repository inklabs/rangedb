package rangedb

import (
	"context"
	"time"
)

// PublishRecordOrCancel publishes a Record to a ResultRecord channel, or times out.
func PublishRecordOrCancel(ctx context.Context, resultRecords chan ResultRecord, record *Record, timeout time.Duration) bool {
	select {
	case <-ctx.Done():
		select {
		case <-time.After(timeout):
		case resultRecords <- ResultRecord{Err: ctx.Err()}:
		}
		return false

	default:
	}

	select {
	case <-ctx.Done():
		select {
		case <-time.After(timeout):
		case resultRecords <- ResultRecord{Err: ctx.Err()}:
		}
		return false

	case resultRecords <- ResultRecord{Record: record}:
	}

	return true
}
