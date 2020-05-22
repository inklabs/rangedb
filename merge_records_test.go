package rangedb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
)

func Test_MergeRecordChannelsInOrder_CombinesTwoChannels(t *testing.T) {
	// Given
	record1 := getRecord(1)
	record2 := getRecord(2)
	channels := []<-chan *rangedb.Record{
		loadChannel(record1),
		loadChannel(record2),
	}

	// When
	actualRecords := rangedb.MergeRecordChannelsInOrder(channels, 0)

	// Then
	assert.Equal(t, record1, <-actualRecords)
	assert.Equal(t, record2, <-actualRecords)
	assert.Nil(t, <-actualRecords)
}

func Test_MergeRecordChannelsInOrder_CombinesThreeChannels_WithSequentialRecordsInSecondChannel(t *testing.T) {
	// Given
	record1 := getRecord(1)
	record2 := getRecord(2)
	record3 := getRecord(3)
	record4 := getRecord(4)
	record5 := getRecord(5)
	channels := []<-chan *rangedb.Record{
		loadChannel(record1),
		loadChannel(record2, record3, record4),
		loadChannel(record5),
	}

	// When
	actualRecords := rangedb.MergeRecordChannelsInOrder(channels, 0)

	// Then
	assert.Equal(t, record1, <-actualRecords)
	assert.Equal(t, record2, <-actualRecords)
	assert.Equal(t, record3, <-actualRecords)
	assert.Equal(t, record4, <-actualRecords)
	assert.Equal(t, record5, <-actualRecords)
	assert.Nil(t, <-actualRecords)
}

func Test_MergeRecordChannelsInOrder_CombinesThreeChannels_StartingWithSecondRecord(t *testing.T) {
	// Given
	record1 := getRecord(1)
	record2 := getRecord(2)
	record3 := getRecord(3)
	record4 := getRecord(4)
	record5 := getRecord(5)
	channels := []<-chan *rangedb.Record{
		loadChannel(record1),
		loadChannel(record2, record3, record4),
		loadChannel(record5),
	}

	// When
	actualRecords := rangedb.MergeRecordChannelsInOrder(channels, 1)

	// Then
	assert.Equal(t, record2, <-actualRecords)
	assert.Equal(t, record3, <-actualRecords)
	assert.Equal(t, record4, <-actualRecords)
	assert.Equal(t, record5, <-actualRecords)
	assert.Nil(t, <-actualRecords)
}

func getRecord(globalSequenceNumber uint64) *rangedb.Record {
	return &rangedb.Record{GlobalSequenceNumber: globalSequenceNumber}
}

func loadChannel(records ...*rangedb.Record) <-chan *rangedb.Record {
	channel := make(chan *rangedb.Record)

	go func() {
		defer close(channel)

		for _, record := range records {
			channel <- record
		}
	}()

	return channel
}
