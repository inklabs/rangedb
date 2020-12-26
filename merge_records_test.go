package rangedb_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	actualRecords := rangedb.MergeRecordChannelsInOrder(channels)

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
	actualRecords := rangedb.MergeRecordChannelsInOrder(channels)

	// Then
	assert.Equal(t, record1, <-actualRecords)
	assert.Equal(t, record2, <-actualRecords)
	assert.Equal(t, record3, <-actualRecords)
	assert.Equal(t, record4, <-actualRecords)
	assert.Equal(t, record5, <-actualRecords)
	assert.Nil(t, <-actualRecords)
}

func BenchmarkMergeRecordChannelsInOrder(b *testing.B) {
	benchmarkMergeRecordChannelsInOrder(b, 1, 10000)
	benchmarkMergeRecordChannelsInOrder(b, 2, 10000)
	benchmarkMergeRecordChannelsInOrder(b, 5, 10000)
	benchmarkMergeRecordChannelsInOrder(b, 10, 10000)
	benchmarkMergeRecordChannelsInOrder(b, 20, 10000)
}

func benchmarkMergeRecordChannelsInOrder(b *testing.B, totalChannels int, totalRecords int) {
	rand.Seed(100)
	globalSequenceNumber := uint64(0)
	channelRecords := make([][]*rangedb.Record, totalChannels)
	for i := 0; i < totalRecords; i++ {
		channel := rand.Intn(totalChannels)
		channelRecords[channel] = append(channelRecords[channel], getRecord(globalSequenceNumber))
		globalSequenceNumber++
	}

	channels := make([]<-chan *rangedb.Record, totalChannels)

	testName := fmt.Sprintf("Merge %d channels with %d records", totalChannels, totalRecords)
	b.Run(testName, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i, records := range channelRecords {
				channels[i] = loadChannel(records...)
			}
			actualRecords := rangedb.MergeRecordChannelsInOrder(channels)
			cnt := 0
			for range actualRecords {
				cnt++
			}

			require.Equal(b, totalRecords, cnt)
		}
	})
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
