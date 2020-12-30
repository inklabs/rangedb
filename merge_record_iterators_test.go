package rangedb_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_MergeRecordIteratorsInOrder(t *testing.T) {
	t.Run("combines two channels", func(t *testing.T) {
		// Given
		record1 := getRecord(1)
		record2 := getRecord(2)
		recordIterators := []rangedb.RecordIterator{
			loadIterator(record1),
			loadIterator(record2),
		}

		// When
		recordIterator := rangedb.MergeRecordIteratorsInOrder(recordIterators)

		// Then
		rangedbtest.AssertRecordsInIterator(t, recordIterator,
			record1,
			record2,
		)
	})

	t.Run("combines three channels with sequential records in second channel", func(t *testing.T) {
		// Given
		record1 := getRecord(1)
		record2 := getRecord(2)
		record3 := getRecord(3)
		record4 := getRecord(4)
		record5 := getRecord(5)
		recordIterators := []rangedb.RecordIterator{
			loadIterator(record1),
			loadIterator(record2, record3, record4),
			loadIterator(record5),
		}

		// When
		recordIterator := rangedb.MergeRecordIteratorsInOrder(recordIterators)

		// Then
		rangedbtest.AssertRecordsInIterator(t, recordIterator,
			record1,
			record2,
			record3,
			record4,
			record5,
		)
	})

}

func BenchmarkMergeRecordIteratorsInOrder(b *testing.B) {
	benchmarkMergeRecordIteratorsInOrder(b, 1, 10000)
	benchmarkMergeRecordIteratorsInOrder(b, 2, 10000)
	benchmarkMergeRecordIteratorsInOrder(b, 5, 10000)
	benchmarkMergeRecordIteratorsInOrder(b, 10, 10000)
	benchmarkMergeRecordIteratorsInOrder(b, 20, 10000)
}

func benchmarkMergeRecordIteratorsInOrder(b *testing.B, totalIterators int, totalRecords int) {
	rand.Seed(100)
	globalSequenceNumber := uint64(0)
	iteratorRecords := make([][]*rangedb.Record, totalIterators)
	for i := 0; i < totalRecords; i++ {
		iteratorID := rand.Intn(totalIterators)
		iteratorRecords[iteratorID] = append(iteratorRecords[iteratorID], getRecord(globalSequenceNumber))
		globalSequenceNumber++
	}

	recordIterators := make([]rangedb.RecordIterator, totalIterators)

	testName := fmt.Sprintf("Merge %d channels with %d records", totalIterators, totalRecords)
	b.Run(testName, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for i, records := range iteratorRecords {
				recordIterators[i] = loadIterator(records...)
			}
			iter := rangedb.MergeRecordIteratorsInOrder(recordIterators)
			cnt := 0
			for iter.Next() {
				if iter.Err() != nil {
					require.NoError(b, iter.Err())
				}
				cnt++
			}
			require.NoError(b, iter.Err())
			require.Equal(b, totalRecords, cnt)
		}
	})
}

func getRecord(globalSequenceNumber uint64) *rangedb.Record {
	return &rangedb.Record{GlobalSequenceNumber: globalSequenceNumber}
}

func loadIterator(records ...*rangedb.Record) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		for _, record := range records {
			resultRecords <- rangedb.ResultRecord{
				Record: record,
				Err:    nil,
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}
