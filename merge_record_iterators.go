package rangedb

type pipe struct {
	recordIterator      RecordIterator
	currentResultRecord ResultRecord
}

func newPipe(recordIterator RecordIterator) *pipe {
	return &pipe{recordIterator: recordIterator}
}

func (p *pipe) ReadNext() bool {
	p.recordIterator.Next()
	p.currentResultRecord = ResultRecord{
		Record: p.recordIterator.Record(),
		Err:    p.recordIterator.Err(),
	}
	return p.currentResultRecord.Record != nil
}

func (p *pipe) IsNextGlobalSequenceNumber(currentPosition uint64) bool {
	return p.currentResultRecord.Record.GlobalSequenceNumber == currentPosition+1
}

// MergeRecordIteratorsInOrder combines record channels ordered by record.GlobalSequenceNumber.
func MergeRecordIteratorsInOrder(recordIterators []RecordIterator) RecordIterator {
	resultRecords := make(chan ResultRecord)

	go func() {
		defer close(resultRecords)

		var pipes []*pipe
		for _, recordIterator := range recordIterators {
			pipe := newPipe(recordIterator)
			if pipe.ReadNext() {
				pipes = append(pipes, pipe)
			}
		}

		var currentPosition uint64

		for len(pipes) > 0 {
			i := getIndexWithSmallestGlobalSequenceNumber(pipes)

			resultRecords <- pipes[i].currentResultRecord

			currentPosition = pipes[i].currentResultRecord.Record.GlobalSequenceNumber

			if !pipes[i].ReadNext() {
				pipes = remove(pipes, i)
				continue
			}

			for pipes[i].IsNextGlobalSequenceNumber(currentPosition) {
				resultRecords <- pipes[i].currentResultRecord

				currentPosition = pipes[i].currentResultRecord.Record.GlobalSequenceNumber

				if !pipes[i].ReadNext() {
					pipes = remove(pipes, i)
					break
				}
			}
		}
	}()

	return NewRecordIterator(resultRecords)
}

func remove(s []*pipe, i int) []*pipe {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func getIndexWithSmallestGlobalSequenceNumber(pipes []*pipe) int {
	smallestIndex := 0
	min := pipes[smallestIndex].currentResultRecord.Record.GlobalSequenceNumber
	for i, pipe := range pipes {
		if pipe.currentResultRecord.Record.GlobalSequenceNumber < min {
			smallestIndex = i
			min = pipe.currentResultRecord.Record.GlobalSequenceNumber
		}
	}
	return smallestIndex
}
