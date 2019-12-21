package rangedb

type pipe struct {
	recordChannel <-chan *Record
	currentRecord *Record
}

func newPipe(recordChannel <-chan *Record) *pipe {
	return &pipe{recordChannel: recordChannel}
}

func (p *pipe) ReadNext() bool {
	p.currentRecord = <-p.recordChannel
	return p.currentRecord != nil
}

func (p *pipe) IsNextGlobalSequenceNumber(currentPosition uint64) bool {
	return p.currentRecord.GlobalSequenceNumber == currentPosition+1
}

func MergeRecordChannelsInOrder(channels []<-chan *Record) <-chan *Record {
	records := make(chan *Record)

	go func() {
		defer close(records)

		var pipes []*pipe
		for _, channel := range channels {
			pipe := newPipe(channel)
			if pipe.ReadNext() {
				pipes = append(pipes, pipe)
			}
		}

		currentPosition := uint64(0)

		for len(pipes) > 0 {
			i := getIndexWithSmallestGlobalSequenceNumber(pipes)

			records <- pipes[i].currentRecord
			currentPosition = pipes[i].currentRecord.GlobalSequenceNumber

			if !pipes[i].ReadNext() {
				pipes = remove(pipes, i)
				continue
			}

			for pipes[i].IsNextGlobalSequenceNumber(currentPosition) {
				records <- pipes[i].currentRecord
				currentPosition = pipes[i].currentRecord.GlobalSequenceNumber

				if !pipes[i].ReadNext() {
					pipes = remove(pipes, i)
					break
				}
			}
		}
	}()

	return records
}

func remove(s []*pipe, i int) []*pipe {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func getIndexWithSmallestGlobalSequenceNumber(pipes []*pipe) int {
	smallestIndex := 0
	min := pipes[smallestIndex].currentRecord.GlobalSequenceNumber
	for i, pipe := range pipes {
		if pipe.currentRecord.GlobalSequenceNumber < min {
			smallestIndex = i
			min = pipe.currentRecord.GlobalSequenceNumber
		}
	}
	return smallestIndex
}
