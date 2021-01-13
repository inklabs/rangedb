package recordsubscriber

import (
	"context"
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
)

// GetRecordsIteratorFunc defines a function to get events from a rangedb.Store.
// Typically from store.Events or store.Events store.EventsByAggregateTypes.
type GetRecordsIteratorFunc func(globalSequenceNumber uint64) rangedb.RecordIterator

// ConsumeRecordFunc defines a function to receiver a rangedb.Record
type ConsumeRecordFunc func(record *rangedb.Record) error

// SubscribeFunc defines a function to receive a record subscriber
type SubscribeFunc func(subscriber broadcast.RecordSubscriber)

type recordSubscriber struct {
	stopChan                 chan struct{}
	finishChan               chan struct{}
	bufferedRecords          chan *rangedb.Record
	getRecords               GetRecordsIteratorFunc
	consumeRecord            ConsumeRecordFunc
	subscribe                SubscribeFunc
	unsubscribe              SubscribeFunc
	doneChan                 <-chan struct{}
	lastGlobalSequenceNumber uint64
	totalRecordsSent         uint64 // TODO: Remove and refactor zero based sequence numbers to begin with 1
	closeOnce                sync.Once
}

// New constructs a record subscriber.
func New(config Config) *recordSubscriber {
	return &recordSubscriber{
		stopChan:        make(chan struct{}),
		finishChan:      make(chan struct{}),
		bufferedRecords: make(chan *rangedb.Record, config.BufferSize),
		getRecords:      config.GetRecords,
		consumeRecord:   config.ConsumeRecord,
		subscribe:       config.Subscribe,
		unsubscribe:     config.Unsubscribe,
		doneChan:        config.DoneChan,
	}
}

func (s *recordSubscriber) Done() <-chan struct{} {
	return s.finishChan
}

func (s *recordSubscriber) Receiver() broadcast.SendRecordChan {
	return s.bufferedRecords
}

func (s *recordSubscriber) StartFrom(globalSequenceNumber uint64) error {
	s.lastGlobalSequenceNumber = globalSequenceNumber
	err := s.writeRecords(globalSequenceNumber)
	if err != nil {
		return err
	}

	s.subscribe(s)

	err = s.writeRecords(s.lastGlobalSequenceNumber + 1)
	if err != nil {
		s.unsubscribe(s)
		return err
	}

	go s.work()

	return nil
}

func (s *recordSubscriber) Start() error {
	select {
	case <-s.doneChan:
		return context.Canceled

	default:
	}

	s.subscribe(s)
	go s.work()
	return nil
}

func (s *recordSubscriber) work() {
	for {
		select {
		case <-s.stopChan:
			s.unsubscribe(s)
			close(s.finishChan)
			return

		case <-s.doneChan:
			s.unsubscribe(s)
			close(s.finishChan)
			return

		case record := <-s.bufferedRecords:
			if s.recordHasAlreadyBeenSent(record) {
				continue
			}

			err := s.writeRecord(record)
			if err != nil {
				s.unsubscribe(s)
				return
			}
		}
	}
}

func (s *recordSubscriber) recordHasAlreadyBeenSent(record *rangedb.Record) bool {
	return s.totalRecordsSent > 0 && record.GlobalSequenceNumber <= s.lastGlobalSequenceNumber
}

func (s *recordSubscriber) Stop() {
	s.closeOnce.Do(func() {
		close(s.stopChan)
	})
}

func (s *recordSubscriber) writeRecords(globalSequenceNumber uint64) error {
	iter := s.getRecords(globalSequenceNumber)
	for iter.Next() {
		if iter.Err() != nil {
			return iter.Err()
		}

		err := s.writeRecord(iter.Record())
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *recordSubscriber) writeRecord(record *rangedb.Record) error {
	s.lastGlobalSequenceNumber = record.GlobalSequenceNumber
	s.totalRecordsSent++
	return s.consumeRecord(record)
}
