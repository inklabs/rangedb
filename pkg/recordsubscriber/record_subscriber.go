package recordsubscriber

import (
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
)

type GetRecordsIteratorFunc func(globalSequenceNumber uint64) rangedb.RecordIterator

type ConsumeRecordFunc func(record *rangedb.Record) error

type SubscribeFunc func(subscriber broadcast.RecordSubscriber)

type recordSubscriber struct {
	bufferedRecords          chan *rangedb.Record
	getRecords               GetRecordsIteratorFunc
	consumeRecord            ConsumeRecordFunc
	subscribe                SubscribeFunc
	unsubscribe              SubscribeFunc
	doneChan                 <-chan struct{}
	lastGlobalSequenceNumber uint64
	totalEventsSent          uint64 // TODO: Remove and refactor zero based sequence numbers to begin with 1

	closeOnce sync.Once
	stopChan  chan struct{}
}

func New(config Config) *recordSubscriber {
	return &recordSubscriber{
		stopChan:        make(chan struct{}),
		bufferedRecords: make(chan *rangedb.Record, config.BufferSize),
		getRecords:      config.GetRecords,
		consumeRecord:   config.ConsumeRecord,
		subscribe:       config.Subscribe,
		unsubscribe:     config.Unsubscribe,
		doneChan:        config.DoneChan,
	}
}

func (s *recordSubscriber) Receiver() broadcast.SendRecordChan {
	return s.bufferedRecords
}

func (s *recordSubscriber) StartFrom(globalSequenceNumber uint64) error {
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
	s.subscribe(s)
	go s.work()
	return nil
}

func (s *recordSubscriber) work() {
	for {
		select {
		case <-s.stopChan:
			s.unsubscribe(s)
			return

		case <-s.doneChan:
			s.unsubscribe(s)
			return

		case record := <-s.bufferedRecords:
			if record.GlobalSequenceNumber <= s.lastGlobalSequenceNumber && s.totalEventsSent > 0 {
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
	s.totalEventsSent++
	return s.consumeRecord(record)
}
