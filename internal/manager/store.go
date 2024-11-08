package manager

import (
	"github.com/vandathron/bcaster/internal/cfg"
	"github.com/vandathron/bcaster/internal/model"
	"github.com/vandathron/bcaster/internal/storage"
)

type Store struct {
	cMgr             *Consumer
	topicToPartition map[string]*storage.Partition
	config           cfg.Store
}

func NewStore(config cfg.Store) (*Store, error) {
	s := &Store{}
	s.topicToPartition = make(map[string]*storage.Partition)
	mgr, err := NewConsumerMgr(config.Consumer)
	if err != nil {
		return nil, err
	}
	s.cMgr = mgr
	s.config = config
	return s, nil
}

func (s *Store) Read(consumer model.Consumer) (msg []byte, err error) {
	readOff, err := s.cMgr.Read(consumer.ID, consumer.Topic)
	if err != nil {
		return nil, err
	}

	p, ok := s.topicToPartition[consumer.Topic]
	if !ok { // partition may have not been loaded or closed
		p, err = storage.NewPartition(consumer.Topic, s.config.Partition)

		if err != nil {
			return nil, err
		}
	}

	msg, err = p.Read(readOff)

	if err != nil {
		return nil, err
	}

	if consumer.AutoCommit {
		err = s.cMgr.Ack(consumer.ID, consumer.Topic)

		if err != nil {
			return nil, err
		}
	}

	return msg, nil
}

func (s *Store) Append(msg []byte, topic string) error {
	p, ok := s.topicToPartition[topic]
	var err error
	if !ok { // partition may have not been loaded or closed
		p, err = storage.NewPartition(topic, s.config.Partition)
		if err != nil {
			return err
		}
		s.topicToPartition[topic] = p
	}

	_, err = p.Append(msg)
	return err
}

func (s *Store) Close() error {
	if err := s.cMgr.Close(); err != nil {
		return err
	}

	for _, p := range s.topicToPartition {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}
