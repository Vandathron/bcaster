package manager

import (
	"errors"
	"fmt"
	"github.com/vandathron/bcaster/internal/cfg"
	"github.com/vandathron/bcaster/internal/model"
	"github.com/vandathron/bcaster/internal/storage"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Consumer struct {
	consumers       []*storage.Consumer
	topicToConsumer map[string][]*model.Consumer
	cfg             cfg.Consumer
	activeConsumer  *storage.Consumer
	lock            sync.Mutex
	cOffLock        sync.Mutex
}

func NewConsumerMgr(cfg cfg.Consumer) (*Consumer, error) {

	if cfg.MaxSizeByte == 0 || cfg.MaxSizeByte < 1024*70 { // 70kb
		cfg.MaxSizeByte = (1024 * 1024) / 0.5 // 0.5mb
	}

	m := &Consumer{cfg: cfg}
	consumerFiles, err := os.ReadDir(m.cfg.Dir)
	if err != nil {
		return nil, err
	}

	sort.Slice(consumerFiles, func(i, j int) bool {
		parse := func(name string) int {
			i, _ := strconv.Atoi(strings.TrimSuffix(name, path.Ext(name)))
			return i
		}
		return parse(consumerFiles[i].Name()) < parse(consumerFiles[j].Name())
	})

	m.topicToConsumer = make(map[string][]*model.Consumer)
	for _, file := range consumerFiles {
		if file.IsDir() || path.Ext(file.Name()) != ".consumer" {
			continue
		}
		baseOff, _ := strconv.Atoi(strings.TrimSuffix(file.Name(), path.Ext(file.Name())))
		filePath := filepath.Join(m.cfg.Dir, file.Name())
		c, err := storage.NewConsumer(filePath, m.cfg.MaxSizeByte, uint32(baseOff))
		if err != nil {
			func() {
				for _, consumer := range m.consumers {
					_ = consumer.Close()
				}
			}()
			return nil, err
		}

		for i := baseOff; ; i++ {
			id, topic, readOff, err := c.Read(uint32(i), true)
			if err != nil {
				if err == io.EOF {
					break
				}
				_ = c.Close()
				return nil, err
			}
			topicStr := string(topic)
			idStr := string(id)
			if topicStr == "" && idStr == "" { // no need to load consumers that have unsubscribed
				continue
			}

			m.topicToConsumer[topicStr] = append(m.topicToConsumer[topicStr], &model.Consumer{
				ID:         string(id),
				Topic:      topicStr,
				ReadOffset: readOff,
				Off:        uint32(i),
			})
		}
		m.consumers = append(m.consumers, c)
		m.activeConsumer = c // updates eventually to latest
	}

	if len(m.consumers) == 0 {
		err := m.injectNewActiveConsumer("0.consumer", uint32(0))
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *Consumer) Add(consumer model.Consumer) error {
	if err := m.validate(consumer); err != nil {
		return err
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if consumers, ok := m.topicToConsumer[consumer.Topic]; ok {
		for _, c := range consumers {
			if c.ID == consumer.ID {
				return nil
			}
		}
	}

	off, err := m.activeConsumer.Append([]byte(consumer.ID), []byte(consumer.Topic), consumer.ReadOffset)
	if err != nil {
		if err == io.EOF { // indicates activeConsumer is maxed out. get consumer's latest committed offset
			baseOff := m.activeConsumer.LatestCommitedOff() + 1
			err = m.injectNewActiveConsumer(fmt.Sprintf("%v.consumer", baseOff), baseOff)
			if err != nil {
				return err
			}
			off, err = m.activeConsumer.Append([]byte(consumer.ID), []byte(consumer.Topic), consumer.ReadOffset)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	consumer.Off = off
	m.topicToConsumer[consumer.Topic] = append(m.topicToConsumer[consumer.Topic], &consumer)
	return nil
}

func (m *Consumer) Read(id, topic string) (readOff uint64, err error) {
	if err = m.validate(model.Consumer{
		ID:    id,
		Topic: topic,
	}); err != nil {
		return 0, err
	}

	if consumers, ok := m.topicToConsumer[topic]; ok {
		for _, c := range consumers {
			if c.ID == id {
				return c.ReadOffset, nil
			}
		}
		return 0, fmt.Errorf("consumer not found for topic: %s", topic)
	}

	return 0, fmt.Errorf("topic not found")
}

func (m *Consumer) ReadTopic(topic string) ([]model.Consumer, error) {
	if consumers, ok := m.topicToConsumer[topic]; ok {
		var c []model.Consumer
		for _, con := range consumers {
			c = append(c, m.getConsumer(con))
		}
		return c, nil
	}

	return nil, fmt.Errorf("topic not found")
}

func (m *Consumer) Ack(id, topic string) error {
	if consumers, ok := m.topicToConsumer[topic]; ok {
		for _, c := range consumers {
			if c.ID == id {
				m.cOffLock.Lock()
				c.ReadOffset++
				m.cOffLock.Unlock()
				return nil
			}
		}
		return fmt.Errorf("consumer not found for topic: %s", topic)
	}

	return fmt.Errorf("topic not found")
}

func (m *Consumer) Remove(id, topic string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if consumers, ok := m.topicToConsumer[topic]; ok {
		for i, c := range consumers {
			if c.ID == id {
				cs := m.consumerStoreByOffset(c.Off)
				if cs == nil {
					return fmt.Errorf("consumer %s not found for topic: %s", id, topic)
				}
				err := cs.WriteAt(c.Off, []byte{}, []byte{}, c.ReadOffset)
				if err != nil {
					return err
				}
				consumers = append(consumers[:i], consumers[i+1:]...)
				m.topicToConsumer[topic] = consumers
				if len(consumers) == 0 {
					delete(m.topicToConsumer, topic)
				}
				return nil
			}
		}
		return fmt.Errorf("consumer not found for topic: %s", topic)
	}

	return fmt.Errorf("topic not found")
}

func (m *Consumer) Close() error {
	for _, consumer := range m.consumers {
		err := consumer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Consumer) getConsumer(c *model.Consumer) model.Consumer {
	return *c
}

func (m *Consumer) injectNewActiveConsumer(name string, baseOff uint32) error {
	p := filepath.Join(m.cfg.Dir, name)
	c, err := storage.NewConsumer(p, m.cfg.MaxSizeByte, baseOff)
	if err != nil {
		return err
	}
	m.consumers = append(m.consumers, c)
	m.activeConsumer = c
	return err
}

func (m *Consumer) validate(consumer model.Consumer) error {
	if len([]byte(consumer.Topic)) > storage.TopicSize {
		return errors.New("topic exceeds allowed size")
	}

	if len([]byte(consumer.ID)) > storage.IdSize {
		return errors.New("id exceeds allowed size")
	}

	return nil
}

func (m *Consumer) consumerStoreByOffset(off uint32) *storage.Consumer {
	if len(m.consumers) == 0 {
		return nil
	}

	// m.consumers is sorted by baseOffset (ASC)
	for _, c := range m.consumers {
		if off <= c.LatestCommitedOff() {
			return c
		}
	}
	return nil
}
