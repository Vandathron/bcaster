package managers

import (
	"errors"
	"fmt"
	"github.com/vandathron/bcaster/internal/cfg"
	"github.com/vandathron/bcaster/internal/model"
	"github.com/vandathron/bcaster/internal/storage"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type ConsumerMgr struct {
	consumers       []*storage.Consumer
	topicToConsumer map[string][]*model.Consumer
	cfg             cfg.Consumer
	activeConsumer  *storage.Consumer
	lock            sync.Mutex
}

func NewConsumerMgr(cfg cfg.Consumer) (*ConsumerMgr, error) {

	if cfg.MaxSize == 0 || cfg.MaxSize < 1024*70 { // 70kb
		cfg.MaxSize = (1024 * 1024) / 0.5 // 0.5mb
	}

	m := &ConsumerMgr{cfg: cfg}
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

	for _, file := range consumerFiles {
		if file.IsDir() || path.Ext(file.Name()) != ".consumer" {
			continue
		}

		c, err := storage.NewConsumer(file.Name(), m.cfg.MaxSize)
		if err != nil {
			func() {
				for _, consumer := range m.consumers {
					_ = consumer.Close()
				}
			}()
			return nil, err
		}

		for i := 0; ; i++ {
			id, topic, readOff, err := c.Read(uint32(i), true)
			if err != nil {
				if err == io.EOF {
					break
				}
				_ = c.Close()
				return nil, err
			}
			topicStr := string(topic)
			m.topicToConsumer[topicStr] = append(m.topicToConsumer[topicStr], &model.Consumer{
				ID:         string(id),
				Topic:      topicStr,
				ReadOffset: readOff,
				Off:        uint32(i),
			})
		}
		m.consumers = append(m.consumers, c)
	}

	if len(m.consumers) == 0 {
		err := m.injectNewActiveConsumer("0.consumer")
		if err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *ConsumerMgr) Subscribe(consumer model.Consumer) error {
	if err := m.validate(consumer); err != nil {
		return err
	}
	// TODO: should lock/unlock here
	if consumers, ok := m.topicToConsumer[consumer.Topic]; ok {
		for _, c := range consumers {
			if c.ID == consumer.ID {
				return nil
			}
		}
	}

	off, err := m.activeConsumer.Append([]byte(consumer.ID), []byte(consumer.Topic), consumer.ReadOffset)
	if err != nil {
		if err == io.EOF {
			err = m.injectNewActiveConsumer(fmt.Sprintf("%v.consumers", len(m.consumers)))
			if err != nil {
				return err
			}
			return m.Subscribe(consumer)
		}
		return err
	}
	consumer.Off = off
	m.topicToConsumer[consumer.Topic] = append(m.topicToConsumer[consumer.Topic], &consumer)
	return nil
}

func (m *ConsumerMgr) Read(id, topic string) (readOff uint64, err error) {
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

func (m *ConsumerMgr) ReadTopic(topic string) ([]model.Consumer, error) {
	if consumers, ok := m.topicToConsumer[topic]; ok {
		var c []model.Consumer
		for _, con := range consumers {
			c = append(c, m.getConsumer(con))
		}
		return c, nil
	}

	return nil, fmt.Errorf("topic not found")
}
func (m *ConsumerMgr) Ack(id, topic string) error {
	if consumers, ok := m.topicToConsumer[topic]; ok {
		for _, c := range consumers {
			if c.ID == id {
				c.ReadOffset++
				return nil
			}
		}
		return fmt.Errorf("consumer not found for topic: %s", topic)
	}

	return fmt.Errorf("topic not found")
}

func (m *ConsumerMgr) Unsubscribe(id, topic string) error {
	if consumers, ok := m.topicToConsumer[topic]; ok {
		for _, c := range consumers {
			if c.ID == id {
				c.ReadOffset = 0 // TODO: Should empty consumer block in file
				return nil
			}
		}
		return fmt.Errorf("consumer not found for topic: %s", topic)
	}

	return fmt.Errorf("topic not found")
}

func (m *ConsumerMgr) Close() error {
	for _, consumer := range m.consumers {
		err := consumer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ConsumerMgr) getConsumer(c *model.Consumer) model.Consumer {
	return *c
}

func (m *ConsumerMgr) injectNewActiveConsumer(name string) error {
	p := path.Join(m.cfg.Dir, name)
	c, err := storage.NewConsumer(p, m.cfg.MaxSize)
	if err != nil {
		return err
	}
	m.consumers = append(m.consumers, c)
	m.activeConsumer = c
	return err
}
func (m *ConsumerMgr) validate(consumer model.Consumer) error {
	if len([]byte(consumer.Topic)) > storage.TopicSize {
		return errors.New("topic exceeds allowed size")
	}

	if len([]byte(consumer.ID)) > storage.IdSize {
		return errors.New("id exceeds allowed size")
	}

	return nil
}
