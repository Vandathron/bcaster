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
}

func NewConsumerMgr(cfg cfg.Consumer) (*ConsumerMgr, error) {
	if cfg.MaxSize > 1024*1024*2 {
		return nil, errors.New("consumer size too large")
	}
	if cfg.MaxSize == 0 || cfg.MaxSize < 1024*500 { // 500kb
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
