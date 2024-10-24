package managers

import (
	"github.com/stretchr/testify/require"
	"github.com/vandathron/bcaster/internal/cfg"
	"github.com/vandathron/bcaster/internal/model"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestNewConsumerMgr(t *testing.T) {
	dir, err := os.MkdirTemp("", "consumers")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	m, err := NewConsumerMgr(cfg.Consumer{
		MaxSize: 1024 * 600, // 600KB
		Dir:     dir,
	})

	require.NoError(t, err)
	require.NotNil(t, m)
	c := model.Consumer{
		ID:         "user_service",
		Topic:      "user_created",
		ReadOffset: 50,
	}
	err = m.Subscribe(c)
	require.NoError(t, err)

	require.NotNil(t, m.activeConsumer)
	require.Equal(t, 1, len(m.consumers))
	require.Equal(t, 1, len(m.topicToConsumer))
	require.Equal(t, 1, len(m.topicToConsumer[c.Topic]))
}

func TestConsumerMgr_Subscribe(t *testing.T) {
	dir, err := os.MkdirTemp("", "consumers")
	require.NoError(t, err)

	m, err := NewConsumerMgr(cfg.Consumer{
		MaxSize: 1024 * 600,
		Dir:     dir,
	})
	defer func() {
		//os.RemoveAll(dir)
		m.Close()
	}()

	require.NoError(t, err)
	require.NotNil(t, m)
	var sub sync.WaitGroup
	sub.Add(5)

	for i := 0; i < 5; i++ {
		go func() {
			defer sub.Done()
			for j := i * 20; j < (i*20)+20; j++ {
				c := model.Consumer{
					ID:         "user_service_" + strconv.Itoa(j),
					Topic:      "user_created_" + strconv.Itoa(i),
					ReadOffset: uint64(j + 1),
				}
				err = m.Subscribe(c)
				require.NoError(t, err)
			}
		}()
	}
	sub.Wait()
	require.NotNil(t, m.activeConsumer)
	require.Equal(t, 1, len(m.consumers))
	require.Equal(t, 5, len(m.topicToConsumer))
}
