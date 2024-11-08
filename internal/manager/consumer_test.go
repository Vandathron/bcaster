package manager

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
		MaxSizeByte: 1024 * 600, // 600KB
		Dir:         dir,
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
	c := cfg.Consumer{
		MaxSizeByte: 1024 * 70, // 70kb
		Dir:         dir,
	}
	m, err := NewConsumerMgr(c)
	defer func() {
		m.Close()
		os.RemoveAll(dir)
	}()

	require.NoError(t, err)
	require.NotNil(t, m)
	var sub sync.WaitGroup
	sub.Add(5)

	// each consumer in file has a defined size of 78bytes hence a consumer file should be capable of storing max 918 entries
	// (considering maxSize defined above).

	// write 1k entries
	for i := 0; i < 5; i++ {
		go func() {
			defer sub.Done()
			for j := i * 200; j < (i*200)+200; j++ {
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
	requireCommon := func(m *Consumer) {
		require.NotNil(t, m.activeConsumer)
		require.Equal(t, 2, len(m.consumers))                                  // should have exactly 2 consumer files (918 & 82 entries)
		require.Equal(t, uint32(918-1), m.consumers[0].LatestCommitedOff())    // zero based offset
		require.Equal(t, uint32(918+82-1), m.consumers[1].LatestCommitedOff()) // zero based offset
	}

	requireCommon(m)
	require.Equal(t, 5, len(m.topicToConsumer))
	require.NoError(t, m.Close())

	// reopen manager
	m, err = NewConsumerMgr(c)
	require.NoError(t, err)
	requireCommon(m)

	// unsubscribe all consumers in user_created_1 topic
	for i := 200; i < 300; i++ {
		err := m.Unsubscribe("user_service_"+strconv.Itoa(i), "user_created_"+strconv.Itoa(1))
		require.NoError(t, err)
	}
	requireCommon(m)
	require.Equal(t, 5-1, len(m.topicToConsumer)) // topics should now be 4 as no consumers

}

func TestConsumerMgr_Read(t *testing.T) {
	dir, err := os.MkdirTemp("", "consumers")
	require.NoError(t, err)
	m, err := NewConsumerMgr(cfg.Consumer{
		MaxSizeByte: 1024 * 600,
		Dir:         dir,
	})
	require.NoError(t, err)
	defer func() {
		m.Close()
		os.RemoveAll(dir)
	}()

}
