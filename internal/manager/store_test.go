package manager

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/vandathron/bcaster/internal/cfg"
	"github.com/vandathron/bcaster/internal/model"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNewStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")

	defer func() {
		_ = os.RemoveAll(dir)
	}()
	require.NoError(t, err)
	cDir := filepath.Join(dir, "consumers")
	pDir := filepath.Join(dir, "partitions")
	require.NoError(t, os.MkdirAll(cDir, 0666))
	require.NoError(t, os.MkdirAll(pDir, 0666))

	config := getCfg(cDir, pDir)

	store, err := NewStore(config)
	require.NoError(t, err)
	require.NotNil(t, store)
	require.Equal(t, 0, len(store.topicToPartition))
	require.NotNil(t, store.cMgr)
}

func TestStore_Append_Read(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	require.NoError(t, err)
	cDir := filepath.Join(dir, "consumers")
	pDir := filepath.Join(dir, "partitions")
	require.NoError(t, os.MkdirAll(cDir, 0666))
	require.NoError(t, os.MkdirAll(pDir, 0666))
	config := getCfg(cDir, pDir)

	store, err := NewStore(config)
	require.NoError(t, err)

	topic := "topic_A"
	err = store.Append([]byte("Hello world"), topic)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	store, err = NewStore(config)
	require.NoError(t, err)

	c := model.Consumer{ID: "new_consumer", Topic: topic}
	c.AutoCommit = true

	require.NoError(t, store.AddConsumer(c))
	msgByte, err := store.Read(c)
	require.Equal(t, io.EOF, err)
	require.Nil(t, msgByte)

	require.NoError(t, store.Append([]byte("Second hello world"), topic))
	require.NoError(t, store.AddConsumer(c)) // expects not to duplicate consumer in storage
	msgByte, err = store.Read(c)
	require.NoError(t, err)
	require.Equal(t, []byte("Second hello world"), msgByte)
	require.NoError(t, store.Append([]byte("Third hello world"), topic))
	require.NoError(t, store.RemoveConsumer(c))

	msgByte, err = store.Read(c) // attempts to consumer messages for a consumer already removed from topic
	require.NotNil(t, err)
	require.Equal(t, fmt.Sprintf("consumer not found for topic: %s", topic), err.Error())
	require.Nil(t, msgByte)
	require.NoError(t, store.Close())
}

func getCfg(cdir, pDir string) cfg.Store {
	return cfg.Store{
		Consumer: cfg.Consumer{
			MaxSizeByte: 1024 * 50,
			Dir:         cdir,
		},
		Partition: cfg.Partition{
			Dir: pDir,
			Segment: cfg.Segment{
				MaxIdxSizeByte: 1024 * 20,
				MaxMsgSizeByte: 1024 * 1024 / 2,
			},
		},
	}
}
