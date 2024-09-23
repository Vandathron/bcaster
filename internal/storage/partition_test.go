package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestNewPartition(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	topic := "customer_created"
	config := PartitionConfig{
		dir:            dir,
		maxIdxSizeByte: 1024,
		maxMsgSizeByte: 800,
	}

	partition, err := NewPartition(topic, config)
	require.NoError(t, err)
	require.Equal(t, topic, partition.topic)
	require.Equal(t, 1, len(partition.segments)) // Initial empty partition should in turn contain one segment
	require.NotNil(t, partition.writableSegment)
	require.Equal(t, partition.writableSegment, partition.segments[0])
	require.Equal(t, uint32(0), partition.writableSegment.c.startOffset)
	require.Equal(t, filepath.Join(dir, "part_customer_created"), partition.Name())
	require.NoError(t, partition.Close())
}

func TestNewPartitionWithExistingSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	topic := "customer_created"
	config := PartitionConfig{
		dir:            dir,
		maxIdxSizeByte: 1024,
		maxMsgSizeByte: 800,
	}

	partition, err := NewPartition(topic, config)
	require.NoError(t, err)
	require.Equal(t, topic, partition.topic)
	require.Equal(t, 1, len(partition.segments)) // initial segment
	require.NotNil(t, partition.writableSegment)
	require.Nil(t, partition.Close())

	partition, err = NewPartition(topic, config) // reopen partition
	require.NoError(t, err)
	require.Equal(t, topic, partition.topic)
	require.Equal(t, 1, len(partition.segments)) // Still one segment as it's potentially latest and writable segment
	require.NotNil(t, partition.writableSegment)
	require.NoError(t, partition.Close())
}
