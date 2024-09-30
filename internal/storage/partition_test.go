package storage

import (
	"encoding/json"
	"fmt"
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
		baseDir:        dir,
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
	config := getPartitionConfig(dir)

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

func TestPartition_BasicAppendRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	topic := "customer_created"
	config := getPartitionConfig(dir)

	partition, err := NewPartition(topic, config)
	require.NoError(t, err)
	msgByte := getTestMsgByte(0)
	require.NoError(t, err)
	offset, err := partition.Append(msgByte)
	require.NoError(t, err)
	require.Equal(t, uint32(0), offset)
	require.Equal(t, uint32(0), partition.writableSegment.c.startOffset)
	require.Equal(t, 1, len(partition.segments))
	msg, err := partition.Read(offset)
	require.NoError(t, err)
	require.Equal(t, msgByte, msg)
	require.NoError(t, partition.Close())
}

func TestPartition_MultiWritesReads(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_partition")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	topic := "customer_created"
	config := getPartitionConfig(dir)

	partition, err := NewPartition(topic, config)
	require.NoError(t, err)
	segmentCnt := uint64(len(partition.segments))
	require.Equal(t, uint64(1), segmentCnt)
	currMsgFileSizeByte := uint64(0)
	nextOffset := uint32(0)

	for i := 0; i < 200; i++ {
		msgByte := getTestMsgByte(i + 1)
		msgSizeByte := uint64(len(msgByte) + 8)
		offset, err := partition.Append(msgByte)
		require.NoError(t, err)
		require.Equal(t, uint32(i), offset)
		currMsgFileSizeByte += msgSizeByte // msgSizeByte varies
		currIdxEntrySizeByte := indexEntryWidth * uint64(offset+1)
		segmentLen := uint64(len(partition.segments))
		if currMsgFileSizeByte > config.maxMsgSizeByte*segmentLen || currIdxEntrySizeByte > config.maxIdxSizeByte*segmentLen { // indicates maxed out segment, hence partition should add new segment
			segmentCnt++
			require.Equal(t, int(segmentCnt), len(partition.segments), fmt.Sprintf("offset: %d \n msgSize: %d totalMsgSize: %d, max: %d", offset, msgSizeByte, currMsgFileSizeByte, config.maxMsgSizeByte*segmentCnt))
			require.Equal(t, partition.writableSegment, partition.segments[segmentCnt-1]) // latest segment should be the writable segment
		}
		nextOffset = offset + 1
	}

	require.NoError(t, partition.Close())
	// reopen partition
	partition, err = NewPartition(topic, config)
	require.NoError(t, err)
	require.Equal(t, segmentCnt, uint64(len(partition.segments)))
	require.Equal(t, partition.writableSegment, partition.segments[0]) // latest segment
	require.Equal(t, nextOffset, partition.writableSegment.nextOffset)

	for i := 0; i < 200; i++ {
		msg, err := partition.Read(uint32(i))
		require.NoError(t, err, fmt.Sprintf("offset: %d", i))
		m := message{}
		err = json.Unmarshal(msg, &m)
		require.NoError(t, err)
		require.Equal(t, i+1, m.Id)
	}

	require.NoError(t, partition.Close())
}

func getTestMsgByte(id int) []byte {
	msgByte, err := json.Marshal(message{
		Name:  "Van",
		Event: "New customer",
		Id:    id,
	})
	if err != nil {
		panic(err)
	}
	return msgByte
}

func getPartitionConfig(dir string) PartitionConfig {
	return PartitionConfig{
		baseDir:        dir,
		maxIdxSizeByte: 1024 * 500,
		maxMsgSizeByte: 1024 * 1024,
	}
}
