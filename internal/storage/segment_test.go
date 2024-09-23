package storage

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

const (
	maxIdxSizeByte     = uint64(1024)     // 1 KB
	maxMessageSizeByte = uint64(1024 * 3) // 3 KB
)

func TestNewSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment")

	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := segmentConfig{
		maxIdxSizeByte: uint64(1024),
		maxMsgSizeByte: uint64(1024 * 3),
		startOffset:    0,
	}

	segment, err := NewSegment(dir, c)
	require.NoError(t, err)

	testMaxSize(t, maxIdxSizeByte, maxMessageSizeByte, segment)
	msgByte, err := json.Marshal(message{Name: "Tosin", Event: "Test Segment"})
	msgSize := uint64(len(msgByte))
	require.NoError(t, err)
	off, err := segment.Append(msgByte)
	require.NoError(t, err)
	require.Equal(t, segment.msgFile.currSize, msgSize+8) // 8 bytes inclusive considering the length of message saved in an 8 bytes block
	require.Equal(t, uint32(0), off)
	require.NoError(t, segment.Close())
}

func TestSegment_Read(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := segmentConfig{
		maxIdxSizeByte: uint64(1024),
		maxMsgSizeByte: uint64(1024 * 3),
		startOffset:    0,
	}

	segment, err := NewSegment(dir, c)
	require.NoError(t, err)
	testMaxSize(t, maxIdxSizeByte, maxMessageSizeByte, segment)
	msgByte, err := json.Marshal(message{Name: "Tosin", Event: "Test Segment"})
	msgSize := uint64(len(msgByte))
	require.NoError(t, err)
	off, err := segment.Append(msgByte)
	require.NoError(t, err)
	require.Equal(t, segment.msgFile.currSize, msgSize+8)
	require.Equal(t, uint32(1), segment.nextOffset)
	require.Equal(t, uint32(0), off)
	msg, err := segment.Read(0)
	require.NoError(t, err)
	m := &message{}
	err = json.Unmarshal(msg, m)
	require.NoError(t, err)
	require.Equal(t, "Tosin", m.Name)
	require.Equal(t, "Test Segment", m.Event)
	require.NoError(t, segment.Close())
}

func TestSegment_Full(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := segmentConfig{
		maxIdxSizeByte: uint64(300),
		maxMsgSizeByte: uint64(300),
		startOffset:    0,
	}

	segment, err := NewSegment(dir, c)
	require.NoError(t, err)
	testMaxSize(t, 300, 300, segment)
	msgByte, err := json.Marshal(message{Name: "Tosin", Event: "Test Segment"})
	require.NoError(t, err)
	msgBlockSize := uint64(len(msgByte)) + 8 // msgSize is roughly 39 bytes + 8 bytes = 47 bytes
	for i := 0; i < 10; i++ {                // msgSize + 8 = roughly
		offset, err := segment.Append(msgByte)
		if i >= 6 {
			require.Error(t, io.EOF, err)
			continue
		}
		require.Equal(t, uint32(i+1), segment.nextOffset)
		require.Equal(t, uint64(i+1)*indexEntryWidth, segment.index.currSize)
		require.Equal(t, uint32(i), offset)
		require.Equal(t, uint64(offset+1)*msgBlockSize, segment.msgFile.currSize)
	}

	require.Equal(t, 6*msgBlockSize, segment.msgFile.currSize)
	require.NoError(t, segment.Close())
}

func TestSegment_ExistingSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := segmentConfig{
		maxIdxSizeByte: uint64(300),
		maxMsgSizeByte: uint64(300),
		startOffset:    0,
	}

	segment, err := NewSegment(dir, c)
	require.NoError(t, err)
	testMaxSize(t, 300, 300, segment)
	msgByte, err := json.Marshal(message{Name: "Tosin", Event: "Test Segment"})
	msgBlockSize := uint64(len(msgByte)) + 8
	require.NoError(t, err)
	offset, err := segment.Append(msgByte)
	require.Equal(t, uint32(0), offset)
	require.NoError(t, err)
	require.NoError(t, segment.Close())

	segment2, err := NewSegment(dir, c) // reopen files with existing data
	require.NoError(t, err)

	msgByte2, err := segment2.Read(uint32(0))
	require.NoError(t, err)
	require.Equal(t, msgByte, msgByte2)
	require.Equal(t, uint32(1), segment2.nextOffset)
	offset, err = segment2.Append(msgByte)
	require.NoError(t, err)
	require.Equal(t, uint32(1), offset)
	require.Equal(t, msgBlockSize*2, segment2.msgFile.currSize) // confirm 2 messages were appended
	require.NoError(t, segment2.Close())
}

func testMaxSize(t *testing.T, maxIdxSize, maxMsgSize uint64, s *Segment) {
	require.Equal(t, maxIdxSize, s.index.maxSize)
	require.Equal(t, maxMsgSize, s.msgFile.maxFileSize)
}

type message struct {
	Name  string
	Event string
}
