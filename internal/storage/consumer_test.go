package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
)

func TestNewConsumer(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer file.Close()
	defer os.Remove(file.Name())

	c, err := NewConsumer(file.Name(), 1024*1024, 0)

	require.NoError(t, err)
	require.Equal(t, uint32(0), c.currSize)
	require.Equal(t, uint32(0), c.nextOff)
	require.NoError(t, c.Close())
}

func TestConsumer_Append(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	c, err := NewConsumer(file.Name(), 1024*1024, 0)
	require.NoError(t, err)
	id := []byte("new_server_20")
	topic := []byte("new_user")
	readOff := uint64(20)
	off, err := c.Append(id, topic, readOff)
	require.NoError(t, err)
	require.Equal(t, uint32(0), off)
	require.Equal(t, uint32(consumerSize), c.currSize)
	require.Equal(t, uint32(1), c.nextOff)
	require.NoError(t, c.Close())
}

func TestConsumer_AppendReadMultiple(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer func() {
		file.Close()
		os.RemoveAll(file.Name())
	}()

	c, err := NewConsumer(file.Name(), 1024*1024, 0)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		id := []byte("new_server_" + strconv.Itoa(i))
		topic := []byte("new_user_event_" + strconv.Itoa(i))
		off, err := c.Append(id, topic, uint64(i))
		require.NoError(t, err)
		require.Equal(t, uint32(i), off)
		require.Equal(t, uint32((i+1)*consumerSize), c.currSize)
	}
	require.NoError(t, c.Close())

	// reopen file
	c, err = NewConsumer(file.Name(), 1024*1024, 0)
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		id, topic, readOff, err := c.Read(uint32(i), false)
		require.NoError(t, err)
		expectedByte := []byte("new_server_" + strconv.Itoa(i))
		topicByte := []byte("new_user_event_" + strconv.Itoa(i))
		require.Equal(t, expectedByte, id)
		require.Equal(t, topicByte, topic)
		require.Equal(t, uint64(i), readOff)

		// test consumer id 5 offset was updated/increased
		if i == 5 {
			_, _, readOff, err = c.Read(uint32(i), false)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), readOff)
		}
	}
	require.NoError(t, c.Close())

	// reopen file
	c, err = NewConsumer(file.Name(), 1024*1024, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(20*consumerSize), c.currSize)
	require.Equal(t, uint32(20), c.nextOff)
	require.Equal(t, uint32(20-1), c.LatestCommitedOff())

	// re-read consumer 6, consumer next read offset should increase by 1
	id, topic, off, err := c.Read(6, true)
	require.NoError(t, err)
	require.Equal(t, []byte("new_server_6"), id)
	require.Equal(t, []byte("new_user_event_6"), topic)
	require.Equal(t, uint64(7), off)
	require.NoError(t, c.Close())
}

func TestConsumer_WriteAt(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	c, err := NewConsumer(file.Name(), 1024*1024, 0)
	require.NoError(t, err)

	// write 3 consumers
	for i := 0; i < 3; i++ {
		id := []byte("new_server_" + strconv.Itoa(i))
		topic := []byte("new_user_event_" + strconv.Itoa(i))
		off, err := c.Append(id, topic, uint64(i))
		require.NoError(t, err)
		require.Equal(t, uint32(i), off)
	}

	// update consumer 2
	require.NoError(t, c.WriteAt(uint32(1), []byte("updated server"), []byte("updated topic"), uint64(30)))

	// verify update
	idBytes, topic, readOff, err := c.Read(uint32(1), false)
	require.NoError(t, err)
	require.Equal(t, []byte("updated server"), idBytes)
	require.Equal(t, []byte("updated topic"), topic)
	require.Equal(t, uint64(30), readOff)

	idBytes, topic, _, err = c.Read(uint32(0), false)
	require.NoError(t, err)
	require.Equal(t, []byte("new_server_0"), idBytes)
	require.Equal(t, []byte("new_user_event_0"), topic)
}
