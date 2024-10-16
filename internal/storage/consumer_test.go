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

	c, err := NewConsumer(file.Name(), config{
		maxSize: 1024 * 1024,
	})

	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestConsumer_Append(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	c, err := NewConsumer(file.Name(), config{
		maxSize: 1024 * 1024,
	})
	require.NoError(t, err)

	id := []byte("new_server_20")
	nxtOff := uint64(20)
	pos, err := c.Append(id, nxtOff)
	require.NoError(t, err)
	require.Equal(t, uint32(0), pos)
	require.Equal(t, uint32(consumerSize*1), c.nextPos)
	require.NoError(t, c.Close())
}

func TestConsumer_AppendReadMultiple(t *testing.T) {
	file, err := os.CreateTemp("", "con.consumer")
	require.NoError(t, err)
	defer file.Close()
	defer os.RemoveAll(file.Name())

	c, err := NewConsumer(file.Name(), config{
		maxSize: 1024 * 1024,
	})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		id := []byte("new_server_" + strconv.Itoa(i))
		pos, err := c.Append(id, uint64(i))
		require.NoError(t, err)
		require.Equal(t, uint32(i*consumerSize), pos)
	}
	require.NoError(t, c.Close())

	// reopen file
	c, err = NewConsumer(file.Name(), config{
		maxSize: 1024 * 1024,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		idBytes, off, err := c.Read(uint32(i*consumerSize), true)
		require.NoError(t, err)
		expectedByte := []byte("new_server_" + strconv.Itoa(i))
		require.Equal(t, expectedByte, idBytes)
		require.Equal(t, uint64(i), off)
	}
	require.NoError(t, c.Close())

	// reopen file
	c, err = NewConsumer(file.Name(), config{
		maxSize: 1024 * 1024,
	})
	require.NoError(t, err)

	// re-read consumer, consumer next read offset should add by 1
	for i := 0; i < 20; i++ {
		_, off, err := c.Read(uint32(i*consumerSize), true)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), off)
	}
	require.NoError(t, c.Close())
}
