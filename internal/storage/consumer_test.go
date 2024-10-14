package storage

import (
	"github.com/stretchr/testify/require"
	"os"
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
	defer
}