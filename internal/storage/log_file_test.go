package storage

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestAppend_And_Read(t *testing.T) {
	file, err := os.CreateTemp("", "test_append")
	require.NoError(t, err)
	//defer os.Remove(file.Name())

	log, err := NewLogFile(file.Name(), 1024)
	require.NoError(t, err)
	currentPos := 0

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("I love golan!, %d", i))
		entryWidth := len(msg) + msgLenWidth
		testAppend(t, log, msg, int64(currentPos))
		msgBytes, err := log.Read(uint64(currentPos))
		require.NoError(t, err)
		require.Equal(t, msg, msgBytes)
		currentPos += entryWidth
	}

	require.NoError(t, log.Close())
}

func testAppend(t *testing.T, log *LogFile, msg []byte, expectedPos int64) {
	pos, err := log.Append(msg)
	require.NoError(t, err)
	require.Equal(t, uint64(expectedPos), pos)
}