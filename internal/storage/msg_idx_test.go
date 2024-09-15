package storage

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestNewIndexTruncateFile(t *testing.T) {
	var indexFileSize = uint64(500)
	file, err := os.CreateTemp("", "index_file_test")
	require.NoError(t, err)
	defer func(name string) {
		file.Close()
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(file.Name())

	fInfo, err := os.Stat(file.Name())
	require.NoError(t, err)
	require.Equal(t, int64(0), fInfo.Size()) // Empty file at this point

	index, err := NewIndex(file.Name(), indexFileSize)
	require.NoError(t, err)
	fInfo, err = index.file.Stat()
	require.NoError(t, err)
	require.Equal(t, indexFileSize, uint64(fInfo.Size()))
	require.Equal(t, uint64(0), index.currSize) // Current size should be 0 (regardless of truncate()) as no message has been appended to file
	require.NoError(t, index.Close())
}

func TestIndexAppend(t *testing.T) {
	maxIndexSize := uint64(500)
	file, err := os.CreateTemp("", "index_file_test")
	require.NoError(t, err)
	defer func(name string) {
		file.Close()
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(file.Name())

	fInfo, err := os.Stat(file.Name())
	require.NoError(t, err)
	require.Equal(t, int64(0), fInfo.Size())
	index, err := NewIndex(file.Name(), maxIndexSize)
	require.NoError(t, err)
	err = index.Append(0, 10)
	require.NoError(t, err)
	require.NoError(t, index.Close())
	require.Equal(t, uint64(12), index.currSize)
}

func TestIndexRead(t *testing.T) {
	maxIndexSize := uint64(1024)
	file, err := os.CreateTemp("", "index_file_test")
	require.NoError(t, err)
	defer func(name string) {
		file.Close()
		err := os.Remove(name)
		if err != nil {
			panic(err)
		}
	}(file.Name())
	fInfo, err := os.Stat(file.Name())
	require.NoError(t, err)
	require.Equal(t, int64(0), fInfo.Size())

	index, err := NewIndex(file.Name(), maxIndexSize)
	require.NoError(t, err)
	idxSize, err := index.file.Stat()
	require.NoError(t, err)
	entryCount := 50
	for i := 0; i < entryCount; i++ {
		require.NoError(t, index.Append(uint32(i), uint64(5*i)), i)
	}

	for i := 0; i < entryCount; i++ {
		pos, err := index.Read(int32(i))
		require.NoError(t, err)
		require.Equal(t, uint64(i*5), pos)
	}

	require.Equal(t, uint64(entryCount*indexEntryWidth), index.currSize)
	require.Equal(t, int64(maxIndexSize), idxSize.Size())
	require.NoError(t, index.Close())
}