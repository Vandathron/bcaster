package manager

import (
	"github.com/stretchr/testify/require"
	"github.com/vandathron/bcaster/internal/cfg"
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

func TestStore_Append(t *testing.T) {
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

	err = store.Append([]byte("Hello world"), "created")
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

func TestStore_Read(t *testing.T) {

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

//
//
//
//
//
//
