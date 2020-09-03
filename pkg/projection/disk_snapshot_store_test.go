package projection_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/projection"
)

func TestNewDiskSnapshotStore(t *testing.T) {
	t.Run("saves and loads data", func(t *testing.T) {
		// Given
		basePath := getBasePath(t)
		snapshotStore := projection.NewDiskSnapshotStore(basePath)
		dummySnapshotProjection := &dummySnapshotProjection{}

		// When
		require.NoError(t, snapshotStore.Save(dummySnapshotProjection))
		require.NoError(t, snapshotStore.Load(dummySnapshotProjection))

		// Then
		assert.Equal(t, ".", string(dummySnapshotProjection.bytesRead))
	})

	t.Run("fails to save from invalid base path", func(t *testing.T) {
		// Given
		basePath := "invalid-path"
		snapshotStore := projection.NewDiskSnapshotStore(basePath)
		dummySnapshotProjection := &dummySnapshotProjection{}

		// When
		err := snapshotStore.Save(dummySnapshotProjection)

		// Then
		assert.EqualError(t, err, "unable to create/open snapshot file: open invalid-path/testSnapshot.snap: no such file or directory")
	})

	t.Run("fails to save from failing snapshot load", func(t *testing.T) {
		// Given
		basePath := getBasePath(t)
		snapshotStore := projection.NewDiskSnapshotStore(basePath)
		failingSnapshotProjection := &failingSnapshotProjection{}
		_, err := os.Create(snapshotStore.SnapshotPath(failingSnapshotProjection))
		require.NoError(t, err)

		// When
		err = snapshotStore.Save(failingSnapshotProjection)

		// Then
		assert.EqualError(t, err, "unable to save snapshot: failingSnapshotProjection:SaveSnapshot")
	})

	t.Run("fails to load from missing file", func(t *testing.T) {
		// Given
		basePath := "invalid-path"
		snapshotStore := projection.NewDiskSnapshotStore(basePath)
		dummySnapshotProjection := &dummySnapshotProjection{}

		// When
		err := snapshotStore.Load(dummySnapshotProjection)

		// Then
		assert.EqualError(t, err, "unable to open snapshot file: open invalid-path/testSnapshot.snap: no such file or directory")
	})

	t.Run("fails to load from failing snapshot load", func(t *testing.T) {
		// Given
		basePath := getBasePath(t)
		snapshotStore := projection.NewDiskSnapshotStore(basePath)
		failingSnapshotProjection := &failingSnapshotProjection{}
		_, err := os.Create(snapshotStore.SnapshotPath(failingSnapshotProjection))
		require.NoError(t, err)

		// When
		err = snapshotStore.Load(failingSnapshotProjection)

		// Then
		assert.EqualError(t, err, "unable to load snapshot: failingSnapshotProjection:LoadFromSnapshot")
	})
}

func getBasePath(t *testing.T) string {
	basePath, err := ioutil.TempDir("", "snapshotstore_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(basePath))
	})
	return basePath
}

type dummySnapshotProjection struct {
	bytesRead []byte
}

func (d *dummySnapshotProjection) SnapshotName() string {
	return "testSnapshot"
}

func (d *dummySnapshotProjection) SaveSnapshot(w io.Writer) error {
	_, _ = io.WriteString(w, ".")
	return nil
}

func (d *dummySnapshotProjection) LoadFromSnapshot(r io.Reader) error {
	d.bytesRead, _ = ioutil.ReadAll(r)
	return nil
}

type failingSnapshotProjection struct{}

func (f failingSnapshotProjection) SnapshotName() string {
	return "testSnapshot"
}

func (f failingSnapshotProjection) SaveSnapshot(_ io.Writer) error {
	return fmt.Errorf("failingSnapshotProjection:SaveSnapshot")
}

func (f failingSnapshotProjection) LoadFromSnapshot(_ io.Reader) error {
	return fmt.Errorf("failingSnapshotProjection:LoadFromSnapshot")
}
