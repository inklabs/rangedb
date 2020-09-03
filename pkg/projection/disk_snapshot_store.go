package projection

import (
	"fmt"
	"os"
)

type diskSnapshotStore struct {
	basePath string
}

func NewDiskSnapshotStore(basePath string) *diskSnapshotStore {
	return &diskSnapshotStore{
		basePath: basePath,
	}
}

func (d *diskSnapshotStore) Load(projection SnapshotProjection) error {
	file, err := os.Open(d.SnapshotPath(projection))
	if err != nil {
		return fmt.Errorf("unable to open snapshot file: %v", err)
	}
	defer file.Close()

	err = projection.LoadFromSnapshot(file)
	if err != nil {
		return fmt.Errorf("unable to load snapshot: %v", err)
	}

	return nil
}

func (d *diskSnapshotStore) Save(projection SnapshotProjection) error {
	file, err := os.Create(d.SnapshotPath(projection))
	if err != nil {
		return fmt.Errorf("unable to create/open snapshot file: %v", err)
	}
	defer file.Close()

	err = projection.SaveSnapshot(file)
	if err != nil {
		return fmt.Errorf("unable to save snapshot: %v", err)
	}

	_ = file.Sync()

	return nil
}

func (d *diskSnapshotStore) SnapshotPath(projection SnapshotProjection) string {
	return fmt.Sprintf("%s/%s.snap", d.basePath, projection.SnapshotName())
}
