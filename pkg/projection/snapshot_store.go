package projection

import (
	"io"
)

// SnapshotStore defines the interface for loading/saving a SnapshotProjection.
type SnapshotStore interface {
	Load(projection SnapshotProjection) error
	Save(projection SnapshotProjection) error
}

// SnapshotProjection defines the interface for loading/saving a projection snapshot.
type SnapshotProjection interface {
	SnapshotName() string
	SaveSnapshot(w io.Writer) error
	LoadFromSnapshot(r io.Reader) error
}
