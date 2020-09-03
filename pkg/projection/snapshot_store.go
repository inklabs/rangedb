package projection

import (
	"io"
)

type SnapshotStore interface {
	Load(projection SnapshotProjection) error
	Save(projection SnapshotProjection) error
}

type SnapshotProjection interface {
	SnapshotName() string
	SaveSnapshot(w io.Writer) error
	LoadFromSnapshot(r io.Reader) error
}
