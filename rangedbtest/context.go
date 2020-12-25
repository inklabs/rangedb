package rangedbtest

import (
	"context"
	"time"
)

type cleaner interface {
	Cleanup(f func())
}

func TimeoutContext(c cleaner) context.Context {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	c.Cleanup(done)
	return ctx
}
