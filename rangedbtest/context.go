package rangedbtest

import (
	"context"
	"testing"
	"time"
)

func TimeoutContext(t *testing.T) context.Context {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(done)
	return ctx
}
