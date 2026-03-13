package kubemq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerInfo_String(t *testing.T) {
	si := &ServerInfo{Host: "localhost", Version: "2.0.0", ServerStartTime: 1000, ServerUpTimeSeconds: 3600}
	s := si.String()
	assert.Contains(t, s, "localhost")
	assert.Contains(t, s, "2.0.0")
}

func TestSubscription_IsDone_NotDone(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	sub := newSubscription("sub-1", cancel, done)
	assert.False(t, sub.IsDone())
	assert.Equal(t, "sub-1", sub.ID())
}

func TestSubscription_IsDone_Done(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sub := newSubscription("sub-2", cancel, done)
	close(done)
	assert.True(t, sub.IsDone())
}

func TestSubscription_Done_Channel(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	sub := newSubscription("sub-3", cancel, done)
	ch := sub.Done()
	assert.NotNil(t, ch)
	close(done)
	<-ch // should not block
}

func TestSubscription_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sub := newSubscription("sub-4", cancel, done)
	sub.Cancel()
	assert.Error(t, ctx.Err())
	close(done)
}
