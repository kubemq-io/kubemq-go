package transport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithDefaultTimeout_NoDeadline(t *testing.T) {
	ctx := context.Background()
	newCtx, cancel := withDefaultTimeout(ctx, 5*time.Second)
	defer cancel()
	deadline, ok := newCtx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(5*time.Second), deadline, 200*time.Millisecond)
}

func TestWithDefaultTimeout_HasDeadline(t *testing.T) {
	userDeadline := time.Now().Add(30 * time.Second)
	ctx, userCancel := context.WithDeadline(context.Background(), userDeadline)
	defer userCancel()

	newCtx, cancel := withDefaultTimeout(ctx, 5*time.Second)
	defer cancel()
	deadline, ok := newCtx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, userDeadline, deadline)
}

func TestCloseOnce_DoOnce(t *testing.T) {
	co := closeOnce{}
	assert.True(t, co.Do())
	assert.False(t, co.Do())
}

func TestFirstNonEmpty_Variants(t *testing.T) {
	assert.Equal(t, "a", firstNonEmpty("a", "b"))
	assert.Equal(t, "b", firstNonEmpty("", "b"))
	assert.Equal(t, "", firstNonEmpty("", ""))
}
