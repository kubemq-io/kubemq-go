package middleware

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsIdempotent_SendRequest(t *testing.T) {
	assert.False(t, isIdempotent("/kubemq.Kubemq/SendRequest"))
}

func TestIsIdempotent_SendQueueMessage(t *testing.T) {
	assert.False(t, isIdempotent("/kubemq.Kubemq/SendQueueMessage"))
}

func TestIsIdempotent_SendQueueMessagesBatch(t *testing.T) {
	assert.False(t, isIdempotent("/kubemq.Kubemq/SendQueueMessagesBatch"))
}

func TestIsIdempotent_UnknownMethod(t *testing.T) {
	assert.True(t, isIdempotent("/kubemq.Kubemq/UnknownMethod"))
	assert.True(t, isIdempotent("/some.Other/Method"))
	assert.True(t, isIdempotent(""))
}
