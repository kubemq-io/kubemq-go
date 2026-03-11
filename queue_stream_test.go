package kubemq

import (
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	"github.com/stretchr/testify/assert"
)

func TestNewQueueDownstreamSendRequest(t *testing.T) {
	req := NewQueueDownstreamSendRequest()
	assert.NotNil(t, req)
	assert.IsType(t, &transport.QueueDownstreamSendRequest{}, req)
}

func TestQueueDownstreamRequestTypeConstants(t *testing.T) {
	assert.Equal(t, int32(1), QueueDownstreamGet)
	assert.Equal(t, int32(2), QueueDownstreamAckAll)
	assert.Equal(t, int32(4), QueueDownstreamNAckAll)
	assert.Equal(t, int32(6), QueueDownstreamReQueueAll)
	assert.Equal(t, int32(10), QueueDownstreamCloseByClient)
}
