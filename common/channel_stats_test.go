package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueuesStats_String(t *testing.T) {
	s := &QueuesStats{Messages: 100, Volume: 5000}
	assert.NotEmpty(t, s.String())
	assert.Contains(t, s.String(), "100")
}

func TestQueuesChannel_String(t *testing.T) {
	c := &QueuesChannel{Name: "test-queue", Type: "queues", IsActive: true}
	assert.NotEmpty(t, c.String())
	assert.Contains(t, c.String(), "test-queue")
}

func TestPubSubStats_String(t *testing.T) {
	s := &PubSubStats{Messages: 200, Volume: 10000}
	assert.NotEmpty(t, s.String())
}

func TestPubSubChannel_String(t *testing.T) {
	c := &PubSubChannel{Name: "test-pubsub"}
	assert.NotEmpty(t, c.String())
	assert.Contains(t, c.String(), "test-pubsub")
}

func TestCQStats_String(t *testing.T) {
	s := &CQStats{Messages: 300, Volume: 15000}
	assert.NotEmpty(t, s.String())
}

func TestCQChannel_String(t *testing.T) {
	c := &CQChannel{Name: "test-cq"}
	assert.NotEmpty(t, c.String())
	assert.Contains(t, c.String(), "test-cq")
}
