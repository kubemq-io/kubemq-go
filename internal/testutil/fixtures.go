package testutil

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
)

var seq atomic.Int64

func nextSeq() int64 { return seq.Add(1) }

// UniqueChannel returns a channel name unique to the current test.
func UniqueChannel(t *testing.T, pattern string) string {
	t.Helper()
	return fmt.Sprintf("test-%s-%s-%d", pattern, t.Name(), nextSeq())
}

// SampleSendEventRequest returns a SendEventRequest with all fields populated.
func SampleSendEventRequest(channel string) *transport.SendEventRequest {
	return &transport.SendEventRequest{
		ID:       fmt.Sprintf("evt-%d", nextSeq()),
		ClientID: "test-client",
		Channel:  channel,
		Metadata: "test-metadata",
		Body:     []byte("test-body"),
		Tags:     map[string]string{"env": "test"},
	}
}

// SampleSendCommandRequest returns a SendCommandRequest with all fields populated.
func SampleSendCommandRequest(channel string) *transport.SendCommandRequest {
	return &transport.SendCommandRequest{
		ID:       fmt.Sprintf("cmd-%d", nextSeq()),
		ClientID: "test-client",
		Channel:  channel,
		Metadata: "test-metadata",
		Body:     []byte("test-body"),
		Tags:     map[string]string{"env": "test"},
	}
}

// SampleSendQueryRequest returns a SendQueryRequest with all fields populated.
func SampleSendQueryRequest(channel string) *transport.SendQueryRequest {
	return &transport.SendQueryRequest{
		ID:       fmt.Sprintf("qry-%d", nextSeq()),
		ClientID: "test-client",
		Channel:  channel,
		Metadata: "test-metadata",
		Body:     []byte("test-body"),
		Tags:     map[string]string{"env": "test"},
	}
}

// SampleQueueMessageItem returns a QueueMessageItem with all fields populated.
func SampleQueueMessageItem(channel string) *transport.QueueMessageItem {
	return &transport.QueueMessageItem{
		ID:       fmt.Sprintf("qmsg-%d", nextSeq()),
		ClientID: "test-client",
		Channel:  channel,
		Metadata: "test-metadata",
		Body:     []byte("test-body"),
		Tags:     map[string]string{"env": "test"},
	}
}

// LargePayload returns a byte slice of the specified size in bytes.
func LargePayload(sizeBytes int) []byte {
	b := make([]byte, sizeBytes)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}
