package transport

import (
	"errors"
	"testing"
	"time"

	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
)

func TestConvertEventRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *SendEventRequest
	}{
		{
			name: "full event",
			req: &SendEventRequest{
				ID: "test-id", ClientID: "client-1", Channel: "ch1",
				Metadata: "meta", Body: []byte("body"),
				Tags: map[string]string{"k": "v"},
			},
		},
		{
			name: "minimal event",
			req:  &SendEventRequest{Channel: "ch1"},
		},
		{
			name: "nil tags",
			req:  &SendEventRequest{Channel: "ch1", Tags: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := EventToProto(tt.req, "default-client")
			assert.Equal(t, tt.req.ID, proto.EventID)
			assert.Equal(t, tt.req.Channel, proto.Channel)
			assert.Equal(t, tt.req.Metadata, proto.Metadata)
			assert.Equal(t, tt.req.Body, proto.Body)
			assert.False(t, proto.Store)
			if tt.req.ClientID != "" {
				assert.Equal(t, tt.req.ClientID, proto.ClientID)
			} else {
				assert.Equal(t, "default-client", proto.ClientID)
			}
		})
	}
}

func TestConvertEventStoreRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *SendEventStoreRequest
	}{
		{
			name: "full event store",
			req: &SendEventStoreRequest{
				ID: "test-id", ClientID: "client-1", Channel: "ch1",
				Metadata: "meta", Body: []byte("body"),
				Tags: map[string]string{"k": "v"},
			},
		},
		{
			name: "minimal event store",
			req:  &SendEventStoreRequest{Channel: "ch1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := EventStoreToProto(tt.req, "default-client")
			assert.Equal(t, tt.req.ID, proto.EventID)
			assert.Equal(t, tt.req.Channel, proto.Channel)
			assert.True(t, proto.Store)
		})
	}
}

func TestConvertEventStoreResult(t *testing.T) {
	t.Run("non-nil result", func(t *testing.T) {
		pr := &pb.Result{EventID: "ev-1", Sent: true}
		result := EventStoreResultFromProto(pr)
		assert.Equal(t, "ev-1", result.ID)
		assert.True(t, result.Sent)
	})

	t.Run("nil result", func(t *testing.T) {
		result := EventStoreResultFromProto(nil)
		assert.NotNil(t, result)
	})
}

func TestConvertCommandRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *SendCommandRequest
	}{
		{
			name: "full command",
			req: &SendCommandRequest{
				ID: "cmd-1", ClientID: "client-1", Channel: "ch1",
				Metadata: "meta", Body: []byte("body"),
				Timeout: 5 * time.Second,
				Tags:    map[string]string{"k": "v"},
			},
		},
		{
			name: "minimal command",
			req:  &SendCommandRequest{Channel: "ch1", Timeout: time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := CommandToProto(tt.req, "default-client")
			assert.Equal(t, tt.req.ID, proto.RequestID)
			assert.Equal(t, tt.req.Channel, proto.Channel)
			assert.Equal(t, tt.req.Metadata, proto.Metadata)
			assert.Equal(t, tt.req.Body, proto.Body)
			assert.Equal(t, int32(tt.req.Timeout.Milliseconds()), proto.Timeout)
			assert.Equal(t, pb.Request_Command, proto.RequestTypeData)
		})
	}
}

func TestConvertCommandResult(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		pr := &pb.Response{
			RequestID: "cmd-1",
			ClientID:  "responder",
			Executed:  true,
			Timestamp: 1000,
			Tags:      map[string]string{"k": "v"},
		}
		result := CommandResultFromProto(pr)
		assert.Equal(t, "cmd-1", result.CommandID)
		assert.Equal(t, "responder", result.ResponseClientID)
		assert.True(t, result.Executed)
		assert.Equal(t, time.Unix(1000, 0), result.ExecutedAt)
		assert.Equal(t, map[string]string{"k": "v"}, result.Tags)
	})

	t.Run("nil result", func(t *testing.T) {
		result := CommandResultFromProto(nil)
		assert.NotNil(t, result)
	})
}

func TestConvertQueryRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *SendQueryRequest
	}{
		{
			name: "full query",
			req: &SendQueryRequest{
				ID: "q-1", ClientID: "client-1", Channel: "ch1",
				Metadata: "meta", Body: []byte("body"),
				Timeout:  5 * time.Second,
				CacheKey: "cache-key",
				CacheTTL: 10 * time.Second,
				Tags:     map[string]string{"k": "v"},
			},
		},
		{
			name: "minimal query",
			req:  &SendQueryRequest{Channel: "ch1", Timeout: time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := QueryToProto(tt.req, "default-client")
			assert.Equal(t, tt.req.ID, proto.RequestID)
			assert.Equal(t, tt.req.Channel, proto.Channel)
			assert.Equal(t, pb.Request_Query, proto.RequestTypeData)
			assert.Equal(t, tt.req.CacheKey, proto.CacheKey)
		})
	}
}

func TestConvertQueryResult(t *testing.T) {
	t.Run("successful query", func(t *testing.T) {
		pr := &pb.Response{
			RequestID: "q-1",
			ClientID:  "responder",
			Executed:  true,
			Metadata:  "resp-meta",
			Body:      []byte("resp-body"),
			CacheHit:  true,
			Timestamp: 2000,
			Tags:      map[string]string{"k": "v"},
		}
		result := QueryResultFromProto(pr)
		assert.Equal(t, "q-1", result.QueryID)
		assert.True(t, result.Executed)
		assert.Equal(t, "resp-meta", result.Metadata)
		assert.Equal(t, []byte("resp-body"), result.Body)
		assert.True(t, result.CacheHit)
		assert.Equal(t, time.Unix(2000, 0), result.ExecutedAt)
	})

	t.Run("nil result", func(t *testing.T) {
		result := QueryResultFromProto(nil)
		assert.NotNil(t, result)
	})
}

func TestConvertResponseToProto(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		req := &SendResponseRequest{
			RequestID:  "req-1",
			ResponseTo: "reply-ch",
			Metadata:   "meta",
			Body:       []byte("body"),
			ClientID:   "client-1",
			ExecutedAt: time.Unix(3000, 0),
			Err:        errors.New("something failed"),
			Tags:       map[string]string{"k": "v"},
		}
		proto := ResponseToProto(req)
		assert.Equal(t, "req-1", proto.RequestID)
		assert.Equal(t, "reply-ch", proto.ReplyChannel)
		assert.Equal(t, "something failed", proto.Error)
		assert.False(t, proto.Executed)
	})

	t.Run("without error", func(t *testing.T) {
		req := &SendResponseRequest{
			RequestID: "req-2",
			ClientID:  "client-1",
		}
		proto := ResponseToProto(req)
		assert.Equal(t, "", proto.Error)
		assert.True(t, proto.Executed)
	})
}

func TestConvertQueueMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		msg  *QueueMessageItem
	}{
		{
			name: "full message with policy and attributes",
			msg: &QueueMessageItem{
				ID: "msg-1", ClientID: "client-1", Channel: "q1",
				Metadata: "meta", Body: []byte("body"),
				Tags: map[string]string{"k": "v"},
				Policy: &QueueMessagePolicy{
					ExpirationSeconds: 60,
					DelaySeconds:      5,
					MaxReceiveCount:   3,
					MaxReceiveQueue:   "dead-letter",
				},
				Attributes: &QueueMessageAttributes{
					Timestamp:         1000,
					Sequence:          42,
					ReceiveCount:      1,
					ReRouted:          false,
					ReRoutedFromQueue: "",
					ExpirationAt:      2000,
					DelayedTo:         1500,
				},
			},
		},
		{
			name: "minimal message",
			msg:  &QueueMessageItem{Channel: "q1"},
		},
		{
			name: "nil policy nil attributes nil tags",
			msg:  &QueueMessageItem{Channel: "q1", Policy: nil, Attributes: nil, Tags: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := QueueMessageToProto(tt.msg, "default-client")
			roundTripped := QueueMessageFromProto(proto)

			assert.Equal(t, tt.msg.ID, roundTripped.ID)
			assert.Equal(t, tt.msg.Channel, roundTripped.Channel)
			assert.Equal(t, tt.msg.Metadata, roundTripped.Metadata)
			assert.Equal(t, tt.msg.Body, roundTripped.Body)
			assert.Equal(t, tt.msg.Tags, roundTripped.Tags)

			if tt.msg.Policy != nil {
				assert.NotNil(t, roundTripped.Policy)
				assert.Equal(t, tt.msg.Policy.ExpirationSeconds, roundTripped.Policy.ExpirationSeconds)
				assert.Equal(t, tt.msg.Policy.DelaySeconds, roundTripped.Policy.DelaySeconds)
				assert.Equal(t, tt.msg.Policy.MaxReceiveCount, roundTripped.Policy.MaxReceiveCount)
				assert.Equal(t, tt.msg.Policy.MaxReceiveQueue, roundTripped.Policy.MaxReceiveQueue)
			} else {
				assert.Nil(t, roundTripped.Policy)
			}
		})
	}
}

func TestConvertQueueMessageFromProtoWithAttributes(t *testing.T) {
	pbMsg := &pb.QueueMessage{
		MessageID: "msg-1",
		ClientID:  "client-1",
		Channel:   "q1",
		Metadata:  "meta",
		Body:      []byte("body"),
		Tags:      map[string]string{"k": "v"},
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: 60,
			DelaySeconds:      5,
			MaxReceiveCount:   3,
			MaxReceiveQueue:   "dead-letter",
		},
		Attributes: &pb.QueueMessageAttributes{
			Timestamp:         1000,
			Sequence:          42,
			ReceiveCount:      1,
			ReRouted:          false,
			ReRoutedFromQueue: "",
			ExpirationAt:      2000,
			DelayedTo:         1500,
		},
	}

	item := QueueMessageFromProto(pbMsg)
	assert.NotNil(t, item)
	assert.Equal(t, "msg-1", item.ID)
	assert.NotNil(t, item.Attributes)
	assert.Equal(t, int64(1000), item.Attributes.Timestamp)
	assert.Equal(t, uint64(42), item.Attributes.Sequence)
	assert.Equal(t, 1, item.Attributes.ReceiveCount)
	assert.Equal(t, int64(2000), item.Attributes.ExpirationAt)
	assert.Equal(t, int64(1500), item.Attributes.DelayedTo)
}

func TestConvertNilFields(t *testing.T) {
	t.Run("nil queue message", func(t *testing.T) {
		result := QueueMessageFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("nil server info", func(t *testing.T) {
		result := ServerInfoFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("nil event store result", func(t *testing.T) {
		result := EventStoreResultFromProto(nil)
		assert.NotNil(t, result)
	})

	t.Run("nil command result", func(t *testing.T) {
		result := CommandResultFromProto(nil)
		assert.NotNil(t, result)
	})

	t.Run("nil query result", func(t *testing.T) {
		result := QueryResultFromProto(nil)
		assert.NotNil(t, result)
	})

	t.Run("nil send queue message result", func(t *testing.T) {
		result := SendQueueMessageResultFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("nil receive queue messages response", func(t *testing.T) {
		result := ReceiveQueueMessagesRespFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("nil ack all response", func(t *testing.T) {
		result := AckAllRespFromProto(nil)
		assert.Nil(t, result)
	})
}

func TestConvertServerInfoFromProto(t *testing.T) {
	pr := &pb.PingResult{
		Host:                "localhost",
		Version:             "2.0.0",
		ServerStartTime:     5000,
		ServerUpTimeSeconds: 100,
	}
	result := ServerInfoFromProto(pr)
	assert.Equal(t, "localhost", result.Host)
	assert.Equal(t, "2.0.0", result.Version)
	assert.Equal(t, int64(5000), result.ServerStartTime)
	assert.Equal(t, int64(100), result.ServerUpTimeSeconds)
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "a", firstNonEmpty("a", "b"))
	assert.Equal(t, "b", firstNonEmpty("", "b"))
	assert.Equal(t, "", firstNonEmpty("", ""))
}

func TestEventStreamItemToProto(t *testing.T) {
	tests := []struct {
		name     string
		item     *EventStreamItem
		clientID string
	}{
		{
			name: "full fields",
			item: &EventStreamItem{
				ID: "ev-1", ClientID: "item-client", Channel: "ch1",
				Metadata: "meta", Body: []byte("body"),
				Tags:  map[string]string{"k": "v"},
				Store: false,
			},
			clientID: "default-client",
		},
		{
			name: "empty clientID falls back to item.ClientID",
			item: &EventStreamItem{
				ID: "ev-2", ClientID: "item-client", Channel: "ch2",
			},
			clientID: "",
		},
		{
			name: "empty item.ClientID uses provided clientID",
			item: &EventStreamItem{
				ID: "ev-3", Channel: "ch3",
			},
			clientID: "default-client",
		},
		{
			name: "store true",
			item: &EventStreamItem{
				ID: "ev-4", Channel: "ch4", Store: true,
			},
			clientID: "default-client",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := EventStreamItemToProto(tt.item, tt.clientID)
			assert.Equal(t, tt.item.ID, proto.EventID)
			assert.Equal(t, tt.item.Channel, proto.Channel)
			assert.Equal(t, tt.item.Metadata, proto.Metadata)
			assert.Equal(t, tt.item.Body, proto.Body)
			assert.Equal(t, tt.item.Store, proto.Store)
			assert.Equal(t, tt.item.Tags, proto.Tags)
			if tt.item.ClientID != "" {
				assert.Equal(t, tt.item.ClientID, proto.ClientID)
			} else {
				assert.Equal(t, tt.clientID, proto.ClientID)
			}
		})
	}
}

func TestEventStreamResultFromProto(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		result := EventStreamResultFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("normal conversion", func(t *testing.T) {
		pr := &pb.Result{EventID: "ev-1", Sent: true, Error: ""}
		result := EventStreamResultFromProto(pr)
		assert.NotNil(t, result)
		assert.Equal(t, "ev-1", result.EventID)
		assert.True(t, result.Sent)
		assert.Empty(t, result.Error)
	})

	t.Run("with error", func(t *testing.T) {
		pr := &pb.Result{EventID: "ev-2", Sent: false, Error: "send failed"}
		result := EventStreamResultFromProto(pr)
		assert.NotNil(t, result)
		assert.Equal(t, "ev-2", result.EventID)
		assert.False(t, result.Sent)
		assert.Equal(t, "send failed", result.Error)
	})
}

func TestQueueUpstreamResponseFromProto(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		result := QueueUpstreamResponseFromProto(nil)
		assert.Nil(t, result)
	})

	t.Run("with results", func(t *testing.T) {
		pr := &pb.QueuesUpstreamResponse{
			RefRequestID: "ref-1",
			Results: []*pb.SendQueueMessageResult{
				{
					MessageID:    "msg-1",
					SentAt:       1000,
					ExpirationAt: 2000,
					DelayedTo:    1500,
					IsError:      false,
					Error:        "",
				},
				{
					MessageID:    "msg-2",
					SentAt:       1001,
					ExpirationAt: 0,
					DelayedTo:    0,
					IsError:      true,
					Error:        "queue full",
				},
			},
			IsError: false,
			Error:   "",
		}
		result := QueueUpstreamResponseFromProto(pr)
		assert.NotNil(t, result)
		assert.Equal(t, "ref-1", result.RefRequestID)
		assert.False(t, result.IsError)
		assert.Empty(t, result.Error)
		assert.Len(t, result.Results, 2)

		assert.Equal(t, "msg-1", result.Results[0].MessageID)
		assert.Equal(t, int64(1000), result.Results[0].SentAt)
		assert.Equal(t, int64(2000), result.Results[0].ExpirationAt)
		assert.Equal(t, int64(1500), result.Results[0].DelayedTo)
		assert.False(t, result.Results[0].IsError)

		assert.Equal(t, "msg-2", result.Results[1].MessageID)
		assert.True(t, result.Results[1].IsError)
		assert.Equal(t, "queue full", result.Results[1].Error)
	})

	t.Run("empty results", func(t *testing.T) {
		pr := &pb.QueuesUpstreamResponse{
			RefRequestID: "ref-2",
			Results:      nil,
			IsError:      true,
			Error:        "upstream error",
		}
		result := QueueUpstreamResponseFromProto(pr)
		assert.NotNil(t, result)
		assert.Equal(t, "ref-2", result.RefRequestID)
		assert.True(t, result.IsError)
		assert.Equal(t, "upstream error", result.Error)
		assert.Empty(t, result.Results)
	})
}
