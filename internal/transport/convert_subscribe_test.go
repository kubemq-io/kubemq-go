package transport

import (
	"testing"

	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
)

func TestEventReceiveFromProto(t *testing.T) {
	pbEvent := &pb.EventReceive{
		EventID:  "evt-1",
		Channel:  "test-channel",
		Metadata: "meta",
		Body:     []byte("hello"),
		Tags:     map[string]string{"key": "val"},
	}

	item := EventReceiveFromProto(pbEvent)
	assert.Equal(t, "evt-1", item.ID)
	assert.Equal(t, "test-channel", item.Channel)
	assert.Equal(t, "meta", item.Metadata)
	assert.Equal(t, []byte("hello"), item.Body)
	assert.Equal(t, map[string]string{"key": "val"}, item.Tags)
}

func TestEventReceiveFromProto_Nil(t *testing.T) {
	assert.Nil(t, EventReceiveFromProto(nil))
}

func TestEventStoreReceiveFromProto(t *testing.T) {
	pbEvent := &pb.EventReceive{
		EventID:   "evt-2",
		Channel:   "store-channel",
		Metadata:  "store-meta",
		Body:      []byte("stored"),
		Timestamp: 1234567890,
		Sequence:  42,
		Tags:      map[string]string{"s": "v"},
	}

	item := EventStoreReceiveFromProto(pbEvent)
	assert.Equal(t, "evt-2", item.ID)
	assert.Equal(t, "store-channel", item.Channel)
	assert.Equal(t, uint64(42), item.Sequence)
	assert.Equal(t, int64(1234567890), item.Timestamp)
}

func TestEventStoreReceiveFromProto_Nil(t *testing.T) {
	assert.Nil(t, EventStoreReceiveFromProto(nil))
}

func TestCommandReceiveFromProto(t *testing.T) {
	pbReq := &pb.Request{
		RequestID:    "cmd-1",
		ClientID:     "client-1",
		Channel:      "cmd-channel",
		Metadata:     "cmd-meta",
		Body:         []byte("do-something"),
		ReplyChannel: "reply-ch",
		Tags:         map[string]string{"t": "v"},
	}

	item := CommandReceiveFromProto(pbReq)
	assert.Equal(t, "cmd-1", item.ID)
	assert.Equal(t, "client-1", item.ClientID)
	assert.Equal(t, "cmd-channel", item.Channel)
	assert.Equal(t, "reply-ch", item.ResponseTo)
	assert.Equal(t, []byte("do-something"), item.Body)
}

func TestCommandReceiveFromProto_Nil(t *testing.T) {
	assert.Nil(t, CommandReceiveFromProto(nil))
}

func TestQueryReceiveFromProto(t *testing.T) {
	pbReq := &pb.Request{
		RequestID:    "q-1",
		ClientID:     "client-2",
		Channel:      "q-channel",
		Metadata:     "q-meta",
		Body:         []byte("fetch"),
		ReplyChannel: "reply-q",
		Tags:         map[string]string{"a": "b"},
	}

	item := QueryReceiveFromProto(pbReq)
	assert.Equal(t, "q-1", item.ID)
	assert.Equal(t, "client-2", item.ClientID)
	assert.Equal(t, "q-channel", item.Channel)
	assert.Equal(t, "reply-q", item.ResponseTo)
}

func TestQueryReceiveFromProto_Nil(t *testing.T) {
	assert.Nil(t, QueryReceiveFromProto(nil))
}
