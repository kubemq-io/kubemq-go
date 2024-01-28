package events_store

import (
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

type EventStoreReceived struct {
	Id           string
	FromClientId string
	Timestamp    time.Time
	Sequence     uint64
	Channel      string
	Metadata     string
	Body         []byte
	Tags         map[string]string
}

func newEventStoreReceived() *EventStoreReceived {
	return &EventStoreReceived{}
}

func (s *EventStoreReceived) setId(id string) *EventStoreReceived {
	s.Id = id
	return s
}

func (s *EventStoreReceived) setFromClientId(fromClientId string) *EventStoreReceived {
	s.FromClientId = fromClientId
	return s
}
func (s *EventStoreReceived) setTimestamp(timestamp time.Time) *EventStoreReceived {
	s.Timestamp = timestamp
	return s
}

func (s *EventStoreReceived) setSequence(sequence uint64) *EventStoreReceived {
	s.Sequence = sequence
	return s
}
func (s *EventStoreReceived) setChannel(channel string) *EventStoreReceived {
	s.Channel = channel
	return s
}

func (s *EventStoreReceived) setMetadata(metadata string) *EventStoreReceived {
	s.Metadata = metadata
	return s
}

func (s *EventStoreReceived) setBody(body []byte) *EventStoreReceived {
	s.Body = body
	return s
}

func (s *EventStoreReceived) setTags(tags map[string]string) *EventStoreReceived {
	s.Tags = tags
	return s
}

func fromEventReceived(event *pb.EventReceive) *EventStoreReceived {
	fromClientId := ""
	if event.Tags != nil {
		fromClientId = event.Tags["x-kubemq-client-id"]
	}
	return newEventStoreReceived().
		setId(event.EventID).
		setSequence(event.Sequence).
		setTimestamp(time.Unix(0, event.Timestamp)).
		setFromClientId(fromClientId).
		setChannel(event.Channel).
		setMetadata(event.Metadata).
		setBody(event.Body).
		setTags(event.Tags)
}
