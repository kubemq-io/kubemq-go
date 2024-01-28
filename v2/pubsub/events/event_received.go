package events

import (
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

type EventReceived struct {
	Id           string
	FromClientId string
	Timestamp    time.Time
	Channel      string
	Metadata     string
	Body         []byte
	Tags         map[string]string
}

func newEventReceived() *EventReceived {
	return &EventReceived{}
}

func (s *EventReceived) setId(id string) *EventReceived {
	s.Id = id
	return s
}

func (s *EventReceived) setFromClientId(fromClientId string) *EventReceived {
	s.FromClientId = fromClientId
	return s
}
func (s *EventReceived) setTimestamp(timestamp time.Time) *EventReceived {
	s.Timestamp = timestamp
	return s
}
func (s *EventReceived) setChannel(channel string) *EventReceived {
	s.Channel = channel
	return s
}

func (s *EventReceived) setMetadata(metadata string) *EventReceived {
	s.Metadata = metadata
	return s
}

func (s *EventReceived) setBody(body []byte) *EventReceived {
	s.Body = body
	return s
}

func (s *EventReceived) setTags(tags map[string]string) *EventReceived {
	s.Tags = tags
	return s
}

func fromEvent(event *pb.EventReceive) *EventReceived {
	fromClientId := ""
	if event.Tags != nil {
		fromClientId = event.Tags["x-kubemq-client-id"]
	}
	return newEventReceived().
		setId(event.EventID).
		setTimestamp(time.Now()).
		setFromClientId(fromClientId).
		setChannel(event.Channel).
		setMetadata(event.Metadata).
		setBody(event.Body).
		setTags(event.Tags)
}
