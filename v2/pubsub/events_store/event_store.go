package events_store

import (
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type EventStore struct {
	Id       string            `json:"id"`
	Channel  string            `json:"channel"`
	Metadata string            `json:"metadata"`
	Body     []byte            `json:"body"`
	Tags     map[string]string `json:"tags"`
}

func NewEventStore() *EventStore {
	return &EventStore{}
}

func (e *EventStore) SetId(id string) *EventStore {
	e.Id = id
	return e
}

func (e *EventStore) SetChannel(channel string) *EventStore {
	e.Channel = channel
	return e
}

func (e *EventStore) SetMetadata(metadata string) *EventStore {
	e.Metadata = metadata
	return e
}

func (e *EventStore) SetBody(body []byte) *EventStore {
	e.Body = body
	return e
}

func (e *EventStore) SetTags(tags map[string]string) *EventStore {
	e.Tags = tags
	return e
}

func (e *EventStore) validate() error {
	if e.Channel == "" {
		return fmt.Errorf("event store messsage must have a channel")
	}
	if e.Metadata == "" && e.Body == nil && len(e.Tags) == 0 {
		return fmt.Errorf("event store message must have at least one of the following: metadata, body or tags")
	}
	return nil
}

func (e *EventStore) toEventStore(clientId string) *pb.Event {
	if e.Id == "" {
		e.Id = uuid.New()
	}
	if e.Tags == nil {
		e.Tags = map[string]string{
			"x-kubemq-client-id": clientId,
		}
	}
	return &pb.Event{
		EventID:  e.Id,
		ClientID: clientId,
		Channel:  e.Channel,
		Metadata: e.Metadata,
		Body:     e.Body,
		Store:    true,
		Tags:     e.Tags,
	}
}
