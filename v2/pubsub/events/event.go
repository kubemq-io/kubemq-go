package events

import (
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type Event struct {
	Id       string            `json:"id"`
	Channel  string            `json:"channel"`
	Metadata string            `json:"metadata"`
	Body     []byte            `json:"body"`
	Tags     map[string]string `json:"tags"`
}

func NewEvent() *Event {
	return &Event{}
}

func (e *Event) SetId(id string) *Event {
	e.Id = id
	return e
}

func (e *Event) SetChannel(channel string) *Event {
	e.Channel = channel
	return e
}

func (e *Event) SetMetadata(metadata string) *Event {
	e.Metadata = metadata
	return e
}

func (e *Event) SetBody(body []byte) *Event {
	e.Body = body
	return e
}

func (e *Event) SetTags(tags map[string]string) *Event {
	e.Tags = tags
	return e
}

func (e *Event) validate() error {
	if e.Channel == "" {
		return fmt.Errorf("event message must have a channel")
	}
	if e.Metadata == "" && e.Body == nil && len(e.Tags) == 0 {
		return fmt.Errorf("event message must have at least one of the following: metadata, body or tags")
	}
	return nil
}

func (e *Event) toEvent(clientId string) *pb.Event {
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
		Store:    false,
		Tags:     e.Tags,
	}
}
