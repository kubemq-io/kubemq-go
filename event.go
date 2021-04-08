package kubemq

import (
	"context"
)

type Event struct {
	Id        string
	Channel   string
	Metadata  string
	Body      []byte
	ClientId  string
	Tags      map[string]string
	transport Transport
}

func NewEvent() *Event {
	return &Event{}
}

// SetId - set event id otherwise new random uuid will be set
func (e *Event) SetId(id string) *Event {
	e.Id = id
	return e
}

// SetClientId - set event ClientId - mandatory if default client was not set
func (e *Event) SetClientId(clientId string) *Event {
	e.ClientId = clientId
	return e
}

// SetMetadata - set event metadata - mandatory if body field was not set
func (e *Event) SetMetadata(metadata string) *Event {
	e.Metadata = metadata
	return e
}

// SetChannel - set event channel - mandatory if default channel was not set
func (e *Event) SetChannel(channel string) *Event {
	e.Channel = channel
	return e
}

// SetBody - set event body - mandatory if metadata field was not set
func (e *Event) SetBody(body []byte) *Event {
	e.Body = body
	return e
}

// SetTags - set key value tags to event message
func (e *Event) SetTags(tags map[string]string) *Event {
	e.Tags = map[string]string{}
	for key, value := range tags {
		e.Tags[key] = value
	}
	return e
}

// AddTag - add key value tags to event message
func (e *Event) AddTag(key, value string) *Event {
	if e.Tags == nil {
		e.Tags = map[string]string{}
	}
	e.Tags[key] = value
	return e
}

func (e *Event) Send(ctx context.Context) error {
	if e.transport == nil {
		return ErrNoTransportDefined
	}
	return e.transport.SendEvent(ctx, e)
}
