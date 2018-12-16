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
	transport Transport
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

func (e *Event) Send(ctx context.Context) error {
	return e.transport.SendEvent(ctx, e)
}
