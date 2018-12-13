package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/go/pb"
)

type Event struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	ClientId string
	client   pb.KubemqClient
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
	result, err := e.client.SendEvent(ctx, &pb.Event{
		EventID:  e.Id,
		ClientID: e.ClientId,
		Channel:  e.Channel,
		Metadata: e.Metadata,
		Body:     e.Body,
	})
	if err != nil {
		return err
	}
	if !result.Sent {
		return fmt.Errorf("%s", result.Error)
	}
	return nil
}
