package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/go/pb"
)

type Event struct {
	EventID  string
	Channel  string
	Metadata string
	Body     []byte
	ctx      context.Context
	client   pb.KubemqClient
	clientId string
}

func (e *Event) SetEventId(id string) *Event {
	e.EventID = id
	return e
}

func (e *Event) SetMetadata(metadata string) *Event {
	e.Metadata = metadata
	return e
}

func (e *Event) SetChannel(channel string) *Event {
	e.Channel = channel
	return e
}

func (e *Event) SetBody(body []byte) *Event {
	e.Body = body
	return e
}

func (e *Event) Send() error {
	result, err := e.client.SendEvent(e.ctx, &pb.Event{
		EventID:  e.EventID,
		ClientID: e.clientId,
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
