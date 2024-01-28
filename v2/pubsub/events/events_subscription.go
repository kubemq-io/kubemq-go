package events

import "fmt"

type EventsSubscription struct {
	Channel        string
	Group          string
	OnReceiveEvent func(event *EventReceived)
	OnError        func(err error)
}

func NewEventsEventSubscription() *EventsSubscription {
	return &EventsSubscription{}
}

func (s *EventsSubscription) SetChannel(value string) *EventsSubscription {
	s.Channel = value
	return s
}

func (s *EventsSubscription) SetGroup(value string) *EventsSubscription {
	s.Group = value
	return s
}

func (s *EventsSubscription) SetOnReceiveEvent(fn func(event *EventReceived)) *EventsSubscription {
	s.OnReceiveEvent = fn
	return s
}

func (s *EventsSubscription) SetOnError(value func(err error)) *EventsSubscription {
	s.OnError = value
	return s
}

func (s *EventsSubscription) Validate() error {
	if s.Channel == "" {
		return fmt.Errorf("event subscription must have a channel")
	}
	if s.OnReceiveEvent == nil {
		return fmt.Errorf("event subscription must have a OnReceiveEvent callback function")
	}
	return nil
}
