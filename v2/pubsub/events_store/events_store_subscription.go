package events_store

import (
	"fmt"
	"time"
)

type EventStoreSubscription struct {
	Channel              string
	Group                string
	StartAt              StartAtType
	StartAtTimeValue     time.Time
	StartAtSequenceValue int64
	OnReceiveEvent       func(event *EventStoreReceived)
	OnError              func(err error)
}

func NewEventsEventSubscription() *EventStoreSubscription {
	return &EventStoreSubscription{}
}

func (s *EventStoreSubscription) SetChannel(value string) *EventStoreSubscription {
	s.Channel = value
	return s
}

func (s *EventStoreSubscription) SetGroup(value string) *EventStoreSubscription {
	s.Group = value
	return s
}

func (s *EventStoreSubscription) SetOnReceiveEvent(fn func(event *EventStoreReceived)) *EventStoreSubscription {
	s.OnReceiveEvent = fn
	return s
}

func (s *EventStoreSubscription) SetOnError(value func(err error)) *EventStoreSubscription {
	s.OnError = value
	return s
}

func (s *EventStoreSubscription) SetStartAt(value StartAtType) *EventStoreSubscription {
	s.StartAt = value
	return s
}

func (s *EventStoreSubscription) SetStartAtTimeValue(value time.Time) *EventStoreSubscription {
	s.StartAtTimeValue = value
	return s
}

func (s *EventStoreSubscription) SetStartAtSequenceValue(value int64) *EventStoreSubscription {
	s.StartAtSequenceValue = value
	return s
}

func (s *EventStoreSubscription) Validate() error {
	if s.Channel == "" {
		return fmt.Errorf("event subscription must have a channel")
	}
	if s.OnReceiveEvent == nil {
		return fmt.Errorf("event subscription must have a OnReceiveEvent callback function")
	}
	if s.StartAt == StartAtTypeUndefined {
		return fmt.Errorf("event subscription must have a StartAt value")
	}
	if s.StartAt == StartAtTypeFromTime && s.StartAtTimeValue.IsZero() {
		return fmt.Errorf("event subscription must have a StartAtTimeValue value")
	}
	if s.StartAt == StartAtTypeFromSequence && s.StartAtSequenceValue <= 0 {
		return fmt.Errorf("event subscription must have a StartAtSequenceValue value")
	}
	return nil
}
