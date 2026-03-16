package kubemq

import "fmt"

// Event is an outbound event message. It is NOT safe for concurrent use —
// create a new Event for each send operation. Do not share Event instances
// across goroutines.
//
// Fields:
//   - Id: unique event identifier. Auto-generated UUID if empty at send time.
//   - Channel: target channel name (required unless WithDefaultChannel is set).
//     Wildcards are not supported for publishing; they are only valid for
//     subscriptions.
//   - Metadata: arbitrary string metadata delivered to subscribers.
//   - Body: binary payload. At least one of Body or Metadata must be non-empty.
//   - ClientId: sender identifier. Auto-populated from client defaults if empty.
//   - Tags: key-value string pairs delivered alongside the event.
//
// Events are fire-and-forget: there is no delivery confirmation. If no
// subscriber is connected, the event is silently dropped. For persistent
// delivery, use EventStore instead.
//
// See also: SendEvent, SubscribeToEvents, EventStore.
type Event struct {
	Id       string
	Channel  string
	Metadata string
	Body     []byte
	ClientId string
	Tags     map[string]string
}

// NewEvent creates an empty Event.
func NewEvent() *Event {
	return &Event{}
}

// SetId sets the event ID. If not set, a random UUID is generated.
func (e *Event) SetId(id string) *Event {
	e.Id = id
	return e
}

// SetClientId sets the client identifier for this event.
func (e *Event) SetClientId(clientId string) *Event {
	e.ClientId = clientId
	return e
}

// SetMetadata sets the event metadata.
func (e *Event) SetMetadata(metadata string) *Event {
	e.Metadata = metadata
	return e
}

// SetChannel sets the target channel for this event.
func (e *Event) SetChannel(channel string) *Event {
	e.Channel = channel
	return e
}

// SetBody sets the event body payload.
func (e *Event) SetBody(body []byte) *Event {
	e.Body = body
	return e
}

// SetTags replaces all tags on this event.
func (e *Event) SetTags(tags map[string]string) *Event {
	e.Tags = make(map[string]string, len(tags))
	for key, value := range tags {
		e.Tags[key] = value
	}
	return e
}

// AddTag adds a single key-value tag to this event.
func (e *Event) AddTag(key, value string) *Event {
	if e.Tags == nil {
		e.Tags = map[string]string{}
	}
	e.Tags[key] = value
	return e
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (e *Event) Validate() error {
	return validateEvent(e, nil)
}

// String returns a human-readable representation of the event.
func (e *Event) String() string {
	return fmt.Sprintf("Id: %s, Channel: %s, Metadata: %s, Body: %s, ClientId: %s, Tags: %s",
		e.Id, e.Channel, e.Metadata, e.Body, e.ClientId, e.Tags)
}

// EventStreamResult represents a server acknowledgement for a streamed event,
// received via EventStreamHandle.Errors or EventStoreStreamHandle.Results.
//
// Fields:
//   - EventID: the Id of the event this result corresponds to.
//   - Sent: true if the server accepted the event.
//   - Error: non-empty if the server rejected the event. Contains the reason
//     for rejection (e.g., channel validation failure).
//
// See also: SendEventStream, SendEventStoreStream.
type EventStreamResult struct {
	EventID string
	Sent    bool
	Error   string
}
