// Package otel provides OpenTelemetry semantic convention constants for the
// KubeMQ SDK instrumentation. These follow OTel messaging semconv v1.27.0.
package otel

import (
	"go.opentelemetry.io/otel/attribute"
)

const (
	InstrumentationName = "github.com/kubemq-io/kubemq-go/v2"
)

// Messaging semantic convention attributes (OTel semconv v1.27.0).
const (
	AttrMessagingSystem          = "messaging.system"
	AttrMessagingOperationName   = "messaging.operation.name"
	AttrMessagingOperationType   = "messaging.operation.type"
	AttrMessagingDestinationName = "messaging.destination.name"
	AttrMessagingMessageID       = "messaging.message.id"
	AttrMessagingClientID        = "messaging.client.id"
	AttrMessagingConsumerGroup   = "messaging.consumer.group.name"
	AttrMessagingMessageBodySize = "messaging.message.body.size"
	AttrMessagingBatchCount      = "messaging.batch.message_count"
)

// Server attributes.
const (
	AttrServerAddress = "server.address"
	AttrServerPort    = "server.port"
)

// Error attributes.
const (
	AttrErrorType = "error.type"
)

// Retry span event attributes.
const (
	AttrRetryAttempt      = "retry.attempt"
	AttrRetryDelaySeconds = "retry.delay_seconds"
)

// System value.
const MessagingSystemKubeMQ = "kubemq"

// Operation name values.
const (
	OpPublish = "publish"
	OpProcess = "process"
	OpReceive = "receive"
	OpSettle  = "settle"
	OpSend    = "send"
)

// MessagingSystemAttr returns the messaging.system attribute.
func MessagingSystemAttr() attribute.KeyValue {
	return attribute.String(AttrMessagingSystem, MessagingSystemKubeMQ)
}
