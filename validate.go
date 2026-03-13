package kubemq

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxChannelLength  = 256
	maxBodySize       = 100 * 1024 * 1024 // 100 MB (matches gRPC max)
	maxTagKeyLength   = 256
	maxTagValueLength = 4096
)

var channelNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._\-/>*]+$`)

func validateChannel(channel string) error {
	if channel == "" {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "channel name is required",
			Cause:   ErrValidation,
		}
	}
	if !utf8.ValidString(channel) {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("channel name contains invalid UTF-8: %q", channel),
			Cause:   ErrValidation,
		}
	}
	if len(channel) > maxChannelLength {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("channel name exceeds maximum length of %d characters", maxChannelLength),
			Cause:   ErrValidation,
		}
	}
	if !channelNameRegex.MatchString(channel) {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("channel name %q contains invalid characters (allowed: a-z, A-Z, 0-9, '.', '_', '-', '/', '>', '*')", channel),
			Cause:   ErrValidation,
		}
	}
	if strings.HasSuffix(channel, ".") {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("channel name %q cannot end with '.'", channel),
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateChannelStrict(channel string) error {
	if err := validateChannel(channel); err != nil {
		return err
	}
	if strings.ContainsAny(channel, "*>") {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("wildcard characters '*' and '>' are not allowed in channel %q for this operation", channel),
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateClientID(id string) error {
	if id == "" {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "clientId is required (set via WithClientId or let the SDK auto-generate one)",
			Cause:   ErrValidation,
		}
	}
	if !utf8.ValidString(id) {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "clientId contains invalid UTF-8",
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateContent(metadata string, body []byte) error {
	if metadata == "" && len(body) == 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "at least one of metadata or body must be non-empty",
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateQueueReceive(maxMsgs int32, waitSec int32) error {
	if maxMsgs < 1 || maxMsgs > 1024 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("MaxNumberOfMessages must be between 1 and 1024, got %d", maxMsgs),
			Cause:   ErrValidation,
		}
	}
	if waitSec < 0 || waitSec > 3600 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("WaitTimeSeconds must be between 0 and 3600, got %d", waitSec),
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateMessageBody(body []byte, maxSize int) error {
	if maxSize <= 0 {
		maxSize = maxBodySize
	}
	if len(body) > maxSize {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: fmt.Sprintf("message body size %d bytes exceeds maximum of %d bytes", len(body), maxSize),
			Cause:   ErrValidation,
		}
	}
	return nil
}

func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if k == "" {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "tag key must be non-empty",
				Cause:   ErrValidation,
			}
		}
		if len(k) > maxTagKeyLength {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("tag key %q exceeds maximum length of %d", k, maxTagKeyLength),
				Cause:   ErrValidation,
			}
		}
		if len(v) > maxTagValueLength {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: fmt.Sprintf("tag value for key %q exceeds maximum length of %d", k, maxTagValueLength),
				Cause:   ErrValidation,
			}
		}
	}
	return nil
}

func validateEvent(e *Event, opts *Options) error {
	channel := e.Channel
	if channel == "" && opts != nil {
		channel = opts.defaultChannel
	}
	if err := validateChannelStrict(channel); err != nil {
		return err
	}
	if err := validateContent(e.Metadata, e.Body); err != nil {
		return err
	}
	maxSize := maxBodySize
	if opts != nil && opts.maxSendMsgSize > 0 {
		maxSize = opts.maxSendMsgSize
	}
	if err := validateMessageBody(e.Body, maxSize); err != nil {
		return err
	}
	if err := validateTags(e.Tags); err != nil {
		return err
	}
	return nil
}

func validateEventStore(es *EventStore, opts *Options) error {
	channel := es.Channel
	if channel == "" && opts != nil {
		channel = opts.defaultChannel
	}
	if err := validateChannelStrict(channel); err != nil {
		return err
	}
	if err := validateContent(es.Metadata, es.Body); err != nil {
		return err
	}
	maxSize := maxBodySize
	if opts != nil && opts.maxSendMsgSize > 0 {
		maxSize = opts.maxSendMsgSize
	}
	if err := validateMessageBody(es.Body, maxSize); err != nil {
		return err
	}
	if err := validateTags(es.Tags); err != nil {
		return err
	}
	return nil
}

func validateCommand(cmd *Command, opts *Options) error {
	channel := cmd.Channel
	if channel == "" && opts != nil {
		channel = opts.defaultChannel
	}
	if err := validateChannelStrict(channel); err != nil {
		return err
	}
	if cmd.Timeout <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "command timeout must be positive",
			Cause:   ErrValidation,
		}
	}
	if err := validateContent(cmd.Metadata, cmd.Body); err != nil {
		return err
	}
	maxSize := maxBodySize
	if opts != nil && opts.maxSendMsgSize > 0 {
		maxSize = opts.maxSendMsgSize
	}
	if err := validateMessageBody(cmd.Body, maxSize); err != nil {
		return err
	}
	if err := validateTags(cmd.Tags); err != nil {
		return err
	}
	return nil
}

func validateQuery(q *Query, opts *Options) error {
	channel := q.Channel
	if channel == "" && opts != nil {
		channel = opts.defaultChannel
	}
	if err := validateChannelStrict(channel); err != nil {
		return err
	}
	if q.Timeout <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "query timeout must be positive",
			Cause:   ErrValidation,
		}
	}
	if q.CacheKey != "" && q.CacheTTL <= 0 {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "CacheTTL must be > 0 when CacheKey is set",
			Cause:   ErrValidation,
		}
	}
	if err := validateContent(q.Metadata, q.Body); err != nil {
		return err
	}
	maxSize2 := maxBodySize
	if opts != nil && opts.maxSendMsgSize > 0 {
		maxSize2 = opts.maxSendMsgSize
	}
	if err := validateMessageBody(q.Body, maxSize2); err != nil {
		return err
	}
	if err := validateTags(q.Tags); err != nil {
		return err
	}
	return nil
}

func validateQueueMessage(msg *QueueMessage, opts *Options) error {
	channel := msg.Channel
	if channel == "" && opts != nil {
		channel = opts.defaultChannel
	}
	if err := validateChannelStrict(channel); err != nil {
		return err
	}
	if err := validateContent(msg.Metadata, msg.Body); err != nil {
		return err
	}
	maxSize := maxBodySize
	if opts != nil && opts.maxSendMsgSize > 0 {
		maxSize = opts.maxSendMsgSize
	}
	if err := validateMessageBody(msg.Body, maxSize); err != nil {
		return err
	}
	if err := validateTags(msg.Tags); err != nil {
		return err
	}
	if msg.Policy != nil {
		if msg.Policy.ExpirationSeconds < 0 {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "expiration seconds must be non-negative",
				Cause:   ErrValidation,
			}
		}
		if msg.Policy.DelaySeconds < 0 {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "delay seconds must be non-negative",
				Cause:   ErrValidation,
			}
		}
		if msg.Policy.MaxReceiveCount < 0 {
			return &KubeMQError{
				Code:    ErrCodeValidation,
				Message: "max receive count must be non-negative",
				Cause:   ErrValidation,
			}
		}
	}
	return nil
}

func validateResponse(r *Response) error {
	if r.RequestId == "" {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "response requestId is required",
			Cause:   ErrValidation,
		}
	}
	if r.ResponseTo == "" {
		return &KubeMQError{
			Code:    ErrCodeValidation,
			Message: "response responseTo channel is required",
			Cause:   ErrValidation,
		}
	}
	return nil
}
