package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
)

// EventsWorker implements the Worker interface for the Events pub/sub pattern.
type EventsWorker struct {
	*BaseWorker
}

// NewEventsWorker creates a new events pattern worker.
func NewEventsWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *EventsWorker {
	bw := NewBaseWorker(PatternEvents, cfg, cp, logger)
	bw.channelType = "events"
	return &EventsWorker{BaseWorker: bw}
}

// Start launches consumers and producers for events.
func (w *EventsWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, "events"); err != nil {
		w.Logger().Error("failed to create events channel", "error", err)
		return fmt.Errorf("create events channel: %w", err)
	}

	// Determine consumer group
	group := ""
	if cfg.Concurrency.EventsConsumerGroup {
		group = "go_burnin_events_group"
	}

	// Start consumers first
	for i := 0; i < cfg.Concurrency.EventsConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%03d", PatternEvents, i)
		if err := w.startConsumer(ctx, consumerID, group); err != nil {
			return fmt.Errorf("start events consumer %s: %w", consumerID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start producers
	for i := 0; i < cfg.Concurrency.EventsProducers; i++ {
		producerID := fmt.Sprintf("p-%s-%03d", PatternEvents, i)
		w.startProducer(w.producerCtx, producerID)
	}

	return nil
}

// startConsumer subscribes to events and processes incoming messages.
func (w *EventsWorker) startConsumer(ctx context.Context, consumerID, group string) error {
	sub, err := w.Client().SubscribeToEvents(ctx, w.ChannelName(), group,
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			msg, decErr := payload.Decode(event.Body)
			if decErr != nil {
				w.Logger().Error("failed to decode event", "consumer", consumerID, "error", decErr)
				w.RecordError("decode_failure")
				return
			}
			w.RecordReceive(consumerID, event.Body, event.Tags["content_hash"], msg.ProducerID, msg.Sequence)
		}),
		kubemq.WithOnError(func(err error) {
			w.Logger().Error("consumer subscription error", "consumer", consumerID, "error", err)
			w.RecordError("subscription_error")
		}),
	)
	if err != nil {
		return fmt.Errorf("subscribe to events: %w", err)
	}

	// Monitor subscription in a goroutine
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		select {
		case <-ctx.Done():
		case <-sub.Done():
			w.Logger().Warn("consumer subscription closed", "consumer", consumerID)
			metrics.IncReconnection(w.Pattern())
			w.IncReconnection()
		}
	}()

	return nil
}

// startProducer launches a goroutine that sends events via the streaming API.
func (w *EventsWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		// Open the event stream
		handle, err := w.Client().SendEventStream(ctx)
		if err != nil {
			w.Logger().Error("failed to open event stream", "producer", producerID, "error", err)
			w.RecordError("stream_open_failure")
			return
		}
		defer handle.Close()

		// Drain stream errors in background
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-handle.Done:
					return
				case streamErr, ok := <-handle.Errors:
					if !ok {
						return
					}
					w.Logger().Error("event stream error", "producer", producerID, "error", streamErr)
					w.RecordError("stream_error")
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-handle.Done:
				w.Logger().Warn("event stream closed", "producer", producerID)
				return
			default:
			}

			if err := w.WaitForRate(ctx); err != nil {
				return
			}

			for w.BackpressureCheck() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}

			currentSeq := seq.Add(1)
			body, crcHex := payload.Encode(metrics.SDK(), w.Pattern(), producerID, currentSeq, w.MessageSize())

			event := &kubemq.Event{
				Id:       fmt.Sprintf("%s-%d", producerID, currentSeq),
				Channel:  w.ChannelName(),
				Metadata: "",
				Body:     body,
				ClientId: producerID,
				Tags:     map[string]string{"content_hash": crcHex},
			}

			sendStart := time.Now()
			sendErr := handle.Send(event)
			sendDuration := time.Since(sendStart)

			metrics.ObserveSendDuration(w.Pattern(), sendDuration)

			if sendErr != nil {
				w.RecordError("send_failure")
				w.Logger().Error("failed to send event", "producer", producerID, "seq", currentSeq, "error", sendErr)
				continue
			}

			metrics.AddBytesSent(w.Pattern(), float64(len(body)))
			w.RecordSend(producerID, currentSeq)
		}
	}()
}
