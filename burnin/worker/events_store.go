package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
)

// EventsStoreWorker implements the Worker interface for the Events Store pattern.
type EventsStoreWorker struct {
	*BaseWorker
}

// NewEventsStoreWorker creates a new events store pattern worker.
func NewEventsStoreWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *EventsStoreWorker {
	bw := NewBaseWorker(PatternEventsStore, cfg, cp, logger)
	bw.channelType = "events_store"
	return &EventsStoreWorker{
		BaseWorker: bw,
	}
}

// Start launches consumers and producers for events store.
func (w *EventsStoreWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, "events_store"); err != nil {
		w.Logger().Error("failed to create events_store channel", "error", err)
		return fmt.Errorf("create events_store channel: %w", err)
	}

	// Determine consumer group
	group := ""
	if cfg.Concurrency.EventsStoreConsGroup {
		group = "go_burnin_events_store_group"
	}

	// Start consumers first
	for i := 0; i < cfg.Concurrency.EventsStoreConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%03d", PatternEventsStore, i)
		if err := w.startConsumer(ctx, consumerID, group); err != nil {
			return fmt.Errorf("start events_store consumer %s: %w", consumerID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start producers
	for i := 0; i < cfg.Concurrency.EventsStoreProducers; i++ {
		producerID := fmt.Sprintf("p-%s-%03d", PatternEventsStore, i)
		w.startProducer(w.producerCtx, producerID)
	}

	return nil
}

// startConsumer subscribes to the events store channel and processes incoming messages.
// It automatically re-subscribes when the subscription is closed by the server.
func (w *EventsStoreWorker) startConsumer(ctx context.Context, consumerID, group string) error {
	var lastReceivedSeq atomic.Uint64

	onReceive := kubemq.WithOnEventStoreReceive(func(receive *kubemq.EventStoreReceive) {
		msg, decErr := payload.Decode(receive.Body)
		if decErr != nil {
			w.Logger().Error("failed to decode events_store message", "consumer", consumerID, "error", decErr)
			w.RecordError("decode_failure")
			return
		}
		lastReceivedSeq.Store(receive.Sequence)
		w.RecordReceive(consumerID, receive.Body, receive.Tags["content_hash"], msg.ProducerID, msg.Sequence)
	})

	onError := kubemq.WithOnError(func(err error) {
		w.Logger().Error("consumer subscription error", "consumer", consumerID, "error", err)
		w.RecordError("subscription_error")
	})

	// Initial subscription starts from new events
	sub, err := w.Client().SubscribeToEventsStore(ctx, w.ChannelName(), group,
		kubemq.StartFromNewEvents(), onReceive, onError)
	if err != nil {
		return fmt.Errorf("subscribe to events_store: %w", err)
	}

	// Monitor subscription and re-subscribe on closure
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		currentSub := sub
		for {
			select {
			case <-ctx.Done():
				return
			case <-currentSub.Done():
				seq := lastReceivedSeq.Load()
				w.Logger().Warn("consumer subscription closed, re-subscribing",
					"consumer", consumerID,
					"last_received_seq", seq)
				metrics.IncReconnection(w.Pattern())
				w.IncReconnection()
				w.StartDowntime()

				// Retry loop to re-subscribe
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}

					newSub, subErr := w.Client().SubscribeToEventsStore(ctx, w.ChannelName(), group,
						kubemq.StartFromSequence(int(seq+1)), onReceive, onError)
					if subErr != nil {
						w.Logger().Error("re-subscribe failed, retrying",
							"consumer", consumerID, "error", subErr)
						w.RecordError("resubscribe_failure")
						continue
					}

					w.StopDowntime()
					w.Logger().Info("re-subscribed successfully",
						"consumer", consumerID,
						"from_sequence", seq+1)
					currentSub = newSub
					break
				}
			}
		}
	}()

	return nil
}

// startProducer launches a goroutine that sends events store messages via the streaming API
// and awaits confirmations before counting messages as sent.
func (w *EventsStoreWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		// Open the event store stream
		handle, err := w.Client().SendEventStoreStream(ctx)
		if err != nil {
			w.Logger().Error("failed to open event store stream", "producer", producerID, "error", err)
			w.RecordError("stream_open_failure")
			return
		}
		defer handle.Close()

		// Pending confirmations: eventID -> sequence number
		var pendingMu sync.Mutex
		pending := make(map[string]uint64)

		// Drain results channel in background to process confirmations
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-handle.Done:
					return
				case result, ok := <-handle.Results:
					if !ok {
						return
					}

					pendingMu.Lock()
					pendingSeq, found := pending[result.EventID]
					if found {
						delete(pending, result.EventID)
					}
					pendingMu.Unlock()

					if result.Sent {
						if found {
							w.RecordSend(producerID, pendingSeq)
						}
					} else {
						metrics.IncUnconfirmed(w.Pattern())
						errMsg := result.Error
						if errMsg == "" {
							errMsg = "unconfirmed"
						}
						w.Logger().Warn("event store message not confirmed",
							"producer", producerID,
							"event_id", result.EventID,
							"error", errMsg)
					}
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-handle.Done:
				w.Logger().Warn("event store stream closed", "producer", producerID)
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
			eventID := fmt.Sprintf("%s-%d", producerID, currentSeq)

			event := &kubemq.EventStore{
				Id:       eventID,
				Channel:  w.ChannelName(),
				Metadata: "",
				Body:     body,
				ClientId: producerID,
				Tags:     map[string]string{"content_hash": crcHex},
			}

			// Register pending before sending
			pendingMu.Lock()
			pending[eventID] = currentSeq
			pendingMu.Unlock()

			sendStart := time.Now()
			sendErr := handle.Send(event)
			sendDuration := time.Since(sendStart)

			metrics.ObserveSendDuration(w.Pattern(), sendDuration)

			if sendErr != nil {
				// Remove from pending on send failure
				pendingMu.Lock()
				delete(pending, eventID)
				pendingMu.Unlock()

				w.RecordError("send_failure")
				w.Logger().Error("failed to send event store message",
					"producer", producerID, "seq", currentSeq, "error", sendErr)
				continue
			}

			metrics.AddBytesSent(w.Pattern(), float64(len(body)))
		}
	}()
}
