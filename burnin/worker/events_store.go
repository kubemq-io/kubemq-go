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
	producersPerChannel int
	consumersPerChannel int
	consumerGroup       bool
	runID               string
}

// NewEventsStoreWorker creates a new events store pattern worker for a specific channel.
func NewEventsStoreWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, pc *config.PatternConfig, runID string,
	patternLatAccum *metrics.LatencyAccumulator) *EventsStoreWorker {

	bw := NewBaseWorker(PatternEventsStore, cfg, cp, logger, channelName, channelIndex, pc.Rate, patternLatAccum)
	bw.channelType = "events_store"
	return &EventsStoreWorker{
		BaseWorker:          bw,
		producersPerChannel: pc.ProducersPerChannel,
		consumersPerChannel: pc.ConsumersPerChannel,
		consumerGroup:       pc.ConsumerGroup,
		runID:               runID,
	}
}

// Start launches consumers for events store on this channel.
func (w *EventsStoreWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)

	// Determine consumer group
	group := ""
	if w.consumerGroup {
		group = ConsumerGroupName(w.runID, PatternEventsStore, w.channelIdx)
	}

	// Start consumers
	for i := 0; i < w.consumersPerChannel; i++ {
		consumerID := ConsumerID(PatternEventsStore, w.channelIdx, i)
		if err := w.startConsumer(ctx, consumerID, group); err != nil {
			return fmt.Errorf("start events_store consumer %s: %w", consumerID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	return nil
}

// StartProducers launches producer goroutines. Called after warmup.
func (w *EventsStoreWorker) StartProducers() {
	for i := 0; i < w.producersPerChannel; i++ {
		producerID := ProducerID(PatternEventsStore, w.channelIdx, i)
		w.startProducer(w.producerCtx, producerID)
	}
}

// startConsumer subscribes to the events store channel and processes incoming messages.
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

// startProducer launches a goroutine that sends events store messages via the streaming API.
func (w *EventsStoreWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ws := w.getOrCreateProducerStat(producerID)

		handle, err := w.Client().SendEventStoreStream(ctx)
		if err != nil {
			w.Logger().Error("failed to open event store stream", "producer", producerID, "error", err)
			w.RecordError("stream_open_failure")
			ws.errors.Add(1)
			return
		}
		defer handle.Close()

		var pendingMu sync.Mutex
		pending := make(map[string]uint64)

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

					w.unconfirmed.Add(-1)

					if result.Sent {
						if found {
							w.RecordSend(producerID, pendingSeq)
						}
					} else {
						metrics.IncUnconfirmed(w.Pattern())
						ws.errors.Add(1)
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

			pendingMu.Lock()
			pending[eventID] = currentSeq
			pendingMu.Unlock()
			w.unconfirmed.Add(1)

			sendStart := time.Now()
			sendErr := handle.Send(event)
			sendDuration := time.Since(sendStart)

			metrics.ObserveSendDuration(w.Pattern(), sendDuration)

			if sendErr != nil {
				pendingMu.Lock()
				delete(pending, eventID)
				pendingMu.Unlock()
				w.unconfirmed.Add(-1)

				w.RecordError("send_failure")
				ws.errors.Add(1)
				w.Logger().Error("failed to send event store message",
					"producer", producerID, "seq", currentSeq, "error", sendErr)
				continue
			}

			ws.latAccum.Record(sendDuration)
			w.RecordBytesSent(producerID, len(body))
		}
	}()
}
