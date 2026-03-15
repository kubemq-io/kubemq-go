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

// QueueSimpleWorker implements the Worker interface for the simple (unary)
// queue API pattern using SendQueueMessage / ReceiveQueueMessages.
type QueueSimpleWorker struct {
	*BaseWorker
}

// NewQueueSimpleWorker creates a new queue simple pattern worker.
func NewQueueSimpleWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *QueueSimpleWorker {
	bw := NewBaseWorker(PatternQueueSimple, cfg, cp, logger)
	// Override channel name to avoid conflict with queue_stream worker
	bw.channelName = fmt.Sprintf("go_burnin_%s_queue_simple_001", cfg.RunID)
	bw.channelType = "queues"
	return &QueueSimpleWorker{BaseWorker: bw}
}

// Start launches consumers and producers for the simple queue pattern.
func (w *QueueSimpleWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, kubemq.ChannelTypeQueues); err != nil {
		w.Logger().Error("failed to create queue simple channel", "error", err)
		return fmt.Errorf("create queue simple channel: %w", err)
	}

	// Start consumers first
	for i := 0; i < cfg.Concurrency.QueueSimpleConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%03d", PatternQueueSimple, i)
		w.startConsumer(ctx, consumerID)
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start producers
	for i := 0; i < cfg.Concurrency.QueueSimpleProducers; i++ {
		producerID := fmt.Sprintf("p-%s-%03d", PatternQueueSimple, i)
		w.startProducer(w.producerCtx, producerID)
	}

	return nil
}

// startProducer launches a goroutine that sends queue messages via the unary API.
func (w *QueueSimpleWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for {
			select {
			case <-ctx.Done():
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

			msg := &kubemq.QueueMessage{
				ClientID: producerID,
				Channel:  w.ChannelName(),
				Metadata: "",
				Body:     body,
				Tags:     map[string]string{"content_hash": crcHex},
			}

			sendStart := time.Now()
			result, err := w.Client().SendQueueMessage(ctx, msg)
			sendDuration := time.Since(sendStart)
			metrics.ObserveSendDuration(w.Pattern(), sendDuration)

			if err != nil {
				w.RecordError("send_failure")
				w.Logger().Error("failed to send queue message",
					"producer", producerID, "seq", currentSeq, "error", err)
				continue
			}

			if result.IsError {
				w.RecordError("send_result_error")
				w.Logger().Error("queue send result error",
					"producer", producerID, "seq", currentSeq, "error", result.Error)
				continue
			}

			// Only record send after confirmed success
			metrics.AddBytesSent(w.Pattern(), float64(len(body)))
			w.RecordSend(producerID, currentSeq)
		}
	}()
}

// startConsumer launches a goroutine that polls for queue messages via the unary API.
func (w *QueueSimpleWorker) startConsumer(ctx context.Context, consumerID string) {
	cfg := w.Config()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			resp, err := w.Client().ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
				Channel:             w.ChannelName(),
				MaxNumberOfMessages: int32(cfg.Queue.PollMaxMessages),
				WaitTimeSeconds:     int32(cfg.Queue.PollWaitTimeoutSeconds),
			})
			if err != nil {
				// Check if context cancelled (normal shutdown)
				if ctx.Err() != nil {
					return
				}
				w.RecordError("receive_failure")
				w.Logger().Error("failed to receive queue messages",
					"consumer", consumerID, "error", err)
				// Brief backoff before retry
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}

			if resp.IsError {
				w.RecordError("receive_result_error")
				w.Logger().Error("queue receive result error",
					"consumer", consumerID, "error", resp.Error)
				continue
			}

			// Process received messages (auto-consumed on receipt, no explicit ack needed)
			for _, qMsg := range resp.Messages {
				if qMsg == nil {
					continue
				}

				msg, decErr := payload.Decode(qMsg.Body)
				if decErr != nil {
					w.Logger().Error("failed to decode queue simple message",
						"consumer", consumerID, "error", decErr)
					w.RecordError("decode_failure")
					continue
				}

				w.RecordReceive(consumerID, qMsg.Body,
					qMsg.Tags["content_hash"], msg.ProducerID, msg.Sequence)
			}
		}
	}()
}
