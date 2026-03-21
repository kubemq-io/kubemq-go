package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/burnin/config"
	"github.com/kubemq-io/kubemq-go/v2/burnin/metrics"
	"github.com/kubemq-io/kubemq-go/v2/burnin/payload"
)

// QueueSimpleWorker implements the Worker interface for the simple (unary)
// queue API pattern using SendQueueMessage / PollQueue.
type QueueSimpleWorker struct {
	*BaseWorker
	producersPerChannel int
	consumersPerChannel int
}

// NewQueueSimpleWorker creates a new queue simple pattern worker for a specific channel.
func NewQueueSimpleWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, pc *config.PatternConfig,
	patternLatAccum *metrics.LatencyAccumulator) *QueueSimpleWorker {

	bw := NewBaseWorker(PatternQueueSimple, cfg, cp, logger, channelName, channelIndex, pc.Rate, patternLatAccum)
	bw.channelType = "queues"
	return &QueueSimpleWorker{
		BaseWorker:          bw,
		producersPerChannel: pc.ProducersPerChannel,
		consumersPerChannel: pc.ConsumersPerChannel,
	}
}

// Start launches consumers for the simple queue pattern on this channel.
func (w *QueueSimpleWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)

	// Start consumers
	for i := 0; i < w.consumersPerChannel; i++ {
		consumerID := ConsumerID(PatternQueueSimple, w.channelIdx, i)
		w.startConsumer(ctx, consumerID)
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	return nil
}

// StartProducers launches producer goroutines. Called after warmup.
func (w *QueueSimpleWorker) StartProducers() {
	for i := 0; i < w.producersPerChannel; i++ {
		producerID := ProducerID(PatternQueueSimple, w.channelIdx, i)
		w.startProducer(w.producerCtx, producerID)
	}
}

// startProducer launches a goroutine that sends queue messages via the unary API.
func (w *QueueSimpleWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ws := w.getOrCreateProducerStat(producerID)

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
				ws.errors.Add(1)
				// Backoff on send errors to avoid retry-throttle cascade
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			if result.IsError {
				w.RecordError("send_result_error")
				ws.errors.Add(1)
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			ws.latAccum.Record(sendDuration)
			w.RecordBytesSent(producerID, len(body))
			w.RecordSend(producerID, currentSeq)
		}
	}()
}

// startConsumer launches a goroutine that polls for queue messages using a
// persistent QueueDownstreamReceiver (reused across polls) instead of
// PollQueue which creates/destroys a gRPC stream per call.
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
			w.runConsumerLoop(ctx, consumerID, cfg)
		}
	}()
}

// runConsumerLoop creates a persistent receiver and polls in a loop.
// If the receiver errors, it returns so the outer loop can retry.
func (w *QueueSimpleWorker) runConsumerLoop(ctx context.Context, consumerID string, cfg *config.Config) {
	receiver, err := w.Client().NewQueueDownstreamReceiver(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.Logger().Error("failed to create queue receiver",
			"consumer", consumerID, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		return
	}
	defer func() { _ = receiver.Close() }()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resp, err := receiver.Poll(ctx, &kubemq.PollRequest{
			Channel:            w.ChannelName(),
			MaxItems:           int32(cfg.Queue.PollMaxMessages),
			WaitTimeoutSeconds: int32(cfg.Queue.PollWaitTimeoutSeconds),
			AutoAck:            true,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.RecordError("receive_failure")
			w.Logger().Error("poll failed, recreating receiver",
				"consumer", consumerID, "error", err)
			return // exit to outer loop to create a new receiver
		}

		if resp.IsError {
			w.RecordError("receive_result_error")
			w.Logger().Error("queue poll result error",
				"consumer", consumerID, "error", resp.Error)
			continue
		}

		for _, dsMsg := range resp.Messages {
			if dsMsg == nil || dsMsg.Message == nil {
				continue
			}
			qMsg := dsMsg.Message

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
}
