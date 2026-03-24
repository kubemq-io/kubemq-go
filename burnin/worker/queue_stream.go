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

// QueueStreamWorker implements the Worker interface for the bidirectional
// gRPC queue stream pattern (upstream/downstream).
type QueueStreamWorker struct {
	*BaseWorker
	producersPerChannel int
	consumersPerChannel int
}

// NewQueueStreamWorker creates a new queue stream pattern worker for a specific channel.
func NewQueueStreamWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, pc *config.PatternConfig,
	patternLatAccum *metrics.LatencyAccumulator) *QueueStreamWorker {

	bw := NewBaseWorker(PatternQueueStream, cfg, cp, logger, channelName, channelIndex, pc.Rate, patternLatAccum)
	bw.channelType = "queues"
	return &QueueStreamWorker{
		BaseWorker:          bw,
		producersPerChannel: pc.ProducersPerChannel,
		consumersPerChannel: pc.ConsumersPerChannel,
	}
}

// Start launches consumers for queue stream on this channel.
func (w *QueueStreamWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)

	// Start consumers
	for i := 0; i < w.consumersPerChannel; i++ {
		consumerID := ConsumerID(PatternQueueStream, w.channelIdx, i)
		w.startConsumer(ctx, consumerID)
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	return nil
}

// StartProducers launches producer goroutines. Called after warmup.
func (w *QueueStreamWorker) StartProducers() {
	for i := 0; i < w.producersPerChannel; i++ {
		producerID := ProducerID(PatternQueueStream, w.channelIdx, i)
		w.startProducer(w.producerCtx, producerID)
	}
}

// startProducer launches a goroutine that sends queue messages via the upstream stream.
// It wraps the producer logic in a retry loop: when the upstream handle closes,
// it waits 2 seconds and recreates the handle (GO-3 fix).
func (w *QueueStreamWorker) startProducer(ctx context.Context, producerID string) {
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

			w.runProducerStream(ctx, producerID, ws, &seq)

			// If context is done, exit cleanly
			if ctx.Err() != nil {
				return
			}

			// Wait before retrying (GO-3: don't exit permanently on handle.Done)
			w.Logger().Warn("producer stream ended, retrying in 2s", "producer", producerID)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
}

// runProducerStream opens an upstream handle and sends messages until it closes or ctx is done.
func (w *QueueStreamWorker) runProducerStream(ctx context.Context, producerID string, ws *WorkerStat, seq *atomic.Uint64) {
	handle, err := w.Client().QueueUpstream(ctx)
	if err != nil {
		w.Logger().Error("failed to open queue upstream", "producer", producerID, "error", err)
		w.RecordError("stream_open_failure")
		ws.errors.Add(1)
		return
	}
	defer handle.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-handle.Done:
			w.Logger().Warn("queue upstream closed", "producer", producerID)
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

		requestID := fmt.Sprintf("%s-%d", producerID, currentSeq)
		msg := &kubemq.QueueMessage{
			ClientID: producerID,
			Channel:  w.ChannelName(),
			Metadata: "",
			Body:     body,
			Tags:     map[string]string{"content_hash": crcHex},
		}

		sendStart := time.Now()
		sendErr := handle.Send(requestID, []*kubemq.QueueMessage{msg})
		sendDuration := time.Since(sendStart)
		metrics.ObserveSendDuration(w.Pattern(), sendDuration)

		if sendErr != nil {
			w.RecordError("send_failure")
			ws.errors.Add(1)
			w.Logger().Error("failed to send queue upstream message",
				"producer", producerID, "seq", currentSeq, "error", sendErr)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-handle.Done:
			w.Logger().Warn("queue upstream closed while waiting for result", "producer", producerID)
			return
		case result, ok := <-handle.Results:
			if !ok {
				w.Logger().Warn("queue upstream results channel closed", "producer", producerID)
				return
			}
			if result.IsError {
				w.RecordError("upstream_result_error")
				ws.errors.Add(1)
				w.Logger().Error("queue upstream result error",
					"producer", producerID, "seq", currentSeq, "error", result.Error)
				continue
			}
			ws.latAccum.Record(sendDuration)
			w.RecordBytesSent(producerID, len(body))
			w.RecordSend(producerID, currentSeq)
		}
	}
}

// startConsumer launches a goroutine that receives queue messages via the downstream receiver.
func (w *QueueStreamWorker) startConsumer(ctx context.Context, consumerID string) {
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

			w.runConsumerStream(ctx, consumerID, cfg)

			// GO-4: sleep between runConsumerStream restarts to avoid rapid spin
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
}

// runConsumerStream creates a downstream receiver and polls in a loop.
func (w *QueueStreamWorker) runConsumerStream(ctx context.Context, consumerID string, cfg *config.Config) {
	receiver, err := w.Client().NewQueueDownstreamReceiver(ctx)
	if err != nil {
		w.Logger().Error("failed to open queue downstream receiver", "consumer", consumerID, "error", err)
		w.RecordError("stream_open_failure")
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
		return
	}
	defer receiver.Close()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for streamErr := range receiver.Errors() {
			w.Logger().Warn("queue downstream error",
				"consumer", consumerID, "error", streamErr)
			metrics.IncReconnection(w.Pattern())
			w.IncReconnection()
		}
	}()

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
			AutoAck:            cfg.Queue.AutoAck,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.Logger().Error("poll failed", "consumer", consumerID, "error", err)
			w.RecordError("downstream_get_failure")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if resp.IsError {
			w.Logger().Error("poll error", "consumer", consumerID, "error", resp.Error)
			w.RecordError("downstream_get_failure")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		for _, msg := range resp.Messages {
			if msg.Message == nil || msg.Message.Body == nil {
				continue
			}
			decoded, decErr := payload.Decode(msg.Message.Body)
			if decErr != nil {
				w.Logger().Error("decode failed", "consumer", consumerID, "error", decErr)
				w.RecordError("decode_failure")
			} else {
				w.RecordReceive(consumerID, msg.Message.Body,
					msg.Message.Tags["content_hash"], decoded.ProducerID, decoded.Sequence)
			}
		}
		if !cfg.Queue.AutoAck && len(resp.Messages) > 0 {
			if ackErr := resp.AckAll(); ackErr != nil {
				w.Logger().Error("ack_all failed", "consumer", consumerID, "error", ackErr)
				w.RecordError("ack_failure")
			}
		}
	}
}
