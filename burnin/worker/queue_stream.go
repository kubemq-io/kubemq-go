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
}

// NewQueueStreamWorker creates a new queue stream pattern worker.
func NewQueueStreamWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *QueueStreamWorker {
	bw := NewBaseWorker(PatternQueueStream, cfg, cp, logger)
	bw.channelType = "queues"
	return &QueueStreamWorker{
		BaseWorker: bw,
	}
}

// Start launches consumers and producers for queue stream.
func (w *QueueStreamWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, kubemq.ChannelTypeQueues); err != nil {
		w.Logger().Error("failed to create queue stream channel", "error", err)
		return fmt.Errorf("create queue stream channel: %w", err)
	}

	// Start consumers first
	for i := 0; i < cfg.Concurrency.QueueStreamConsumers; i++ {
		consumerID := fmt.Sprintf("c-%s-%03d", PatternQueueStream, i)
		w.startConsumer(ctx, consumerID)
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start producers
	for i := 0; i < cfg.Concurrency.QueueStreamProducers; i++ {
		producerID := fmt.Sprintf("p-%s-%03d", PatternQueueStream, i)
		w.startProducer(w.producerCtx, producerID)
	}

	return nil
}

// startProducer launches a goroutine that sends queue messages via the upstream stream.
func (w *QueueStreamWorker) startProducer(ctx context.Context, producerID string) {
	var seq atomic.Uint64

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		// Open the upstream stream
		handle, err := w.Client().QueueUpstream(ctx)
		if err != nil {
			w.Logger().Error("failed to open queue upstream", "producer", producerID, "error", err)
			w.RecordError("stream_open_failure")
			return
		}
		defer handle.Close()

		// Drain Done channel in background
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			select {
			case <-ctx.Done():
			case <-handle.Done:
			}
		}()

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
				w.Logger().Error("failed to send queue upstream message",
					"producer", producerID, "seq", currentSeq, "error", sendErr)
				continue
			}

			// Wait for upstream result to confirm send
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
					w.Logger().Error("queue upstream result error",
						"producer", producerID, "seq", currentSeq, "error", result.Error)
					continue
				}
				// Confirm success
				metrics.AddBytesSent(w.Pattern(), float64(len(body)))
				w.RecordSend(producerID, currentSeq)
			}
		}
	}()
}

// startConsumer launches a goroutine that receives queue messages via the downstream stream.
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
		}
	}()
}

// runConsumerStream opens a downstream stream, sends a Get request, and processes messages.
// Returns when the stream encounters an error or ctx is cancelled, allowing the caller to reconnect.
func (w *QueueStreamWorker) runConsumerStream(ctx context.Context, consumerID string, cfg *config.Config) {
	handle, err := w.Client().QueueDownstream(ctx)
	if err != nil {
		w.Logger().Error("failed to open queue downstream", "consumer", consumerID, "error", err)
		w.RecordError("stream_open_failure")
		// Brief backoff before retry
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
		return
	}
	defer handle.Close()

	// Send initial Get request
	if err := w.sendGetRequest(handle, cfg); err != nil {
		w.Logger().Error("failed to send initial get request", "consumer", consumerID, "error", err)
		w.RecordError("downstream_get_failure")
		return
	}

	// Process messages and errors in a poll loop.
	for {
		select {
		case <-ctx.Done():
			return
		case streamErr, ok := <-handle.Errors:
			if !ok {
				return
			}
			w.Logger().Warn("queue downstream error, reconnecting",
				"consumer", consumerID, "error", streamErr)
			metrics.IncReconnection(w.Pattern())
			w.IncReconnection()
			return // Return to reconnect
		case txMsg, ok := <-handle.Messages:
			if !ok {
				w.Logger().Warn("queue downstream messages channel closed", "consumer", consumerID)
				return
			}
			// Process this message and drain any remaining from the batch.
			w.processQueueMsg(consumerID, txMsg, cfg)
			w.drainBatch(consumerID, handle, cfg)

			// Send another Get request to continue polling.
			if err := w.sendGetRequest(handle, cfg); err != nil {
				w.Logger().Error("failed to send get request after batch",
					"consumer", consumerID, "error", err)
				w.RecordError("downstream_get_failure")
				return
			}
		}
	}
}

// processQueueMsg handles a single queue transaction message.
func (w *QueueStreamWorker) processQueueMsg(consumerID string, txMsg *kubemq.QueueTransactionMessage, cfg *config.Config) {
	if txMsg.Message == nil {
		return
	}
	msg, decErr := payload.Decode(txMsg.Message.Body)
	if decErr != nil {
		w.Logger().Error("failed to decode queue stream message",
			"consumer", consumerID, "error", decErr)
		w.RecordError("decode_failure")
	} else {
		w.RecordReceive(consumerID, txMsg.Message.Body,
			txMsg.Message.Tags["content_hash"], msg.ProducerID, msg.Sequence)
	}
	if !cfg.Queue.AutoAck {
		if ackErr := txMsg.Ack(); ackErr != nil {
			w.Logger().Error("failed to ack queue message",
				"consumer", consumerID, "error", ackErr)
			w.RecordError("ack_failure")
		}
	}
}

// drainBatch reads any remaining messages from the current batch without sending a new Get.
func (w *QueueStreamWorker) drainBatch(consumerID string, handle *kubemq.QueueDownstreamHandle, cfg *config.Config) {
	for {
		select {
		case txMsg, ok := <-handle.Messages:
			if !ok {
				return
			}
			w.processQueueMsg(consumerID, txMsg, cfg)
		default:
			return
		}
	}
}

// sendGetRequest sends a QueueDownstreamGet request on the downstream handle.
func (w *QueueStreamWorker) sendGetRequest(handle *kubemq.QueueDownstreamHandle, cfg *config.Config) error {
	return handle.Send(&kubemq.QueueDownstreamRequest{
		RequestType:        kubemq.QueueDownstreamGet,
		Channel:            w.ChannelName(),
		MaxItems:           int32(cfg.Queue.PollMaxMessages),
		WaitTimeoutSeconds: int32(cfg.Queue.PollWaitTimeoutSeconds),
		AutoAck:            cfg.Queue.AutoAck,
	})
}
