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

// QueriesWorker implements the Worker interface for the Queries (RPC request/response with data) pattern.
type QueriesWorker struct {
	*BaseWorker
}

// NewQueriesWorker creates a new queries pattern worker.
func NewQueriesWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *QueriesWorker {
	bw := NewBaseWorker(PatternQueries, cfg, cp, logger)
	bw.channelType = "queries"
	return &QueriesWorker{
		BaseWorker: bw,
	}
}

// Start launches responders and senders for queries.
func (w *QueriesWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, "queries"); err != nil {
		w.Logger().Error("failed to create queries channel", "error", err)
		return fmt.Errorf("create queries channel: %w", err)
	}

	// Start responders first so they are ready to handle queries
	for i := 0; i < cfg.Concurrency.QueriesResponders; i++ {
		responderID := fmt.Sprintf("c-%s-%03d", PatternQueries, i)
		if err := w.startResponder(ctx, responderID); err != nil {
			return fmt.Errorf("start queries responder %s: %w", responderID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start senders
	for i := 0; i < cfg.Concurrency.QueriesSenders; i++ {
		senderID := fmt.Sprintf("p-%s-%03d", PatternQueries, i)
		w.startSender(w.producerCtx, senderID)
	}

	return nil
}

// startResponder subscribes to queries and sends responses with the echoed body.
func (w *QueriesWorker) startResponder(ctx context.Context, responderID string) error {
	sub, err := w.Client().SubscribeToQueries(ctx, w.ChannelName(), "",
		kubemq.WithOnQueryReceive(func(query *kubemq.QueryReceive) {
			// Skip warmup messages
			if query.Tags != nil && query.Tags["warmup"] == "true" {
				_ = w.Client().SendResponse(ctx, &kubemq.Response{
					RequestId: query.Id, ResponseTo: query.ResponseTo, Body: query.Body, ExecutedAt: time.Now(),
				})
				return
			}

			// Verify CRC of the received body
			msg, decErr := payload.Decode(query.Body)
			if decErr != nil {
				w.Logger().Error("failed to decode query", "responder", responderID, "error", decErr)
				w.RecordError("decode_failure")
				return
			}

			if !payload.VerifyCRC(query.Body, query.Tags["content_hash"]) {
				w.corrupted.Add(1)
				metrics.IncCorrupted(w.Pattern())
				w.Logger().Error("CRC mismatch in query", "responder", responderID, "producer", msg.ProducerID, "seq", msg.Sequence)
			}

			// Send response back, echoing the body
			resp := &kubemq.Response{
				RequestId:  query.Id,
				ResponseTo: query.ResponseTo,
				Metadata:   "",
				Body:       query.Body,
				ClientId:   responderID,
				ExecutedAt: time.Now(),
				Tags:       query.Tags,
			}

			if sendErr := w.Client().SendResponse(ctx, resp); sendErr != nil {
				w.Logger().Error("failed to send query response", "responder", responderID, "error", sendErr)
				w.RecordError("response_send_failure")
			}
		}),
		kubemq.WithOnError(func(err error) {
			w.Logger().Error("responder subscription error", "responder", responderID, "error", err)
			w.RecordError("subscription_error")
		}),
	)
	if err != nil {
		return fmt.Errorf("subscribe to queries: %w", err)
	}

	// Monitor subscription in a goroutine
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		select {
		case <-ctx.Done():
		case <-sub.Done():
			w.Logger().Warn("responder subscription closed", "responder", responderID)
			metrics.IncReconnection(w.Pattern())
			w.IncReconnection()
		}
	}()

	return nil
}

// startSender launches a goroutine that sends queries and measures RPC latency.
func (w *QueriesWorker) startSender(ctx context.Context, senderID string) {
	var seq atomic.Uint64
	timeout := time.Duration(w.Config().RPC.TimeoutMS) * time.Millisecond

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
			body, crcHex := payload.Encode(metrics.SDK(), w.Pattern(), senderID, currentSeq, w.MessageSize())

			query := &kubemq.Query{
				Id:       fmt.Sprintf("%s-%d", senderID, currentSeq),
				Channel:  w.ChannelName(),
				Metadata: "",
				Body:     body,
				Timeout:  timeout,
				ClientId: senderID,
				Tags:     map[string]string{"content_hash": crcHex},
			}

			sendStart := time.Now()
			resp, err := w.Client().SendQuery(ctx, query)
			elapsed := time.Since(sendStart)

			metrics.ObserveRPCDuration(w.Pattern(), elapsed)

			if err != nil {
				metrics.IncRPCResponse(w.Pattern(), "error")
				w.IncRPCError()
				w.RecordError("send_failure")
				w.Logger().Error("failed to send query", "sender", senderID, "seq", currentSeq, "error", err)
				continue
			}

			// Transport succeeded - count as sent
			metrics.AddBytesSent(w.Pattern(), float64(len(body)))
			w.sent.Add(1)
			metrics.IncSent(w.Pattern(), senderID)

			if resp.Error != "" {
				if resp.Error == "timeout" || resp.Error == "rpc timeout" {
					metrics.IncRPCResponse(w.Pattern(), "timeout")
					w.IncRPCTimeout()
					w.RecordError("timeout")
					w.Logger().Warn("query timed out", "sender", senderID, "seq", currentSeq)
				} else {
					metrics.IncRPCResponse(w.Pattern(), "error")
					w.IncRPCError()
					w.RecordError("rpc_error")
					w.Logger().Error("query error response", "sender", senderID, "seq", currentSeq, "error", resp.Error)
				}
				continue
			}

			if resp.Executed {
				metrics.IncRPCResponse(w.Pattern(), "success")
				w.IncRPCSuccess()
				w.received.Add(1)
				metrics.IncReceived(w.Pattern(), senderID)

				// Verify CRC of the echoed response body
				if !payload.VerifyCRC(resp.Body, resp.Tags["content_hash"]) {
					w.corrupted.Add(1)
					metrics.IncCorrupted(w.Pattern())
					w.Logger().Error("CRC mismatch in query response", "sender", senderID, "seq", currentSeq)
				}

				metrics.AddBytesReceived(w.Pattern(), float64(len(resp.Body)))
				metrics.ObserveLatency(w.Pattern(), elapsed)
				w.latAccum.Record(elapsed)
				w.RPCLatencyAccumulator().Record(elapsed)

				// Record sequence for tracking
				isDup, isOOO := w.trk.Record(senderID, currentSeq)
				if isDup {
					metrics.IncDuplicated(w.Pattern())
				}
				if isOOO {
					metrics.IncOutOfOrder(w.Pattern())
				}
			} else {
				metrics.IncRPCResponse(w.Pattern(), "error")
				w.IncRPCError()
				w.RecordError("not_executed")
				w.Logger().Warn("query not executed", "sender", senderID, "seq", currentSeq)
			}
		}
	}()
}
