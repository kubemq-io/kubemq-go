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
	sendersPerChannel    int
	respondersPerChannel int
}

// NewQueriesWorker creates a new queries pattern worker for a specific channel.
func NewQueriesWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, pc *config.PatternConfig,
	patternLatAccum *metrics.LatencyAccumulator) *QueriesWorker {

	bw := NewBaseWorker(PatternQueries, cfg, cp, logger, channelName, channelIndex, pc.Rate, patternLatAccum)
	bw.channelType = "queries"
	return &QueriesWorker{
		BaseWorker:           bw,
		sendersPerChannel:    pc.SendersPerChannel,
		respondersPerChannel: pc.RespondersPerChannel,
	}
}

// Start launches responders for queries on this channel.
func (w *QueriesWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)

	// Start responders first so they are ready to handle queries
	for i := 0; i < w.respondersPerChannel; i++ {
		responderID := ResponderID(PatternQueries, w.channelIdx, i)
		if err := w.startResponder(ctx, responderID); err != nil {
			return fmt.Errorf("start queries responder %s: %w", responderID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	return nil
}

// StartProducers launches sender goroutines. Called after warmup.
func (w *QueriesWorker) StartProducers() {
	for i := 0; i < w.sendersPerChannel; i++ {
		senderID := SenderID(PatternQueries, w.channelIdx, i)
		w.startSender(w.producerCtx, senderID)
	}
}

// startResponder subscribes to queries and sends responses with the echoed body.
func (w *QueriesWorker) startResponder(ctx context.Context, responderID string) error {
	ws := w.getOrCreateConsumerStat(responderID)

	sub, err := w.Client().SubscribeToQueries(ctx, w.ChannelName(), "",
		kubemq.WithOnQueryReceive(func(query *kubemq.QueryReceive) {
			if query.Tags != nil && query.Tags["warmup"] == "true" {
				_ = w.Client().SendQueryResponse(ctx, &kubemq.QueryReply{
					RequestId: query.Id, ResponseTo: query.ResponseTo, Body: query.Body, ExecutedAt: time.Now(),
				})
				return
			}

			msg, decErr := payload.Decode(query.Body)
			if decErr != nil {
				w.Logger().Error("failed to decode query", "responder", responderID, "error", decErr)
				w.RecordError("decode_failure")
				ws.errors.Add(1)
				return
			}

			if !payload.VerifyCRC(query.Body, query.Tags["content_hash"]) {
				w.corrupted.Add(1)
				metrics.IncCorrupted(w.Pattern())
				w.Logger().Error("CRC mismatch in query", "responder", responderID, "producer", msg.ProducerID, "seq", msg.Sequence)
			}

			resp := &kubemq.QueryReply{
				RequestId:  query.Id,
				ResponseTo: query.ResponseTo,
				Metadata:   "",
				Body:       query.Body,
				ClientId:   responderID,
				ExecutedAt: time.Now(),
				Tags:       query.Tags,
			}

			if sendErr := w.Client().SendQueryResponse(ctx, resp); sendErr != nil {
				w.Logger().Error("failed to send query response", "responder", responderID, "error", sendErr)
				w.RecordError("response_send_failure")
				ws.errors.Add(1)
			} else {
				ws.responded.Add(1)
			}
		}),
		kubemq.WithOnError(func(err error) {
			w.Logger().Error("responder subscription error", "responder", responderID, "error", err)
			w.RecordError("subscription_error")
			ws.errors.Add(1)
		}),
	)
	if err != nil {
		return fmt.Errorf("subscribe to queries: %w", err)
	}

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
		ws := w.getOrCreateProducerStat(senderID)

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
				ws.errors.Add(1)
				ws.rpcError.Add(1)
				// Backoff on send errors to avoid retry-throttle cascade
				select {
				case <-ctx.Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			w.RecordBytesSent(senderID, len(body))
			w.sent.Add(1)
			metrics.IncSent(w.Pattern(), senderID)
			w.peakRate.Record()
			w.rateWindow.Record()
			ws.sent.Add(1)
			ws.rateWindow.Record()

			if resp.Error != "" {
				if resp.Error == "timeout" || resp.Error == "rpc timeout" {
					metrics.IncRPCResponse(w.Pattern(), "timeout")
					w.IncRPCTimeout()
					w.RecordError("timeout")
					ws.rpcTimeout.Add(1)
					ws.errors.Add(1)
					w.Logger().Warn("query timed out", "sender", senderID, "seq", currentSeq)
				} else {
					metrics.IncRPCResponse(w.Pattern(), "error")
					w.IncRPCError()
					w.RecordError("rpc_error")
					ws.rpcError.Add(1)
					ws.errors.Add(1)
					w.Logger().Error("query error response", "sender", senderID, "seq", currentSeq, "error", resp.Error)
				}
				continue
			}

			if resp.Executed {
				metrics.IncRPCResponse(w.Pattern(), "success")
				w.IncRPCSuccess()
				w.received.Add(1)
				metrics.IncReceived(w.Pattern(), senderID)

				if !payload.VerifyCRC(resp.Body, resp.Tags["content_hash"]) {
					w.corrupted.Add(1)
					metrics.IncCorrupted(w.Pattern())
					w.Logger().Error("CRC mismatch in query response", "sender", senderID, "seq", currentSeq)
				}

				metrics.AddBytesReceived(w.Pattern(), float64(len(resp.Body)))
				w.bytesReceived.Add(uint64(len(resp.Body)))
				metrics.ObserveLatency(w.Pattern(), elapsed)
				w.latAccum.Record(elapsed)
				// Dual-write to pattern-level accumulator
				if w.patternLatAccum != nil {
					w.patternLatAccum.Record(elapsed)
				}
				w.RPCLatencyAccumulator().Record(elapsed)
				ws.rpcSuccess.Add(1)
				ws.latAccum.Record(elapsed)

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
				ws.rpcError.Add(1)
				ws.errors.Add(1)
				w.Logger().Warn("query not executed", "sender", senderID, "seq", currentSeq)
			}
		}
	}()
}
