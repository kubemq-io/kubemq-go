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

// CommandsWorker implements the Worker interface for the Commands (RPC request/response) pattern.
type CommandsWorker struct {
	*BaseWorker
	sendersPerChannel    int
	respondersPerChannel int
}

// NewCommandsWorker creates a new commands pattern worker for a specific channel.
func NewCommandsWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger,
	channelName string, channelIndex int, pc *config.PatternConfig,
	patternLatAccum *metrics.LatencyAccumulator) *CommandsWorker {

	bw := NewBaseWorker(PatternCommands, cfg, cp, logger, channelName, channelIndex, pc.Rate, patternLatAccum)
	bw.channelType = "commands"
	return &CommandsWorker{
		BaseWorker:           bw,
		sendersPerChannel:    pc.SendersPerChannel,
		respondersPerChannel: pc.RespondersPerChannel,
	}
}

// Start launches responders for commands on this channel.
func (w *CommandsWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)

	// Start responders first so they are ready to handle commands
	for i := 0; i < w.respondersPerChannel; i++ {
		responderID := ResponderID(PatternCommands, w.channelIdx, i)
		if err := w.startResponder(ctx, responderID); err != nil {
			return fmt.Errorf("start commands responder %s: %w", responderID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	return nil
}

// StartProducers launches sender goroutines. Called after warmup.
func (w *CommandsWorker) StartProducers() {
	for i := 0; i < w.sendersPerChannel; i++ {
		senderID := SenderID(PatternCommands, w.channelIdx, i)
		w.startSender(w.producerCtx, senderID)
	}
}

// startResponder subscribes to commands and sends responses.
func (w *CommandsWorker) startResponder(ctx context.Context, responderID string) error {
	ws := w.getOrCreateConsumerStat(responderID)

	sub, err := w.Client().SubscribeToCommands(ctx, w.ChannelName(), "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			if cmd.Tags != nil && cmd.Tags["warmup"] == "true" {
				_ = w.Client().SendCommandResponse(ctx, &kubemq.CommandReply{
					RequestId: cmd.Id, ResponseTo: cmd.ResponseTo, ExecutedAt: time.Now(),
				})
				return
			}

			msg, decErr := payload.Decode(cmd.Body)
			if decErr != nil {
				w.Logger().Error("failed to decode command", "responder", responderID, "error", decErr)
				w.RecordError("decode_failure")
				ws.errors.Add(1)
				return
			}

			if !payload.VerifyCRC(cmd.Body, cmd.Tags["content_hash"]) {
				w.corrupted.Add(1)
				metrics.IncCorrupted(w.Pattern())
				w.Logger().Error("CRC mismatch in command", "responder", responderID, "producer", msg.ProducerID, "seq", msg.Sequence)
			}

			resp := &kubemq.CommandReply{
				RequestId:  cmd.Id,
				ResponseTo: cmd.ResponseTo,
				Metadata:   "",
				Body:       cmd.Body,
				ClientId:   responderID,
				ExecutedAt: time.Now(),
				Tags:       cmd.Tags,
			}

			if sendErr := w.Client().SendCommandResponse(ctx, resp); sendErr != nil {
				w.Logger().Error("failed to send command response", "responder", responderID, "error", sendErr)
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
		return fmt.Errorf("subscribe to commands: %w", err)
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

// startSender launches a goroutine that sends commands and measures RPC latency.
func (w *CommandsWorker) startSender(ctx context.Context, senderID string) {
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

			cmd := &kubemq.Command{
				Id:       fmt.Sprintf("%s-%d", senderID, currentSeq),
				Channel:  w.ChannelName(),
				Metadata: "",
				Body:     body,
				Timeout:  timeout,
				ClientId: senderID,
				Tags:     map[string]string{"content_hash": crcHex},
			}

			sendStart := time.Now()
			resp, err := w.Client().SendCommand(ctx, cmd)
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
					w.Logger().Warn("command timed out", "sender", senderID, "seq", currentSeq)
				} else {
					metrics.IncRPCResponse(w.Pattern(), "error")
					w.IncRPCError()
					w.RecordError("rpc_error")
					ws.rpcError.Add(1)
					ws.errors.Add(1)
					w.Logger().Error("command error response", "sender", senderID, "seq", currentSeq, "error", resp.Error)
				}
				continue
			}

			if resp.Executed {
				metrics.IncRPCResponse(w.Pattern(), "success")
				w.IncRPCSuccess()
				w.received.Add(1)
				metrics.IncReceived(w.Pattern(), senderID)
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
				w.Logger().Warn("command not executed", "sender", senderID, "seq", currentSeq)
			}
		}
	}()
}
