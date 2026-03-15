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
}

// NewCommandsWorker creates a new commands pattern worker.
func NewCommandsWorker(cfg *config.Config, cp *ClientProvider, logger *slog.Logger) *CommandsWorker {
	bw := NewBaseWorker(PatternCommands, cfg, cp, logger)
	bw.channelType = "commands"
	return &CommandsWorker{
		BaseWorker: bw,
	}
}

// Start launches responders and senders for commands.
func (w *CommandsWorker) Start(ctx context.Context) error {
	ctx, w.cancel = context.WithCancel(ctx)
	w.producerCtx, w.producerCancel = context.WithCancel(ctx)
	cfg := w.Config()

	// Create channel
	if err := w.CreateChannel(ctx, "commands"); err != nil {
		w.Logger().Error("failed to create commands channel", "error", err)
		return fmt.Errorf("create commands channel: %w", err)
	}

	// Start responders first so they are ready to handle commands
	for i := 0; i < cfg.Concurrency.CommandsResponders; i++ {
		responderID := fmt.Sprintf("c-%s-%03d", PatternCommands, i)
		if err := w.startResponder(ctx, responderID); err != nil {
			return fmt.Errorf("start commands responder %s: %w", responderID, err)
		}
	}

	// Mark consumers ready
	w.MarkConsumerReady()

	// Start senders
	for i := 0; i < cfg.Concurrency.CommandsSenders; i++ {
		senderID := fmt.Sprintf("p-%s-%03d", PatternCommands, i)
		w.startSender(w.producerCtx, senderID)
	}

	return nil
}

// startResponder subscribes to commands and sends responses.
func (w *CommandsWorker) startResponder(ctx context.Context, responderID string) error {
	sub, err := w.Client().SubscribeToCommands(ctx, w.ChannelName(), "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			// Skip warmup messages
			if cmd.Tags != nil && cmd.Tags["warmup"] == "true" {
				_ = w.Client().SendResponse(ctx, &kubemq.Response{
					RequestId: cmd.Id, ResponseTo: cmd.ResponseTo, ExecutedAt: time.Now(),
				})
				return
			}

			// Verify CRC of the received body
			msg, decErr := payload.Decode(cmd.Body)
			if decErr != nil {
				w.Logger().Error("failed to decode command", "responder", responderID, "error", decErr)
				w.RecordError("decode_failure")
				return
			}

			if !payload.VerifyCRC(cmd.Body, cmd.Tags["content_hash"]) {
				w.corrupted.Add(1)
				metrics.IncCorrupted(w.Pattern())
				w.Logger().Error("CRC mismatch in command", "responder", responderID, "producer", msg.ProducerID, "seq", msg.Sequence)
			}

			// Send response back, echoing the body
			resp := &kubemq.Response{
				RequestId:  cmd.Id,
				ResponseTo: cmd.ResponseTo,
				Metadata:   "",
				Body:       cmd.Body,
				ClientId:   responderID,
				ExecutedAt: time.Now(),
				Tags:       cmd.Tags,
			}

			if sendErr := w.Client().SendResponse(ctx, resp); sendErr != nil {
				w.Logger().Error("failed to send command response", "responder", responderID, "error", sendErr)
				w.RecordError("response_send_failure")
			}
		}),
		kubemq.WithOnError(func(err error) {
			w.Logger().Error("responder subscription error", "responder", responderID, "error", err)
			w.RecordError("subscription_error")
		}),
	)
	if err != nil {
		return fmt.Errorf("subscribe to commands: %w", err)
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

// startSender launches a goroutine that sends commands and measures RPC latency.
func (w *CommandsWorker) startSender(ctx context.Context, senderID string) {
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
				w.Logger().Error("failed to send command", "sender", senderID, "seq", currentSeq, "error", err)
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
					w.Logger().Warn("command timed out", "sender", senderID, "seq", currentSeq)
				} else {
					metrics.IncRPCResponse(w.Pattern(), "error")
					w.IncRPCError()
					w.RecordError("rpc_error")
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
				w.Logger().Warn("command not executed", "sender", senderID, "seq", currentSeq)
			}
		}
	}()
}
