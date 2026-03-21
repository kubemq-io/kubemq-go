package benchmarks_test

import (
	"context"
	"testing"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

// BenchmarkPublishThroughput measures event publish throughput (messages/sec).
// Payload: 1KB. Metric: ops/sec via testing.B.
func BenchmarkPublishThroughput(b *testing.B) {
	client := newBenchClient(b)
	payload := makePayload(benchPayload)
	ctx := context.Background()

	b.SetBytes(int64(benchPayload))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.SendEvent(ctx, &kubemq.Event{
			Channel: "bench.publish.throughput",
			Body:    payload,
		})
		if err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}

// BenchmarkPublishLatency measures per-message publish latency.
// Payload: 1KB. Reports ns/op.
func BenchmarkPublishLatency(b *testing.B) {
	client := newBenchClient(b)
	payload := makePayload(benchPayload)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.SendEvent(ctx, &kubemq.Event{
			Channel: "bench.publish.latency",
			Body:    payload,
		})
		if err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}

// BenchmarkQueueRoundtrip measures queue send → receive roundtrip latency.
// Payload: 1KB. Measures the full cycle: send to queue, receive from queue.
func BenchmarkQueueRoundtrip(b *testing.B) {
	client := newBenchClient(b)
	payload := makePayload(benchPayload)
	ctx := context.Background()
	channel := "bench.queue.roundtrip"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.SendQueueMessage(ctx, &kubemq.QueueMessage{
			Channel: channel,
			Body:    payload,
		})
		if err != nil {
			b.Fatalf("queue send failed: %v", err)
		}

		resp, err := client.PollQueue(ctx, &kubemq.PollRequest{
			Channel:            channel,
			MaxItems:           1,
			WaitTimeoutSeconds: 5,
			AutoAck:            true,
		})
		if err != nil {
			b.Fatalf("queue receive failed: %v", err)
		}
		if len(resp.Messages) == 0 {
			b.Fatal("queue receive returned no messages")
		}
	}
}

// BenchmarkConnectionSetup measures time to establish a new client connection.
// Metric: ns/op (time to first ready state).
func BenchmarkConnectionSetup(b *testing.B) {
	host := getBenchHost()
	port := getBenchPort()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), benchTimeout)
		client, err := kubemq.NewClient(ctx,
			kubemq.WithAddress(host, port),
			kubemq.WithClientId("bench-conn-setup"),
		)
		if err != nil {
			cancel()
			b.Fatalf("connection setup failed: %v", err)
		}
		_ = client.Close()
		cancel()
	}
}

// Optional benchmarks: publish throughput across payload sizes.

// BenchmarkPublishThroughput_64B runs publish throughput with 64B payload.
func BenchmarkPublishThroughput_64B(b *testing.B) {
	benchPublishThroughput(b, 64)
}

// BenchmarkPublishThroughput_1KB runs publish throughput with 1KB payload.
func BenchmarkPublishThroughput_1KB(b *testing.B) {
	benchPublishThroughput(b, 1024)
}

// BenchmarkPublishThroughput_64KB runs publish throughput with 64KB payload.
func BenchmarkPublishThroughput_64KB(b *testing.B) {
	benchPublishThroughput(b, 64*1024)
}

func benchPublishThroughput(b *testing.B, payloadSize int) {
	client := newBenchClient(b)
	payload := makePayload(payloadSize)
	ctx := context.Background()

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := client.SendEvent(ctx, &kubemq.Event{
			Channel: "bench.publish.multi",
			Body:    payload,
		})
		if err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}
