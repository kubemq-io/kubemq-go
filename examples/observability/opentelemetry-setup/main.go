// Example: observability/opentelemetry-setup
//
// Demonstrates setting up OpenTelemetry tracing and metrics with
// the KubeMQ Go SDK. The SDK integrates with OTel via TracerProvider
// and MeterProvider options.
//
// This example uses the OTel SDK providers available in the module.
// In production, you would configure exporters (e.g., OTLP, Jaeger)
// to send telemetry data to your observability backend.
//
// Channel: go-observability.opentelemetry-setup
// Client ID: go-observability-opentelemetry-setup-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a TracerProvider. In production, add a real exporter
	// (e.g., sdktrace.WithBatcher(otlpExporter)).
	tp := sdktrace.NewTracerProvider()
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("TracerProvider shutdown: %v", err)
		}
	}()
	otel.SetTracerProvider(tp)

	// Create a MeterProvider for metrics instrumentation.
	mp := sdkmetric.NewMeterProvider()
	defer func() {
		if err := mp.Shutdown(ctx); err != nil {
			log.Printf("MeterProvider shutdown: %v", err)
		}
	}()

	// Create a KubeMQ client with OTel providers.
	// The SDK automatically instruments all operations with traces and metrics.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-observability-opentelemetry-setup-client"),
		// Pass the OTel providers to the SDK.
		kubemq.WithTracerProvider(tp),
		kubemq.WithMeterProvider(mp),
		// Optional: control metric cardinality for high-throughput scenarios.
		kubemq.WithCardinalityThreshold(100),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	channel := "go-observability.opentelemetry-setup"

	// Send an event — this operation will be traced and metered by the SDK.
	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel(channel).
		SetBody([]byte("instrumented event")).
		SetMetadata("otel-demo"))
	if err != nil {
		log.Printf("SendEvent: %v", err)
	} else {
		fmt.Println("Event sent with OpenTelemetry instrumentation")
	}

	fmt.Println("OpenTelemetry setup demo complete")
	fmt.Println("In production, traces and metrics would be exported to your backend")
}
