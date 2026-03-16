# How to Monitor with OpenTelemetry

Instrument your KubeMQ client with OpenTelemetry tracing and metrics for end-to-end observability.

## Install Dependencies

```bash
go get go.opentelemetry.io/otel \
       go.opentelemetry.io/otel/sdk/trace \
       go.opentelemetry.io/otel/sdk/metric \
       go.opentelemetry.io/otel/exporters/stdout/stdouttrace \
       go.opentelemetry.io/otel/exporters/stdout/stdoutmetric
```

For production, add your preferred exporter:

```bash
# Jaeger via OTLP
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp
go get go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp
```

## Configure Tracing

Create a `TracerProvider` and pass it to the KubeMQ client via `WithTracerProvider`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	defer func() { _ = tp.Shutdown(ctx) }()
	otel.SetTracerProvider(tp)

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("traced-client"),
		kubemq.WithTracerProvider(tp),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.SendEvent(ctx, kubemq.NewEvent().
		SetChannel("traced.events").
		SetBody([]byte("hello from traced client")))
	if err != nil {
		log.Printf("SendEvent: %v", err)
	}

	fmt.Println("Trace exported — check console output above")
}
```

The SDK automatically creates spans for every operation (`SendEvent`, `SendCommand`, `SendQuery`, etc.) with channel name, client ID, and error status as span attributes.

## Configure Metrics

Create a `MeterProvider` and pass it via `WithMeterProvider`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	exporter, err := stdoutmetric.New()
	if err != nil {
		log.Fatal(err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			sdkmetric.WithInterval(5*time.Second))),
	)
	defer func() { _ = mp.Shutdown(ctx) }()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("metered-client"),
		kubemq.WithMeterProvider(mp),
		kubemq.WithCardinalityThreshold(100),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for i := 0; i < 10; i++ {
		_ = client.SendEvent(ctx, kubemq.NewEvent().
			SetChannel("metered.events").
			SetBody([]byte(fmt.Sprintf("event-%d", i))))
	}

	time.Sleep(6 * time.Second)
	fmt.Println("Metrics exported — check console output above")
}
```

`WithCardinalityThreshold(n)` caps the number of unique channel labels in metrics to prevent high-cardinality series in high-throughput scenarios.

## Combined Tracing + Metrics

Pass both providers to a single client:

```go
client, err := kubemq.NewClient(ctx,
	kubemq.WithAddress("localhost", 50000),
	kubemq.WithClientId("fully-instrumented"),
	kubemq.WithTracerProvider(tp),
	kubemq.WithMeterProvider(mp),
	kubemq.WithCardinalityThreshold(100),
)
```

## Export to Jaeger via OTLP

Replace the console exporter with an OTLP exporter for production:

```go
import (
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

exporter, err := otlptracehttp.New(ctx,
	otlptracehttp.WithEndpoint("localhost:4318"),
	otlptracehttp.WithInsecure(),
)
if err != nil {
	log.Fatal(err)
}

tp := sdktrace.NewTracerProvider(
	sdktrace.WithBatcher(exporter),
)
```

Then view traces at `http://localhost:16686` (Jaeger UI).

## Provider Shutdown

Always shut down providers before exiting to flush pending telemetry:

```go
defer func() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tp.Shutdown(shutdownCtx); err != nil {
		log.Printf("TracerProvider shutdown error: %v", err)
	}
	if err := mp.Shutdown(shutdownCtx); err != nil {
		log.Printf("MeterProvider shutdown error: %v", err)
	}
}()
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| No traces appear | `TracerProvider` not passed to client | Pass it via `kubemq.WithTracerProvider(tp)` |
| No metrics appear | `MeterProvider` not passed or export interval too long | Pass via `WithMeterProvider(mp)` and reduce the reader interval |
| High-cardinality metric warnings | Too many unique channel names | Lower `WithCardinalityThreshold` or use fewer distinct channels |
| `Shutdown` context deadline exceeded | Exporter can't reach the collector | Verify the OTLP endpoint is reachable and increase the shutdown timeout |
| Traces missing after program exits | Provider not shut down | Always call `tp.Shutdown(ctx)` before exit |
