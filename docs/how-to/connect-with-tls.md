# How to Connect with TLS/mTLS

Secure your KubeMQ client connections using TLS (server verification) or mTLS (mutual authentication).

## Prerequisites

| File | Purpose | Required for |
|------|---------|-------------|
| `ca-cert.pem` | CA certificate that signed the server cert | TLS and mTLS |
| `client-cert.pem` | Client certificate signed by the CA | mTLS only |
| `client-key.pem` | Client private key | mTLS only |

Certificates must be PEM-encoded. Verify your CA cert matches the server's issuing CA before proceeding.

## TLS — Server Verification Only

The client verifies the server's identity but does not present a certificate of its own.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithClientId("my-tls-client"),
		kubemq.WithTLS("certs/ca-cert.pem"),
	)
	if err != nil {
		log.Fatalf("TLS connection failed: %v", err)
	}
	defer client.Close()

	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Connected via TLS: host=%s version=%s\n", info.Host, info.Version)
}
```

`WithTLS(caFile)` loads the CA certificate and configures the gRPC transport to verify the server's certificate chain against it.

## mTLS — Mutual Authentication

Both the client and server present certificates. The server verifies the client's identity.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithClientId("my-mtls-client"),
		kubemq.WithMTLS(
			"certs/client-cert.pem",
			"certs/client-key.pem",
			"certs/ca-cert.pem",
		),
	)
	if err != nil {
		log.Fatalf("mTLS connection failed: %v", err)
	}
	defer client.Close()

	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Connected via mTLS: host=%s version=%s\n", info.Host, info.Version)
}
```

`WithMTLS(certFile, keyFile, caFile)` loads the client keypair for mutual authentication and the CA cert for server verification.

## Combining TLS with Auth Tokens

TLS and token authentication can be layered:

```go
client, err := kubemq.NewClient(ctx,
	kubemq.WithAddress("kubemq.example.com", 50000),
	kubemq.WithClientId("secure-client"),
	kubemq.WithTLS("certs/ca-cert.pem"),
	kubemq.WithAuthToken("your-jwt-token"),
)
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `certificate signed by unknown authority` | CA cert doesn't match server's issuing CA | Use the correct CA cert that signed the server certificate |
| `certificate has expired` | Server or client cert past its validity period | Renew the expired certificate |
| `tls: bad certificate` | Server rejected the client cert (mTLS) | Ensure the client cert is signed by the CA the server trusts |
| `connection refused` on port 50000 | Server not configured for TLS | Enable TLS in the KubeMQ server configuration |
| `x509: certificate is not valid for` | Hostname mismatch | Connect using the hostname in the server cert's SAN field |

**Tip:** Test cert validity before connecting:

```bash
openssl x509 -in certs/ca-cert.pem -noout -dates
openssl verify -CAfile certs/ca-cert.pem certs/client-cert.pem
```
