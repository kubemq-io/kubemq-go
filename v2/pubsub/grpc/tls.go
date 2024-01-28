package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func getTLSConnectionOptions(cfg *config.TlsConfig) ([]grpc.DialOption, error) {
	var options []grpc.DialOption
	if cfg == nil {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return options, nil
	}

	if !cfg.Enabled {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return options, nil
	}
	if cfg.SkipVerifyInsecure {
		options = append(options)
	}
	if cfg.Cert == "" && cfg.Key == "" {
		return options, nil
	}
	certBlock, _ := pem.Decode([]byte(cfg.Cert))
	if certBlock == nil {
		return nil, fmt.Errorf("failed to parse tls certificate PEM")
	}
	keyBlock, _ := pem.Decode([]byte(cfg.Key))
	if keyBlock == nil {
		return nil, fmt.Errorf("failed to parse tls key PEM")
	}
	clientCert, err := tls.X509KeyPair(pem.EncodeToMemory(certBlock), pem.EncodeToMemory(keyBlock))
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert and key: %w", err)
	}
	var certPool *x509.CertPool
	if cfg.Ca != "" {
		caBlock, _ := pem.Decode([]byte(cfg.Ca))
		if caBlock == nil {
			return nil, fmt.Errorf("failed to parse tls ca PEM")
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caBlock.Bytes); !ok {
			return nil, fmt.Errorf("failed to append ca certs")
		}
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
	}
	if certPool != nil {
		tlsConfig.RootCAs = certPool
	}
	options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	return options, nil
}
