package grpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"os"
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

	if cfg.CertFile == "" && cfg.KeyFile == "" {
		return options, nil
	}
	clientCert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert %s and key %s, %w", cfg.CertFile, cfg.KeyFile, err)
	}
	var certPool *x509.CertPool
	if cfg.CaFile != "" {
		caCert, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca cert file %s, %w", cfg.CaFile, err)
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("failed to append ca cert %s to cert pool", cfg.CaFile)
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
