package grpc

import (
	"github.com/kubemq-io/kubemq-go/v2/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"time"
)

func getKeepAliveConnectionOptions(cfg *config.KeepAliveConfig) []grpc.DialOption {
	if !cfg.Enabled {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(cfg.PingIntervalInSeconds) * time.Second,
			Timeout:             time.Duration(cfg.PingTimeOutInSeconds) * time.Second,
			PermitWithoutStream: true,
		}),
	}
}
