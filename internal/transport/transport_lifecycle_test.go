package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGRPCTransport_State(t *testing.T) {
	gt, _ := newTestTransport(t)
	assert.Equal(t, types.StateReady, gt.State())
}

func newTestTransportWithReconnect(t *testing.T) (*grpcTransport, *inlineServer) {
	t.Helper()
	gt, impl := newTestTransport(t)
	gt.reconnect = newReconnectLoop(
		types.ReconnectPolicy{},
		gt.stateMachine,
		gt.logger,
		func(_ context.Context) error { return nil },
		func(_ context.Context) {},
	)
	gt.callbackTimeout = 5 * time.Second
	gt.reconnectNotifyCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	gt.cancelCtx = cancel
	t.Cleanup(func() { cancel() })
	_ = ctx
	return gt, impl
}

func TestGRPCTransport_Close(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	err := gt.Close()
	assert.NoError(t, err)
	assert.Equal(t, types.StateClosed, gt.State())
}

func TestGRPCTransport_Close_Idempotent(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	err1 := gt.Close()
	err2 := gt.Close()
	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestGRPCTransport_EnterOperation_AfterClose(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	_ = gt.Close()
	assert.False(t, gt.enterOperation())
}

func TestGRPCTransport_WaitForReady_AlreadyReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	err := gt.waitForReady(context.Background())
	assert.NoError(t, err)
}

func TestGRPCTransport_WaitForReady_Closed(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	_ = gt.Close()
	err := gt.waitForReady(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client closed")
}

func TestGRPCTransport_WaitForReady_ContextTimeout(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)
	gt.waitForReadyEnabled = true
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := gt.checkReady(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestGRPCTransport_CheckReady_NotReady(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.stateMachine.transition(types.StateReconnecting)
	err := gt.checkReady(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready")
}

func TestGRPCTransport_CheckReady_WaitForReadyEnabled(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.waitForReadyEnabled = true
	err := gt.checkReady(context.Background())
	assert.NoError(t, err)
}

func TestGRPCTransport_DrainInFlight_NoOps(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.drainInFlight()
}

func TestGRPCTransport_DrainInFlight_Timeout(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.drainTimeout = 50 * time.Millisecond
	gt.inFlight.wg.Add(1)
	gt.drainInFlight()
	gt.inFlight.wg.Done()
}

func TestGRPCTransport_LogSecurityWarnings_InsecureSkipVerify(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.InsecureSkipVerify = true
	gt.logSecurityWarnings()
}

func TestGRPCTransport_LogSecurityWarnings_NoTLS(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.logSecurityWarnings()
}

func TestGRPCTransport_BuildDialOptions_Insecure(t *testing.T) {
	gt, _ := newTestTransport(t)
	ctx := context.Background()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
}

func TestGRPCTransport_BuildDialOptions_WithAuthToken(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.AuthToken = "test-token"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
	if gt.authClose != nil {
		gt.authClose()
	}
}

func TestGRPCTransport_BuildDialOptions_InsecureSkipVerify(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.InsecureSkipVerify = true
	ctx := context.Background()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
}

func TestGRPCTransport_BuildLegacyTLSCredentials_NoCert(t *testing.T) {
	gt, _ := newTestTransport(t)
	_, err := gt.buildLegacyTLSCredentials()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no valid TLS certificate")
}

func TestGRPCTransport_BuildLegacyTLSCredentials_InvalidCertData(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.CertData = "not-valid-pem"
	_, err := gt.buildLegacyTLSCredentials()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to append")
}

func TestBuildLegacyTLSCredentials_CertFile(t *testing.T) {
	certPEM, _ := generateSelfSignedCert(t)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))

	gt, _ := newTestTransport(t)
	gt.cfg.CertFile = certFile
	creds, err := gt.buildLegacyTLSCredentials()
	require.NoError(t, err)
	assert.NotNil(t, creds)
}

func TestBuildLegacyTLSCredentials_CertData(t *testing.T) {
	certPEM, _ := generateSelfSignedCert(t)
	gt, _ := newTestTransport(t)
	gt.cfg.CertData = string(certPEM)
	creds, err := gt.buildLegacyTLSCredentials()
	require.NoError(t, err)
	assert.NotNil(t, creds)
}

func TestGRPCTransport_BuildDialOptions_LegacySecured_NoCert(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.cfg.IsSecured = true
	ctx := context.Background()
	opts := gt.buildDialOptions(ctx)
	assert.NotEmpty(t, opts)
}

func TestGRPCTransport_CreateChannel(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, req *pb.Request) (*pb.Response, error) {
		assert.Equal(t, "create-channel", req.Metadata)
		assert.Equal(t, "events", req.Tags["channel_type"])
		assert.Equal(t, "test-ch", req.Tags["channel"])
		return &pb.Response{Executed: true}, nil
	}
	impl.mu.Unlock()

	err := gt.CreateChannel(context.Background(), &CreateChannelRequest{
		ClientID:    "test-client",
		Channel:     "test-ch",
		ChannelType: "events",
	})
	assert.NoError(t, err)
}

func TestGRPCTransport_CreateChannel_Error(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, fmt.Errorf("rpc error")
	}
	impl.mu.Unlock()

	err := gt.CreateChannel(context.Background(), &CreateChannelRequest{
		Channel:     "test-ch",
		ChannelType: "events",
	})
	assert.Error(t, err)
}

func TestGRPCTransport_CreateChannel_ResponseError(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Error: "channel exists"}, nil
	}
	impl.mu.Unlock()

	err := gt.CreateChannel(context.Background(), &CreateChannelRequest{
		Channel:     "test-ch",
		ChannelType: "events",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "channel exists")
}

func TestGRPCTransport_DeleteChannel(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, req *pb.Request) (*pb.Response, error) {
		assert.Equal(t, "delete-channel", req.Metadata)
		return &pb.Response{Executed: true}, nil
	}
	impl.mu.Unlock()

	err := gt.DeleteChannel(context.Background(), &DeleteChannelRequest{
		Channel:     "test-ch",
		ChannelType: "events",
	})
	assert.NoError(t, err)
}

func TestGRPCTransport_DeleteChannel_Error(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, fmt.Errorf("rpc error")
	}
	impl.mu.Unlock()

	err := gt.DeleteChannel(context.Background(), &DeleteChannelRequest{
		Channel:     "test-ch",
		ChannelType: "events",
	})
	assert.Error(t, err)
}

func TestGRPCTransport_ListChannels(t *testing.T) {
	gt, impl := newTestTransport(t)
	channels := []channelListItem{
		{Name: "ch1", Type: "events", IsActive: true},
	}
	data, _ := json.Marshal(channels)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, req *pb.Request) (*pb.Response, error) {
		assert.Equal(t, "list-channels", req.Metadata)
		return &pb.Response{Executed: true, Body: data}, nil
	}
	impl.mu.Unlock()

	result, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ChannelType: "events",
		Search:      "ch",
	})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "ch1", result[0].Name)
}

func TestGRPCTransport_ListChannels_Error(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return nil, fmt.Errorf("rpc error")
	}
	impl.mu.Unlock()

	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ChannelType: "events",
	})
	assert.Error(t, err)
}

func TestGRPCTransport_ListChannels_ResponseError(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Error: "not found"}, nil
	}
	impl.mu.Unlock()

	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ChannelType: "events",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGRPCTransport_ListChannels_ParseError(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.sendRequestFn = func(_ context.Context, _ *pb.Request) (*pb.Response, error) {
		return &pb.Response{Executed: true, Body: []byte("invalid json")}, nil
	}
	impl.mu.Unlock()

	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{
		ChannelType: "events",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse")
}

func TestCheckServerVersion_Success(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.pingFn = func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return &pb.PingResult{Host: "h", Version: "2.4.0"}, nil
	}
	impl.mu.Unlock()
	gt.checkServerVersion(context.Background())
}

func TestCheckServerVersion_OutOfRange(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.pingFn = func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return &pb.PingResult{Host: "h", Version: "1.0.0"}, nil
	}
	impl.mu.Unlock()
	gt.checkServerVersion(context.Background())
}

func TestCheckServerVersion_PingError(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.pingFn = func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return nil, fmt.Errorf("ping failed")
	}
	impl.mu.Unlock()
	gt.checkServerVersion(context.Background())
}

func TestCheckServerVersion_EmptyVersion(t *testing.T) {
	gt, impl := newTestTransport(t)
	impl.mu.Lock()
	impl.pingFn = func(_ context.Context, _ *pb.Empty) (*pb.PingResult, error) {
		return &pb.PingResult{Host: "h", Version: ""}, nil
	}
	impl.mu.Unlock()
	gt.checkServerVersion(context.Background())
}

func TestCloseOnce_Do(t *testing.T) {
	co := closeOnce{}
	assert.True(t, co.Do())
	assert.False(t, co.Do())
}

func TestGRPCTransport_NotifyReconnected(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	ch := gt.waitReconnect()
	gt.notifyReconnected()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waitReconnect channel not closed")
	}
}

func TestNoopTransportLogger(t *testing.T) {
	var l noopLogger
	l.Info("test", "k", "v")
	l.Warn("test", "k", "v")
	l.Error("test", "k", "v")
}

func TestGRPCTransport_GetClient(t *testing.T) {
	gt, _ := newTestTransport(t)
	c := gt.getClient()
	assert.NotNil(t, c)
}

func TestGRPCTransport_Ping_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.Ping(context.Background())
	assert.Error(t, err)
}

func TestGRPCTransport_SendEvent_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	err := gt.SendEvent(context.Background(), &SendEventRequest{})
	assert.Error(t, err)
}

func TestGRPCTransport_CreateChannel_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	err := gt.CreateChannel(context.Background(), &CreateChannelRequest{})
	assert.Error(t, err)
}

func TestGRPCTransport_DeleteChannel_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	err := gt.DeleteChannel(context.Background(), &DeleteChannelRequest{})
	assert.Error(t, err)
}

func TestGRPCTransport_ListChannels_Closed(t *testing.T) {
	gt, _ := newTestTransport(t)
	gt.closed.Store(true)
	_, err := gt.ListChannels(context.Background(), &ListChannelsRequest{})
	assert.Error(t, err)
}

func TestGRPCTransport_MonitorConnection_CancelledContext(t *testing.T) {
	gt, _ := newTestTransport(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gt.monitorConnection(ctx)
}

func TestKeepaliveConfig_DialOption(t *testing.T) {
	ka := KeepaliveConfig{
		Time:                10 * time.Second,
		Timeout:             5 * time.Second,
		PermitWithoutStream: true,
	}
	opt := ka.dialOption()
	assert.NotNil(t, opt)
}

func TestGRPCTransport_Close_FromReconnecting(t *testing.T) {
	gt, _ := newTestTransportWithReconnect(t)
	gt.stateMachine.transition(types.StateReconnecting)
	err := gt.Close()
	assert.NoError(t, err)
	assert.Equal(t, types.StateClosed, gt.State())
}
