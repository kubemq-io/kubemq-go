package grpc

type ServerInfo struct {
	Host                string
	Version             string
	ServerStartTime     int64
	ServerUpTimeSeconds int64
}
