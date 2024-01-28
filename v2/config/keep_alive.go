package config

import "fmt"

type KeepAliveConfig struct {
	Enabled               bool `json:"enabled"`
	PingIntervalInSeconds int  `json:"pingIntervalInSeconds"`
	PingTimeOutInSeconds  int  `json:"pingTimeOutInSeconds"`
}

func NewKeepAliveConfig() *KeepAliveConfig {
	return &KeepAliveConfig{
		PingIntervalInSeconds: 0,
		PingTimeOutInSeconds:  0,
	}
}

func (k *KeepAliveConfig) SetEnable(enable bool) *KeepAliveConfig {
	k.Enabled = enable
	return k
}

func (k *KeepAliveConfig) SetPingIntervalInSeconds(pingIntervalInSeconds int) *KeepAliveConfig {
	k.PingIntervalInSeconds = pingIntervalInSeconds
	return k
}

func (k *KeepAliveConfig) SetPingTimeOutInSeconds(pingTimeOutInSeconds int) *KeepAliveConfig {
	k.PingTimeOutInSeconds = pingTimeOutInSeconds
	return k
}

func (k *KeepAliveConfig) validate() error {
	if !k.Enabled {
		return nil
	}
	if k.PingIntervalInSeconds <= 0 {
		return fmt.Errorf("keep alive ping interval must be greater than 0")
	}
	if k.PingTimeOutInSeconds <= 0 {
		return fmt.Errorf("keep alive ping timeout must be greater than 0")
	}
	return nil
}
