package config

import (
	"fmt"
	"time"
)

const (
	defaultMaxSendSize              = 1024 * 1024 * 100 // 100MB
	defaultMaxRcvSize               = 1024 * 1024 * 100 // 100MB
	defaultReconnectIntervalSeconds = 5
)

type Connection struct {
	Address                  string           `json:"address"`
	ClientId                 string           `json:"clientId"`
	AuthToken                string           `json:"authToken"`
	MaxSendSize              int              `json:"maxSendSize"`
	MaxReceiveSize           int              `json:"maxReceiveSize"`
	DisableAutoReconnect     bool             `json:"disableAutoReconnect"`
	ReconnectIntervalSeconds int              `json:"reconnectIntervalSeconds"`
	Tls                      *TlsConfig       `json:"tls"`
	KeepAlive                *KeepAliveConfig `json:"keepAlive"`
}

func NewConnection() *Connection {
	return &Connection{
		Address:        "",
		ClientId:       "",
		AuthToken:      "",
		MaxSendSize:    defaultMaxSendSize,
		MaxReceiveSize: defaultMaxRcvSize,
		Tls:            NewTlsConfig(),
		KeepAlive:      NewKeepAliveConfig(),
	}
}

func (c *Connection) SetAddress(address string) *Connection {
	c.Address = address
	return c
}

func (c *Connection) SetClientId(clientId string) *Connection {
	c.ClientId = clientId
	return c
}

func (c *Connection) SetAuthToken(authToken string) *Connection {
	c.AuthToken = authToken
	return c
}

func (c *Connection) SetTls(tls *TlsConfig) *Connection {
	c.Tls = tls
	return c
}

func (c *Connection) SetMaxSendSize(maxSendSize int) *Connection {
	c.MaxSendSize = maxSendSize
	return c
}

func (c *Connection) SetMaxReceiveSize(maxReceiveSize int) *Connection {
	c.MaxReceiveSize = maxReceiveSize
	return c
}
func (c *Connection) SetDisableAutoReconnect(disableAutoReconnect bool) *Connection {
	c.DisableAutoReconnect = disableAutoReconnect
	return c
}
func (c *Connection) SetReconnectIntervalSeconds(reconnectIntervalSeconds int) *Connection {
	c.ReconnectIntervalSeconds = reconnectIntervalSeconds
	return c
}

func (c *Connection) SetKeepAlive(keepAlive *KeepAliveConfig) *Connection {
	c.KeepAlive = keepAlive
	return c
}

func (c *Connection) GetReconnectIntervalDuration() time.Duration {
	return time.Duration(c.ReconnectIntervalSeconds) * time.Second
}
func (c *Connection) Complete() *Connection {
	if c.MaxSendSize == 0 {
		c.MaxSendSize = defaultMaxSendSize
	}
	if c.MaxReceiveSize == 0 {
		c.MaxReceiveSize = defaultMaxRcvSize
	}
	if c.ReconnectIntervalSeconds == 0 {
		c.ReconnectIntervalSeconds = defaultReconnectIntervalSeconds
	}
	return c
}

func (c *Connection) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("connection must have an address")
	}
	if c.ClientId == "" {
		return fmt.Errorf("connection must have a clientId")
	}
	if c.MaxSendSize < 0 {
		return fmt.Errorf("connection max send size must be greater than 0")
	}
	if c.MaxReceiveSize < 0 {
		return fmt.Errorf("connection max receive size must be greater than 0")
	}
	if c.ReconnectIntervalSeconds < 0 {
		return fmt.Errorf("connection reconnect interval must be greater than 0")
	}
	if c.Tls != nil {
		if err := c.Tls.validate(); err != nil {
			return err
		}
	}
	if c.KeepAlive != nil {
		if err := c.KeepAlive.validate(); err != nil {
			return err
		}
	}
	return nil
}
