package config

import (
	"fmt"
	"os"
)

type TlsConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert"`
	KeyFile  string `json:"key"`
	CaFile   string `json:"ca"`
}

func NewTlsConfig() *TlsConfig {
	return &TlsConfig{
		Enabled:  false,
		CertFile: "",
		KeyFile:  "",
		CaFile:   "",
	}
}

func (t *TlsConfig) SetEnabled(enabled bool) *TlsConfig {
	t.Enabled = enabled
	return t
}

func (t *TlsConfig) SetCertFile(cert string) *TlsConfig {
	t.CertFile = cert
	return t
}

func (t *TlsConfig) SetKeyFile(key string) *TlsConfig {
	t.KeyFile = key
	return t
}

func (t *TlsConfig) SetCaFile(ca string) *TlsConfig {
	t.CaFile = ca
	return t
}

func (t *TlsConfig) validate() error {
	if !t.Enabled {
		return nil
	}
	if t.CertFile != "" && !fileExists(t.CertFile) {
		return fmt.Errorf("tls configuration cert file %s not found", t.CertFile)
	}

	if t.KeyFile != "" && !fileExists(t.KeyFile) {
		return fmt.Errorf("tls configuration key file %s not found", t.KeyFile)
	}

	if t.CaFile != "" && !fileExists(t.CaFile) {
		return fmt.Errorf("tls configuration ca file %s not found", t.CaFile)
	}

	return nil
}

func fileExists(filePath string) bool {
	info, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
