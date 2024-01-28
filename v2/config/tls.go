package config

type TlsConfig struct {
	Enabled            bool   `json:"enabled"`
	Cert               string `json:"cert"`
	Key                string `json:"key"`
	Ca                 string `json:"ca"`
	SkipVerifyInsecure bool   `json:"skipVerifyInsecure"`
}

func NewTlsConfig() *TlsConfig {
	return &TlsConfig{
		Enabled:            false,
		Cert:               "",
		Key:                "",
		Ca:                 "",
		SkipVerifyInsecure: false,
	}
}

func (t *TlsConfig) SetEnabled(enabled bool) *TlsConfig {
	t.Enabled = enabled
	return t
}

func (t *TlsConfig) SetCert(cert string) *TlsConfig {
	t.Cert = cert
	return t
}

func (t *TlsConfig) SetKey(key string) *TlsConfig {
	t.Key = key
	return t
}

func (t *TlsConfig) SetCa(ca string) *TlsConfig {
	t.Ca = ca
	return t
}

func (t *TlsConfig) SetSkipVerifyInsecure(skipVerifyInsecure bool) *TlsConfig {
	t.SkipVerifyInsecure = skipVerifyInsecure
	return t
}
