package payload

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Message is the JSON payload embedded in every burn-in message body.
type Message struct {
	SDK            string `json:"sdk"`
	Pattern        string `json:"pattern"`
	ProducerID     string `json:"producer_id"`
	Sequence       uint64 `json:"sequence"`
	TimestampNS    int64  `json:"timestamp_ns"`
	PayloadPadding string `json:"payload_padding,omitempty"`
}

// Encode serializes a Message to JSON bytes with random padding to reach targetSize.
// Returns the body bytes and the CRC32 hex string for tagging.
func Encode(sdk, pattern, producerID string, seq uint64, targetSize int) ([]byte, string) {
	m := Message{
		SDK:         sdk,
		Pattern:     pattern,
		ProducerID:  producerID,
		Sequence:    seq,
		TimestampNS: time.Now().UnixNano(),
	}

	// Marshal without padding to measure overhead
	base, _ := json.Marshal(m)
	overhead := len(base)

	if targetSize > overhead+2 { // +2 for quotes around padding
		paddingLen := targetSize - overhead - 2 // subtract quotes
		// Account for JSON key "payload_padding" already in struct
		// Re-measure with a 1-byte padding
		m.PayloadPadding = "x"
		withKey, _ := json.Marshal(m)
		keyOverhead := len(withKey) - overhead - 1 // extra bytes from key + quotes - 1 padding byte

		paddingLen = targetSize - overhead - keyOverhead
		if paddingLen > 0 {
			m.PayloadPadding = randomPadding(paddingLen)
		} else {
			m.PayloadPadding = ""
		}
	}

	body, _ := json.Marshal(m)

	crcVal := crc32.ChecksumIEEE(body)
	crcHex := fmt.Sprintf("%08x", crcVal)

	return body, crcHex
}

// Decode parses a Message from JSON body bytes.
func Decode(body []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("payload decode: %w", err)
	}
	return &m, nil
}

// VerifyCRC checks the CRC32 hash tag against the actual body bytes.
func VerifyCRC(body []byte, crcHex string) bool {
	actual := crc32.ChecksumIEEE(body)
	actualHex := fmt.Sprintf("%08x", actual)
	return actualHex == crcHex
}

// SizeDistribution represents weighted size options.
type SizeDistribution struct {
	sizes   []int
	weights []int
	total   int
}

// ParseDistribution parses a "size:weight,size:weight" string.
func ParseDistribution(s string) (*SizeDistribution, error) {
	d := &SizeDistribution{}
	parts := strings.Split(s, ",")
	for _, p := range parts {
		kv := strings.SplitN(strings.TrimSpace(p), ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid distribution entry: %q", p)
		}
		size, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid size in distribution: %q", kv[0])
		}
		weight, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid weight in distribution: %q", kv[1])
		}
		d.sizes = append(d.sizes, size)
		d.weights = append(d.weights, weight)
		d.total += weight
	}
	if d.total == 0 {
		return nil, fmt.Errorf("distribution total weight must be > 0")
	}
	return d, nil
}

// SelectSize returns a size based on the weighted distribution.
func (d *SizeDistribution) SelectSize() int {
	r := rand.IntN(d.total)
	cumulative := 0
	for i, w := range d.weights {
		cumulative += w
		if r < cumulative {
			return d.sizes[i]
		}
	}
	return d.sizes[len(d.sizes)-1]
}

// randomPadding generates random printable ASCII padding bytes.
func randomPadding(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	// Fill with random bytes using math/rand, then map to printable ASCII.
	// Use Uint32 to generate 4 bytes at a time, mapping each byte
	// to the printable range via masking (faster than modulo).
	for i := 0; i < n; {
		v := rand.Uint32()
		for j := 0; j < 4 && i < n; j++ {
			// Map byte to [33, 126] — 94 values in printable ASCII
			b[i] = 33 + byte(v&0x7F)%94
			v >>= 8
			i++
		}
	}
	return string(b)
}
