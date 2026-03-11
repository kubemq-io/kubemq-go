package types

// JitterMode controls how jitter is applied to reconnection backoff delays.
type JitterMode int

const (
	// JitterNone applies no jitter to backoff delays.
	JitterNone JitterMode = iota
	// JitterFull applies full jitter: delay = rand(0, calculated_delay).
	JitterFull
	// JitterEqual applies equal jitter: delay = calculated_delay/2 + rand(0, calculated_delay/2).
	JitterEqual
)

// String returns a human-readable representation of the JitterMode.
func (j JitterMode) String() string {
	switch j {
	case JitterNone:
		return "NONE"
	case JitterFull:
		return "FULL"
	case JitterEqual:
		return "EQUAL"
	default:
		return "UNKNOWN"
	}
}
