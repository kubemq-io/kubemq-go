package payload

import (
	"testing"
)

// TestRandomPaddingDoesNotUseCryptoRand verifies that randomPadding
// produces valid printable ASCII without relying on crypto/rand syscalls.
func TestRandomPaddingProducesValidASCII(t *testing.T) {
	result := randomPadding(1000)
	if len(result) != 1000 {
		t.Fatalf("expected 1000 bytes, got %d", len(result))
	}
	for i, b := range []byte(result) {
		if b < 33 || b > 126 {
			t.Errorf("byte %d: got %d, want [33, 126]", i, b)
		}
	}
}

// TestSizeDistributionReturnsValidSize verifies SelectSize returns a valid size.
func TestSizeDistributionReturnsValidSize(t *testing.T) {
	d, err := ParseDistribution("512:1,1024:2,2048:1")
	if err != nil {
		t.Fatal(err)
	}

	validSizes := map[int]bool{512: true, 1024: true, 2048: true}
	for i := 0; i < 1000; i++ {
		s := d.SelectSize()
		if !validSizes[s] {
			t.Fatalf("iteration %d: got invalid size %d", i, s)
		}
	}
}

// BenchmarkRandomPadding benchmarks the padding function.
// After fix: should be significantly faster (no crypto/rand syscalls).
func BenchmarkRandomPadding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		randomPadding(1024)
	}
}

// BenchmarkEncode benchmarks full message encoding.
func BenchmarkEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Encode("go", "events", "producer-1", uint64(i), 1024)
	}
}

// BenchmarkSizeDistribution benchmarks the size distribution selection.
func BenchmarkSizeDistribution(b *testing.B) {
	d, _ := ParseDistribution("512:1,1024:2,2048:1")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.SelectSize()
	}
}
