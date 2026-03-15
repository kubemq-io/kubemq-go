package tracker

import (
	"sync"
	"sync/atomic"
)

// Tracker tracks message sequences per producer using a bitset-based approach.
// It detects gaps (loss), duplicates, and out-of-order delivery.
// Thread-safe for concurrent access from multiple consumer goroutines.
type Tracker struct {
	mu            sync.Mutex
	reorderWindow int

	// Per-producer state
	producers map[string]*producerState
}

type producerState struct {
	// The first sequence we've seen
	firstSeq uint64
	// The highest sequence we've committed (all seqs <= this are accounted for)
	highContiguous uint64
	// Bitset for sequences in the reorder window above highContiguous
	window []uint64
	// Total received count
	received atomic.Uint64
	// Total duplicate count
	duplicates atomic.Uint64
	// Total out-of-order count
	outOfOrder atomic.Uint64
	// Total confirmed lost
	confirmedLost atomic.Uint64
	// Last reported lost (for delta computation in DetectGaps)
	lastReportedLost uint64
	// Last seen sequence (for out-of-order detection)
	lastSeen uint64
	// Whether we've received any message
	initialized bool
}

func New(reorderWindow int) *Tracker {
	return &Tracker{
		reorderWindow: reorderWindow,
		producers:     make(map[string]*producerState),
	}
}

// Record records a received sequence for the given producer.
// Returns: isDuplicate, isOutOfOrder
func (t *Tracker) Record(producerID string, seq uint64) (isDuplicate bool, isOutOfOrder bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ps, ok := t.producers[producerID]
	if !ok {
		ps = &producerState{
			firstSeq:       seq,
			highContiguous: seq - 1,
			window:         make([]uint64, (t.reorderWindow+63)/64),
			initialized:    true,
			lastSeen:       seq,
		}
		t.producers[producerID] = ps
		setBit(ps.window, 0)
		ps.received.Add(1)
		t.advanceContiguous(ps)
		return false, false
	}

	ps.received.Add(1)

	// Check out-of-order
	if seq < ps.lastSeen {
		isOutOfOrder = true
		ps.outOfOrder.Add(1)
	}
	ps.lastSeen = seq

	// Sequence is already accounted for (below or at contiguous line)
	if seq <= ps.highContiguous {
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	// Position in window relative to highContiguous
	offset := seq - ps.highContiguous - 1
	if offset >= uint64(t.reorderWindow) {
		// Sequence is beyond the window — advance the window
		t.slideWindow(ps, seq)
		return false, isOutOfOrder
	}

	// Check if already set (duplicate)
	if getBit(ps.window, int(offset)) {
		ps.duplicates.Add(1)
		return true, isOutOfOrder
	}

	setBit(ps.window, int(offset))
	t.advanceContiguous(ps)
	return false, isOutOfOrder
}

// DetectGaps scans all producers and returns newly confirmed lost messages since the last call.
// Returns only the delta (new losses) to avoid double-counting when the caller adds to a counter.
func (t *Tracker) DetectGaps() map[string]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]uint64)
	for pid, ps := range t.producers {
		currentTotal := ps.confirmedLost.Load()
		delta := currentTotal - ps.lastReportedLost
		if delta > 0 {
			result[pid] = delta
			ps.lastReportedLost = currentTotal
		}
	}
	return result
}

// Stats returns aggregate stats for a producer.
func (t *Tracker) Stats(producerID string) (received, duplicates, outOfOrder, confirmedLost uint64) {
	t.mu.Lock()
	ps, ok := t.producers[producerID]
	t.mu.Unlock()
	if !ok {
		return 0, 0, 0, 0
	}
	return ps.received.Load(), ps.duplicates.Load(), ps.outOfOrder.Load(), ps.confirmedLost.Load()
}

// AllStats returns stats for all tracked producers.
func (t *Tracker) AllStats() map[string][4]uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string][4]uint64, len(t.producers))
	for pid, ps := range t.producers {
		result[pid] = [4]uint64{
			ps.received.Load(),
			ps.duplicates.Load(),
			ps.outOfOrder.Load(),
			ps.confirmedLost.Load(),
		}
	}
	return result
}

// TotalReceived returns the total received count across all producers.
func (t *Tracker) TotalReceived() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.received.Load()
	}
	return total
}

// TotalDuplicates returns the total duplicate count across all producers.
func (t *Tracker) TotalDuplicates() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.duplicates.Load()
	}
	return total
}

// TotalLost returns the total confirmed lost count across all producers.
func (t *Tracker) TotalLost() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.confirmedLost.Load()
	}
	return total
}

// TotalOutOfOrder returns the total out-of-order count across all producers.
func (t *Tracker) TotalOutOfOrder() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var total uint64
	for _, ps := range t.producers {
		total += ps.outOfOrder.Load()
	}
	return total
}

// Reset clears all tracking state (used after warmup).
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.producers = make(map[string]*producerState)
}

func (t *Tracker) advanceContiguous(ps *producerState) {
	for {
		if !getBit(ps.window, 0) {
			break
		}
		ps.highContiguous++
		// Shift the window left by 1
		shiftLeft(ps.window)
	}
}

func (t *Tracker) slideWindow(ps *producerState, newSeq uint64) {
	newHigh := newSeq - uint64(t.reorderWindow)
	if newHigh <= ps.highContiguous {
		// Just set the bit
		offset := newSeq - ps.highContiguous - 1
		if int(offset) < t.reorderWindow {
			setBit(ps.window, int(offset))
		}
		return
	}

	// Count gaps being pushed out of the window
	slideDist := newHigh - ps.highContiguous
	if slideDist > uint64(t.reorderWindow) {
		slideDist = uint64(t.reorderWindow)
	}

	var gaps uint64
	for i := uint64(0); i < slideDist; i++ {
		if !getBit(ps.window, int(i)) {
			gaps++
		}
	}
	if gaps > 0 {
		ps.confirmedLost.Add(gaps)
	}

	// Shift the window
	for i := uint64(0); i < slideDist; i++ {
		shiftLeft(ps.window)
	}
	ps.highContiguous = newHigh

	// Set the bit for the new sequence
	offset := newSeq - ps.highContiguous - 1
	if int(offset) < t.reorderWindow {
		setBit(ps.window, int(offset))
	}

	t.advanceContiguous(ps)
}

func setBit(window []uint64, pos int) {
	if pos < 0 || pos >= len(window)*64 {
		return
	}
	window[pos/64] |= 1 << (uint(pos) % 64)
}

func getBit(window []uint64, pos int) bool {
	if pos < 0 || pos >= len(window)*64 {
		return false
	}
	return window[pos/64]&(1<<(uint(pos)%64)) != 0
}

func shiftLeft(window []uint64) {
	carry := false
	for i := len(window) - 1; i >= 0; i-- {
		newCarry := window[i]&(1<<63) != 0
		window[i] <<= 1
		if carry {
			window[i] |= 1
		}
		carry = newCarry
	}
}
