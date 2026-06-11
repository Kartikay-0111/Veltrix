package watermark

import "veltrix/artifact-checker/internal/models"

// queuedEvent wraps an OrderEvent with a monotonic sequence number assigned at
// ingest time. The heap is ordered primarily by EventTimestamp, but this
// sequence number gives deterministic FIFO behavior for events that share the
// same microsecond timestamp.
type queuedEvent struct {
	event    models.OrderEvent
	sequence uint64
}

// EventHeap implements heap.Interface as a min-heap ordered by event time.
//
// container/heap itself does not know whether a heap is a min-heap or max-heap.
// It repeatedly calls Less(i, j), Swap(i, j), Push(x), and Pop() on this type.
// Because Less returns true for the earlier EventTimestamp, heap.Init/Push/Pop
// arrange the slice so the earliest event is always at index 0.
//
// The slice is intentionally private to the watermark package's hot path: a
// Processor owns one EventHeap and mutates it from one goroutine only.
type EventHeap []queuedEvent

// Len returns the number of queued events. It is part of heap.Interface.
func (h EventHeap) Len() int {
	return len(h)
}

// Less defines heap priority.
//
// For a min-heap, "less" must mean "should be closer to the root." The earliest
// EventTimestamp therefore wins. When two events have the same timestamp, the
// lower ingest sequence wins so replay remains stable and predictable.
func (h EventHeap) Less(i, j int) bool {
	left := h[i]
	right := h[j]

	if left.event.EventTimestamp == right.event.EventTimestamp {
		return left.sequence < right.sequence
	}

	return left.event.EventTimestamp < right.event.EventTimestamp
}

// Swap exchanges two slice positions. container/heap calls this while bubbling
// an item up or down to restore the heap invariant after Push or Pop.
func (h EventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push appends a new queuedEvent to the backing slice.
//
// container/heap calls Push first, then performs its own sift-up operation using
// Less and Swap. This method must use a pointer receiver because appending can
// reallocate the slice header.
func (h *EventHeap) Push(value any) {
	*h = append(*h, value.(queuedEvent))
}

// Pop removes and returns the last element in the backing slice.
//
// This looks surprising at first: callers conceptually pop the root, but
// container/heap swaps the root with the last element before invoking this
// method. By removing the last element here, the public heap.Pop call receives
// the previous root while the remaining slice is ready to be sifted back into a
// valid heap.
func (h *EventHeap) Pop() any {
	old := *h
	lastIndex := len(old) - 1
	item := old[lastIndex]

	old[lastIndex] = queuedEvent{}
	*h = old[:lastIndex]

	return item
}

// Peek returns the current root without mutating the heap. Because this is a
// min-heap, the root is the earliest event by EventTimestamp.
func (h EventHeap) Peek() (models.OrderEvent, bool) {
	if len(h) == 0 {
		return models.OrderEvent{}, false
	}

	return h[0].event, true
}
