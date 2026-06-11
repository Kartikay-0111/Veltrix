package watermark

import (
	"container/heap"
	"context"
	"time"

	"veltrix/artifact-checker/internal/models"
)

const (
	// DefaultAllowedLateness is the event-time grace period used before an event
	// is considered safe to emit. The incoming timestamps are microseconds, but a
	// time.Duration constant keeps the default human-readable.
	DefaultAllowedLateness = 2 * time.Second

	// DefaultAllowedLatenessMicros is 2000ms expressed in epoch microseconds.
	DefaultAllowedLatenessMicros = int64(DefaultAllowedLateness / time.Microsecond)
)

// Processor reorders out-of-order OrderEvents by event time using a min-heap.
//
// A Processor is deliberately not thread-safe. Run exactly one Processor
// goroutine per SubmissionID so the heap and MaxSeenTimestamp never require a
// mutex in the hot path.
type Processor struct {
	// MaxSeenTimestamp is the largest EventTimestamp observed so far.
	MaxSeenTimestamp int64

	// AllowedLateness is measured in microseconds and subtracted from
	// MaxSeenTimestamp to derive the current watermark.
	AllowedLateness int64

	events       EventHeap
	nextSequence uint64
}

// NewDefaultProcessor creates a Processor with the required default 2000ms
// event-time lateness window.
func NewDefaultProcessor() *Processor {
	return NewProcessor(DefaultAllowedLatenessMicros)
}

// NewProcessor creates a Processor with a caller-provided lateness window in
// microseconds. Pass DefaultAllowedLatenessMicros for the production default.
func NewProcessor(allowedLatenessMicros int64) *Processor {
	if allowedLatenessMicros < 0 {
		allowedLatenessMicros = DefaultAllowedLatenessMicros
	}

	processor := &Processor{
		AllowedLateness: allowedLatenessMicros,
		events:          EventHeap{},
	}

	heap.Init(&processor.events)

	return processor
}

// Run consumes events from in, buffers them in event-time order, and emits safe
// events to out. The caller owns out and should close it after Run returns when
// appropriate.
//
// The algorithm follows the event-time watermark contract:
//  1. Receive OrderEvent from the input channel.
//  2. Push it into the min-heap.
//  3. Update MaxSeenTimestamp if this is the newest event-time observed.
//  4. Compute Watermark = MaxSeenTimestamp - AllowedLateness.
//  5. Pop and emit every heap root with EventTimestamp <= Watermark.
//
// When in is closed, Run drains all remaining events in strict heap order because
// no later input can arrive to move the watermark forward.
func (p *Processor) Run(ctx context.Context, in <-chan models.OrderEvent, out chan<- models.OrderEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return p.Flush(ctx, out)
			}

			if err := p.Process(ctx, event, out); err != nil {
				return err
			}
		}
	}
}

// Process pushes one event and emits every event now behind the watermark.
func (p *Processor) Process(ctx context.Context, event models.OrderEvent, out chan<- models.OrderEvent) error {
	p.Enqueue(event)
	return p.EmitReady(ctx, out)
}

// Enqueue adds one event to the heap and advances MaxSeenTimestamp when needed.
func (p *Processor) Enqueue(event models.OrderEvent) {
	heap.Push(&p.events, queuedEvent{
		event:    event,
		sequence: p.nextSequence,
	})
	p.nextSequence++

	if event.EventTimestamp > p.MaxSeenTimestamp {
		p.MaxSeenTimestamp = event.EventTimestamp
	}
}

// Watermark returns the current event-time cutoff. Any event at or before this
// timestamp is safe to release to the shadow engine.
func (p *Processor) Watermark() int64 {
	return p.MaxSeenTimestamp - p.AllowedLateness
}

// EmitReady pops every event whose timestamp is no newer than the current
// watermark. Because the heap root is always the earliest event, the loop can
// stop as soon as the root is newer than the watermark.
func (p *Processor) EmitReady(ctx context.Context, out chan<- models.OrderEvent) error {
	for p.events.Len() > 0 {
		nextEvent, _ := p.events.Peek()
		if nextEvent.EventTimestamp > p.Watermark() {
			return nil
		}

		item := heap.Pop(&p.events).(queuedEvent)
		if err := send(ctx, out, item.event); err != nil {
			return err
		}
	}

	return nil
}

// Flush emits all buffered events in heap order. Use this when the upstream
// consumer closes or a benchmark finishes and no more events for a submission can
// arrive.
func (p *Processor) Flush(ctx context.Context, out chan<- models.OrderEvent) error {
	for p.events.Len() > 0 {
		item := heap.Pop(&p.events).(queuedEvent)
		if err := send(ctx, out, item.event); err != nil {
			return err
		}
	}

	return nil
}

func send(ctx context.Context, out chan<- models.OrderEvent, event models.OrderEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- event:
		return nil
	}
}
