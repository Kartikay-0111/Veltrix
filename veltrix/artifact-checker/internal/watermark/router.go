package watermark

import (
	"context"
	"log"
	"sync"

	"veltrix/artifact-checker/internal/models"
)

const defaultRouterBuffer = 8_192

// Router fans unordered events into one single-goroutine Processor per
// SubmissionID. This is the small but important bridge between the Redpanda
// consumer and the shadow engine: the consumer can be highly concurrent, while
// each submission's heap remains owned by exactly one goroutine.
type Router struct {
	AllowedLateness int64
	WorkerBuffer    int
	Logger          *log.Logger
}

func NewRouter(allowedLatenessMicros int64) *Router {
	return &Router{
		AllowedLateness: allowedLatenessMicros,
		WorkerBuffer:    defaultRouterBuffer,
		Logger:          log.Default(),
	}
}

// Run consumes raw events, dispatches by SubmissionID, and closes out after all
// per-submission processors have drained.
func (router *Router) Run(ctx context.Context, in <-chan models.OrderEvent, out chan<- models.OrderEvent) error {
	defer close(out)

	if router.WorkerBuffer <= 0 {
		router.WorkerBuffer = defaultRouterBuffer
	}
	if router.Logger == nil {
		router.Logger = log.Default()
	}

	workers := make(map[string]chan models.OrderEvent)
	var wg sync.WaitGroup

	closeWorkers := func() {
		for submissionID, ch := range workers {
			close(ch)
			delete(workers, submissionID)
		}
	}

	defer func() {
		closeWorkers()
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				closeWorkers()
				wg.Wait()
				return nil
			}
			if event.SubmissionID == "" {
				router.Logger.Printf("[watermark-router] dropping event without submission_id")
				continue
			}

			workerInput, ok := workers[event.SubmissionID]
			if !ok {
				workerInput = make(chan models.OrderEvent, router.WorkerBuffer)
				workers[event.SubmissionID] = workerInput
				wg.Add(1)
				go router.runWorker(ctx, event.SubmissionID, workerInput, out, &wg)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case workerInput <- event:
			}
		}
	}
}

func (router *Router) runWorker(
	ctx context.Context,
	submissionID string,
	in <-chan models.OrderEvent,
	out chan<- models.OrderEvent,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	processor := NewProcessor(router.AllowedLateness)
	if err := processor.Run(ctx, in, out); err != nil && ctx.Err() == nil {
		router.Logger.Printf("[watermark-router] processor for submission=%s stopped: %v", submissionID, err)
	}
}
