package shadowengine

import (
	"testing"

	"veltrix/artifact-checker/internal/models"
)

// applyAll feeds events in order and returns the index of the first event that
// flipped the submission to incorrect, or -1 if it stayed correct throughout.
func applyAll(engine *Engine, events []models.OrderEvent) (int, models.CorrectnessUpdate) {
	for i, event := range events {
		update, changed := engine.Apply(event)
		if changed && !update.IsCorrect {
			return i, update
		}
	}
	return -1, models.CorrectnessUpdate{}
}

func TestEngineAcceptsValidFillWithinLimitAndVolume(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		// A resting ask at 100, then a marketable buy that fills against it.
		{SubmissionID: "s1", OrderID: "sell-1", Action: "SELL", Price: 100, Volume: 5},
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 105, Volume: 5},
		// Contestant reports a fill of the buy at 100 (price improvement), qty 5.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 100, Volume: 5},
	}

	if idx, update := applyAll(engine, events); idx != -1 {
		t.Fatalf("expected all events valid, but event %d failed: %+v", idx, update)
	}
}

func TestEngineRejectsBuyExecutedAboveLimit(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 100, Volume: 5},
		// Filled at 101 — above the buy limit of 100. Incorrect.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 101, Volume: 1},
	}

	idx, update := applyAll(engine, events)
	if idx != 1 || update.IsCorrect {
		t.Fatalf("expected fill (event 1) to be rejected, got idx=%d update=%+v", idx, update)
	}
}

func TestEngineRejectsSellExecutedBelowLimit(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "sell-1", Action: "SELL", Price: 100, Volume: 5},
		// Filled at 99 — below the sell limit of 100. Incorrect.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "sell-1", ExecutionPrice: 99, Volume: 1},
	}

	idx, update := applyAll(engine, events)
	if idx != 1 || update.IsCorrect {
		t.Fatalf("expected fill (event 1) to be rejected, got idx=%d update=%+v", idx, update)
	}
}

func TestEngineRejectsOverFill(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 100, Volume: 3},
		// Two fills totalling 4 > submitted 3. The second must be rejected.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 100, Volume: 2},
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 100, Volume: 2},
	}

	idx, update := applyAll(engine, events)
	if idx != 2 || update.IsCorrect {
		t.Fatalf("expected over-fill (event 2) to be rejected, got idx=%d update=%+v", idx, update)
	}
}

func TestEngineAcceptsMarketOrderFillIgnoringLimitBound(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		// MARKET buy (price 0) — no limit bound; any execution price is allowed.
		{SubmissionID: "s1", OrderID: "mkt-1", Action: "BUY", Price: 0, Volume: 10},
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "mkt-1", ExecutionPrice: 250, Volume: 10},
	}

	if idx, update := applyAll(engine, events); idx != -1 {
		t.Fatalf("expected market-order fill to be valid, but event %d failed: %+v", idx, update)
	}
}

func TestEngineToleratesFillForUnknownAggressor(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		// No intent was ever seen for this aggressor (telemetry gap) — tolerate.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "ghost-1", ExecutionPrice: 100, Volume: 1},
	}

	if idx, update := applyAll(engine, events); idx != -1 {
		t.Fatalf("expected unknown-aggressor fill to be tolerated, got idx=%d update=%+v", idx, update)
	}
}

func TestEngineRejectsNonPositiveFillQuantity(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 100, Volume: 5},
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 100, Volume: 0},
	}

	idx, update := applyAll(engine, events)
	if idx != 1 || update.IsCorrect {
		t.Fatalf("expected non-positive fill (event 1) to be rejected, got idx=%d update=%+v", idx, update)
	}
}

// Priority check is gated; with the flag on, a fill worse than the top of book
// the aggressor saw on arrival is rejected.
func TestEngineRejectsWorseThanTopOfBookWhenStrictEnabled(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	engine.strictPriority = true

	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "ask-1", Action: "SELL", Price: 100, Volume: 5},
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 105, Volume: 5},
		// Executed at 101 although the best ask on arrival was 100 — worse than
		// top of book. Rejected only because strictPriority is enabled.
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 101, Volume: 1},
	}

	idx, update := applyAll(engine, events)
	if idx != 2 || update.IsCorrect {
		t.Fatalf("expected priority violation (event 2) to be rejected, got idx=%d update=%+v", idx, update)
	}
}

func TestEngineIgnoresPriorityViolationWhenStrictDisabled(t *testing.T) {
	t.Parallel()

	engine := New(nil) // strictPriority defaults to false
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "ask-1", Action: "SELL", Price: 100, Volume: 5},
		{SubmissionID: "s1", OrderID: "buy-1", Action: "BUY", Price: 105, Volume: 5},
		// 101 is worse than top-of-book ask 100 but still within the buy limit of
		// 105, so with priority disabled this is accepted (the sound checks pass).
		{SubmissionID: "s1", Action: "FILL", AggressorOrderID: "buy-1", ExecutionPrice: 101, Volume: 1},
	}

	if idx, update := applyAll(engine, events); idx != -1 {
		t.Fatalf("expected fill to be tolerated with priority disabled, but event %d failed: %+v", idx, update)
	}
}

func TestEngineRejectsUnknownAction(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "x-1", Action: "TELEPORT", Price: 100, Volume: 1},
	}

	idx, update := applyAll(engine, events)
	if idx != 0 || update.IsCorrect {
		t.Fatalf("expected unknown action to be rejected, got idx=%d update=%+v", idx, update)
	}
}
