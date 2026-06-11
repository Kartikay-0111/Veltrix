package shadowengine

import (
	"testing"

	"veltrix/artifact-checker/internal/models"
)

func TestEngineRejectsFIFOViolationWhenMatchedOrderProvided(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "sell-old", Action: "SELL", Price: 100, Volume: 1},
		{SubmissionID: "s1", OrderID: "sell-new", Action: "SELL", Price: 100, Volume: 1},
		{SubmissionID: "s1", OrderID: "buy", Action: "BUY", Price: 101, Volume: 1, MatchedOrderID: "sell-new"},
	}

	for i, event := range events {
		update, changed := engine.Apply(event)
		if i < len(events)-1 && changed {
			t.Fatalf("unexpected failure before violation: %+v", update)
		}
		if i == len(events)-1 {
			if !changed || update.IsCorrect {
				t.Fatalf("expected incorrect update, got changed=%v update=%+v", changed, update)
			}
		}
	}
}

func TestEngineRejectsWorseThanTopOfBookExecution(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "ask", Action: "SELL", Price: 100, Volume: 1},
		{SubmissionID: "s1", OrderID: "buy", Action: "BUY", Price: 105, Volume: 1, ExecutionPrice: 101},
	}

	engine.Apply(events[0])
	update, changed := engine.Apply(events[1])
	if !changed || update.IsCorrect {
		t.Fatalf("expected incorrect update, got changed=%v update=%+v", changed, update)
	}
}

func TestEngineReplaysValidCrossingOrders(t *testing.T) {
	t.Parallel()

	engine := New(nil)
	events := []models.OrderEvent{
		{SubmissionID: "s1", OrderID: "ask", Action: "SELL", Price: 100, Volume: 2},
		{SubmissionID: "s1", OrderID: "buy", Action: "BUY", Price: 100, Volume: 1, MatchedOrderID: "ask", ExecutionPrice: 100},
		{SubmissionID: "s1", OrderID: "cancel-ask", Action: "CANCEL", Price: 0, Volume: 0},
	}

	if _, changed := engine.Apply(events[0]); changed {
		t.Fatal("first event should be valid")
	}
	if _, changed := engine.Apply(events[1]); changed {
		t.Fatal("crossing buy should be valid")
	}

	events[2].OrderID = "ask"
	if _, changed := engine.Apply(events[2]); changed {
		t.Fatal("cancel of remaining partial order should be valid")
	}
}
