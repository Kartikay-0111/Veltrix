package shadowengine

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"veltrix/artifact-checker/internal/models"
)

// Engine consumes watermark-ordered events and validates each submission on one
// goroutine. No mutex is used in the hot path; sequential channel delivery is
// the synchronization boundary.
//
// Validation model
// ────────────────
// The bot emits two kinds of events per submission, both keyed by the
// bot-generated order_id:
//
//   - Intents  (Action = BUY | SELL | CANCEL): what the bot asked the
//     contestant's exchange to do. These build a reference order book.
//   - Fills    (Action = FILL): what the contestant's engine actually did,
//     carrying the AggressorOrderID join key back to the intent that caused it.
//
// A fill is validated against its own intent:
//
//   - Limit-price bound (sound, ordering-independent): a BUY must not execute
//     above its limit; a SELL must not execute below its limit.
//   - Volume conservation (sound, ordering-independent): the cumulative filled
//     quantity attributed to an order (as the aggressor) must not exceed the
//     quantity it submitted.
//
// These two checks never touch the order book and hold under full concurrent
// load. A third, price-time priority check ("executed worse than top of book")
// does depend on event-time ordering — which under concurrent multi-bot load is
// not the server's true arrival order — so it is gated behind
// STRICT_PRICE_TIME_PRIORITY and defaults off to avoid failing correct
// submissions. It is intended for deterministic / single-bot grading.
type Engine struct {
	subs           map[string]*submission
	states         map[string]validationState
	logger         *log.Logger
	strictPriority bool
}

type validationState struct {
	correct bool
	reason  string
}

// submission holds the per-submission validation context.
type submission struct {
	book           *orderBook
	intents        map[string]*intent // aggressor (bot) order_id -> intent
	filled         map[string]int     // aggressor (bot) order_id -> cumulative filled qty
	fillsValidated int                // count of fills checked against an intent
}

// intent captures the aggressor information a fill is validated against.
type intent struct {
	side         string  // BUY | SELL
	limitPrice   float64 // limit price (0 for MARKET)
	market       bool    // MARKET order (no limit bound)
	submittedQty int     // quantity the bot asked for
	bestOpposing float64 // best opposing price in the book when this intent arrived
	hasOpposing  bool    // whether bestOpposing is meaningful
}

func New(logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.Default()
	}

	engine := &Engine{
		subs:           make(map[string]*submission),
		states:         make(map[string]validationState),
		logger:         logger,
		strictPriority: strictPriorityFromEnv(),
	}
	logger.Printf("[shadowengine] ready (strict price-time priority: %t)", engine.strictPriority)
	return engine
}

// strictPriorityFromEnv reads STRICT_PRICE_TIME_PRIORITY. The priority check is
// reliable only under deterministic grading, so it is opt-in.
func strictPriorityFromEnv() bool {
	raw := strings.TrimSpace(os.Getenv("STRICT_PRICE_TIME_PRIORITY"))
	if raw == "" {
		return false
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return enabled
}

// Run consumes globally routed, per-submission ordered events and emits an
// update only when a submission transitions into an incorrect state.
func (engine *Engine) Run(
	ctx context.Context,
	in <-chan models.OrderEvent,
	updates chan<- models.CorrectnessUpdate,
) error {
	defer close(updates)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			if update, changed := engine.Apply(event); changed {
				if err := send(ctx, updates, update); err != nil {
					return err
				}
			}
		}
	}
}

// Apply validates and replays one event. It returns changed=true only when a
// submission first fails validation.
func (engine *Engine) Apply(event models.OrderEvent) (models.CorrectnessUpdate, bool) {
	submissionID := event.SubmissionID
	if submissionID == "" {
		return models.CorrectnessUpdate{}, false
	}

	state, ok := engine.states[submissionID]
	if !ok {
		state = validationState{correct: true}
		engine.states[submissionID] = state
	}
	if !state.correct {
		return models.CorrectnessUpdate{SubmissionID: submissionID, IsCorrect: false, Reason: state.reason}, false
	}

	sub := engine.subs[submissionID]
	if sub == nil {
		sub = newSubmission()
		engine.subs[submissionID] = sub
	}

	if err := engine.process(sub, event); err != nil {
		engine.states[submissionID] = validationState{correct: false, reason: err.Error()}
		engine.logger.Printf("[shadowengine] submission=%s incorrect: %v", submissionID, err)
		return models.CorrectnessUpdate{
			SubmissionID: submissionID,
			IsCorrect:    false,
			Reason:       err.Error(),
		}, true
	}

	return models.CorrectnessUpdate{SubmissionID: submissionID, IsCorrect: true}, false
}

// process routes one event to the intent replay path or the fill validation
// path. A returned error means the submission is incorrect.
func (engine *Engine) process(sub *submission, event models.OrderEvent) error {
	action := strings.ToUpper(strings.TrimSpace(event.Action))
	switch action {
	case "BUY", "SELL":
		return sub.onAggressorIntent(action, event)
	case "CANCEL":
		return sub.book.applyCancel(event)
	case "FILL":
		return engine.validateFill(sub, event)
	default:
		return fmt.Errorf("unknown action %q for order_id=%s", event.Action, event.OrderID)
	}
}

func (engine *Engine) IsCorrect(submissionID string) bool {
	state, ok := engine.states[submissionID]
	return !ok || state.correct
}

func newSubmission() *submission {
	return &submission{
		book:    newOrderBook(),
		intents: make(map[string]*intent),
		filled:  make(map[string]int),
	}
}

// onAggressorIntent records a BUY/SELL intent and replays it into the reference
// book. The book is used only by the (gated) price-time priority check; the
// sound limit/volume checks rely only on the recorded intent.
func (s *submission) onAggressorIntent(side string, event models.OrderEvent) error {
	if err := validateNewOrder(event); err != nil {
		return err
	}
	if _, exists := s.book.liveOrderIDs[event.OrderID]; exists {
		return fmt.Errorf("duplicate live order_id=%s", event.OrderID)
	}

	market := event.Price <= 0

	// Capture the best opposing price BEFORE matching consumes liquidity, so the
	// priority check can compare a fill against the top of book the aggressor saw.
	bestOpposing, hasOpposing := 0.0, false
	if side == "BUY" {
		if len(s.book.asks) > 0 {
			bestOpposing, hasOpposing = s.book.asks[0].price, true
		}
	} else { // SELL
		if len(s.book.bids) > 0 {
			bestOpposing, hasOpposing = s.book.bids[0].price, true
		}
	}

	s.intents[event.OrderID] = &intent{
		side:         side,
		limitPrice:   event.Price,
		market:       market,
		submittedQty: event.Volume,
		bestOpposing: bestOpposing,
		hasOpposing:  hasOpposing,
	}

	if side == "BUY" {
		s.book.applyBuy(event)
	} else {
		s.book.applySell(event)
	}
	return nil
}

// validateFill checks one contestant-reported fill against the intent that
// produced it (joined via AggressorOrderID).
func (engine *Engine) validateFill(sub *submission, event models.OrderEvent) error {
	if event.Volume <= 0 {
		return fmt.Errorf("fill with non-positive quantity=%d (aggressor_order_id=%s)", event.Volume, event.AggressorOrderID)
	}

	agg, ok := sub.intents[event.AggressorOrderID]
	if !ok {
		// No matching intent — most likely a telemetry gap (e.g. the intent was
		// dropped, or a cancel/market edge case). Tolerate rather than risk a
		// false negative; there is nothing trustworthy to validate against.
		engine.logger.Printf("[shadowengine] submission=%s fill for unknown aggressor_order_id=%s (tolerated)",
			event.SubmissionID, event.AggressorOrderID)
		return nil
	}

	if sub.fillsValidated == 0 {
		engine.logger.Printf("[shadowengine] submission=%s validating fills against intents (first fill: aggressor_order_id=%s)",
			event.SubmissionID, event.AggressorOrderID)
	}
	sub.fillsValidated++

	// ── Volume conservation (sound, ordering-independent) ─────────────────────
	sub.filled[event.AggressorOrderID] += event.Volume
	if sub.filled[event.AggressorOrderID] > agg.submittedQty {
		return fmt.Errorf(
			"over-fill: order_id=%s filled %d > submitted %d",
			event.AggressorOrderID, sub.filled[event.AggressorOrderID], agg.submittedQty,
		)
	}

	// ── Limit-price bound (sound, ordering-independent) ───────────────────────
	// MARKET orders have no limit, so skip the bound for them.
	if !agg.market {
		switch agg.side {
		case "BUY":
			if event.ExecutionPrice > agg.limitPrice {
				return fmt.Errorf(
					"buy order_id=%s executed at %.6f above its limit %.6f",
					event.AggressorOrderID, event.ExecutionPrice, agg.limitPrice,
				)
			}
		case "SELL":
			if event.ExecutionPrice < agg.limitPrice {
				return fmt.Errorf(
					"sell order_id=%s executed at %.6f below its limit %.6f",
					event.AggressorOrderID, event.ExecutionPrice, agg.limitPrice,
				)
			}
		}
	}

	// ── Price-time priority (gated, default off) ──────────────────────────────
	// Reliable only under deterministic grading; see type doc. Compares the fill
	// against the top of book the aggressor saw on arrival.
	if engine.strictPriority && agg.hasOpposing && event.ExecutionPrice > 0 {
		switch agg.side {
		case "BUY":
			if event.ExecutionPrice > agg.bestOpposing {
				return fmt.Errorf(
					"buy order_id=%s executed at %.6f worse than top-of-book ask %.6f",
					event.AggressorOrderID, event.ExecutionPrice, agg.bestOpposing,
				)
			}
		case "SELL":
			if event.ExecutionPrice < agg.bestOpposing {
				return fmt.Errorf(
					"sell order_id=%s executed at %.6f worse than top-of-book bid %.6f",
					event.AggressorOrderID, event.ExecutionPrice, agg.bestOpposing,
				)
			}
		}
	}

	return nil
}

type orderBook struct {
	bids         []priceLevel
	asks         []priceLevel
	liveOrderIDs map[string]struct{}
	nextSequence uint64
}

type priceLevel struct {
	price  float64
	orders []restingOrder
}

type restingOrder struct {
	id       string
	price    float64
	volume   int
	sequence uint64
}

func newOrderBook() *orderBook {
	return &orderBook{
		liveOrderIDs: make(map[string]struct{}),
	}
}

func (book *orderBook) applyBuy(event models.OrderEvent) {
	market := event.Price <= 0 // MARKET order: sweep all available asks

	remaining := event.Volume
	for remaining > 0 && len(book.asks) > 0 && (market || book.asks[0].price <= event.Price) {
		remaining = book.fillBestAsk(remaining)
	}

	// MARKET orders do not rest in the book if unmatched.
	if remaining > 0 && !market {
		book.addBid(restingOrder{
			id:       event.OrderID,
			price:    event.Price,
			volume:   remaining,
			sequence: book.nextSequence,
		})
		book.nextSequence++
	}
}

func (book *orderBook) applySell(event models.OrderEvent) {
	market := event.Price <= 0 // MARKET order: sweep all available bids

	remaining := event.Volume
	for remaining > 0 && len(book.bids) > 0 && (market || book.bids[0].price >= event.Price) {
		remaining = book.fillBestBid(remaining)
	}

	// MARKET orders do not rest in the book if unmatched.
	if remaining > 0 && !market {
		book.addAsk(restingOrder{
			id:       event.OrderID,
			price:    event.Price,
			volume:   remaining,
			sequence: book.nextSequence,
		})
		book.nextSequence++
	}
}

func (book *orderBook) applyCancel(event models.OrderEvent) error {
	if event.OrderID == "" {
		return fmt.Errorf("cancel without order_id")
	}
	// Tolerate cancel for an unknown order ID: the bot's cancel carries the
	// server-assigned ID, which does not exist in this bot-ID-keyed reference
	// book. This is a known telemetry-mapping limitation, not a contestant error.
	book.removeLiveOrder(event.OrderID)
	return nil
}

func validateNewOrder(event models.OrderEvent) error {
	if event.OrderID == "" {
		return fmt.Errorf("%s without order_id", event.Action)
	}
	if event.Volume <= 0 {
		return fmt.Errorf("non-positive volume=%d for order_id=%s", event.Volume, event.OrderID)
	}
	// Price=0 is valid for MARKET orders — skip price check in that case.
	if event.Price < 0 {
		return fmt.Errorf("negative price=%.6f for order_id=%s", event.Price, event.OrderID)
	}

	return nil
}

func (book *orderBook) fillBestAsk(remaining int) int {
	level := &book.asks[0]
	best := &level.orders[0]
	if best.volume > remaining {
		best.volume -= remaining
		return 0
	}

	remaining -= best.volume
	delete(book.liveOrderIDs, best.id)
	level.orders = level.orders[1:]
	if len(level.orders) == 0 {
		book.asks = book.asks[1:]
	}

	return remaining
}

func (book *orderBook) fillBestBid(remaining int) int {
	level := &book.bids[0]
	best := &level.orders[0]
	if best.volume > remaining {
		best.volume -= remaining
		return 0
	}

	remaining -= best.volume
	delete(book.liveOrderIDs, best.id)
	level.orders = level.orders[1:]
	if len(level.orders) == 0 {
		book.bids = book.bids[1:]
	}

	return remaining
}

func (book *orderBook) addBid(order restingOrder) {
	book.liveOrderIDs[order.id] = struct{}{}

	for i := range book.bids {
		if book.bids[i].price == order.price {
			book.bids[i].orders = append(book.bids[i].orders, order)
			return
		}
		if book.bids[i].price < order.price {
			book.bids = append(book.bids, priceLevel{})
			copy(book.bids[i+1:], book.bids[i:])
			book.bids[i] = priceLevel{price: order.price, orders: []restingOrder{order}}
			return
		}
	}

	book.bids = append(book.bids, priceLevel{price: order.price, orders: []restingOrder{order}})
}

func (book *orderBook) addAsk(order restingOrder) {
	book.liveOrderIDs[order.id] = struct{}{}

	for i := range book.asks {
		if book.asks[i].price == order.price {
			book.asks[i].orders = append(book.asks[i].orders, order)
			return
		}
		if book.asks[i].price > order.price {
			book.asks = append(book.asks, priceLevel{})
			copy(book.asks[i+1:], book.asks[i:])
			book.asks[i] = priceLevel{price: order.price, orders: []restingOrder{order}}
			return
		}
	}

	book.asks = append(book.asks, priceLevel{price: order.price, orders: []restingOrder{order}})
}

func (book *orderBook) removeLiveOrder(orderID string) bool {
	if _, ok := book.liveOrderIDs[orderID]; !ok {
		return false
	}
	if book.removeFromLevels(&book.bids, orderID) || book.removeFromLevels(&book.asks, orderID) {
		delete(book.liveOrderIDs, orderID)
		return true
	}
	delete(book.liveOrderIDs, orderID)
	return false
}

func (book *orderBook) removeFromLevels(levels *[]priceLevel, orderID string) bool {
	for levelIndex := range *levels {
		orders := (*levels)[levelIndex].orders
		for orderIndex := range orders {
			if orders[orderIndex].id != orderID {
				continue
			}

			orders = append(orders[:orderIndex], orders[orderIndex+1:]...)
			(*levels)[levelIndex].orders = orders
			if len(orders) == 0 {
				*levels = append((*levels)[:levelIndex], (*levels)[levelIndex+1:]...)
			}
			return true
		}
	}

	return false
}

func send(ctx context.Context, ch chan<- models.CorrectnessUpdate, value models.CorrectnessUpdate) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}
