package shadowengine

import (
	"context"
	"fmt"
	"log"
	"strings"

	"veltrix/artifact-checker-go/internal/models"
)

// Engine consumes watermark-ordered events and validates each submission on one
// goroutine. No mutex is used in the hot path; sequential channel delivery is
// the synchronization boundary.
type Engine struct {
	books  map[string]*orderBook
	states map[string]validationState
	logger *log.Logger
}

type validationState struct {
	correct bool
	reason  string
}

func New(logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.Default()
	}

	return &Engine{
		books:  make(map[string]*orderBook),
		states: make(map[string]validationState),
		logger: logger,
	}
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

	book := engine.books[submissionID]
	if book == nil {
		book = newOrderBook()
		engine.books[submissionID] = book
	}

	if err := book.apply(event); err != nil {
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

func (engine *Engine) IsCorrect(submissionID string) bool {
	state, ok := engine.states[submissionID]
	return !ok || state.correct
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

func (book *orderBook) apply(event models.OrderEvent) error {
	action := strings.ToUpper(strings.TrimSpace(event.Action))
	switch action {
	case "BUY":
		return book.applyBuy(event)
	case "SELL":
		return book.applySell(event)
	case "CANCEL":
		return book.applyCancel(event)
	default:
		return fmt.Errorf("unknown action %q for order_id=%s", event.Action, event.OrderID)
	}
}

func (book *orderBook) applyBuy(event models.OrderEvent) error {
	if err := validateNewOrder(event); err != nil {
		return err
	}
	if _, exists := book.liveOrderIDs[event.OrderID]; exists {
		return fmt.Errorf("duplicate live order_id=%s", event.OrderID)
	}

	remaining := event.Volume
	checkedExternalMatch := false
	for remaining > 0 && len(book.asks) > 0 && book.asks[0].price <= event.Price {
		if !checkedExternalMatch {
			if err := book.validateExpectedMatch("BUY", event, book.asks[0].orders[0]); err != nil {
				return err
			}
			checkedExternalMatch = true
		}
		remaining = book.fillBestAsk(remaining)
	}

	if remaining > 0 {
		book.addBid(restingOrder{
			id:       event.OrderID,
			price:    event.Price,
			volume:   remaining,
			sequence: book.nextSequence,
		})
		book.nextSequence++
	}

	return nil
}

func (book *orderBook) applySell(event models.OrderEvent) error {
	if err := validateNewOrder(event); err != nil {
		return err
	}
	if _, exists := book.liveOrderIDs[event.OrderID]; exists {
		return fmt.Errorf("duplicate live order_id=%s", event.OrderID)
	}

	remaining := event.Volume
	checkedExternalMatch := false
	for remaining > 0 && len(book.bids) > 0 && book.bids[0].price >= event.Price {
		if !checkedExternalMatch {
			if err := book.validateExpectedMatch("SELL", event, book.bids[0].orders[0]); err != nil {
				return err
			}
			checkedExternalMatch = true
		}
		remaining = book.fillBestBid(remaining)
	}

	if remaining > 0 {
		book.addAsk(restingOrder{
			id:       event.OrderID,
			price:    event.Price,
			volume:   remaining,
			sequence: book.nextSequence,
		})
		book.nextSequence++
	}

	return nil
}

func (book *orderBook) applyCancel(event models.OrderEvent) error {
	if event.OrderID == "" {
		return fmt.Errorf("cancel without order_id")
	}
	if !book.removeLiveOrder(event.OrderID) {
		return fmt.Errorf("cancel for unknown or already-filled order_id=%s", event.OrderID)
	}

	return nil
}

func validateNewOrder(event models.OrderEvent) error {
	if event.OrderID == "" {
		return fmt.Errorf("%s without order_id", event.Action)
	}
	if event.Volume <= 0 {
		return fmt.Errorf("non-positive volume=%d for order_id=%s", event.Volume, event.OrderID)
	}
	if event.Price <= 0 {
		return fmt.Errorf("non-positive price=%.6f for order_id=%s", event.Price, event.OrderID)
	}

	return nil
}

func (book *orderBook) validateExpectedMatch(side string, event models.OrderEvent, best restingOrder) error {
	if event.MatchedOrderID != "" && event.MatchedOrderID != best.id {
		return fmt.Errorf(
			"%s order_id=%s skipped older/better resting order_id=%s and matched order_id=%s",
			side,
			event.OrderID,
			best.id,
			event.MatchedOrderID,
		)
	}

	if event.ExecutionPrice <= 0 {
		return nil
	}

	switch side {
	case "BUY":
		if event.ExecutionPrice > best.price {
			return fmt.Errorf(
				"buy order_id=%s executed at %.6f worse than best ask %.6f",
				event.OrderID,
				event.ExecutionPrice,
				best.price,
			)
		}
	case "SELL":
		if event.ExecutionPrice < best.price {
			return fmt.Errorf(
				"sell order_id=%s executed at %.6f worse than best bid %.6f",
				event.OrderID,
				event.ExecutionPrice,
				best.price,
			)
		}
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
