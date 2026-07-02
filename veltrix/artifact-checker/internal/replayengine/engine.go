// Package replayengine validates a contestant's matching engine by golden-model
// differential replay.
//
// Method
// ──────
// The correctness benchmark runs a single serialized writer (one bot, fixed
// seed), so every order the contestant processed carries a monotonic `seq` that
// defines the exact order the contestant matched in. This package replays that
// identical, ordered stream through a trusted reference matching engine (the
// "golden model", a standard price-time engine) and compares, per aggressor
// order, the trades the reference produces against the fills the contestant
// reported.
//
// Because the input order is known, the reference's output is the unique correct
// answer: any divergence is a real bug, and agreement is provably correct — so a
// correct engine is never failed (soundness), while every matching bug is caught
// (completeness).
//
// Definition of "correct" (standard price-time spec, not byte-conformance to the
// sample server):
//   - counterparty selected by price-time priority (best price, then FIFO) and
//     per-fill quantity: compared EXACTLY;
//   - order-level outcome (fully filled / rested / dropped) for LIMIT/MARKET/
//     FOK/FAK/GFD: compared EXACTLY (MARKET sweeps then cancels its remainder);
//   - execution price: TOLERANT — accepted anywhere within the crossing band, so
//     any reporting convention (maker-price, aggressor-price, mid) passes, while
//     a price outside the band is a real bug.
package replayengine

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"

	"veltrix/artifact-checker/internal/models"
)

// Engine consumes per-submission events and, on the end-of-run marker, replays
// the submission through the golden model and emits a single final verdict.
//
// The engine buffers a submission's events and replays them in `seq` order; the
// serialized correctness run keeps this buffer tiny (one writer). State is keyed
// by SubmissionID and mutated from a single goroutine (Run), so no locks.
type Engine struct {
	buffers   map[string][]models.OrderEvent
	finalized map[string]struct{}
	logger    *log.Logger
}

func New(logger *log.Logger) *Engine {
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("[replayengine] ready (golden-model differential replay, standard price-time spec)")
	return &Engine{
		buffers:   make(map[string][]models.OrderEvent),
		finalized: make(map[string]struct{}),
		logger:    logger,
	}
}

// Run buffers events per submission and finalizes each on its end-of-run marker.
// Any submissions still buffered when the input closes are flushed (best effort).
func (e *Engine) Run(
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
				return e.flushAll(ctx, updates)
			}
			if event.SubmissionID == "" {
				continue
			}
			if _, done := e.finalized[event.SubmissionID]; done {
				continue // ignore stragglers after the verdict is final
			}

			e.buffers[event.SubmissionID] = append(e.buffers[event.SubmissionID], event)

			if event.EndOfRun {
				update := e.finalize(event.SubmissionID)
				if err := send(ctx, updates, update); err != nil {
					return err
				}
			}
		}
	}
}

func (e *Engine) flushAll(ctx context.Context, updates chan<- models.CorrectnessUpdate) error {
	// Deterministic order for reproducible shutdown logs.
	ids := make([]string, 0, len(e.buffers))
	for id := range e.buffers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := send(ctx, updates, e.finalize(id)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) finalize(submissionID string) models.CorrectnessUpdate {
	events := e.buffers[submissionID]
	delete(e.buffers, submissionID)
	e.finalized[submissionID] = struct{}{}

	// A verdict is only trustworthy if the whole serialized stream arrived. The
	// end-of-run sentinel is that proof; without it the stream may be truncated
	// (a consumer-group rebalance resuming AtEnd, or a lost final batch), so
	// emitting correct/incorrect would be the fail-open trap. Report Unverified
	// so a never-completed run is never mistaken for a pass.
	hadEnd := false
	for _, ev := range events {
		if ev.EndOfRun {
			hadEnd = true
			break
		}
	}
	if !hadEnd {
		e.logger.Printf("[replayengine] submission=%s unverified: no end-of-run marker (stream incomplete, %d events)", submissionID, len(events))
		return models.CorrectnessUpdate{
			SubmissionID: submissionID,
			Verdict:      models.VerdictUnverified,
			Reason:       "run did not complete: end-of-run marker never received",
		}
	}

	verdict, reason := Replay(events)
	switch verdict {
	case models.VerdictCorrect:
		e.logger.Printf("[replayengine] submission=%s correct (%d events replayed)", submissionID, len(events))
	case models.VerdictUnverified:
		e.logger.Printf("[replayengine] submission=%s unverified: %s", submissionID, reason)
	default:
		e.logger.Printf("[replayengine] submission=%s incorrect: %s", submissionID, reason)
	}
	return models.CorrectnessUpdate{SubmissionID: submissionID, Verdict: verdict, Reason: reason}
}

// Replay runs the golden model over the submission's events (sorted by seq) and
// diffs each aggressor order's expected trades against the contestant's reported
// fills. It returns VerdictCorrect when the contestant conforms, VerdictIncorrect
// with a reason on the first real divergence, or VerdictUnverified when the input
// is too incomplete to judge (a fill with no captured intent, or a counterparty
// that was never submitted) — we never turn missing input into a false failure.
// Exported for direct unit testing. (Stream completeness — the end-of-run marker
// — is checked by the caller in finalize, not here.)
func Replay(events []models.OrderEvent) (models.Verdict, string) {
	sorted := make([]models.OrderEvent, len(events))
	copy(sorted, events)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })

	books := make(map[string]*orderBook)
	serverToBot := make(map[uint64]string) // contestant server id -> bot order_id
	seen := make(map[uint64]struct{})       // seq dedup (idempotent under at-least-once)
	expected := make(map[string]*aggressorExpectation)
	observed := make(map[string][]observedFill)
	var aggressorOrder []string // bot order_ids of intents, in seq order

	for _, ev := range sorted {
		if ev.Seq != 0 {
			if _, dup := seen[ev.Seq]; dup {
				continue
			}
			seen[ev.Seq] = struct{}{}
		}
		if ev.EndOfRun {
			continue
		}

		switch strings.ToUpper(strings.TrimSpace(ev.Action)) {
		case "BUY", "SELL":
			side := strings.ToUpper(strings.TrimSpace(ev.Action))
			if ev.ContestantOrderID != 0 {
				serverToBot[ev.ContestantOrderID] = ev.OrderID
			}
			book := getBook(books, ev.Ticker)
			trades := book.submit(ev.OrderID, side, ev.OrderType, toTick(ev.Price), ev.Volume)
			expected[ev.OrderID] = &aggressorExpectation{trades: trades}
			aggressorOrder = append(aggressorOrder, ev.OrderID)

		case "CANCEL":
			if ev.CancelTargetID == 0 {
				continue
			}
			botID, ok := serverToBot[ev.CancelTargetID]
			if !ok {
				continue // canceling an order we never saw resting (already filled, or a gap) — no-op
			}
			for _, b := range books {
				if b.cancel(botID) {
					break
				}
			}

		case "FILL":
			agg := ev.AggressorOrderID
			if agg == "" {
				continue
			}
			makerBot, known := serverToBot[ev.MatchedOrderID]
			observed[agg] = append(observed[agg], observedFill{
				makerBot:   makerBot,
				makerKnown: known,
				makerID:    ev.MatchedOrderID,
				qty:        ev.Volume,
				price:      toTick(ev.ExecutionPrice),
			})
		}
	}

	// A fill whose aggressor never submitted an intent means the captured stream
	// is incomplete (telemetry loss, or a 200 response whose order_id we could not
	// parse). We cannot soundly compare counterparties on missing input, so the
	// verdict is Unverified — never a false "incorrect" (the paramount constraint)
	// and never a silent "correct". Sorted for a reproducible verdict reason.
	var orphans []string
	for agg := range observed {
		if _, ok := expected[agg]; !ok {
			orphans = append(orphans, agg)
		}
	}
	if len(orphans) > 0 {
		sort.Strings(orphans)
		return models.VerdictUnverified, fmt.Sprintf(
			"order_id=%s reported fills but was never captured as an intent (incomplete telemetry)", orphans[0])
	}

	for _, aggID := range aggressorOrder {
		if v, reason := diffAggressor(aggID, expected[aggID], observed[aggID]); v != models.VerdictCorrect {
			return v, reason
		}
	}
	return models.VerdictCorrect, ""
}

// ── Differential comparison ──────────────────────────────────────────────────

type observedFill struct {
	makerBot   string
	makerKnown bool
	makerID    uint64
	qty        int
	price      int64
}

type aggressorExpectation struct {
	trades []trade
}

func diffAggressor(aggID string, exp *aggressorExpectation, obs []observedFill) (models.Verdict, string) {
	// A fill whose maker id maps to no submitted order can't be checked against
	// price-time priority: it is either lost telemetry or a response shape we did
	// not recognize. Return Unverified rather than risk a false "incorrect".
	for _, f := range obs {
		if !f.makerKnown {
			return models.VerdictUnverified, fmt.Sprintf(
				"order_id=%s: fill against maker id %d that was never submitted as an order — counterparty cannot be verified (incomplete telemetry or unrecognized response shape)",
				aggID, f.makerID)
		}
	}

	type expEntry struct {
		qty        int
		makerPrice int64
		aggLimit   int64
		aggMarket  bool
	}
	expMap := make(map[string]*expEntry)
	expTotal := 0
	if exp != nil {
		for _, t := range exp.trades {
			e := expMap[t.makerBot]
			if e == nil {
				e = &expEntry{makerPrice: t.makerPrice, aggLimit: t.aggLimit, aggMarket: t.aggMarket}
				expMap[t.makerBot] = e
			}
			e.qty += t.qty
			expTotal += t.qty
		}
	}

	obsTotal := 0
	obsMap := make(map[string]int)
	for _, f := range obs {
		obsTotal += f.qty
		if f.makerKnown {
			obsMap[f.makerBot] += f.qty
		}
	}

	// Total filled quantity is a sound check independent of counterparty mapping:
	// catches over-fill, under-fill, spurious fills, and missing fills.
	if obsTotal != expTotal {
		return models.VerdictIncorrect, fmt.Sprintf(
			"order_id=%s: contestant filled total qty %d but the golden model matches %d",
			aggID, obsTotal, expTotal)
	}

	// Counterparty identity + per-maker quantity (price-time priority): exact.
	for maker, e := range expMap {
		if obsMap[maker] != e.qty {
			return models.VerdictIncorrect, fmt.Sprintf(
				"order_id=%s: price-time priority selects %d qty against maker %s, contestant filled %d",
				aggID, e.qty, maker, obsMap[maker])
		}
	}
	for maker, q := range obsMap {
		if _, ok := expMap[maker]; !ok {
			return models.VerdictIncorrect, fmt.Sprintf(
				"order_id=%s: contestant filled %d against maker %s which price-time priority did not select",
				aggID, q, maker)
		}
	}

	// Execution price: tolerant within the crossing band.
	for _, f := range obs {
		if !f.makerKnown {
			continue
		}
		e := expMap[f.makerBot]
		if e == nil {
			continue
		}
		lo, hi := priceBand(e.makerPrice, e.aggLimit, e.aggMarket)
		if f.price < lo || f.price > hi {
			return models.VerdictIncorrect, fmt.Sprintf(
				"order_id=%s: execution price %s against maker %s is outside the valid band [%s, %s]",
				aggID, tickStr(f.price), f.makerBot, tickStr(lo), tickStr(hi))
		}
	}

	return models.VerdictCorrect, ""
}

// priceBand is the inclusive range of execution prices at which both parties are
// willing to trade: between the maker's resting price and the aggressor's limit
// (accepting the maker-price, aggressor-price, and mid conventions). For a market
// aggressor, aggLimit carries the synthesized worst-opposing limit at arrival
// (see match), so the same span applies — a market fill may execute anywhere
// within the opposing book's price range present when the order arrived.
func priceBand(makerPrice, aggLimit int64, _ bool) (int64, int64) {
	if makerPrice <= aggLimit {
		return makerPrice, aggLimit
	}
	return aggLimit, makerPrice
}

// ── Golden model (standard price-time matching) ──────────────────────────────

type trade struct {
	makerBot   string
	qty        int
	makerPrice int64
	aggLimit   int64
	aggMarket  bool
}

type orderBook struct {
	bids []priceLevel // sorted by price descending (best bid first)
	asks []priceLevel // sorted by price ascending  (best ask first)
	live map[string]liveRef
}

type priceLevel struct {
	price  int64
	orders []restingOrder
}

type restingOrder struct {
	botID string
	qty   int
}

type liveRef struct {
	side  string
	price int64
}

func newOrderBook() *orderBook {
	return &orderBook{live: make(map[string]liveRef)}
}

func getBook(books map[string]*orderBook, ticker string) *orderBook {
	book := books[ticker]
	if book == nil {
		book = newOrderBook()
		books[ticker] = book
	}
	return book
}

// submit matches an aggressor order against the book and applies remainder
// handling per order type, returning the trades produced (aggressor is always
// one party of each). LIMIT/GFD rest the remainder; MARKET/FAK cancel it; FOK is
// all-or-nothing.
func (b *orderBook) submit(botID, side, orderType string, limit int64, qty int) []trade {
	otype := strings.ToUpper(strings.TrimSpace(orderType))
	market := otype == "MARKET"

	if otype == "FOK" && !b.canFullyFill(side, market, limit, qty) {
		return nil // killed: no trades, does not rest
	}

	trades, remaining := b.match(side, market, limit, qty)

	if remaining > 0 {
		switch otype {
		case "MARKET", "FAK":
			// cancel remainder — do not rest
		default: // LIMIT, GFD
			if !market {
				b.rest(botID, side, limit, remaining)
			}
		}
	}
	return trades
}

func (b *orderBook) match(side string, market bool, limit int64, qty int) ([]trade, int) {
	var trades []trade
	remaining := qty

	// A market order carries no user limit. test/main.cpp and standard engines
	// synthesize one at the worst opposing price present when the order arrives,
	// then may report each fill anywhere between the maker's resting price and
	// that synthetic limit. Snapshot it BEFORE the match loop consumes levels
	// (bids are desc / asks asc, so the last level is the worst on each side).
	// aggBand becomes the aggressor bound for the execution-price band, keeping
	// it tolerant of the worst-opposing, maker-price, and mid conventions while
	// still rejecting a price no resting order ever offered.
	aggBand := limit
	if market {
		if side == "BUY" && len(b.asks) > 0 {
			aggBand = b.asks[len(b.asks)-1].price
		} else if side == "SELL" && len(b.bids) > 0 {
			aggBand = b.bids[len(b.bids)-1].price
		}
	}

	if side == "BUY" {
		for remaining > 0 && len(b.asks) > 0 {
			level := &b.asks[0]
			if !market && level.price > limit {
				break
			}
			maker := &level.orders[0]
			m := min(remaining, maker.qty)
			trades = append(trades, trade{makerBot: maker.botID, qty: m, makerPrice: level.price, aggLimit: aggBand, aggMarket: market})
			remaining -= m
			maker.qty -= m
			if maker.qty == 0 {
				delete(b.live, maker.botID)
				level.orders = level.orders[1:]
				if len(level.orders) == 0 {
					b.asks = b.asks[1:]
				}
			}
		}
		return trades, remaining
	}

	for remaining > 0 && len(b.bids) > 0 {
		level := &b.bids[0]
		if !market && level.price < limit {
			break
		}
		maker := &level.orders[0]
		m := min(remaining, maker.qty)
		trades = append(trades, trade{makerBot: maker.botID, qty: m, makerPrice: level.price, aggLimit: aggBand, aggMarket: market})
		remaining -= m
		maker.qty -= m
		if maker.qty == 0 {
			delete(b.live, maker.botID)
			level.orders = level.orders[1:]
			if len(level.orders) == 0 {
				b.bids = b.bids[1:]
			}
		}
	}
	return trades, remaining
}

func (b *orderBook) canFullyFill(side string, market bool, limit int64, qty int) bool {
	need := qty
	levels := b.asks
	if side == "SELL" {
		levels = b.bids
	}
	for _, level := range levels {
		if !market {
			if side == "BUY" && level.price > limit {
				break
			}
			if side == "SELL" && level.price < limit {
				break
			}
		}
		for _, ord := range level.orders {
			need -= ord.qty
			if need <= 0 {
				return true
			}
		}
	}
	return need <= 0
}

func (b *orderBook) rest(botID, side string, price int64, qty int) {
	b.live[botID] = liveRef{side: side, price: price}
	if side == "BUY" {
		insertLevel(&b.bids, price, restingOrder{botID: botID, qty: qty}, true)
	} else {
		insertLevel(&b.asks, price, restingOrder{botID: botID, qty: qty}, false)
	}
}

func (b *orderBook) cancel(botID string) bool {
	ref, ok := b.live[botID]
	if !ok {
		return false
	}
	delete(b.live, botID)

	levels := &b.asks
	if ref.side == "BUY" {
		levels = &b.bids
	}
	for i := range *levels {
		if (*levels)[i].price != ref.price {
			continue
		}
		orders := (*levels)[i].orders
		for j := range orders {
			if orders[j].botID != botID {
				continue
			}
			orders = append(orders[:j], orders[j+1:]...)
			(*levels)[i].orders = orders
			if len(orders) == 0 {
				*levels = append((*levels)[:i], (*levels)[i+1:]...)
			}
			return true
		}
	}
	return true
}

// insertLevel appends to an existing price level (preserving FIFO) or inserts a
// new level at its sorted position. desc=true keeps bids highest-first;
// desc=false keeps asks lowest-first.
func insertLevel(levels *[]priceLevel, price int64, ord restingOrder, desc bool) {
	for i := range *levels {
		if (*levels)[i].price == price {
			(*levels)[i].orders = append((*levels)[i].orders, ord)
			return
		}
		better := (*levels)[i].price > price
		if desc {
			better = (*levels)[i].price < price
		}
		if better {
			*levels = append(*levels, priceLevel{})
			copy((*levels)[i+1:], (*levels)[i:])
			(*levels)[i] = priceLevel{price: price, orders: []restingOrder{ord}}
			return
		}
	}
	*levels = append(*levels, priceLevel{price: price, orders: []restingOrder{ord}})
}

// ── helpers ──────────────────────────────────────────────────────────────────

// toTick converts a price to integer ticks (hundredths) so all comparisons are
// exact integer arithmetic — never float comparison.
func toTick(price float64) int64 {
	return int64(math.Round(price * 100))
}

func tickStr(tick int64) string {
	return fmt.Sprintf("%.2f", float64(tick)/100)
}

func send(ctx context.Context, ch chan<- models.CorrectnessUpdate, value models.CorrectnessUpdate) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}
