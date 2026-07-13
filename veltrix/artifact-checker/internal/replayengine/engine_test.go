package replayengine

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"veltrix/artifact-checker/internal/models"
)

// ── event constructors (the test's independent oracle: fills are hand-computed,
//    never derived from the golden model, so a bug in the model can't hide) ────

func intent(seq uint64, id, side, otype, ticker string, price float64, qty int, serverID uint64) models.OrderEvent {
	return models.OrderEvent{
		SubmissionID:      "sub",
		Seq:               seq,
		OrderID:           id,
		Action:            side,
		OrderType:         otype,
		Ticker:            ticker,
		Price:             price,
		Volume:            qty,
		ContestantOrderID: serverID,
	}
}

func fill(seq uint64, aggBotID string, makerServerID uint64, price float64, qty int) models.OrderEvent {
	return models.OrderEvent{
		SubmissionID:     "sub",
		Seq:              seq,
		Action:           "FILL",
		AggressorOrderID: aggBotID,
		MatchedOrderID:   makerServerID,
		ExecutionPrice:   price,
		Volume:           qty,
	}
}

func cancel(seq uint64, targetServerID uint64) models.OrderEvent {
	return models.OrderEvent{
		SubmissionID:   "sub",
		Seq:            seq,
		Action:         "CANCEL",
		OrderID:        fmt.Sprintf("%d", targetServerID),
		CancelTargetID: targetServerID,
	}
}

func TestReplayDifferential(t *testing.T) {
	tests := []struct {
		name           string
		events         []models.OrderEvent
		wantCorrect    bool
		reasonContains string
	}{
		{
			name: "simple cross — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
				fill(3, "B1", 1, 100, 10),
			},
			wantCorrect: true,
		},
		{
			name: "price improvement, maker-price convention — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 105, 10, 2),
				fill(3, "B1", 1, 100, 10), // maker price
			},
			wantCorrect: true,
		},
		{
			name: "price improvement, aggressor-price convention — correct (tolerant band)",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 105, 10, 2),
				fill(3, "B1", 1, 105, 10), // aggressor price — still within band
			},
			wantCorrect: true,
		},
		{
			name: "execution price above the crossing band — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 105, 10, 2),
				fill(3, "B1", 1, 106, 10), // 106 > buy limit 105
			},
			wantCorrect:    false,
			reasonContains: "outside the valid band",
		},
		{
			name: "over-fill — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
				fill(3, "B1", 1, 100, 15),
			},
			wantCorrect:    false,
			reasonContains: "total qty 15",
		},
		{
			name: "under-fill — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
				fill(3, "B1", 1, 100, 7),
			},
			wantCorrect:    false,
			reasonContains: "total qty 7",
		},
		{
			name: "spurious fill with no cross — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 105, 10, 1), // rests above
				intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),  // does not cross
				fill(3, "B1", 1, 105, 10),
			},
			wantCorrect:    false,
			reasonContains: "golden model matches 0",
		},
		{
			name: "same-price FIFO priority — correct (earliest maker)",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 5, 1),
				intent(2, "S2", "SELL", "LIMIT", "AAA", 100, 5, 2),
				intent(3, "B1", "BUY", "LIMIT", "AAA", 100, 5, 3),
				fill(4, "B1", 1, 100, 5), // matches S1 (arrived first)
			},
			wantCorrect: true,
		},
		{
			name: "same-price FIFO priority violation — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 5, 1),
				intent(2, "S2", "SELL", "LIMIT", "AAA", 100, 5, 2),
				intent(3, "B1", "BUY", "LIMIT", "AAA", 100, 5, 3),
				fill(4, "B1", 2, 100, 5), // matches S2, skipping earlier S1
			},
			wantCorrect:    false,
			reasonContains: "maker S1",
		},
		{
			name: "market sweeps multiple levels then cancels remainder — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 4, 1),
				intent(2, "S2", "SELL", "LIMIT", "AAA", 101, 4, 2),
				intent(3, "M1", "BUY", "MARKET", "AAA", 0, 10, 3),
				fill(4, "M1", 1, 100, 4),
				fill(5, "M1", 2, 101, 4), // remainder 2 cancelled (not rested)
			},
			wantCorrect: true,
		},
		{
			name: "market on empty book drops — correct (no fills)",
			events: []models.OrderEvent{
				intent(1, "M1", "BUY", "MARKET", "AAA", 0, 10, 1),
			},
			wantCorrect: true,
		},
		{
			name: "market buy above the whole book range — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "M1", "BUY", "MARKET", "AAA", 0, 10, 2),
				fill(3, "M1", 1, 101, 10), // 101 > worst ask 100: no resting order offered this
			},
			wantCorrect:    false,
			reasonContains: "outside the valid band",
		},
		{
			// Regression for the seed-42 false positive: a market sell matches the
			// best (highest) bid but the engine reports the worst-opposing price —
			// test/main.cpp's convention. The band must span [worstBid, maker].
			name: "market sell reports worst bid against a higher maker — correct",
			events: []models.OrderEvent{
				intent(1, "B1", "BUY", "LIMIT", "AAA", 93, 74, 1), // best bid 93
				intent(2, "B2", "BUY", "LIMIT", "AAA", 90, 6, 2),  // worse bid 90
				intent(3, "M1", "SELL", "MARKET", "AAA", 0, 1, 3),
				fill(4, "M1", 1, 90, 1), // matched B1@93, reported at worst bid 90
			},
			wantCorrect: true,
		},
		{
			name: "market sell below the whole book range — incorrect",
			events: []models.OrderEvent{
				intent(1, "B1", "BUY", "LIMIT", "AAA", 93, 10, 1),
				intent(2, "B2", "BUY", "LIMIT", "AAA", 90, 10, 2),
				intent(3, "M1", "SELL", "MARKET", "AAA", 0, 1, 3),
				fill(4, "M1", 1, 85, 1), // 85 < worst bid 90: outside the book's range
			},
			wantCorrect:    false,
			reasonContains: "outside the valid band",
		},
		{
			name: "FOK not fully fillable is killed — correct (no fills)",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 4, 1),
				intent(2, "F1", "BUY", "FOK", "AAA", 100, 10, 2), // only 4 available => kill
			},
			wantCorrect: true,
		},
		{
			name: "FOK killed but contestant filled it — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 4, 1),
				intent(2, "F1", "BUY", "FOK", "AAA", 100, 10, 2),
				fill(3, "F1", 1, 100, 4),
			},
			wantCorrect:    false,
			reasonContains: "golden model matches 0",
		},
		{
			name: "FOK fully fillable fills — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "F1", "BUY", "FOK", "AAA", 100, 10, 2),
				fill(3, "F1", 1, 100, 10),
			},
			wantCorrect: true,
		},
		{
			name: "FAK fills available then cancels remainder — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 4, 1),
				intent(2, "K1", "BUY", "FAK", "AAA", 100, 10, 2),
				fill(3, "K1", 1, 100, 4), // remainder 6 cancelled
			},
			wantCorrect: true,
		},
		{
			name: "cancel then no match — correct (no fills)",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				cancel(2, 1),
				intent(3, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
			},
			wantCorrect: true,
		},
		{
			name: "fill against a cancelled order — incorrect",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				cancel(2, 1),
				intent(3, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
				fill(4, "B1", 1, 100, 10),
			},
			wantCorrect:    false,
			reasonContains: "golden model matches 0",
		},
		{
			name: "resting bid then hit by a sell aggressor — correct",
			events: []models.OrderEvent{
				intent(1, "B1", "BUY", "LIMIT", "AAA", 100, 10, 1), // rests, no asks
				intent(2, "S1", "SELL", "LIMIT", "AAA", 100, 10, 2), // aggressor hits B1
				fill(3, "S1", 1, 100, 10),                           // maker is B1 (server 1)
			},
			wantCorrect: true,
		},
		{
			name: "distinct tickers do not cross-match — correct",
			events: []models.OrderEvent{
				intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
				intent(2, "B1", "BUY", "LIMIT", "BBB", 100, 10, 2), // different ticker: no match
			},
			wantCorrect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := models.VerdictCorrect
			if !tc.wantCorrect {
				want = models.VerdictIncorrect
			}
			gotVerdict, reason := Replay(tc.events)
			if gotVerdict != want {
				t.Fatalf("verdict=%s (reason=%q), want %s", gotVerdict, reason, want)
			}
			if tc.reasonContains != "" && !strings.Contains(reason, tc.reasonContains) {
				t.Fatalf("reason %q does not contain %q", reason, tc.reasonContains)
			}
		})
	}
}

// TestReplayUnverifiedInputs proves the fail-safe third state: when the captured
// stream is too incomplete to judge, the verdict is Unverified — never a false
// "incorrect" (the paramount constraint) and never a silent "correct".
func TestReplayUnverifiedInputs(t *testing.T) {
	t.Run("fill against an unmappable maker", func(t *testing.T) {
		events := []models.OrderEvent{
			intent(1, "B1", "BUY", "LIMIT", "AAA", 100, 10, 1),
			fill(2, "B1", 999, 100, 10), // maker id 999 was never submitted
		}
		if v, reason := Replay(events); v != models.VerdictUnverified {
			t.Fatalf("verdict=%s (reason=%q), want unverified", v, reason)
		}
	})

	t.Run("fill for an aggressor with no captured intent", func(t *testing.T) {
		events := []models.OrderEvent{
			intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
			fill(2, "GHOST", 1, 100, 10), // GHOST never submitted an intent
		}
		if v, reason := Replay(events); v != models.VerdictUnverified {
			t.Fatalf("verdict=%s (reason=%q), want unverified", v, reason)
		}
	})
}

// TestReplayLostResponseIsUnverifiedNotIncorrect is the regression for the ghost
// order trap: a single response lost to a network timeout (seq burned, no event
// emitted) leaves the aggressor that consumed liquidity uncaptured, so the golden
// model still sees the resting order and expects a fill a later, fully correct
// order could not produce. Before the sequence-gap check this returned a false
// INCORRECT; a lost order must be Unverified, never a failure.
func TestReplayLostResponseIsUnverifiedNotIncorrect(t *testing.T) {
	events := []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1), // rests ask 100
		// seq 2: BUY 10@100 ate S1 on the server, but the response timed out — no
		// intent, no fill recorded. Only the seq is burned, leaving a gap {1,3}.
		intent(3, "B3", "BUY", "LIMIT", "AAA", 100, 10, 3), // real book empty → no fill
	}
	v, reason := Replay(events)
	if v != models.VerdictUnverified {
		t.Fatalf("lost response must be Unverified, got %s (reason=%q)", v, reason)
	}
	if !strings.Contains(reason, "could not be verified") {
		t.Fatalf("reason %q should cite unverifiable attempts", reason)
	}
}

// withOutcome stamps an attempt outcome onto an intent event.
func withOutcome(outcome string, ev models.OrderEvent) models.OrderEvent {
	ev.Outcome = outcome
	return ev
}

// TestReplayRejectedOrderIsNoOp proves a cleanly-rejected order (4xx) is treated
// as a book no-op: it never entered the server's book, so the golden model must
// not expect a fill from it. The seq stays contiguous (no gap → no false
// Unverified) and a correct contestant stays Correct. Contrast: the same BUY
// without the REJECTED tag would cross S1 and expect a fill the contestant never
// reported → Incorrect.
func TestReplayRejectedOrderIsNoOp(t *testing.T) {
	events := []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1), // rests ask 100
		// seq 2: BUY 10@100 was rejected by the server (4xx) — never entered the book.
		withOutcome("REJECTED", intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2)),
	}
	v, reason := Replay(events)
	if v != models.VerdictCorrect {
		t.Fatalf("rejected order must be a no-op, got %s (reason=%q)", v, reason)
	}
}

// TestReplayUnknownOutcomeIsUnverified proves an attempt whose outcome the bot
// could not determine (timeout / 5xx / unparsable response) forces Unverified:
// the server may have applied it, so the book cannot be trusted — never a false
// Incorrect.
func TestReplayUnknownOutcomeIsUnverified(t *testing.T) {
	events := []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
		withOutcome("UNKNOWN", intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2)),
	}
	// 1 unknown of 2 attempts = 50% — well above tolerance, so Unverified.
	v, reason := Replay(events)
	if v != models.VerdictUnverified {
		t.Fatalf("unknown outcome must be Unverified, got %s (reason=%q)", v, reason)
	}
	if !strings.Contains(reason, "could not be verified") {
		t.Fatalf("reason %q should cite unverifiable attempts", reason)
	}
}

// TestReplayScriptedScenario exercises every order-type path in one deterministic
// sequence with a hand-computed correct contestant, guaranteeing coverage that a
// random single-bot run would only hit by chance.
func TestReplayScriptedScenario(t *testing.T) {
	events := []models.OrderEvent{
		// Build a book on AAA.
		intent(1, "B1", "BUY", "LIMIT", "AAA", 99, 10, 1),  // rests bid 99
		intent(2, "B2", "BUY", "LIMIT", "AAA", 100, 5, 2),  // rests bid 100 (better)
		intent(3, "A1", "SELL", "LIMIT", "AAA", 101, 5, 3), // rests ask 101
		intent(4, "A2", "SELL", "LIMIT", "AAA", 101, 5, 4), // rests ask 101 (behind A1)
		// Aggressor sell crosses best bid (100) first — FIFO/price priority.
		intent(5, "S1", "SELL", "LIMIT", "AAA", 99, 8, 5),
		fill(6, "S1", 2, 100, 5), // hits B2 @100 (best bid)
		fill(7, "S1", 1, 99, 3),  // then B1 @99, remainder rests as ask? no: sell limit 99 filled 8, done
		// Market buy sweeps asks 101 (A1 then A2), remainder cancelled.
		intent(8, "M1", "BUY", "MARKET", "AAA", 0, 12, 6),
		fill(9, "M1", 3, 101, 5),  // A1 (FIFO first)
		fill(10, "M1", 4, 101, 5), // A2; remainder 2 cancelled
		// FOK that cannot fill is killed (book now empty of asks).
		intent(11, "F1", "BUY", "FOK", "AAA", 105, 5, 7),
		// Cancel the remaining resting bid B1 (7 left after the sell above).
		cancel(12, 1),
		// End marker (ignored by Replay; used by Engine.Run).
		{SubmissionID: "sub", Seq: 13, EndOfRun: true},
	}
	if v, reason := Replay(events); v != models.VerdictCorrect {
		t.Fatalf("scripted scenario should be correct, got %s: %s", v, reason)
	}
}

// TestEngineRunFinalizesOnEndMarker verifies the streaming path: buffer, finalize
// on END, emit exactly one verdict.
func TestEngineRunFinalizesOnEndMarker(t *testing.T) {
	e := New(log.New(io.Discard, "", 0))
	in := make(chan models.OrderEvent, 16)
	updates := make(chan models.CorrectnessUpdate, 16)

	ctx, canc := context.WithCancel(context.Background())
	defer canc()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, in, updates) }()

	for _, ev := range []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
		intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
		fill(3, "B1", 1, 100, 10),
		{SubmissionID: "sub", Seq: 4, EndOfRun: true},
	} {
		in <- ev
	}

	u := <-updates
	if u.SubmissionID != "sub" || u.Verdict != models.VerdictCorrect {
		t.Fatalf("got %+v, want correct verdict for sub", u)
	}
	close(in)
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

// TestEngineUnverifiedWithoutEndMarker proves the completeness fail-safe: a
// perfectly conforming stream that never delivers the end-of-run marker (a
// truncated or rebalanced consumer) finalizes as Unverified on shutdown, never
// as a false "correct" (the original fail-open trap).
func TestEngineUnverifiedWithoutEndMarker(t *testing.T) {
	e := New(log.New(io.Discard, "", 0))
	in := make(chan models.OrderEvent, 16)
	updates := make(chan models.CorrectnessUpdate, 16)

	ctx, canc := context.WithCancel(context.Background())
	defer canc()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, in, updates) }()

	for _, ev := range []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
		intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
		fill(3, "B1", 1, 100, 10),
		// no end-of-run marker
	} {
		in <- ev
	}
	close(in) // channel close → flushAll → END never seen

	u := <-updates
	if u.Verdict != models.VerdictUnverified {
		t.Fatalf("got verdict=%s, want unverified (no end-of-run marker)", u.Verdict)
	}
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

// TestReplayDedupesRedeliveredFill proves at-least-once safety: a duplicated fill
// (same seq) must not double-count into an over-fill false positive.
func TestReplayDedupesRedeliveredFill(t *testing.T) {
	events := []models.OrderEvent{
		intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1),
		intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2),
		fill(3, "B1", 1, 100, 10),
		fill(3, "B1", 1, 100, 10), // redelivered duplicate (same seq)
	}
	if v, reason := Replay(events); v != models.VerdictCorrect {
		t.Fatalf("duplicate fill must be deduped and stay correct, got %s: %s", v, reason)
	}
}

// ── tolerance-policy runs ──────────────────────────────────────────────────────
// runBuilder assembles a contiguous-seq run so tests exercise the fractional
// verdict policy without accidental gaps.
type runBuilder struct {
	seq    uint64
	srvID  uint64
	events []models.OrderEvent
}

func (b *runBuilder) tick() uint64 { b.seq++; return b.seq }
func (b *runBuilder) srv() uint64  { b.srvID++; return b.srvID }

// cleanCross: a SELL rests, a BUY fully crosses it, and the reported fill agrees
// with the golden model. Book returns empty, so it may be repeated on one ticker.
func (b *runBuilder) cleanCross(ticker string) {
	s := b.srv()
	bid := fmt.Sprintf("B-%d", s)
	b.events = append(b.events,
		intent(b.tick(), fmt.Sprintf("S-%d", s), "SELL", "LIMIT", ticker, 100, 10, s))
	b.events = append(b.events,
		intent(b.tick(), bid, "BUY", "LIMIT", ticker, 100, 10, b.srv()))
	b.events = append(b.events, fill(b.tick(), bid, s, 100, 10))
}

// unknown: an order whose outcome the bot could not confirm (consumes a seq, emits
// no fill), tainting its ticker.
func (b *runBuilder) unknown(ticker string) {
	b.events = append(b.events, withOutcome("UNKNOWN",
		intent(b.tick(), fmt.Sprintf("U-%d", b.seq+1), "BUY", "LIMIT", ticker, 100, 10, 0)))
}

// overFill: a crossing pair whose reported fill qty (20) exceeds what the golden
// model matches (10) — a real matching divergence on this ticker.
func (b *runBuilder) overFill(ticker string) {
	s := b.srv()
	bid := fmt.Sprintf("B-%d", s)
	b.events = append(b.events,
		intent(b.tick(), fmt.Sprintf("S-%d", s), "SELL", "LIMIT", ticker, 100, 10, s))
	b.events = append(b.events,
		intent(b.tick(), bid, "BUY", "LIMIT", ticker, 100, 10, b.srv()))
	b.events = append(b.events, fill(b.tick(), bid, s, 100, 20))
}

// TestReplayToleratesFewUnknowns: a handful of transient blips among many good
// requests (here 1 of 41 ≈ 2.4%, under the 5% tolerance) must NOT sink a correct
// run — this is the whole point of the policy change.
func TestReplayToleratesFewUnknowns(t *testing.T) {
	b := &runBuilder{}
	for i := 0; i < 20; i++ {
		b.cleanCross("AAA")
	}
	b.unknown("BBB")
	if v, reason := Replay(b.events); v != models.VerdictCorrect {
		t.Fatalf("1 unknown of 41 attempts (2.4%%) must stay Correct, got %s: %s", v, reason)
	}
}

// TestReplayAboveToleranceUnverified: past 5% unverifiable (3 of 43 ≈ 7%) there is
// too much unchecked to certify a pass → Unverified (not a fail).
func TestReplayAboveToleranceUnverified(t *testing.T) {
	b := &runBuilder{}
	for i := 0; i < 20; i++ {
		b.cleanCross("AAA")
	}
	for i := 0; i < 3; i++ {
		b.unknown("BBB")
	}
	if v, _ := Replay(b.events); v != models.VerdictUnverified {
		t.Fatalf("7%% unknown must be Unverified, got %s", v)
	}
}

// TestReplayMostlyErrorsUnhealthy: past 50% failure the server is flagged
// unhealthy (still Unverified, never Incorrect — mass errors are a liveness
// problem, not a proven matching bug).
func TestReplayMostlyErrorsUnhealthy(t *testing.T) {
	b := &runBuilder{}
	for i := 0; i < 2; i++ {
		b.cleanCross("AAA")
	}
	for i := 0; i < 10; i++ {
		b.unknown("BBB")
	}
	v, reason := Replay(b.events)
	if v != models.VerdictUnverified {
		t.Fatalf("mass errors must be Unverified, got %s", v)
	}
	if !strings.Contains(reason, "unhealthy") {
		t.Fatalf("reason %q should flag the server as unhealthy", reason)
	}
}

// TestReplayRealBugSurvivesUnknownElsewhere: a genuine matching bug on a clean
// (untainted) ticker is still Incorrect even though an unrelated request failed —
// real bugs get no tolerance budget.
func TestReplayRealBugSurvivesUnknownElsewhere(t *testing.T) {
	b := &runBuilder{}
	for i := 0; i < 5; i++ {
		b.cleanCross("AAA")
	}
	b.unknown("BBB")  // transient blip on an unrelated ticker
	b.overFill("CCC") // real over-fill bug on a clean ticker
	if v, reason := Replay(b.events); v != models.VerdictIncorrect {
		t.Fatalf("a real bug on a clean ticker must be Incorrect, got %s: %s", v, reason)
	}
}

// TestReplayTaintedMismatchNotIncorrect: a divergence on a ticker already tainted
// by an earlier lost order could be an artifact of that loss, so it must never
// become a false Incorrect — it downgrades the run to Unverified instead.
func TestReplayTaintedMismatchNotIncorrect(t *testing.T) {
	b := &runBuilder{}
	b.unknown("AAA")   // taints AAA from the start
	b.overFill("AAA")  // divergence on tainted AAA — could be an artifact
	for i := 0; i < 30; i++ {
		b.cleanCross("ZZZ") // dilute so the fraction stays under tolerance
	}
	if v, reason := Replay(b.events); v != models.VerdictUnverified {
		t.Fatalf("tainted divergence must be Unverified, not Incorrect, got %s: %s", v, reason)
	}
}

// withSub restamps an event onto a different submission so we can drive many
// independent streams through one engine.
func withSub(id string, ev models.OrderEvent) models.OrderEvent {
	ev.SubmissionID = id
	return ev
}

// TestEngineRunConcurrentSubmissions drives many submissions' end-of-run markers
// through Run in one burst and checks every verdict arrives correctly. It exists
// to exercise the offloaded-replay path: replays run on worker goroutines while
// the loop keeps draining, verdicts may land out of order, and the updates
// channel must not close until every worker has sent (run with -race). One
// submission is deliberately incorrect (an over-fill) to prove parallelism does
// not blur verdicts across streams.
func TestEngineRunConcurrentSubmissions(t *testing.T) {
	const n = 64
	e := New(log.New(io.Discard, "", 0))
	in := make(chan models.OrderEvent, 8)
	updates := make(chan models.CorrectnessUpdate, n)

	ctx, canc := context.WithCancel(context.Background())
	defer canc()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, in, updates) }()

	want := make(map[string]models.Verdict, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("sub-%d", i)
		fillQty := 10
		if i%7 == 0 {
			fillQty = 20 // over-fill: the golden model only matches 10 → Incorrect
			want[id] = models.VerdictIncorrect
		} else {
			want[id] = models.VerdictCorrect
		}
		for _, ev := range []models.OrderEvent{
			withSub(id, intent(1, "S1", "SELL", "LIMIT", "AAA", 100, 10, 1)),
			withSub(id, intent(2, "B1", "BUY", "LIMIT", "AAA", 100, 10, 2)),
			withSub(id, fill(3, "B1", 1, 100, fillQty)),
			{SubmissionID: id, Seq: 4, EndOfRun: true},
		} {
			in <- ev
		}
	}
	close(in)

	got := make(map[string]models.Verdict, n)
	for u := range updates { // ranges until Run closes updates after all workers finish
		got[u.SubmissionID] = u.Verdict
	}
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d verdicts, want %d", len(got), len(want))
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("submission %s: got verdict=%s, want %s", id, got[id], w)
		}
	}
}
