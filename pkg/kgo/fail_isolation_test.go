package kgo

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests drive recBuf / sink directly (no broker), mirroring
// sink_unstage_test.go. They pin the failure-isolation state machine: a
// terminally failed head batch is removed on its own and later batches keep
// delivering, while the default semantics still fail the whole partition.
//
// End-to-end failure-injection coverage (rolling-restart shape, retry/timeout
// exhaustion, idempotent vs non-idempotent) lives in pkg/kfake, where a real
// client runs against the in-process fake broker.

// isolateHarness is sinkHarness with the failure-isolation option and, when
// asked, idempotency disabled.
func isolateHarness(t *testing.T, isolate, disableIdempotency bool) (*Client, *sink, *recBuf) {
	t.Helper()

	opts := []Opt{
		SeedBrokers("127.0.0.1:1"), // metadata never loads; we drive the sink directly
		ManualFlushing(),
	}
	if isolate {
		opts = append(opts, IsolateFailedBatches())
	}
	if disableIdempotency {
		opts = append(opts, DisableIdempotentWrite(), MaxProduceRequestsInflightPerBroker(1))
	}
	cl, err := NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	if !disableIdempotency {
		cl.producer.id.Store(&producerID{id: 5, epoch: 0, err: nil})
	}

	s := cl.newSink(1)
	s.produceVersion.Store(9)

	r := &recBuf{
		cl:                  cl,
		topic:               "t",
		partition:           0,
		maxRecordBatchBytes: 1 << 20,
		recBufsIdx:          -1,
		lastAckedOffset:     -1,
		sink:                s,
	}
	r.lingerFn = r.unlingerAndManuallyDrain
	s.addRecBuf(r)

	cl.producer.topics.storeTopics([]string{"t"})
	tp := &topicPartition{records: r}
	cl.producer.topics.load()["t"].v.Store(&topicPartitionsData{
		topic:              "t",
		partitions:         []*topicPartition{tp},
		writablePartitions: []*topicPartition{tp},
	})

	return cl, s, r
}

// promisedOne buffers one record into a new batch and returns a channel that
// receives its promise error. The caller is responsible for staging (which
// freezes the batch) before buffering the next record.
func promisedOne(t *testing.T, r *recBuf) chan error {
	t.Helper()
	errCh := make(chan error, 1)
	r.bufferRecord(promisedRec{
		ctx:     context.Background(),
		promise: func(_ *Record, err error) { errCh <- err },
		Record:  &Record{Value: []byte("v"), Context: context.Background()},
	}, false)
	return errCh
}

// bufferAndStage buffers a record, then stages the recBuf's head so the batch
// freezes and the next buffer starts a fresh batch. Mirrors the pattern in
// sink_unstage_test.go.
func bufferAndStage(t *testing.T, s *sink, r *recBuf) (*recBatch, chan error, *produceRequest) {
	t.Helper()
	ch := promisedOne(t, r)
	batch := r.batches[len(r.batches)-1]
	req := stageOne(t, s)
	r.okOnSink = true // as if a predecessor acked; lets the next batch pipeline
	return batch, ch, req
}

// freezeNewest seals the recBuf's last batch so the next buffer starts a
// fresh batch, without staging it (leaving it un-drained).
func freezeNewest(r *recBuf) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.batches) > 0 {
		r.batches[len(r.batches)-1].frozen = true
	}
}

func awaitPromise(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("promise was not delivered")
		return nil
	}
}

func assertNoPromise(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("promise fired unexpectedly with %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestIsolateFailHeadKeepsLaterBatchesDefaultFailsAll is the control pair:
// same three-batch buffer, same head failure, isolation on vs off.
func TestIsolateFailHeadKeepsLaterBatchesDefaultFailsAll(t *testing.T) {
	t.Parallel()

	for _, isolate := range []bool{false, true} {
		t.Run(map[bool]string{false: "default_fail_all", true: "isolate_head"}[isolate], func(t *testing.T) {
			t.Parallel()
			_, s, r := isolateHarness(t, isolate, false)

			_, e0, _ := bufferAndStage(t, s, r)
			_, e1, _ := bufferAndStage(t, s, r)
			_, e2, _ := bufferAndStage(t, s, r)
			if len(r.batches) != 3 {
				t.Fatalf("buffered %d batches, want 3", len(r.batches))
			}

			headErr := errors.New("head batch terminal failure")
			r.mu.Lock()
			r.failRecords(headErr)
			r.mu.Unlock()

			if got := awaitPromise(t, e0); !errors.Is(got, headErr) {
				t.Fatalf("head promise err = %v, want %v", got, headErr)
			}

			if isolate {
				// Only the head is gone; the two later batches are intact and
				// still buffered, and their promises have not fired.
				if len(r.batches) != 2 {
					t.Fatalf("isolation left %d batches, want 2", len(r.batches))
				}
				assertNoPromise(t, e1)
				assertNoPromise(t, e2)
				if r.buffered.Load() != 2 {
					t.Fatalf("buffered count = %d, want 2", r.buffered.Load())
				}
			} else {
				// Default: the whole partition fails together.
				if len(r.batches) != 0 {
					t.Fatalf("default semantics left %d batches, want 0", len(r.batches))
				}
				if err := awaitPromise(t, e1); !errors.Is(err, headErr) {
					t.Fatalf("second promise err = %v, want %v", err, headErr)
				}
				if err := awaitPromise(t, e2); !errors.Is(err, headErr) {
					t.Fatalf("third promise err = %v, want %v", err, headErr)
				}
				if r.buffered.Load() != 0 {
					t.Fatalf("buffered count = %d, want 0", r.buffered.Load())
				}
			}
		})
	}
}

// TestIsolateUnpinnedHeadContinuesSequence: a head that was never written has
// no pin, so later batches continue its sequence with no producer-ID reload.
func TestIsolateUnpinnedHeadContinuesSequence(t *testing.T) {
	t.Parallel()

	cl, s, r := isolateHarness(t, true, false)

	// Only the head is staged (in flight); the two later batches stay
	// buffered and unstaged, the topology a single in-flight request has.
	promisedOne(t, r)
	headReq := stageOne(t, s)
	b0 := r.batches[0]
	freezeNewest(r)
	promisedOne(t, r)
	freezeNewest(r)
	promisedOne(t, r)
	if r.seq != 1 || r.batchDrainIdx != 1 || r.inflight != 1 || len(r.batches) != 3 {
		t.Fatalf("after staging head: seq=%d idx=%d inflight=%d n=%d, want 1/1/1/3",
			r.seq, r.batchDrainIdx, r.inflight, len(r.batches))
	}
	if b0.pinned {
		t.Fatal("unwritten head must not be pinned (no AppendTo ran in this harness)")
	}

	idBefore := cl.producer.id.Load().(*producerID)
	r.mu.Lock()
	r.failRecords(errors.New("unwritten head exhausted retries"))
	r.mu.Unlock()

	if len(r.batches) != 2 {
		t.Fatalf("after isolation: %d batches remain, want 2", len(r.batches))
	}
	// No reload was requested for a never-written batch.
	if id := cl.producer.id.Load().(*producerID); id != idBefore {
		t.Fatalf("producer ID changed after dropping an unwritten head: %+v -> %+v", idBefore, id)
	}
	if r.needSeqReset {
		t.Fatal("needSeqReset set without an epoch bump")
	}
	// The head owned sequence zero and never wrote it, so the cursor rewinds
	// to zero and the surviving batches continue from there.
	if r.seq != 0 || r.batch0Seq != 0 || r.batchDrainIdx != 0 {
		t.Fatalf("cursors after unwritten-head isolation: seq=%d batch0Seq=%d idx=%d, want 0/0/0",
			r.seq, r.batch0Seq, r.batchDrainIdx)
	}

	// The head request's response callback releases its in-flight slot. With
	// the head gone, drainIdx is already zero and nothing is in flight.
	headReq.batches.eachOwnerLocked(func(b seqRecBatch) { b.decInflight() })
	if r.inflight != 0 {
		t.Fatalf("inflight = %d after head request completion, want 0", r.inflight)
	}

	// The new head stages at sequence zero with no gap and no ID reload.
	req := stageOne(t, s)
	staged := req.batches.bs["t"][0]
	if staged.seq != 0 {
		t.Fatalf("first surviving batch staged at seq %d, want 0", staged.seq)
	}
}

// TestIsolatePinnedHeadBumpsEpochAndResetsSequence: a head that was written
// (pinned) may be on the broker, so dropping it bumps the epoch and restarts
// the partition's sequence at zero before later batches send.
func TestIsolatePinnedHeadBumpsEpochAndResetsSequence(t *testing.T) {
	t.Parallel()

	cl, s, r := isolateHarness(t, true, false)

	head, _, req := bufferAndStage(t, s, r)
	promisedOne(t, r) // second batch, left unstaged so the head stays unique
	// Simulate AppendTo pinning the head to id 5 / epoch 0, the way a real
	// write does inside produceRequest.AppendTo.
	head.pinned, head.pid, head.epoch = true, 5, int16(0)
	head.canFailFromLoadErrs = false // a written batch is outcome-unknown until a response lands

	r.mu.Lock()
	r.failRecords(errors.New("written head hit retry limit"))
	r.mu.Unlock()

	if len(r.batches) != 1 {
		t.Fatalf("after isolation: %d batches remain, want 1", len(r.batches))
	}
	id := cl.producer.id.Load().(*producerID)
	if !errors.Is(id.err, errReloadProducerID) {
		t.Fatalf("producer ID err = %v, want errReloadProducerID", id.err)
	}
	if !r.needSeqReset {
		t.Fatal("needSeqReset not set after dropping a pinned head")
	}
	// In-flight accounting for the dropped head is owned by the request
	// callback (decInflight), not the failure path; the staged request still
	// holds it until its synthetic completion.
	req.batches.eachOwnerLocked(func(b seqRecBatch) { b.decInflight() })
	if r.inflight != 0 {
		t.Fatalf("inflight = %d after request completion, want 0", r.inflight)
	}

	// Model the reload the way producerID() does for the idempotent
	// non-transactional path: a local epoch bump. resetAllProducerSequences
	// has the same effect the pinned head's needSeqReset already armed.
	cl.producer.id.Store(&producerID{id: 5, epoch: 1, err: nil})

	restage := stageOne(t, s)
	if n := len(restage.batches.bs); n != 1 {
		t.Fatalf("restage staged %d topics, want 1", n)
	}
	staged := restage.batches.bs["t"][0]
	if staged.seq != 0 {
		t.Fatalf("surviving batch staged at seq %d, want 0 after epoch bump", staged.seq)
	}
	if r.needSeqReset {
		t.Fatal("needSeqReset not consumed by the restage")
	}
}

// TestIsolateNonIdempotentDropsHeadAndContinues: without idempotency there is
// no pin and no sequence chain, so the head drops and later batches send with
// the wire sequence left at zero.
func TestIsolateNonIdempotentDropsHeadAndContinues(t *testing.T) {
	t.Parallel()

	cl, s, r := isolateHarness(t, true, true)
	if cl.idempotent() {
		t.Fatal("harness should have idempotency disabled")
	}

	// Stage only the head; the two later batches stay buffered and unstaged.
	e0ch := promisedOne(t, r)
	headReq, _, _ := s.createReq(-1, -1)
	freezeNewest(r)
	promisedOne(t, r)
	freezeNewest(r)
	promisedOne(t, r)
	if len(r.batches) != 3 {
		t.Fatalf("buffered %d batches, want 3", len(r.batches))
	}

	r.mu.Lock()
	r.failRecords(ErrRecordRetries)
	r.mu.Unlock()

	if err := awaitPromise(t, e0ch); !errors.Is(err, ErrRecordRetries) {
		t.Fatalf("head promise err = %v, want ErrRecordRetries", err)
	}
	if len(r.batches) != 2 {
		t.Fatalf("non-idempotent isolation left %d batches, want 2", len(r.batches))
	}
	if r.needSeqReset {
		t.Fatal("non-idempotent mode must not arm a sequence reset")
	}
	if r.buffered.Load() != 2 {
		t.Fatalf("buffered = %d, want 2", r.buffered.Load())
	}

	// The head request's response callback releases its in-flight slot.
	headReq.batches.eachOwnerLocked(func(b seqRecBatch) { b.decInflight() })

	// The surviving head stages and would go on the wire.
	req, _, _ := s.createReq(-1, -1)
	if n := len(req.batches.bs); n != 1 {
		t.Fatalf("surviving head staged %d topics, want 1", n)
	}
}

// TestIsolateUnstagedHeadFailureKeepsDrainIndexNonNegative: a head that fails
// from a pre-send check (tryAddBatch / bumpRepeatedLoadErr) was never staged,
// so its removal must not drive batchDrainIdx negative. A negative index
// corrupts every later createReq / finishBatch modulo and length check.
func TestIsolateUnstagedHeadFailureKeepsDrainIndexNonNegative(t *testing.T) {
	t.Parallel()

	_, s, r := isolateHarness(t, true, false)

	// Two batches, neither staged: batchDrainIdx stays zero.
	e0 := promisedOne(t, r)
	freezeNewest(r)
	promisedOne(t, r)
	if r.batchDrainIdx != 0 || r.inflight != 0 {
		t.Fatalf("setup: idx=%d inflight=%d, want 0/0", r.batchDrainIdx, r.inflight)
	}

	// Model a pre-send terminal failure (e.g. maybeFailErr inside
	// tryAddBatch) on the unstaged head.
	r.mu.Lock()
	r.failRecords(ErrRecordRetries)
	r.mu.Unlock()

	if err := awaitPromise(t, e0); !errors.Is(err, ErrRecordRetries) {
		t.Fatalf("head promise err = %v, want ErrRecordRetries", err)
	}
	if r.batchDrainIdx < 0 {
		t.Fatalf("batchDrainIdx went negative (%d) after an unstaged head failed", r.batchDrainIdx)
	}
	if len(r.batches) != 1 {
		t.Fatalf("left %d batches, want 1", len(r.batches))
	}

	// The surviving head must still stage cleanly through createReq.
	req := stageOne(t, s)
	if n := len(req.batches.bs); n != 1 {
		t.Fatalf("surviving head staged %d topics, want 1", n)
	}
}

// TestIsolateRepeatedHeadFailuresDrainOneAtATime: each failure removes only
// the current head, so a backlog of failing batches drains to zero one batch
// per failure instead of all at once.
func TestIsolateRepeatedHeadFailuresDrainOneAtATime(t *testing.T) {
	t.Parallel()

	_, s, r := isolateHarness(t, true, false)

	const n = 4
	chs := make([]chan error, n)
	for i := range n {
		_, chs[i], _ = bufferAndStage(t, s, r)
	}

	r.mu.Lock()
	r.failRecords(errors.New("failure A"))
	r.mu.Unlock()
	if len(r.batches) != n-1 {
		t.Fatalf("after first failure: %d batches, want %d", len(r.batches), n-1)
	}
	if err := awaitPromise(t, chs[0]); err == nil {
		t.Fatal("first head promise should carry the failure")
	}

	r.mu.Lock()
	r.failRecords(errors.New("failure B"))
	r.mu.Unlock()
	if len(r.batches) != n-2 {
		t.Fatalf("after second failure: %d batches, want %d", len(r.batches), n-2)
	}
	if r.buffered.Load() != int64(n-2) {
		t.Fatalf("buffered = %d, want %d", r.buffered.Load(), n-2)
	}
}

// TestIsolateFlushingReturnsAsBatchesFail: a blocking Flush waits on the
// buffered count; failing heads under isolation frees that count like any
// other promise and must not strand the flush. This drives the real Produce
// path (not the hand-built recBuf harness) so producer-level buffered
// accounting is live, then fails batches directly on the recBuf the way a
// retry-limit response would.
func TestIsolateFlushingReturnsAsBatchesFail(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:1"), // nothing dials: ManualFlushing keeps drains off
		ManualFlushing(),
		IsolateFailedBatches(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	// Wire one recBuf into a sink and the topic map so the real Produce
	// path has somewhere to buffer, reusing the harness's shape.
	s := cl.newSink(1)
	s.produceVersion.Store(9)
	r := &recBuf{
		cl:                  cl,
		topic:               "t",
		partition:           0,
		maxRecordBatchBytes: 1 << 20,
		recBufsIdx:          -1,
		lastAckedOffset:     -1,
		sink:                s,
	}
	r.lingerFn = r.unlingerAndManuallyDrain
	s.addRecBuf(r)
	cl.producer.topics.storeTopics([]string{"t"})
	tp := &topicPartition{records: r}
	cl.producer.topics.load()["t"].v.Store(&topicPartitionsData{
		topic:              "t",
		partitions:         []*topicPartition{tp},
		writablePartitions: []*topicPartition{tp},
	})

	const n = 3
	chs := make([]chan error, n)
	for i := range n {
		chs[i] = make(chan error, 1)
		cl.Produce(context.Background(), &Record{
			Topic:   "t",
			Value:   []byte("v"),
			Context: context.Background(),
		}, func(_ *Record, err error) { chs[i] <- err })
		// Freeze each into its own batch so a head failure isolates cleanly.
		r.mu.Lock()
		if len(r.batches) > r.batchDrainIdx {
			r.batches[len(r.batches)-1].frozen = true
		}
		r.mu.Unlock()
	}
	if got := cl.BufferedProduceRecords(); got != n {
		t.Fatalf("buffered after Produce = %d, want %d", got, n)
	}

	flushErr := make(chan error, 1)
	go func() {
		flushErr <- cl.Flush(context.Background())
	}()

	// Fail every batch one head at a time; each failure drops the buffered
	// count, so once the last head goes the flush must return.
	for range n {
		r.mu.Lock()
		r.failRecords(ErrRecordRetries)
		r.mu.Unlock()
	}
	for _, ch := range chs {
		if err := awaitPromise(t, ch); !errors.Is(err, ErrRecordRetries) {
			t.Fatalf("promise err = %v, want ErrRecordRetries", err)
		}
	}

	select {
	case err := <-flushErr:
		if err != nil {
			t.Fatalf("Flush err = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Flush did not return after every isolated batch failed")
	}
	if cl.BufferedProduceRecords() != 0 {
		t.Fatalf("buffered records = %d, want 0", cl.BufferedProduceRecords())
	}
}

// TestIsolateOptionValidation guards the documented incompatibilities.
func TestIsolateOptionValidation(t *testing.T) {
	t.Parallel()

	if _, err := NewClient(
		SeedBrokers("127.0.0.1:1"),
		IsolateFailedBatches(),
		TransactionalID("tx"),
	); err == nil {
		t.Fatal("expected an error combining IsolateFailedBatches with a transactional id")
	}

	if _, err := NewClient(
		SeedBrokers("127.0.0.1:1"),
		IsolateFailedBatches(),
		StopProducerOnDataLossDetected(),
	); err == nil {
		t.Fatal("expected an error combining IsolateFailedBatches with StopProducerOnDataLossDetected")
	}

	cl, err := NewClient(SeedBrokers("127.0.0.1:1"), IsolateFailedBatches())
	if err != nil {
		t.Fatalf("valid combination rejected: %v", err)
	}
	defer cl.Close()
	vals := cl.OptValues("IsolateFailedBatches")
	if len(vals) != 1 || vals[0] != true {
		t.Fatalf("OptValue(IsolateFailedBatches) = %v, want [true]", vals)
	}
}
