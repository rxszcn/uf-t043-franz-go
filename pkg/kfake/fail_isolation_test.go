package kfake

// Failure-isolation end-to-end coverage for kgo.IsolateFailedBatches. These
// tests run a real kgo client against the in-process fake broker and inject
// the rolling-restart failure shape (a produce request answered with a
// retriable leader error while the partition is briefly unavailable), then
// assert the contrast between the default fail-all semantics and the opt-in
// per-batch isolation, for both the idempotent and non-idempotent producers.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// produceResults collects the promise outcome of every produced record.
type produceResults struct {
	ok   atomic.Int64
	fail atomic.Int64
}

func (p *produceResults) promise() func(*kgo.Record, error) {
	return func(_ *kgo.Record, err error) {
		p.record(err)
	}
}

func (p *produceResults) record(err error) {
	if err == nil {
		p.ok.Add(1)
	} else {
		p.fail.Add(1)
	}
}

// holdFirstProduce installs a control that holds the first produce request
// until release fires, then answers every subsequent produce normally. Holding
// the first request is what lets a backlog of later, separate batches build up
// behind the head while it is in flight. SleepOutOfOrder keeps the cluster
// loop serving the later requests while the first one sleeps.
func holdFirstProduce(c *Cluster) (held chan struct{}, release chan struct{}) {
	held = make(chan struct{})
	release = make(chan struct{})
	var first atomic.Bool
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !first.CompareAndSwap(false, true) {
			return nil, nil, false // later produces run normally
		}
		close(held)
		c.SleepControl(func() { <-release })
		return nil, nil, false
	})
	return held, release
}

// failProduceOnce installs a one-shot fault that answers a produce with a
// retriable leader error, the broker-side signature of a rolling restart.
func failProduceOnce(c *Cluster, topic string, err *kerr.Error) *FaultHandle {
	return c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   err,
		Count: 1,
	})
}

// isolationCase builds one producer client for the matrix.
func isolationCase(t *testing.T, c *Cluster, topic string, isolate, idempotent bool) (*kgo.Client, *produceResults) {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(topic),
		// One record per batch so the held head and the backlog behind it are
		// independent batches, not one merged batch that fails atomically.
		kgo.ProducerBatchMaxBytes(900),
		// No retries: the injected leader error terminates the head at once,
		// which is the condition the two semantics disagree on.
		kgo.RecordRetries(0),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
		// A leader-error retry waits on a metadata refresh, which
		// MetadataMinAge otherwise throttles to seconds; the tests need only
		// milliseconds of spacing.
		kgo.MetadataMinAge(20 * time.Millisecond),
	}
	if isolate {
		opts = append(opts, kgo.IsolateFailedBatches())
	}
	if !idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite(),
			kgo.MaxProduceRequestsInflightPerBroker(1))
	}
	cl := newPlainClient(t, c, opts...)
	return cl, &produceResults{}
}

// TestProduceFailureIsolationRollingRestart is the control pair for the
// reported incident: a rolling restart fails one in-flight head batch while
// later, never-sent batches are buffered behind it. The default client fails
// the whole partition backlog; the isolated client fails only the head and
// delivers the rest.
func TestProduceFailureIsolationRollingRestart(t *testing.T) {
	t.Parallel()

	for _, idempotent := range []bool{false, true} {
		for _, isolate := range []bool{false, true} {
			name := "idempotent=" + b2s(idempotent) + "/isolate=" + b2s(isolate)
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				const topic = "isolate-restart"
				c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), SleepOutOfOrder())
				cl, res := isolationCase(t, c, topic, isolate, idempotent)

				held, release := holdFirstProduce(c)

				const total = 6
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				// The first record becomes the held head batch.
				headDone := make(chan struct{})
				go func() {
					cl.Produce(ctx, &kgo.Record{Value: make([]byte, 700)}, func(_ *kgo.Record, err error) {
						res.record(err)
						close(headDone)
					})
				}()
				select {
				case <-held:
				case <-time.After(5 * time.Second):
					t.Fatal("first produce was never held")
				}

				// Buffer the backlog as separate batches while the head is
				// held. Manual control of the first request keeps these from
				// being sent until we release it.
				for range total - 1 {
					cl.Produce(ctx, &kgo.Record{Value: make([]byte, 700)}, res.promise())
				}

				// Arm the one-shot leader error, then release the head: its
				// response is the injected NOT_LEADER_FOR_PARTITION.
				h := failProduceOnce(c, topic, kerr.NotLeaderForPartition)
				close(release)

				// Wait for every promise to settle.
				deadline := time.Now().Add(10 * time.Second)
				for res.ok.Load()+res.fail.Load() < total && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if got := res.ok.Load() + res.fail.Load(); got != total {
					t.Fatalf("only %d/%d records settled", got, total)
				}
				if hits := h.Hits(); hits != 1 {
					t.Fatalf("leader-error fault hit %d times, want 1", hits)
				}

				if isolate {
					// Only the head fails; every later, never-sent batch
					// delivers once the single leader error is consumed.
					if res.fail.Load() != 1 {
						t.Fatalf("isolation failed %d records, want only the 1 head", res.fail.Load())
					}
					if res.ok.Load() != total-1 {
						t.Fatalf("isolation delivered %d records, want %d", res.ok.Load(), total-1)
					}
				} else {
					// Default gapless semantics: the head's terminal failure
					// fails the entire partition backlog.
					if res.fail.Load() != total {
						t.Fatalf("default semantics failed %d/%d records, want all %d",
							res.fail.Load(), total, total)
					}
					if res.ok.Load() != 0 {
						t.Fatalf("default semantics delivered %d records, want 0", res.ok.Load())
					}
				}
			})
		}
	}
}

// TestProduceFailureIsolationHeadRecovers is the neighbor control: when the
// head's error is retried and eventually succeeds, both modes deliver every
// record - isolation must not turn a recoverable failure into a lost record.
func TestProduceFailureIsolationHeadRecovers(t *testing.T) {
	t.Parallel()

	for _, idempotent := range []bool{false, true} {
		t.Run("idempotent="+b2s(idempotent), func(t *testing.T) {
			t.Parallel()

			const topic = "isolate-recover"
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), SleepOutOfOrder())

			opts := []kgo.Opt{
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.DefaultProduceTopic(topic),
				kgo.ProducerBatchMaxBytes(900),
				kgo.RecordRetries(5), // enough to ride out the one leader error
				kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
				kgo.MetadataMinAge(20 * time.Millisecond),
				kgo.IsolateFailedBatches(),
			}
			if !idempotent {
				opts = append(opts, kgo.DisableIdempotentWrite(),
					kgo.MaxProduceRequestsInflightPerBroker(1))
			}
			cl := newPlainClient(t, c, opts...)
			res := &produceResults{}

			const total = 4
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			for range total {
				cl.Produce(ctx, &kgo.Record{Value: make([]byte, 700)}, res.promise())
			}

			// One retriable error mid-stream; the client retries past it.
			failProduceOnce(c, topic, kerr.NotLeaderForPartition)

			if err := cl.Flush(ctx); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if res.fail.Load() != 0 {
				t.Fatalf("recoverable failure produced %d failed records, want 0", res.fail.Load())
			}
			if res.ok.Load() != total {
				t.Fatalf("delivered %d/%d records, want all", res.ok.Load(), total)
			}
		})
	}
}

// TestProduceFailureIsolationUnknownTopicLimit exercises the metadata-load
// failure path (bumpRepeatedLoadErr), which must also isolate rather than
// amplify once the option is on.
func TestProduceFailureIsolationUnknownTopicLimit(t *testing.T) {
	t.Parallel()

	const topic = "isolate-unknown"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), SleepOutOfOrder())

	cl, res := isolationCase(t, c, topic, true, true)
	// Make the unknown-partition limit trip immediately: every produce answer
	// is UNKNOWN_TOPIC_OR_PARTITION, and the default limit of retries is zero
	// here via RecordRetries(0) in the case builder.
	h := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   kerr.UnknownTopicOrPartition,
		Count: -1,
	})
	defer h.Remove()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two independent batches; the head hitting the unknown limit fails, and
	// the next head is then attempted on its own rather than failing both at
	// once. Both ultimately fail because the fault stays installed, but each
	// fails as its own head through the isolated path.
	const total = 2
	for range total {
		cl.Produce(ctx, &kgo.Record{Value: make([]byte, 700)}, res.promise())
	}

	deadline := time.Now().Add(8 * time.Second)
	for res.fail.Load() < total && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if res.fail.Load() != total {
		t.Fatalf("got %d failures, want %d", res.fail.Load(), total)
	}
	// The fault was hit at least once per head, proving the second batch was
	// attempted on its own after the first failed, not failed alongside it.
	if h.Hits() < total {
		t.Fatalf("fault hit %d times, want at least %d (each head attempted separately)", h.Hits(), total)
	}
}

func b2s(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
