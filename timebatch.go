package timebatch

import (
	"context"
	"sync"
	"time"
)

// TimeBatcher batches messages over a time interval
type TimeBatcher struct {
	ticker   *time.Ticker
	msgs     chan interface{}
	Out      chan []interface{}
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
	closed   sync.Once
	isClosed bool

	// earlySends are user defined functions for "short-circuiting"
	// the time interval, to specify a condition for an early send.
	earlySends []func([]interface{}) bool
}

type Option func(*TimeBatcher)

// WithEarlySend is an option for adding an "early send" check,
// which is a function that is run every time an item is added to the batch
// to check whether an "early send" should occur. An early send
// is the emission of a batch before the next time interval is reached.
func WithEarlySend(f func([]interface{}) bool) Option {
	return func(t *TimeBatcher) {
		t.earlySends = append(t.earlySends, f)
	}
}

// New creates a time batcher using the provided interval. A time batcher receives messages via .Send() and emits a batch of those messages on .Out at the interval provided.
// Close() must be called to clean up/finish the batcher when there are no more messages to send. The .Out channel will be closed when Close is called, after any final batches are sent.
// This should allow for range-ing over the Out channel to receive time-batches of the sent messages.
func New(interval time.Duration, options ...Option) *TimeBatcher {
	ticker := time.NewTicker(interval)
	msgs := make(chan interface{})
	out := make(chan []interface{})
	buffer := make([]interface{}, 0, 500) // TODO(patrickdevivo) make this configurable?
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	t := &TimeBatcher{
		ticker: ticker,
		msgs:   msgs,
		Out:    out,
		cancel: cancel,
		wg:     &wg,
	}

	for _, o := range options {
		o(t)
	}

	wg.Add(1)

	// spawn a go routine that either receives new messages or listens for Close()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				if len(buffer) != 0 {
					out <- buffer
				}
				close(out)
				return
			case msg := <-msgs:
				buffer = append(buffer, msg)
				for _, f := range t.earlySends {
					if f(buffer) {
						out <- buffer
						buffer = make([]interface{}, 0, 500)
						break
					}
				}
			case <-ticker.C:
				if len(buffer) != 0 {
					out <- buffer
					buffer = make([]interface{}, 0, 500)
				}
			}
		}
	}()

	return t
}

// Send sends a new message to the batcher. Note that this is BLOCKING, which means if nothing is reading from
// the Out channel, calls to Send will block.
func (t *TimeBatcher) Send(message interface{}) {
	if !t.isClosed {
		t.msgs <- message
	}
}

// Close blocks until all messages sent using Send have been emitted in a batch on .Out
// and cleans up the time batcher
func (t *TimeBatcher) Close() {
	t.closed.Do(func() {
		t.isClosed = true
		t.cancel()
		t.wg.Wait()
	})
}
