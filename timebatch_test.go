package timebatch_test

import (
	"sync"
	"testing"
	"time"

	"github.com/patrickdevivo/timebatch"
)

func execute(t *testing.T, expectedNumMsgs int, batchingInterval, msgInterval time.Duration, options ...timebatch.Option) [][]interface{} {
	t.Helper()

	batches := make([][]interface{}, 0)

	b := timebatch.New(batchingInterval, options...)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for b := range b.Out {
			batches = append(batches, b)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < expectedNumMsgs; i++ {
			b.Send(i)
			time.Sleep(msgInterval)
		}
		b.Close()
	}()

	wg.Wait()

	return batches
}

func TestBasic(t *testing.T) {
	expectedNumMsgs := 30
	batchingInterval := 20 * time.Millisecond
	msgInterval := 1 * time.Millisecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval)

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	if len(batches) < 2 {
		t.Fatalf("expecting at least 2 batches, got %d", len(batches))
	}

	t.Log(batches)
}

func TestSingleItemBatch(t *testing.T) {
	expectedNumMsgs := 10
	batchingInterval := 1 * time.Millisecond
	msgInterval := 2 * time.Millisecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval)

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
		if len(b) != 1 {
			t.Fatalf("expected batch of length 1")
		}
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	t.Log(batches)
}

func TestLongBatchInterval(t *testing.T) {
	expectedNumMsgs := 15
	batchingInterval := 50 * time.Millisecond
	msgInterval := 1 * time.Millisecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval)

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	if len(batches) != 1 {
		t.Fatalf("expected single batch, got %d batches", len(batches))
	}
}

func TestLargerBatch(t *testing.T) {
	expectedNumMsgs := 1_000_000
	batchingInterval := 2 * time.Nanosecond
	msgInterval := 1 * time.Nanosecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval)

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	t.Log(len(batches))
}

func TestDoubleClose(t *testing.T) {
	b := timebatch.New(50 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Send("first")
		b.Send("second")
		b.Close()
		b.Close()
	}()

	batches := make([][]interface{}, 0)
	for b := range b.Out {
		batches = append(batches, b)
	}

	wg.Wait()

	if len(batches) != 1 {
		t.Fatalf("expected single batch, got %d batches", len(batches))
	}

	if len(batches[0]) != 2 {
		t.Fatalf("expected batch to have 2 messages, got %d", len(batches[0]))
	}
}

func TestSendAfterClose(t *testing.T) {
	b := timebatch.New(50 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Send("first")
		b.Close()
		b.Send("second")
	}()

	batches := make([][]interface{}, 0)
	for b := range b.Out {
		batches = append(batches, b)
	}

	wg.Wait()

	if len(batches) != 1 {
		t.Fatalf("expected single batch, got %d batches", len(batches))
	}

	if len(batches[0]) != 1 {
		t.Fatalf("expected batch to have 1 message, got %d", len(batches[0]))
	}
}

func TestWithEarlySendEveryRow(t *testing.T) {
	expectedNumMsgs := 30
	batchingInterval := 20 * time.Millisecond
	msgInterval := 1 * time.Millisecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval, timebatch.WithEarlySend(func(_ []interface{}) bool {
		return true
	}))

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	if len(batches) != expectedNumMsgs {
		t.Fatalf("expecting %d batches, got %d", expectedNumMsgs, len(batches))
	}

	t.Log(batches)
}

func TestWithEarlySendMaxBatchSize(t *testing.T) {
	expectedNumMsgs := 30
	batchingInterval := 200 * time.Millisecond
	msgInterval := 1 * time.Millisecond

	batches := execute(t, expectedNumMsgs, batchingInterval, msgInterval, timebatch.WithEarlySend(func(b []interface{}) bool {
		return len(b) >= 5
	}))

	var numMsgs int
	for _, b := range batches {
		numMsgs += len(b)
	}

	if expectedNumMsgs != int(numMsgs) {
		t.Fatalf("expected %d messages, got %d", expectedNumMsgs, int(numMsgs))
	}

	if len(batches) != 6 {
		t.Fatalf("expecting 6 batches, got %d", len(batches))
	}

	t.Log(batches)
}
