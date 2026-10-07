package optical

import (
	"sync"
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

// Publish must never send on a channel that Unsubscribe has closed.
// Before the fix, Publish copied the subscriber list under RLock and
// sent after releasing it, so a concurrent Unsubscribe could close a
// copied channel and panic with "send on closed channel".
func TestBroadcasterPublishUnsubscribeRace(t *testing.T) {
	b := NewBroadcaster()
	const subscribers = 8

	var wg sync.WaitGroup
	for i := 0; i < subscribers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				id, ch := b.Subscribe(1)
				b.Unsubscribe(id)
				// Drain after close: must terminate, must not panic.
				for range ch {
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				b.Publish(model.ThicknessSample{Nanometers: float64(j)})
			}
		}()
	}
	wg.Wait()
}

// After Unsubscribe returns, the subscriber must not receive any
// sample published later.
func TestBroadcasterNoDeliveryAfterUnsubscribe(t *testing.T) {
	b := NewBroadcaster()
	id, ch := b.Subscribe(4)
	b.Publish(model.ThicknessSample{Nanometers: 1})
	b.Unsubscribe(id)

	// Everything published before the unsubscribe is still delivered.
	if s, ok := <-ch; !ok || s.Nanometers != 1 {
		t.Fatalf("expected buffered sample before close, got %v ok=%v", s, ok)
	}
	if _, ok := <-ch; ok {
		t.Fatal("expected channel to be closed after Unsubscribe")
	}

	b.Publish(model.ThicknessSample{Nanometers: 2})
	if s, ok := <-ch; ok {
		t.Fatalf("received sample after unsubscribe: %v", s)
	}
}
