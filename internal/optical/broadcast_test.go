package optical

import (
	"sync"
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

// 并发 Publish 与 Unsubscribe：修复前会 panic (send on closed channel)，
// 在 -race 下也会报数据竞争。修复后必须平静退出。
func TestBroadcastPublishUnsubscribeRace(t *testing.T) {
	b := NewBroadcaster()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				b.Publish(model.ThicknessSample{})
			}
		}()
	}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id, _ := b.Subscribe(1)
				b.Unsubscribe(id)
			}
		}()
	}
	wg.Wait()
}

// Unsubscribe 返回后，channel 必须已关闭且不会再收到新样本。
func TestBroadcastNoSampleAfterUnsubscribe(t *testing.T) {
	b := NewBroadcaster()
	id, ch := b.Subscribe(4)
	b.Unsubscribe(id)
	b.Publish(model.ThicknessSample{})
	if _, ok := <-ch; ok {
		t.Fatal("received sample after Unsubscribe")
	}
	// 重复取消不得 panic 或再次 close
	b.Unsubscribe(id)
}
