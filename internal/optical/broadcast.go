package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
)

type Broadcaster struct {
	mu          sync.RWMutex
	subscribers map[int]chan model.ThicknessSample
	next        int
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subscribers: map[int]chan model.ThicknessSample{}}
}
func (b *Broadcaster) Subscribe(size int) (int, <-chan model.ThicknessSample) {
	if size < 1 {
		size = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	ch := make(chan model.ThicknessSample, size)
	b.subscribers[id] = ch
	return id, ch
}
func (b *Broadcaster) Unsubscribe(id int) {
	b.mu.Lock()
	if ch, ok := b.subscribers[id]; ok {
		delete(b.subscribers, id)
		close(ch)
	}
	b.mu.Unlock()
}
func (b *Broadcaster) Publish(sample model.ThicknessSample) {
	// 读锁必须覆盖整个发送过程：发送是非阻塞的（select/default），
	// 持锁期间 Unsubscribe 无法获取写锁关闭 channel，
	// 因此 send 与 close 互斥，不会发生 send on closed channel。
	// Unsubscribe 返回后，channel 已从 map 移除并关闭，不会再收到新样本。
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- sample:
		default:
		}
	}
}
