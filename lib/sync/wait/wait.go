package wait

import (
	"sync"
	"time"
)

// 这里原作者在sync.WaitGroup方法基础上写了一个wait类
// Add,Done,Wait的实现直接用原方法，增加了WaitWithTimeout方法
// 如果返回false代表程序Wait程序正常结束，true代表程序到达指定等待时间

// Wait is similar with sync.WaitGroup which can wait with timeout
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		w.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
