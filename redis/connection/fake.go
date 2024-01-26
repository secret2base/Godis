package connection

import (
	"Godis/lib/logger"
	"fmt"
	"io"
	"sync"
)

// 定义FakeConn用于记录收到的命令
// FakeConn需要实现类似于Connection的方法

type FakeConn struct {
	Connection
	buf    []byte
	offset int
	waitOn chan struct{}
	closed bool
	mu     sync.Mutex
}

func NewFakeConn() *FakeConn {
	return &FakeConn{}
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	c.mu.Lock()
	c.buf = append(c.buf, b...)
	c.mu.Unlock()
	c.notify()
	return len(b), nil
}

func (c *FakeConn) notify() {
	if c.waitOn != nil {
		c.mu.Lock()
		if c.waitOn != nil {
			logger.Debug(fmt.Sprintf("notify %p", c.waitOn))
			close(c.waitOn)
			c.waitOn = nil
		}
		c.mu.Unlock()
	}
}
