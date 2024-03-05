package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	// epoch0 is set to the twitter snowflake epoch of Nov 04 2010 01:42:54 UTC in milliseconds
	// You may customize this to set a different epoch for your application.
	epoch0      int64 = 1288834974657                 // 起始时间戳
	maxSequence int64 = -1 ^ (-1 << uint64(nodeLeft)) // 序列号最大值
	timeLeft    uint8 = 22                            // 时间戳占用位数
	nodeLeft    uint8 = 10                            // 节点标识符占用位数
	nodeMask    int64 = -1 ^ (-1 << uint64(timeLeft-nodeLeft))
)

// IDGenerator generates unique uint64 ID using snowflake algorithm
type IDGenerator struct {
	mu        *sync.Mutex
	lastStamp int64 // 上次生成id的时间戳
	nodeID    int64 // 节点标识
	sequence  int64 // 序列号，用于解决同一毫秒内多个ID的冲突问题
	epoch     time.Time
}

// MakeGenerator creates a new IDGenerator
func MakeGenerator(node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(node))
	nodeID := int64(fnv64.Sum64()) & nodeMask

	var curTime = time.Now()
	epoch := curTime.Add(time.Unix(epoch0/1000, (epoch0%1000)*1000000).Sub(curTime))

	return &IDGenerator{
		mu:        &sync.Mutex{},
		lastStamp: -1,
		nodeID:    nodeID,
		sequence:  1,
		epoch:     epoch,
	}
}

// NextID returns next unique ID
func (w *IDGenerator) NextID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Since(w.epoch).Nanoseconds() / 1000000
	if timestamp < w.lastStamp {
		log.Fatal("can not generate id")
	}
	if w.lastStamp == timestamp {
		w.sequence = (w.sequence + 1) & maxSequence
		if w.sequence == 0 {
			for timestamp <= w.lastStamp {
				timestamp = time.Since(w.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		w.sequence = 0
	}
	w.lastStamp = timestamp
	id := (timestamp << timeLeft) | (w.nodeID << nodeLeft) | w.sequence
	//fmt.Printf("%d %d %d\n", timestamp, w.sequence, id)
	return id
}
