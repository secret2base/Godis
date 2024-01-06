package dict

import (
	"math"
	"sync"
	"sync/atomic"
)

// ConcurrentDict 分段字典，一个Dict中有多个shard
type ConcurrentDict struct {
	// 分段表的切片
	table []*shard
	// 记录Dict的键值对数
	count int32
	// 记录Dict的分段数
	shardCount int
}

// shard 每个分段都有自己的mutex锁
type shard struct {
	m     map[string]interface{}
	mutex sync.Mutex
}

// computeCapacity 对于输入的param，输出不小于param的最小2的幂次
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	// 判断是否溢出
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrentDict 根据输入的分段数构造ConcurrentDict
func MakeConcurrentDict(shardCount int) *ConcurrentDict {
	// 取整计算分段数
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		// 初始化每个shard
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
	return d
}

/*
hash算法
初始化一个哈希值为初始值
遍历输入数据的每个字节，对当前的哈希值执行
- 将哈希值与一个特定的素数（通常是16777619）进行异或运算
- 将当前字节值乘以哈希值
*/
// 我查到的fnv-1a算法for循环中是先异或再乘法，与Godis的作者不同
// 经验证，这两种算法的结果是不同的，我这里采用fnv-1a算法
const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

// 将hashCode映射到对应的分段表中
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	// 计算分段表的数量
	tableSize := uint32(len(dict.table))
	// 将hashCode映射到分段表中
	return (tableSize - 1) & hashCode
}

// 通过下标返回对应的分段表
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// 开始实现 datastruct/dict/dict.go 中定义的Dict接口

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	// 这里为什么用Lock而非RLock,不太明白
	s.mutex.Lock()
	defer s.mutex.Unlock()
	val, exists = s.m[key]
	return val, exists
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	// 通过原子操作确保读取dict.count
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 判断该key是否已经存在
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	// count计数器加一，原子操作
	dict.addCount()
	s.m[key] = val
	return 1
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 判断该key是否已经存在
	if _, ok := s.m[key]; ok {
		// s.m[key] = value
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 判断该key是否已经存在,如果存在
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		// 这里返回1代表Put成功而非键值对数量增加
		return 1
	}
	// count计数器加一，原子操作
	// dict.addCount()
	// s.m[key] = value
	// 不存在则无需操作
	return 0
}

func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// 判断该key是否已经存在,如果存在
	if v, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		// 这里返回1代表Put成功而非键值对数量增加
		return v, 1
	}
	return nil, 0
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}
	// 遍历每个分段
	// _和s是index和value
	for _, s := range dict.table {
		// RLock是在 datastruct/lock/lock_map.go中实现的方法
		s.mutex.RLock()

	}
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}