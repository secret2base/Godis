package dict

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	mutex sync.RWMutex
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

// MakeConcurrent 根据输入的分段数构造ConcurrentDict
func MakeConcurrent(shardCount int) *ConcurrentDict {
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
		panic(any("dict is nil"))
	}
	// 计算分段表的数量
	tableSize := uint32(len(dict.table))
	// 将hashCode映射到分段表中
	return (tableSize - 1) & hashCode
}

// 通过下标返回对应的分段表
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic(any("dict is nil"))
	}
	return dict.table[index]
}

// 开始实现 datastruct/dict/dict.go 中定义的Dict接口

func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic(any("dict is nil"))
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

func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return val, exists
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic(any("dict is nil"))
	}
	// 通过原子操作确保读取dict.count
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic(any("dict is nil"))
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

func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

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
		panic(any("dict is nil"))
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

func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfExist(key string, val interface{}) (result int) {
	if dict == nil {
		panic(any("dict is nil"))
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

func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Remove(key string) (val interface{}, result int) {
	if dict == nil {
		panic(any("dict is nil"))
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

func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic(any("dict is nil"))
	}
	// 遍历每个分段
	// _和s是index和value
	for _, s := range dict.table {
		s.mutex.RLock()
		// 定义函数f
		f := func() bool {
			// 函数结束时释放对应分段的锁
			defer s.mutex.RUnlock()
			// 遍历此分段中的map
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		// 执行f并判断执行结果
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	if dict == nil {
		panic(any("dict is nil"))
	}
	// 创建一个和字典长度相同的字符串切片,但是在遍历过程中Dict也有可能会继续增加新的键值对
	keys := make([]string, dict.Len())
	// 遍历所有分段，存储keys，复用了ForEach方法
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
		} else { // 超过容量就使用append方法继续添加
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey 是RandomKeys的辅助函数，用某一分段中读取随机一个key
func (s *shard) RandomKey() string {
	if s == nil {
		panic(any("shard is nil"))
	}
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// 由于map的读取是随机的，所以返回第一个key即可
	for key := range s.m {
		return key
	}
	// shard为空时返回空字符串
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	// limit大于Dict长度时返回所有Key
	if limit > size {
		return dict.Keys()
	}
	keys := make([]string, limit)
	// 下面将随机选择shard并调用RandomKey方法
	// rand.NewSource(time.Now().UnixNano())创建一个随机数种子
	// rand.New()基于随机数种子创建随机数生成器
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; i++ {
		s := dict.getShard(uint32(nR.Intn(dict.shardCount)))
		keys[i] = s.RandomKey()
	}
	return keys
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit > size {
		return dict.Keys()
	}
	// 为区分不同的key，使用map存储随机出来的key
	// map的值定义为空结构体，因为不需要用到值
	keys := make(map[string]struct{})

	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; i++ {
		s := dict.getShard(uint32(nR.Intn(dict.shardCount)))
		key := s.RandomKey()
		// 先判断是否取到了key,再判断是否已经存在此key
		if key != "" {
			if _, exists := keys[key]; !exists {
				keys[key] = struct{}{}
			}
		}
	}
	// 将map转为切片返回
	result := make([]string, limit)
	i := 0
	for k := range keys {
		result[i] = k
		i++
	}
	return result
}

func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

// 对于ConcurrentDict也要解决锁定一组键的问题
func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	// 求所有keys的哈希值及其对应的段表，并排序
	// 作者在博客中此处的map值为bool型，但github源码中为struct{}
	// indexMap := make(map[uint32]bool)
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(fnv32(key))
		// 得出需要加锁的分段，并加入map中
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	// 给切片排序，如果比较函数返回true则i元素应该排在j元素之前
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}
