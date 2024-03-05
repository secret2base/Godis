package lock

import (
	"sort"
	"sync"
)

// 在concurrent.go中实现的ConCurrentDict中使用了分段锁，保证了对于单个key的并发读写问题
// 即对某个key进行读写时，当前分段无法被其他协程占用
// 但是在数据库的操作中需要锁定一个或一组键值对，并在需要的时候释放
// 例如 Incr 命令需要完成: 读取 -> 做加法 -> 写入 三步操作 这三个步骤是原子执行的
// 单纯的分段锁并不能保证这一命令原子执行
// 例如 MSETNX 命令当且仅当所有给定键都不存在时所有给定键设置值, 因此我们需要锁定所有给定的键直到完成所有键的检查和设置
// 分段锁也不能满足一组键值对的锁定

// 一个自然的想法是为每个key定义一个RWMutex，并在协程占用时加锁。然而此锁无论占用完后是否释放，均会带来问题
// 若锁释放：t1 B协程解锁key1对应的锁 t2 A协程初始化key1的锁 t3 B协程释放key1的锁 t4 A协程锁定key1的锁 t4时刻出错
// 若锁不释放，巨大的key空间会带来严重的性能问题，程序会持有相当数量的锁

// 因此，一个解决方案是为每个key的哈希值定义一个锁，并且不释放锁
// 因为哈希值空间远比key空间小

// Locks 结构体是一个RWMutex的切片，用于管理一组分段的锁定与释放
type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{
		table,
	}
}

func (locks *Locks) Lock(key string) {
	// 通过哈希值查找key对于的锁
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

/* ---- Keys Index ---- */
// 在锁定多个key时需要注意，若协程A持有键a的锁试图获得键b的锁，此时协程B持有键b的锁试图获得键a的锁则会形成死锁。
// 解决方法是所有协程都按照相同顺序加锁，若两个协程都想获得键a和键b的锁，那么必须先获取键a的锁后获取键b的锁，这样就可以避免循环等待。

func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	// 求所有keys的哈希值及其对应的段表，并排序
	// 作者在博客中此处的map值为bool型，但github源码中为struct{}
	// indexMap := make(map[uint32]bool)
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := locks.spread(fnv32(key))
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

// Locks obtains multiple exclusive locks for writing
// invoking Lock in loop may cause dead lock, please use Locks
func (locks *Locks) Locks(keys ...string) {
	// 先得到加锁顺序
	indices := locks.toLockIndices(keys, false)
	// 再依序加锁
	for index := range indices {
		mu := locks.table[index]
		mu.Lock()
	}
}

// RLocks obtains multiple shared locks for reading
// invoking RLock in loop may cause dead lock, please use RLocks
func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RLock()
	}
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	// 需要写的键放在一个map中
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := locks.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		// 如果在writeIndexSet中就调用Lock方法
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	// 需要写的键放在一个map中
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := locks.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		// 如果在writeIndexSet中就调用Lock方法
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

/* ---- Hash Functions ---- */

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic(any("dict is nil"))
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & uint32(hashCode)
}
