package dict

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestConcurrentDict_Len(t *testing.T) {

}

func TestConcurrentDict_Put(t *testing.T) {
	d := MakeConcurrentDict(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			key := "k" + strconv.Itoa(i)
			// 测试Put
			ret := d.Put(key, i)
			if ret != 1 { // 返回1说明插入成功
				t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
			}
			// 测试Get
			val, ok := d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i { // 如果值和插入不符
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else { // 如果不存在该键值对
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentDict_PutIfAbsent(t *testing.T) {
	// 沿用上面Put的测试代码，在插入键值对后再次插入测试PutIfAbsent
	d := MakeConcurrentDict(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			key := "k" + strconv.Itoa(i)
			// 测试Put
			ret := d.Put(key, i)
			if ret != 1 { // 返回1说明插入成功
				t.Error("put test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key: " + key)
			}
			// 测试Get
			val, ok := d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i { // 如果值和插入不符
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else { // 如果不存在该键值对
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}

			// key已经存在，返回值应该是0且不更新键值对
			ret = d.PutIfAbsent(key, i*10)
			if ret != 0 {
				t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
			}
			val, ok = d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != i {
					t.Error("put test failed: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal) + ", key: " + key)
				}
			} else {
				t.Error("put test failed: expected true, actual: false, key: " + key)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentDict_PutIfExists(t *testing.T) {
	d := MakeConcurrentDict(0)
	count := 100
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(i int) {
			// 键值对不存在时插入，应该返回0且不插入
			key := "k" + strconv.Itoa(i)
			// insert
			ret := d.PutIfExists(key, i)
			if ret != 0 { // insert
				t.Error("put test failed: expected result 0, actual: " + strconv.Itoa(ret))
			}

			// 键值对存在时插入应该更新键值对并返回1
			d.Put(key, i)
			d.PutIfExists(key, 10*i)
			val, ok := d.Get(key)
			if ok {
				intVal, _ := val.(int)
				if intVal != 10*i {
					t.Error("put test failed: expected " + strconv.Itoa(10*i) + ", actual: " + strconv.Itoa(intVal))
				}
			} else {
				_, ok := d.Get(key)
				t.Error("put test failed: expected true, actual: false, key: " + key + ", retry: " + strconv.FormatBool(ok))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestConcurrentDict_Remove(t *testing.T) {
	// Godis的原作者验证了头节点，尾节点以及中间节点的删除情况
	d := MakeConcurrentDict(0)
	totalCount := 100
	// 先插入100个键值对
	for i := 0; i < totalCount; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	if d.Len() != totalCount {
		t.Error("put test failed: expected len is 100, actual: " + strconv.Itoa(d.Len()))
	}
	// 测试删除功能
	for i := 0; i < totalCount; i++ {
		key := "k" + strconv.Itoa(i)
		_, ret := d.Remove(key)
		// 检查Remove返回值
		if ret != 1 {
			t.Error("remove test failed: expected result 1, actual: " + strconv.Itoa(ret) + ", key:" + key)
		}
		// 检查字典中是否还存在该键值对
		_, ok := d.Get(key)
		if ok {
			t.Error("remove test failed: expected true, actual false")
		}
		// 检查长度是否正确
		if d.Len() != totalCount-i-1 {
			t.Error("remove test failed: expected len is" + strconv.Itoa(totalCount-i-1) + ", actual: " + strconv.Itoa(d.Len()))
		}
	}
}

func TestConcurrentDict_ForEach(t *testing.T) {
	d := MakeConcurrentDict(0)
	size := 100
	for i := 0; i < size; i++ {
		// insert
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	i := 0
	// 遍历键值对，验证键值
	d.ForEach(func(key string, value interface{}) bool {
		intVal, _ := value.(int)
		expectedKey := "k" + strconv.Itoa(intVal)
		if key != expectedKey {
			t.Error("forEach test failed: expected " + expectedKey + ", actual: " + key)
		}
		i++
		return true
	})
	if i != size {
		t.Error("forEach test failed: expected " + strconv.Itoa(size) + ", actual: " + strconv.Itoa(i))
	}
}

func TestConcurrentDict_Keys(t *testing.T) {
	d := MakeConcurrentDict(0)
	size := 10
	for i := 0; i < size; i++ {
		// 生成长度不超过5的随机字符串
		d.Put(RandString(5), RandString(5))
	}
	if len(d.Keys()) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
	}
}

func TestConcurrentDict_RandomKey(t *testing.T) {
	d := MakeConcurrentDict(0)
	count := 100
	for i := 0; i < count; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	fetchSize := 10
	result := d.RandomKeys(fetchSize)
	// 验证取回指定数量的随机键
	if len(result) != fetchSize {
		t.Errorf("expect %d random keys acturally %d", fetchSize, len(result))
	}
	// 验证不重复的键
	result = d.RandomDistinctKeys(fetchSize)
	distinct := make(map[string]struct{})
	for _, key := range result {
		distinct[key] = struct{}{}
	}
	if len(result) != fetchSize {
		t.Errorf("expect %d random keys acturally %d", fetchSize, len(result))
	}
	if len(result) > len(distinct) {
		t.Errorf("get duplicated keys in result")
	}
}

func TestConcurrentDict_Clear(t *testing.T) {
	d := MakeConcurrentDict(0)
	count := 100
	for i := 0; i < count; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	if d.Len() != count {
		t.Error("put test failed")
	}
	d.Clear()
	if d.Len() != 0 {
		t.Error("clear test failed,expected 0, actual: " + strconv.Itoa(d.Len()))
	}
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString create a random string no longer than n
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}
