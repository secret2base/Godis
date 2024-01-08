package dict

import (
	"strconv"
	"sync"
	"testing"
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
}

func TestConcurrentDict_ForEach(t *testing.T) {
}

func TestConcurrentDict_Keys(t *testing.T) {
}

func TestConcurrentDict_RandomKeys(t *testing.T) {
}

func TestConcurrentDict_RandomDistinctKeys(t *testing.T) {
}

func TestConcurrentDict_Clear(t *testing.T) {
}
