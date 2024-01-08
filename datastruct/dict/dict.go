package dict

// Consumer 用于遍历字典的函数类型，范围false时中断遍历
type Consumer func(key string, val interface{}) bool

// Dict 把字典定义为接口方便更改字典的具体实现
// 字典的键为字符串，值可以为其他类型
type Dict interface {
	// Get 根据收到的键返回字典中的值，如果不存在exists返回false
	Get(key string) (val interface{}, exists bool)
	// Len 返回字典中键值对的数量
	Len() int
	// Put 将键值对插入字典，返回插入键值对的数量
	Put(key string, val interface{}) (result int)
	// PutIfAbsent 如果键值对不存在则插入
	PutIfAbsent(key string, val interface{}) (result int)
	// PutIfExists 如果键值对存在则插入新的键值对比
	PutIfExist(key string, val interface{}) (result int)
	// Remove 根据键删除对应的键值对，result返回删除结果
	Remove(key string) (val interface{}, result int)
	// ForEach 通过consumer遍历字典并执行操作
	ForEach(consumer Consumer)
	// Keys 返回字典中所有键的字符串切片
	Keys() []string
	// RandomKeys 随机选择字典中的一组键
	RandomKeys(limit int) []string
	// RandomDistinctKeys 随机选择一组不重复的键
	RandomDistinctKeys(limit int) []string
	// Clear 清空字典
	Clear() (result int)
}
