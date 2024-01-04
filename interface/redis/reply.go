package redis

// Reply 定义Reply接口,用于响应RESP信息
type Reply interface {
	ToBytes() []byte
}
