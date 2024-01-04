package parser

import (
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"bytes"
	"io"
	"testing"
)

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("Unknown Error"),
		protocol.MakeIntReply(1),
		protocol.MakeBulkReply([]byte("a\r\nb")),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("SET"),
			[]byte("key"),
			[]byte("value"),
		}),
	}
	// 创建缓冲区，并将所有Reply写入
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	// 将replies拷贝以备后续比较验证
	expectedReply := make([]redis.Reply, len(replies))
	copy(expectedReply, replies)

	ch := ParseStream(bytes.NewBuffer(reqs.Bytes()))

	// 从通道中接收Reply并比较
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			// EOF: End OF File，判断是否结束读取
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expectedReply[i]
		i++
		if !BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
			t.Error("Reply data: " + string(payload.Data.ToBytes()))
		}
	}
}

func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}
