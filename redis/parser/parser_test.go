package parser

import (
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"testing"
)

func TestParseStream(t *testing.T) {
	reply := []redis.Reply{
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("Unknown Error"),
		protocol.MakeIntReply(1),
		protocol.MakeBulkReply([]byte("a\r\nb")),
		protocol.MakeMultiBulkReply(),
	}
}
