package aof

import (
	"Godis/interface/database"
	"Godis/redis/protocol"
	"strconv"
	"time"
)

// EntityToCmd serialize data entity to redis command
func EntityToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *protocol.MultiBulkReply
	// 目前只支持string格式的value
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
	}
	return cmd
}

var setCmd = []byte("SET")

func stringToCmd(key string, bytes []byte) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes
	return protocol.MakeMultiBulkReply(args)
}

var pExpireAtBytes = []byte("PEXPIREAT")

// MakeExpireCmd generates command line to set expiration for the given key
func MakeExpireCmd(key string, expireAt time.Time) *protocol.MultiBulkReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
