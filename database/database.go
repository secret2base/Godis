package database

import (
	"Godis/datastruct/dict"
	"Godis/interface/database"
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"strings"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type CmdLine = [][]byte

type DB struct {
	index int
	// key -> DataEntity
	data *dict.ConcurrentDict
	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict
	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	// addaof is used to add command to aof
	addAof func(CmdLine)

	// callbacks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// 创建一个DB，DB的底层是由ConCurrentDict实现的

func makeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrentDict(dataDictSize),
		ttlMap:     dict.MakeConcurrentDict(ttlDictSize),
		versionMap: dict.MakeConcurrentDict(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
}

func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// transaction control commands and other commands which cannot execute within transaction
	// 一个正常的事务例子如下
	// MULTI
	// SET key1 value1
	// INCR key2
	// EXEC
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		// discard 取消当前的事务，如果事务存在
		// 只能取消事务，而不能撤销已执行的事务
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	return db.execNormalCommand(cmdLine)
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func (db *DB) GetVersion(key string) uint32 {

}

func (db *DB) addVersion(write ...string) {

}

func (db *DB) RWLocks(write []string, read []string) {

}

func (db *DB) RWUnLocks(write []string, read []string) {

}
