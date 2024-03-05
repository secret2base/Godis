package database

import (
	"Godis/datastruct/dict"
	"Godis/interface/database"
	"Godis/interface/redis"
	"Godis/lib/timewheel"
	"Godis/redis/protocol"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

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

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// 创建一个DB，DB的底层是由ConCurrentDict实现的

func makeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
}

// makeBasicDB create DB instance only with basic abilities.
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec handles the execution of Redis commands. It checks if the given command
// is a transaction control command or any other command that cannot be executed
// within a transaction. If it's a transaction control command (e.g., MULTI, DISCARD, EXEC),
// it performs the corresponding action. If the command is not related to transactions,
// and the connection is not currently in a MULTI state, it calls execNormalCommand
// to execute the command in a non-transactional context.
//
// Example of a normal transaction:
//
//	MULTI
//	SET key1 value1
//	INCR key2
//	EXEC
//
// Parameters:
//   - db: The Redis database instance.
//   - c: The Redis connection on which the command is executed.
//   - cmdLine: The command line containing the Redis command and its arguments.
//
// Returns:
//   - redis.Reply: The Redis reply generated as a result of the command execution.
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	// transaction control commands and other commands which cannot execute within transaction
	// 一个正常的事务例子如下
	// MULTI
	// SET key1 value1
	// INCR key2
	// EXEC
	// 先判断cmdLine是否为事务处理，如果是则执行相应的操作，否则调用execNormalCommand
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
	// 如果命令的arity为正数，表示只能接收arity个参数；负数则表示可以接收不少于arity个参数
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

	// 调用该命令的准备函数
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	// 将write切片转化为独立的参数传递给addVersion
	// db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

/* ---- Data Access ---- */
// 这里的方法都是调用了ConcurrentDict中的方法并封装了DB额外的功能

func (db DB) GetEntity(key string) (*database.DataEntity, bool) {
	// 从db的ConCurrentDict中读取
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return nil, false
	}
	// 检查是否过期
	if db.IsExpired(key) {
		return nil, false
	}
	// 类型断言
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	// 取消和此键相关的定时任务
	// taskKey := genExpireTask(key)
	// timewheel.Cancel(taskKey)
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint32 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		db.versionMap.Put(key, versionCode+1)
	}
}

/* ---- RWLocks Functions ---- */

func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

/* ---- Expired Functions ---- */

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		db.Remove(key)
	}
	return expired
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		// logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

/* ---- Undo Functions ---- */

// GetUndoLogs return rollback commands
func (db *DB) GetUndoLogs(cmdLine [][]byte) []CmdLine {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, cmdLine[1:])
}

/* ---- Exec Functions ---- */

func (db DB) AfterClientClose(c redis.Connection) {

}
func (db DB) Close() {

}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}

// Flush clean database
// deprecated
// for test only
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}
