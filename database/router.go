package database

import (
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"strings"
)

// cmdTable 是所有已注册的命令集合
var cmdTable = make(map[string]*command)

//// ExecFunc is interface for command executor
//// args don't include cmd line
//type ExecFunc func(db *DB, args [][]byte) redis.Reply
//
//// PreFunc analyses command line when queued command to `multi`
//// returns related write keys and read keys
//type PreFunc func(args [][]byte) ([]string, []string)
//
//// UndoFunc returns undo logs for the given command line
//// execute from head to tail when undo
//type UndoFunc func(db *DB, args [][]byte) []CmdLine

type command struct {
	// 命令的名称，例如'SET'
	name string
	// 用于执行此命令的函数，在database.go中有ExecFunc类型的定义
	// type ExecFunc func(db *DB, args [][]byte) redis.Reply
	executor ExecFunc
	// PreFunc同样在database.go中定义，将命令加入事务队列前，用于分析命令行返回写入和读取键
	// prepare returns related keys command
	prepare PreFunc
	// undo generates undo-log before command actually executed, in case the command needs to be rolled back
	undo UndoFunc
	// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
	// for example: the arity of `get` is 2, `mget` is -2
	arity int
	// 标志位，用于标志命令的属性
	flags int
	// 用于储存额外的命令信息
	extra *commandExtra
}

type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}

const flagWrite = 0

const (
	flagReadOnly = 1 << iota
	flagSpecial  // command invoked in Exec
)

// registerCommand registers a normal command, which only read or modify a limited number of keys
func registerCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}

// registerSpecialCommand registers a special command, such as publish, select, keys, flushAll
func registerSpecialCommand(name string, arity int, flags int) *command {
	name = strings.ToLower(name)
	flags |= flagSpecial
	cmd := &command{
		name:  name,
		arity: arity,
		flags: flags,
	}
	cmdTable[name] = cmd
	return cmd
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagReadOnly > 0
}

func (cmd *command) toDescReply() redis.Reply {
	args := make([]redis.Reply, 0, 6)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(cmd.extra.firstKey)),
			protocol.MakeIntReply(int64(cmd.extra.lastKey)),
			protocol.MakeIntReply(int64(cmd.extra.keyStep)),
		)
	}
	return protocol.MakeMultiRawReply(args)
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}
