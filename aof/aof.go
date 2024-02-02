package aof

import (
	"Godis/config"
	"Godis/interface/database"
	"Godis/lib/logger"
	"Godis/lib/utils"
	"Godis/redis/connection"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CmdLine = [][]byte

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

const (
	aofQueueSize = 1 << 20
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx    context.Context
	cancel context.CancelFunc
	// ?
	tmpDBMaker func() database.DBEngine
	db         database.DBEngine
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFilename is the path of aof file
	aofFilename string
	// aofFsync is the strategy of fsync
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	// ?
	currentDB int
	listeners map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewPersister creates a new aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.db = db
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.tmpDBMaker = tmpDBMaker
	// ?
	persister.currentDB = 0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	// Create a context for AOF operations with cancellation support.
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener removers a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}
	// aofFsync的策略决定了是否每条Cmd都要同步
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // reuse underlying array 重置buffer
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	// 如果数据库不一致增加SELECT命令
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	// 通知AOF的监听器
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		// Sync commits the current contents of the file to stable storage.
		_ = persister.aofFile.Sync()
	}
}

// LoadAof read aof file, can only be used before Persister.listenCmd started
// maxBytes用于限制最大读取量，为0时无限制
func (persister *Persister) LoadAof(maxBytes int) {
	// 避免在加载AOF期间继续有消息进入
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logger.Warn(err)
		}
	}(file)

	// load rdb preamble if needed
	//decoder := rdb.NewDecoder(file)
	//err = persister.db.LoadRDB(decoder)
	//if err != nil {
	//	// no rdb preamble
	//	file.Seek(0, io.SeekStart)
	//} else {
	//	// has rdb preamble
	//	_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
	//	maxBytes = maxBytes - decoder.GetReadCount()
	//}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn()
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 为什么是MultiBulkReply [][]byte
		r, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// Fsync flushes aof file to disk
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	persister.cancel()
}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	// AOF文件的地址
	tmpFile := ctx.tmpFile
	// 生成一个新的Rewrite Handler
	tmpAof := persister.newRewriteHandler()
	// 从persister.aofFilename读入AOF文件
	persister.LoadAof(int(ctx.fileSize))

	// 遍历所有的数据库
	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// 遍历数据库中的所有键值对，并写入AOF
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
