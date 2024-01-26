package aof

import (
	"Godis/interface/database"
	"Godis/lib/logger"
	"Godis/redis/connection"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type CmdLine = [][]byte

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
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
	// ?
	currentDB int
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

}

// RemoveListener removers a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {

}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {

}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {

}

// LoadAof read aof file, can only be used before Persister.listenCmd started
// maxBytes用于限制最大读取量，为0时无限制
func (persister *Persister) LoadAof(maxBytes int) {
	// 避免在加载AOF期间继续有消息进入
	// aofChan := persister.aofChan
	persister.aofChan = nil

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

}

// Close gracefully stops aof persistence procedure
func (persister *Persister) Close() {

}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {

}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {

}
