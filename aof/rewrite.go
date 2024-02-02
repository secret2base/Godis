package aof

import "os"

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// newRewriteHandler 生成一个RewriteCtx实例
func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}
