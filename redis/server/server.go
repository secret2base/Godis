package server

import (
	"Godis/interface/database"
	"Godis/redis/connection"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Bool // Go在1.19版本引入atomic.Bool
}

func MakeHandler() *Handler {
	var db database.DB
	//if config.Properties.ClusterEnable {
	//	db = cluster.MakeCluster()
	//} else {
	//	db = database2.NewStandaloneServer()
	//}
	return &Handler{
		db: db,
	}
}

// closeClient 对Connection,db和activeConn分别关闭
func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Load() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	// 解析接收到的信息
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// 如果发生错误
		if payload.Err != nil {
			// 判断错误类型，如果读取结束或中止
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				// logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				// logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			// logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			// logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write([]byte("-ERR unknown\r\n"))
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	// logger.Info("handler shutting down...")
	h.closing.Store(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key any, val any) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
