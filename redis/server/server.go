package server

import (
	"Godis/cluster"
	"Godis/config"
	database2 "Godis/database"
	"Godis/interface/database"
	"Godis/lib/logger"
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

// MakeHandler 根据是否开启集群模式决定Handler的初始化
func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster()
	} else {
		db = database2.NewStandaloneServer()
	}
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
	/* ---- 判断连接状态并封装 ---- */
	if h.closing.Load() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}
	// 根据传入的conn连接，将其封装为Connection以符合数据库所需要的功能
	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	/* ---- 解析信息并判断是否错误 ---- */
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
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// 发送协议错误回复给客户端
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
			logger.Error("empty payload")
			continue
		}

		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		/* ---- 执行收到的命令 ---- */
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
	h.activeConn.Range(func(key any, val any) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
