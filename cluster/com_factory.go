package cluster

import (
	"Godis/config"
	"Godis/datastruct/dict"
	"Godis/interface/redis"
	"Godis/lib/logger"
	"Godis/lib/pool"
	"Godis/lib/utils"
	"Godis/redis/client"
	"Godis/redis/parser"
	"Godis/redis/protocol"
	"errors"
	"fmt"
	"net"
)

// defaultClientFactory 结构体实现了 peerClientFactory 接口，用于管理连接池和创建客户端连接
type defaultClientFactory struct {
	nodeConnections dict.Dict // map[string]*pool.Pool
}

// connectionPoolConfig 是连接池的配置参数
var connectionPoolConfig = pool.Config{
	MaxIdle:   1,  // 最大空闲连接数
	MaxActive: 16, // 最大活跃连接数
}

// GetPeerClient 方法从连接池中获取与指定节点的客户端连接
func (factory *defaultClientFactory) GetPeerClient(peerAddr string) (peerClient, error) {
	var connectionPool *pool.Pool
	raw, ok := factory.nodeConnections.Get(peerAddr)
	if !ok {
		// 如果连接池中不存在该节点的连接，则创建一个新的连接，并加入连接池
		creator := func() (interface{}, error) {
			c, err := client.MakeClient(peerAddr)
			if err != nil {
				return nil, err
			}
			c.Start()
			// 集群中所有节点使用相同的密码进行认证
			if config.Properties.RequirePass != "" {
				authResp := c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
				if !protocol.IsOKReply(authResp) {
					return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
				}
			}
			return c, nil
		}
		finalizer := func(x interface{}) {
			logger.Debug("destroy client")
			cli, ok := x.(client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(creator, finalizer, connectionPoolConfig)
		factory.nodeConnections.Put(peerAddr, connectionPool)
	} else {
		connectionPool = raw.(*pool.Pool)
	}
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

// ReturnPeerClient 方法将客户端连接放回连接池
func (factory *defaultClientFactory) ReturnPeerClient(peer string, peerClient peerClient) error {
	raw, ok := factory.nodeConnections.Get(peer)
	if !ok {
		return errors.New("connection pool not found")
	}
	raw.(*pool.Pool).Put(peerClient)
	return nil
}

// tcpStream 结构体实现了 peerStream 接口，用于管理 TCP 连接和数据流
type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (s *tcpStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *tcpStream) Close() error {
	return s.conn.Close()
}

// NewStream 方法创建与指定节点的 TCP 连接，并返回一个用于发送和接收数据的流
func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// todo: reuse connection
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect with %s failed: %v", peerAddr, err)
	}
	ch := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-ch
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	if config.Properties.RequirePass != "" {
		authResp := send2node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   ch,
	}, nil
}

// newDefaultClientFactory 函数创建一个新的默认客户端工厂对象
func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent(1), // newDefaultClientFactory 函数创建一个新的默认客户端工厂对象
	}
}

func (factory *defaultClientFactory) Close() error {
	factory.nodeConnections.ForEach(func(key string, val interface{}) bool {
		val.(*pool.Pool).Close()
		return true
	})
	return nil
}
