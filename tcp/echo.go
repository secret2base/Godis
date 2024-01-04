package tcp

import (
	"Godis/wait"
	"bufio"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// 下面实现handler接口中的Handle方法
// 首先，定义一个EchoHandler的结构体
type EchoHandler struct {
	// 所有的存活连接
	activeConn sync.Map
	// 服务器的状态
	// 原博客中的atomic.AtomicBool似乎不存在
	closing atomic.Bool
}

// 为了实现有等待时间的连接，定义如下Client结构体抽象连接
// 结构体内包含一个net.conn类型和一个等待时间wait
type Client struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// 用于创建EchoHandler实例的工厂函数
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// Handle 对于结构体EchoHandler实现Handle接口中的方法
func (h *EchoHandler) Handle(conn net.Conn) {
	// 如果服务器的状态变成关闭了，Handle不再处理新进连接
	if h.closing.Load() == true {
		err := conn.Close()
		if err != nil {
			log.Println("Close error")
		}
		return
	}
	// 实例化一个Client
	client := &Client{
		Conn: conn,
	}
	// 服务器的抽象结构体EchoHandler记录当前所有存活的连接
	h.activeConn.Store(client, struct{}{})

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("connection close")
				// 从服务器上删除此连接
				h.activeConn.Delete(client)
			} else {
				log.Println(err)
				h.activeConn.Delete(client)
				return
			}
		}
		// 发送数据之前置为waiting状态，避免数据发送到一半被关闭造成的错误
		client.Waiting.Add(1)

		b := []byte(msg)
		_, err = conn.Write(b)
		if err != nil {
			log.Println("Send error")
			client.Waiting.Done()
			return
		}
		client.Waiting.Done()
	}
}

// 实现Client和EchoHandler的关闭函数
func (c *Client) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	err := c.Conn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (h *EchoHandler) Close() error {
	log.Println("echoHandler shutting down......")
	h.closing.Swap(true)
	// 遍历存活的连接并关闭
	h.activeConn.Range(func(key any, val any) bool {
		client := key.(*Client)
		err := client.Close()
		if err != nil {
			return false
		}
		return true
	})
	return nil
}
