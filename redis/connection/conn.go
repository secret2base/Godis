package connection

import (
	"Godis/wait"
	"net"
	"sync"
	"time"
)

// 对connection接口的实现，每一个connection代表数据库一个客户端之间的连接

type Connection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	// lock while server sending response
	mu    sync.Mutex
	flags uint64

	// subscribing channels
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// selected db
	selectedDB int
}

func MakeConn(conn net.Conn) *Connection {
	// 查询缓存池中是否存在，若不存在则新建一个
	c, ok := connPool.Get().(*Connection)
	if !ok {
		// logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

// 项目中频繁的创建对象和回收内存，造成了GC的压力；
// 而sync.pool可以缓存对象暂时不用但是之后会用到的对象，并且不需要重新分配内存；
// 这在很大程度上降低了GC的压力，并且提高了程序的性能

var connPool = sync.Pool{
	New: func() any {
		return &Connection{}
	},
}

// Write sends response to client over tcp connection
func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	//defer func() {
	//	c.sendingData.Done()
	//}()
	defer c.sendingData.Done()
	return c.conn.Write(bytes)
}

func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	// 将这个Connection放入缓存池中，避免频繁的创建
	connPool.Put(c)
	return nil
}

func (c *Connection) RemoteAddr() string {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SetPassword(s string) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetPassword() string {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Subscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) UnSubscribe(channel string) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SubsCount() int {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetChannels() []string {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) InMultiState() bool {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SetMultiState(b bool) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetQueuedCmdLine() [][][]byte {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) EnqueueCmd(i [][]byte) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) ClearQueuedCmds() {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetWatching() map[string]uint32 {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) AddTxError(err error) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetTxErrors() []error {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) GetDBIndex() int {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SelectDB(i int) {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SetSlave() {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) IsSlave() bool {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) SetMaster() {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) IsMaster() bool {
	//TODO implement me
	panic("implement me")
}

func (c *Connection) Name() string {
	//TODO implement me
	panic("implement me")
}
