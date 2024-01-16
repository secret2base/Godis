package connection

import (
	"Godis/wait"
	"net"
	"sync"
	"time"
)

// 对connection接口的实现，每一个connection代表数据库一个客户端之间的连接

const (
	// flagSlave means this a connection with slave
	flagSlave = uint64(1 << iota)
	// flagSlave means this a connection with master
	flagMaster
	// flagMulti means this connection is within a transaction
	flagMulti
)

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

// Close disconnect with the client
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

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	// 使用锁确保对subscriber的并发安全访问
	c.mu.Lock()
	defer c.mu.Unlock()
	// 如果Subscribe为空则新建
	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// len(nil) == 0
	if len(c.subs) == 0 {
		return
	}

	delete(c.subs, channel)
}

func (c *Connection) SubsCount() int {
	return len(c.subs)
}

func (c *Connection) GetChannels() []string {
	if len(c.subs) == 0 {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database 用户可以独立选择自己的数据库
func (c *Connection) SelectDB(index int) {
	c.selectedDB = index
}

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}
