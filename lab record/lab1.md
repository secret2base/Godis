### Golang实现Redis服务器（1）：Golang编写TCP服务器
#### 1.编写基本的TCP服务器
基本功能的实现包含两个部分，分别是TCP连接的监听和建立，以及建立连接后的Handler处理，因此定义如下两个函数
```go
/*
    Function: listenAndServe
    Description: 监听并建立TCP连接，调用Handler函数
    Input: 
        @address： TCP服务器所采用的的端口地址
 */
func listenAndServe(address string) {}
/*
   Function: Handler
   Description: 处理TCP连接的请求
   Input:
       @conn： 已建立TCP连接
*/
func Handler(conn net.Conn) {}
```
具体实现在./lab1/tcp server.go

#### 2.实现优雅关闭
在TCP服务器关闭之前需要执行必要的清理工作，包括正在完成的数据传输，关闭TCP连接等，可以避免资源泄露以及客户端未收到完整数据造成的故障
所以需要一个新的函数来监听关闭信号并执行相应操作
```go
// 原有的listenAndServe函数增加一个通过通道处理关闭信号，并关闭服务器的功能
func listenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {}
func listenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {}

```

### 附录
#### tcp example
```go
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func listenAndServe(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(fmt.Sprintf("listen err: %s", err))
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(fmt.Sprintf("accept err: %s", err))
		}
		go Handler(conn)
	}
}

func Handler(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			//log.Fatal(fmt.Sprintf("read err: %s", err))
			log.Println(err)
			return
		}
		b := []byte(msg)
		conn.Write(b)
	}
}

func main() {
	listenAndServe(":8888")
}

```

#### graceful shutdown
```go
package main

import (
   "Godis/wait"
   "bufio"
   "io"
   "log"
   "net"
   "os"
   "os/signal"
   "sync"
   "sync/atomic"
   "syscall"
   "time"
)

type Handler interface {
   Handle(conn net.Conn)
   Close() error
}

/*
函数名：listenAndServeWithSignal
功能：监听关闭信号，调用listenAndServe函数实现监听
输入：端口号，handler
*/
func listenAndServeWithSignal(address string, handler Handler) error {
   // 创建两个通道，closeChan用于将关闭服务器的通知传递给listenAndServe, sigChan用于监听系统关闭通知
   // 当程序需要在不同的 goroutine 之间进行通信，但又不需要传递具体的数据时，使用空结构体的通道是一种有效的方式。
   closeChan := make(chan struct{})
   sigChan := make(chan os.Signal)
   // SIGUP 终端挂起或控制进程终止; SIGQUIT 退出进程，并生成 core 文件; SIGTERM 终止进程;SIGINT 中断进程;
   signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
   // 创建一个匿名函数的协程，用于监听系统信号，若系统信号为上述四种信号，则通知服务器关闭
   // 既然signal.Notify()函数中已经指定了监听的信号，下面的case判断是否必要呢
   go func() {
      sig := <-sigChan
      switch sig {
      case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
         closeChan <- struct{}{}
      }
   }()
   // 开启listener
   listener, err := net.Listen("tcp", address)
   if err != nil {
      log.Println("Listen start error")
      log.Println(err)
      return err
   }
   listenAndServe(listener, handler, closeChan)
   return nil
}

func listenAndServe(listener net.Listener, handler Handler, closeChan <-chan struct{}) {
   // 监听关闭通知，执行关闭函数
   go func() {
      <-closeChan
      log.Println("shutting down......")
      // 先关闭listener阻止新连接的建立，再逐个关闭所有已建立的连接
      _ = listener.Close()
      _ = handler.Close()
   }()

   // 意外中断释放资源
   defer func() {
      _ = listener.Close()
      _ = handler.Close()
   }()
   // 这里作者的博客写了一个空的上下文，没看出来有什么作用，后面有用再补上
   // ctx := context.Background()
   // sync.WaitGroup可用于协程计数和等待一组协程完成，Add()方法增加计数，Done()方法减少计数，Wait()方法等待完成
   // 这里用于统计存活的连接数
   var waitDone sync.WaitGroup
   for {
      conn, err := listener.Accept()
      if err != nil {
         break
      }
      log.Println("new connection")
      waitDone.Add(1)
      // 开启一个新的协程处理新连接
      go func() {
         // 在该协程结束时对计数器减一
         defer func() {
            waitDone.Done()
         }()
         //调用handler接口中的Handle方法处理连接
         handler.Handle(conn)
      }()
   }
   // 主程序等待所有协程执行完毕
   waitDone.Wait()
}

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
func makeEchoHandler() *EchoHandler {
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
      client.Close()
      return true
   })
   return nil
}

func main() {
   echo := makeEchoHandler()
   err := listenAndServeWithSignal(":8888", echo)
   if err != nil {
      return
   }
}

```

#### 1. fmt的各种输出函数
```go
fmt.Print("Hello, ", "World!")
// Output: Hello, World!
// 用于将参数输出到标准输出，不添加换行符。
// 可以接受零个或多个参数，并在输出它们的同时使用空格进行分隔。

fmt.Println("Hello, World!")
// Output: Hello, World!
// 用于将参数输出到标准输出，并在结尾添加换行符 \n。
// 可以接受零个或多个参数，并在输出它们的同时使用空格进行分隔。

name := "Alice"
age := 30
fmt.Printf("Name: %s, Age: %d\n", name, age)
// Output: Name: Alice, Age: 30
// 用于格式化输出，支持格式化字符串（类似于 C 语言中的 printf）。
// 接受一个格式化字符串作为第一个参数，后续参数根据格式化字符串进行输出。

name := "Bob"
age := 25
result := fmt.Sprintf("Name: %s, Age: %d", name, age)
fmt.Println(result)
// Output: Name: Bob, Age: 25
// 用于将格式化的字符串返回为一个新的字符串，而不是直接输出。
// 类似于 fmt.Printf，但返回一个字符串而不是将其输出到标准输出。
```
在Go语言中，`fmt.Printf` 和相关的格式化输出函数使用格式化字符串中的占位符来指定输出的格式。以下是一些常用的格式化占位符：

1. **通用占位符：**
    - `%v`: 值的默认格式。
    - `%+v`: 在结构体中，会添加字段名。
    - `%#v`: Go 语法表示的值。
    - `%T`: 变量的类型。
    - `%t`: `true` 或 `false`。

2. **整数占位符：**
    - `%d`: 十进制整数。
    - `%b`: 二进制整数。
    - `%o`: 八进制整数。
    - `%x`, `%X`: 十六进制整数，字母大小写表示。

3. **浮点数占位符：**
    - `%f`: 默认的浮点数格式。
    - `%e`, `%E`: 科学计数法表示。
    - `%g`, `%G`: 根据值的大小采用 `%f` 或 `%e`。

4. **字符串和字符占位符：**
    - `%s`: 字符串。
    - `%q`: 带双引号的字符串。
    - `%c`: 字符（按照 Unicode 输出）。

5. **指针占位符：**
    - `%p`: 指针的十六进制表示。

6. **宽度和精度：**
    - `%[width]s`: 指定宽度的字符串输出。
    - `%.[precision]f`: 指定浮点数的小数位数。

7. **其他占位符：**
    - `%%`: 百分号。

#### 2. log的各种输出函数

1. **Print:**
   ```go
   log.Print("This is a log message")
   ```

2. **Printf:**
   ```go
   value := 42
   log.Printf("The answer is %d", value)
   ```

3. **Println:**
   ```go
   log.Println("This is a log message with a newline")
   ```

4. **Fatal:**
   ```go
   log.Fatal("Fatal error occurred")
   ```

5. **Fatalf:**
   ```go
   errorCode := 500
   log.Fatalf("Fatal error with status code: %d", errorCode)
   ```

6. **Fatalln:**
   ```go
   log.Fatalln("Fatal error with a newline")
   ```

7. **Panic:**
   ```go
   log.Panic("Panic! Something went wrong.")
   ```

8. **Panicf:**
   ```go
   errorMessage := "Critical error"
   log.Panicf("Panic: %s", errorMessage)
   ```

9. **Panicln:**
   ```go
   log.Panicln("Panic with a newline")
   ```

注意：os.Exit(),log.Fatal(),log.Panic()的区别
ox.Exit()是直接退出，log.Fatal()打印内容后调用os.Exit()后退出，二者均不执行defer的内容  
log.Panic()停止当前 goroutine 的正常执行，任何被 F 推迟执行的函数都以通常的方式运行，然后 F 返回给它的调用者
