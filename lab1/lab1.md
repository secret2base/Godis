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
#### tcp example.go
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

#### graceful shutdown.go
```go

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
