package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
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
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 开启listener
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	err = listenAndServe(listener, handler, closeChan)
	if err != nil {
		return err
	}
	return nil
}

func listenAndServe(listener net.Listener, handler Handler, closeChan <-chan struct{}) error {
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
	// 这里作者的博客写了一个空的上下文，没看出来有什么作用
	// ctx := context.Background()
	// sync.WaitGroup可用于协程计数和等待一组协程完成，Add()方法增加计数，Done()方法减少计数，Wait()方法等待完成
	var waitDone sync.WaitGroup
}

func main() {
	listenAndServeWithSignal()
}
