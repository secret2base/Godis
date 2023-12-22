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
