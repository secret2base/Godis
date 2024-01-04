package parser

import (
	"Godis/interface/redis"
	"Godis/redis/protocol"
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

// Payload 结构体用于处理各种类型的消息Reply以及错误
type Payload struct {
	// 结构体包含接口类型的变量，该变量可以存储任何实现了该接口的类型的值
	Data redis.Reply
	Err  error
}

// ParseStream 创建一个流式处理消息的通道，不断通过reader和通道处理RESP消息
func ParseStream(reader io.Reader) <-chan *Payload {
	// 创建一个传递引用的通道
	ch := make(chan *Payload)
	go parser0(reader, ch)
	return ch
}

// ParseBytes 接收一个字节切片并处理其中多个RESP消息，返回其Reply的切片
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parser0(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// ParseOne 只处理一条RESP消息并返回Reply
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parser0(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Err
}

func parser0(rawReader io.Reader, ch chan<- *Payload) {
	// io.Reader是一个接口，包含Read方法
	// 将io.Reader转化为bufio.Reader类型，后者是一个带缓冲区的读取器，可以提高读取效率减少IO次数
	// 同时bufio.Reader对读取的控制更灵活
	reader := bufio.NewReader(rawReader)

	for {
		// 读取输入流中的字节，直到读取换行符为止
		// returning a slice containing the data up to and including the delimiter.
		line, err := reader.ReadBytes('\n')
		if err != nil {
			// 创建一个新的Payload对象，并将该对象的地址通过通道发送
			// 错误发送完后关闭通道
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		// length := len(line)
		// TrimSuffix 用于去除字节切片末尾的指定字符
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		// 对各种类型RESP的Reply，这里没有涉及，后面再补充
		switch line[0] {
		// 简单字符串，以'+'开头，以'\r\n'结尾，不允许换行
		case '+':
			// content := string(line[1:])
			ch <- &Payload{
				Data: protocol.MakeStatusReply(string(line[1:])),
			}
		case '-':
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(value),
			}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: protocol.MakeMultiBulkReply(args),
			}
		}
	}
}

func parseBulkString(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 一个字符串报文的例子： $3\r\nSET\r\n
	// 第一行为$加上字符串长度，第二行为字符串
	strLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	// 删去末尾的'\r\n'
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 一个数组报文的例子：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	// 读取数组中的元素数, header是第一行的信息
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		protocolError(ch, "illegal array header "+string(header[1:]))
	}
	// 创建一个空二维切片用于后面的RESP消息处理
	lines := make([][]byte, 0, nStrs)

	for i := int64(0); i < nStrs; i++ {
		var line []byte
		// 读取数组中第一个字符串的长度
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		// 读取第一个字符串,包含结尾的'\r\n'
		body := make([]byte, strLen+2)
		_, err = io.ReadFull(reader, body)
		if err != nil {
			return err
		}
		lines = append(lines, body[:len(body)-2])
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(lines),
	}
	return nil
}

// 将错误信息通过通道发回
func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
