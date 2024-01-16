package protocol

import (
	"Godis/interface/redis"
	"bytes"
	"strconv"
)

var (

	// CRLF is the line separator of redis serialization protocol
	CRLF = "\r\n"
)

/* ---- Status Reply ---- */
type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ---- Error Reply ---- */
// 对于错误类型，定义了单独的Error()方法，可能是为了对错误提供足够的信息（在后面的./redis/protocol/error.go中定义有多种错误类型及其回复）

type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

type StandardErrorReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrorReply {
	return &StandardErrorReply{
		Status: status,
	}
}

// 这里的error返回信息为r.Status在ToBytes中也有，为什么单独列出呢
func (r *StandardErrorReply) Error() string {
	return r.Status
}

func (r *StandardErrorReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

/* ---- Bulk Reply ---- */
type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Multi Bulk Reply ---- */

type MultiBulkReply struct {
	Args [][]byte
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

func (r *MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Int Reply ---- */
type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Ok Reply ---- */
type OkReply struct{}

func (o OkReply) ToBytes() []byte {
	return []byte("+Ok\r\n")
}

func MakeOkReply() *OkReply {
	return &OkReply{}
}

/* ---- Multi Raw Reply ---- */

// MultiRawReply store complex list structure, for example GeoPos command
type MultiRawReply struct {
	Replies []redis.Reply
}

// MakeMultiRawReply creates MultiRawReply
func MakeMultiRawReply(replies []redis.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Queued Reply ---- */
// 原作者将其放入godis/redis/protocol/consts.go

type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// ToBytes marshal redis.Reply
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

var theQueuedReply = new(QueuedReply)

// MakeQueuedReply returns a QUEUED protocol
func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}

/* ---- Multi Empty Multi Bulk Reply ---- */

var emptyMultiBulkBytes = []byte("*0\r\n")

// EmptyMultiBulkReply is an empty list
type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// IsErrorReply returns true if the given protocol is error
func IsErrorReply(reply redis.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
