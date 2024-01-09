package protocol

// 此文件下定义RESP报文的几种ErrorType及其回复
// UnknownErr,ArgNumErr,SyntaxError,WrongTypeError,ProtocolErr

// 定义UnknownErrReply并实现ErrorReply接口

// UnknownErrReply 只返回固定的错误信息，因此本身可以为空结构体
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

// 实现Error Reply接口

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func (r *UnknownErrReply) Error() string {
	return "Error unknown"
}

// 定义ArgNumErrReply并实现ErrorReply接口

// ArgNumErrReply represents wrong number of arguments for command
type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + r.Cmd + "' command\r\n")
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + r.Cmd + "' command"
}

// MakeArgNumErrReply represents **wrong number of arguments** for command
func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		cmd,
	}
}

// 定义SyntaxErrReply并实现ErrorReply接口

type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

// WrongTypeErrReply represents operation against a key holding the wrong kind of value
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// ProtocolErrReply represents meeting unexpected byte during parse requests
type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error '" + r.Msg + "' command"
}

func MakeProtocolErrReply(Msg string) *ProtocolErrReply {
	return &ProtocolErrReply{
		Msg,
	}
}
