// Package codec 提供 TinyRPC 的编解码能力。
// 基于自定义二进制协议实现，模拟 protobuf 的紧凑序列化行为，
// 包含消息头的长度字段、魔数校验、消息类型标识等。
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// MagicNumber 协议魔数，用于快速识别 TinyRPC 数据包
	MagicNumber uint16 = 0x4854 // 'HT'

	// HeaderSize 固定消息头长度（字节）
	// 结构: Magic(2) | Version(1) | MsgType(1) | RequestID(8) | ServiceLen(2) | MethodLen(2) | BodyLen(4) = 20
	HeaderSize = 20

	// VersionCurrent 当前协议版本号
	VersionCurrent uint8 = 0x01
)

// MsgType 定义消息类型
type MsgType uint8

const (
	// MsgRequest 客户端请求
	MsgRequest MsgType = 0x01
	// MsgResponse 服务端响应
	MsgResponse MsgType = 0x02
	// MsgHeartbeat 心跳包
	MsgHeartbeat MsgType = 0x03
)

// Header 定义消息头结构
type Header struct {
	Magic       uint16  // 魔数
	Version     uint8   // 协议版本
	MsgType     MsgType // 消息类型
	RequestID   uint64  // 请求唯一标识，用于请求-响应映射
	ServiceLen  uint16  // 服务名长度
	MethodLen   uint16  // 方法名长度
	BodyLen     uint32  // 消息体长度
}

// Message 定义完整 RPC 消息
type Message struct {
	Header  Header // 消息头
	Service string // 服务名
	Method  string // 方法名
	Body    []byte // 消息体（模拟 protobuf 序列化后的 payload）
}

// Codec 定义编解码器接口
type Codec interface {
	// Encode 将 Message 编码为字节流，写入 w
	Encode(w io.Writer, msg *Message) error
	// Decode 从 r 解码出 Message
	Decode(r io.Reader) (*Message, error)
}

// BinaryCodec 基于标准库 binary 实现的编解码器
type BinaryCodec struct{}

// NewBinaryCodec 创建一个新的 BinaryCodec 实例
func NewBinaryCodec() Codec {
	return &BinaryCodec{}
}

// Encode 将 Message 编码为二进制字节流并写入 w。
// 编码顺序：Header（定长） -> Service -> Method -> Body
func (c *BinaryCodec) Encode(w io.Writer, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("codec: nil message")
	}

	msg.Header.Magic = MagicNumber
	msg.Header.Version = VersionCurrent
	msg.Header.ServiceLen = uint16(len(msg.Service))
	msg.Header.MethodLen = uint16(len(msg.Method))
	msg.Header.BodyLen = uint32(len(msg.Body))

	buf := bytes.NewBuffer(make([]byte, 0, HeaderSize+len(msg.Service)+len(msg.Method)+len(msg.Body)))

	// 写入 Header
	if err := binary.Write(buf, binary.BigEndian, msg.Header.Magic); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.Version); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.MsgType); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.RequestID); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.ServiceLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.MethodLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Header.BodyLen); err != nil {
		return err
	}

	// 写入 Service、Method、Body
	if _, err := buf.WriteString(msg.Service); err != nil {
		return err
	}
	if _, err := buf.WriteString(msg.Method); err != nil {
		return err
	}
	if _, err := buf.Write(msg.Body); err != nil {
		return err
	}

	_, err := w.Write(buf.Bytes())
	return err
}

// Decode 从 r 中解码出一个完整的 Message。
// 先读取固定长度的 Header，再根据长度字段读取变长部分。
func (c *BinaryCodec) Decode(r io.Reader) (*Message, error) {
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, fmt.Errorf("codec: read header failed: %w", err)
	}

	msg := &Message{}
	br := bytes.NewReader(headerBuf)

	if err := binary.Read(br, binary.BigEndian, &msg.Header.Magic); err != nil {
		return nil, err
	}
	if msg.Header.Magic != MagicNumber {
		return nil, fmt.Errorf("codec: invalid magic number 0x%04X", msg.Header.Magic)
	}

	if err := binary.Read(br, binary.BigEndian, &msg.Header.Version); err != nil {
		return nil, err
	}
	if msg.Header.Version != VersionCurrent {
		return nil, fmt.Errorf("codec: unsupported version %d", msg.Header.Version)
	}

	if err := binary.Read(br, binary.BigEndian, &msg.Header.MsgType); err != nil {
		return nil, err
	}
	if err := binary.Read(br, binary.BigEndian, &msg.Header.RequestID); err != nil {
		return nil, err
	}
	if err := binary.Read(br, binary.BigEndian, &msg.Header.ServiceLen); err != nil {
		return nil, err
	}
	if err := binary.Read(br, binary.BigEndian, &msg.Header.MethodLen); err != nil {
		return nil, err
	}
	if err := binary.Read(br, binary.BigEndian, &msg.Header.BodyLen); err != nil {
		return nil, err
	}

	// 读取变长部分
	totalVarLen := int(msg.Header.ServiceLen) + int(msg.Header.MethodLen) + int(msg.Header.BodyLen)
	if totalVarLen > 0 {
		varBuf := make([]byte, totalVarLen)
		if _, err := io.ReadFull(r, varBuf); err != nil {
			return nil, fmt.Errorf("codec: read variable payload failed: %w", err)
		}
		msg.Service = string(varBuf[:msg.Header.ServiceLen])
		msg.Method = string(varBuf[msg.Header.ServiceLen : msg.Header.ServiceLen+msg.Header.MethodLen])
		msg.Body = varBuf[msg.Header.ServiceLen+msg.Header.MethodLen:]
	}

	return msg, nil
}
