// Package protocol 定义 TinyRPC 的通信协议核心结构。
// 包含消息类型常量、消息体封装以及请求-响应上下文。
package protocol

import (
	"encoding/json"
	"fmt"
)

// MessageType 标识 RPC 消息的类型
type MessageType byte

const (
	// TypeRequest 客户端发起的调用请求
	TypeRequest MessageType = iota + 1
	// TypeResponse 服务端返回的调用结果
	TypeResponse
	// TypeHeartbeat 用于服务保活的心跳消息
	TypeHeartbeat
)

// Request 封装 RPC 请求体，模拟 protobuf 生成的结构
type Request struct {
	// Service 目标服务名，如 "HelloService"
	Service string `json:"service"`
	// Method 目标方法名，如 "SayHello"
	Method string `json:"method"`
	// Args 调用参数，使用 JSON 模拟 protobuf 的序列化行为
	Args []byte `json:"args"`
	// Metadata 透传元数据，如链路追踪 ID、鉴权 Token 等
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Response 封装 RPC 响应体
type Response struct {
	// RequestID 对应请求的 ID，用于异步映射
	RequestID uint64 `json:"request_id"`
	// Error 错误信息，空字符串表示成功
	Error string `json:"error,omitempty"`
	// Result 调用结果，使用 JSON 模拟 protobuf 的序列化行为
	Result []byte `json:"result,omitempty"`
}

// EncodeRequest 将 Request 编码为字节数组（使用 JSON 模拟 protobuf）
func EncodeRequest(req *Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("protocol: nil request")
	}
	return json.Marshal(req)
}

// DecodeRequest 将字节数组解码为 Request
func DecodeRequest(data []byte) (*Request, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("protocol: empty request data")
	}
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("protocol: decode request failed: %w", err)
	}
	return &req, nil
}

// EncodeResponse 将 Response 编码为字节数组
func EncodeResponse(resp *Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("protocol: nil response")
	}
	return json.Marshal(resp)
}

// DecodeResponse 将字节数组解码为 Response
func DecodeResponse(data []byte) (*Response, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("protocol: empty response data")
	}
	var resp Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("protocol: decode response failed: %w", err)
	}
	return &resp, nil
}

// NewRequest 快速创建一个 Request 实例
func NewRequest(service, method string, args []byte) *Request {
	return &Request{
		Service:  service,
		Method:   method,
		Args:     args,
		Metadata: make(map[string]string),
	}
}

// NewResponse 快速创建一个 Response 实例
func NewResponse(requestID uint64, result []byte, err error) *Response {
	r := &Response{
		RequestID: requestID,
		Result:    result,
	}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}
