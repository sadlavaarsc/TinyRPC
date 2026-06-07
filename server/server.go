// Package server 提供 TinyRPC 的服务端实现。
// 基于标准库 net 构建 TCP 服务端，支持服务注册、请求路由、并发处理及中间件扩展。
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"

	"TinyRPC/codec"
	"TinyRPC/protocol"
	"TinyRPC/ratelimit"
)

// ServiceDesc 描述一个 RPC 服务。
type ServiceDesc struct {
	// ServiceName 服务名
	ServiceName string
	// HandlerType 处理器接口类型，用于类型校验
	HandlerType interface{}
	// Methods 方法列表
	Methods []MethodDesc
}

// MethodDesc 描述一个 RPC 方法。
type MethodDesc struct {
	// MethodName 方法名
	MethodName string
	// Handler 方法处理器，签名与 gRPC 类似：func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor Interceptor) (interface{}, error)
	Handler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor Interceptor) (interface{}, error)
}

// Interceptor 定义服务端拦截器（中间件）。
type Interceptor func(ctx context.Context, req interface{}, handler Handler) (resp interface{}, err error)

// Handler 定义无拦截器时的处理方法。
type Handler func(ctx context.Context, req interface{}) (interface{}, error)

// Server 定义 RPC 服务端。
type Server struct {
	mu       sync.RWMutex
	services map[string]*serviceInfo
	codec    codec.Codec
	limiter  ratelimit.Limiter

	interceptor Interceptor
	listener    net.Listener
}

type serviceInfo struct {
	sd      *ServiceDesc
	srv     interface{}
	methods map[string]*MethodDesc
}

// ServerOption 服务端配置选项。
type ServerOption func(*Server)

// NewServer 创建一个新的 RPC 服务端。
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		services: make(map[string]*serviceInfo),
		codec:    codec.NewBinaryCodec(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithInterceptor 设置服务端拦截器。
func WithInterceptor(in Interceptor) ServerOption {
	return func(s *Server) {
		s.interceptor = in
	}
}

// WithRateLimiter 设置服务端限流器。
func WithRateLimiter(l ratelimit.Limiter) ServerOption {
	return func(s *Server) {
		s.limiter = l
	}
}

// RegisterService 注册一个服务到服务端。
// sd 为服务描述，srv 为实现该接口的具体实例。
func (s *Server) RegisterService(sd *ServiceDesc, srv interface{}) error {
	if sd == nil || srv == nil {
		return fmt.Errorf("server: nil service desc or handler")
	}

	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(srv)
	if !st.Implements(ht) {
		return fmt.Errorf("server: handler type %T does not implement %v", srv, ht)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.services[sd.ServiceName]; ok {
		return fmt.Errorf("server: service %s already registered", sd.ServiceName)
	}

	info := &serviceInfo{
		sd:      sd,
		srv:     srv,
		methods: make(map[string]*MethodDesc, len(sd.Methods)),
	}
	for i := range sd.Methods {
		m := &sd.Methods[i]
		info.methods[m.MethodName] = m
	}
	s.services[sd.ServiceName] = info
	return nil
}

// Serve 在指定地址启动 TCP 监听并处理连接。
func (s *Server) Serve(address string) error {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("server: listen failed: %w", err)
	}
	s.listener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		go s.handleConn(conn)
	}
}

// handleConn 处理单个 TCP 连接上的请求。
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		msg, err := s.codec.Decode(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			continue
		}

		if msg.Header.MsgType == codec.MsgHeartbeat {
			// 回复心跳
			resp := &codec.Message{
				Header: codec.Header{MsgType: codec.MsgHeartbeat},
			}
			_ = s.codec.Encode(conn, resp)
			continue
		}

		go s.processRequest(conn, msg)
	}
}

// processRequest 处理单个 RPC 请求并发送响应。
func (s *Server) processRequest(conn net.Conn, msg *codec.Message) {
	if s.limiter != nil && !s.limiter.Allow() {
		s.sendError(conn, msg.Header.RequestID, fmt.Errorf("rate limited"))
		return
	}

	s.mu.RLock()
	info, ok := s.services[msg.Service]
	s.mu.RUnlock()
	if !ok {
		s.sendError(conn, msg.Header.RequestID, fmt.Errorf("unknown service: %s", msg.Service))
		return
	}

	method, ok := info.methods[msg.Method]
	if !ok {
		s.sendError(conn, msg.Header.RequestID, fmt.Errorf("unknown method: %s.%s", msg.Service, msg.Method))
		return
	}

	req, err := protocol.DecodeRequest(msg.Body)
	if err != nil {
		s.sendError(conn, msg.Header.RequestID, err)
		return
	}

	ctx := context.Background()
	dec := func(v interface{}) error {
		return json.Unmarshal(req.Args, v)
	}

	var resp interface{}
	if s.interceptor != nil {
		resp, err = method.Handler(info.srv, ctx, dec, s.interceptor)
	} else {
		resp, err = method.Handler(info.srv, ctx, dec, nil)
	}

	if err != nil {
		s.sendError(conn, msg.Header.RequestID, err)
		return
	}

	result, err := json.Marshal(resp)
	if err != nil {
		s.sendError(conn, msg.Header.RequestID, err)
		return
	}

	rpcResp := protocol.NewResponse(msg.Header.RequestID, result, nil)
	body, err := protocol.EncodeResponse(rpcResp)
	if err != nil {
		s.sendError(conn, msg.Header.RequestID, err)
		return
	}

	respMsg := &codec.Message{
		Header: codec.Header{
			MsgType:   codec.MsgResponse,
			RequestID: msg.Header.RequestID,
		},
		Service: msg.Service,
		Method:  msg.Method,
		Body:    body,
	}
	_ = s.codec.Encode(conn, respMsg)
}

// sendError 向客户端发送错误响应。
func (s *Server) sendError(conn net.Conn, requestID uint64, err error) {
	rpcResp := protocol.NewResponse(requestID, nil, err)
	body, _ := protocol.EncodeResponse(rpcResp)
	msg := &codec.Message{
		Header: codec.Header{
			MsgType:   codec.MsgResponse,
			RequestID: requestID,
		},
		Body: body,
	}
	_ = s.codec.Encode(conn, msg)
}

// Stop 关闭服务端监听。
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
