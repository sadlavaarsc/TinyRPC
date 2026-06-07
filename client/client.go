// Package client 提供 TinyRPC 的客户端实现。
// 支持同步调用、连接池管理、服务发现、负载均衡、熔断器及限流器的集成。
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"TinyRPC/circuitbreaker"
	"TinyRPC/codec"
	"TinyRPC/loadbalance"
	"TinyRPC/protocol"
	"TinyRPC/ratelimit"
	"TinyRPC/registry"
)

// Client 定义 RPC 客户端。
type Client struct {
	mu       sync.RWMutex
	registry registry.Registry
	balancer loadbalance.Balancer
	breakers map[string]*circuitbreaker.Breaker
	limiter  ratelimit.Limiter
	connPool *pool
	requestID uint64
}

// Option 客户端配置选项。
type Option func(*Client)

// WithRegistry 设置服务注册中心。
func WithRegistry(r registry.Registry) Option {
	return func(c *Client) {
		c.registry = r
	}
}

// WithBalancer 设置负载均衡器。
func WithBalancer(b loadbalance.Balancer) Option {
	return func(c *Client) {
		c.balancer = b
	}
}

// WithRateLimiter 设置客户端限流器。
func WithRateLimiter(l ratelimit.Limiter) Option {
	return func(c *Client) {
		c.limiter = l
	}
}

// NewClient 创建一个新的 RPC 客户端。
func NewClient(opts ...Option) *Client {
	c := &Client{
		breakers: make(map[string]*circuitbreaker.Breaker),
		connPool: newPool(10, 30*time.Second),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Call 发起同步 RPC 调用。
// service: 服务名, method: 方法名, args: 请求参数, reply: 响应接收指针（需为指针类型）。
func (c *Client) Call(ctx context.Context, service, method string, args, reply interface{}) error {
	if c.limiter != nil && !c.limiter.Allow() {
		return ratelimit.ErrRateLimited
	}

	// 获取服务实例
	instance, err := c.selectInstance(service, method)
	if err != nil {
		return fmt.Errorf("client: select instance failed: %w", err)
	}

	// 熔断器检查
	breaker := c.getBreaker(instance.ID)
	if err := breaker.Allow(); err != nil {
		return err
	}

	// 建立连接
	conn, err := c.connPool.get(instance.Host, instance.Port)
	if err != nil {
		breaker.RecordFailure()
		return fmt.Errorf("client: dial failed: %w", err)
	}
	defer c.connPool.put(conn)

	// 编码请求
	argsData, err := json.Marshal(args)
	if err != nil {
		breaker.RecordFailure()
		return fmt.Errorf("client: marshal args failed: %w", err)
	}

	req := protocol.NewRequest(service, method, argsData)
	body, err := protocol.EncodeRequest(req)
	if err != nil {
		breaker.RecordFailure()
		return err
	}

	reqID := atomic.AddUint64(&c.requestID, 1)
	msg := &codec.Message{
		Header: codec.Header{
			MsgType:   codec.MsgRequest,
			RequestID: reqID,
		},
		Service: service,
		Method:  method,
		Body:    body,
	}

	// 发送请求
	co := codec.NewBinaryCodec()
	if err := co.Encode(conn, msg); err != nil {
		breaker.RecordFailure()
		return fmt.Errorf("client: encode request failed: %w", err)
	}

	// 接收响应
	respMsg, err := co.Decode(conn)
	if err != nil {
		breaker.RecordFailure()
		return fmt.Errorf("client: decode response failed: %w", err)
	}

	rpcResp, err := protocol.DecodeResponse(respMsg.Body)
	if err != nil {
		breaker.RecordFailure()
		return err
	}

	if rpcResp.Error != "" {
		breaker.RecordFailure()
		return fmt.Errorf("client: rpc error: %s", rpcResp.Error)
	}

	if reply != nil {
		if err := json.Unmarshal(rpcResp.Result, reply); err != nil {
			breaker.RecordFailure()
			return fmt.Errorf("client: unmarshal reply failed: %w", err)
		}
	}

	breaker.RecordSuccess()
	return nil
}

// selectInstance 根据负载均衡策略选择一个服务实例。
func (c *Client) selectInstance(service, method string) (*registry.ServiceInstance, error) {
	if c.registry != nil && c.balancer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		instances, err := c.registry.Discover(ctx, service)
		if err != nil {
			return nil, err
		}
		c.balancer.UpdateInstances(instances)
		return c.balancer.Select(method)
	}
	return nil, fmt.Errorf("client: no registry or balancer configured")
}

// getBreaker 获取或创建一个熔断器（按实例 ID 隔离）。
func (c *Client) getBreaker(key string) *circuitbreaker.Breaker {
	c.mu.RLock()
	b, ok := c.breakers[key]
	c.mu.RUnlock()
	if ok {
		return b
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok = c.breakers[key]
	if ok {
		return b
	}
	b = circuitbreaker.NewBreaker(key, nil)
	c.breakers[key] = b
	return b
}

// pool 是一个简单的 TCP 连接池。
type pool struct {
	mu          sync.Mutex
	conns       map[string][]net.Conn
	maxIdle     int
	maxIdleTime time.Duration
}

func newPool(maxIdle int, maxIdleTime time.Duration) *pool {
	return &pool{
		conns:       make(map[string][]net.Conn),
		maxIdle:     maxIdle,
		maxIdleTime: maxIdleTime,
	}
}

func (p *pool) get(host string, port int) (net.Conn, error) {
	key := fmt.Sprintf("%s:%d", host, port)
	p.mu.Lock()
	if list, ok := p.conns[key]; ok && len(list) > 0 {
		conn := list[len(list)-1]
		p.conns[key] = list[:len(list)-1]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()
	return net.DialTimeout("tcp", key, 3*time.Second)
}

func (p *pool) put(conn net.Conn) {
	if conn == nil {
		return
	}
	key := conn.RemoteAddr().String()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns[key]) >= p.maxIdle {
		conn.Close()
		return
	}
	p.conns[key] = append(p.conns[key], conn)
}
