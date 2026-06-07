# TinyRPC

TinyRPC 是一个轻量级、教学友好的 Go 语言 RPC 框架。它从零开始构建，涵盖自定义二进制协议、TCP 通信、服务注册与发现、负载均衡、熔断器、限流器等微服务核心能力，适合作为学习 RPC 原理与分布式系统基础设施的参考实现。

## 特性

- **自定义二进制协议**：基于魔数 + 长度字段的消息头设计，支持请求、响应、心跳三种消息类型。
- **服务注册与发现**：内置基于 Etcd 的注册中心实现，支持 Lease 心跳保活与节点变更监听。
- **多种负载均衡策略**：随机（Random）、轮询（RoundRobin）、加权一致性哈希（ConsistentHash）。
- **熔断器**：基于滑动窗口计数器的 CLOSED / OPEN / HALF_OPEN 三态状态机，防止级联故障。
- **限流器**：Token Bucket 令牌桶算法，支持固定速率填充与突发流量控制。
- **连接池管理**：客户端内置简易 TCP 连接池，减少重复建连开销。
- **服务端拦截器**：支持中间件扩展，可方便地接入日志、监控、鉴权等能力。

## 架构

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Client    │<---->|   Codec     |<---->|   Server    |
│  (连接池)    |      | (二进制协议)  |      | (服务注册表) |
└──────┬──────┘      └─────────────┘      └──────┬──────┘
       |                                          |
       v                                          v
┌─────────────┐                          ┌─────────────┐
| LoadBalance |                          |  Registry   |
|CircuitBreaker|                         |   (Etcd)    |
|  RateLimit  |                          └─────────────┘
└─────────────┘
```

## 快速开始

### 1. 启动服务端

```go
package main

import (
    "context"
    "fmt"
    "log"

    "TinyRPC/ratelimit"
    "TinyRPC/server"
)

type HelloService interface {
    SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error)
}

type HelloRequest struct { Name string `json:"name"` }
type HelloResponse struct { Message string `json:"message"` }

type helloServiceImpl struct{}

func (s *helloServiceImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
    return &HelloResponse{Message: fmt.Sprintf("Hello, %s!", req.Name)}, nil
}

func main() {
    limiter := ratelimit.NewTokenBucket(200, 100)
    srv := server.NewServer(
        server.WithRateLimiter(limiter),
    )

    sd := &server.ServiceDesc{
        ServiceName: "HelloService",
        HandlerType: (*HelloService)(nil),
        Methods: []server.MethodDesc{{
            MethodName: "SayHello",
            Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor server.Interceptor) (interface{}, error) {
                in := new(HelloRequest)
                if err := dec(in); err != nil { return nil, err }
                return srv.(HelloService).SayHello(ctx, in)
            },
        }},
    }
    srv.RegisterService(sd, &helloServiceImpl{})
    log.Fatal(srv.Serve(":8888"))
}
```

### 2. 启动客户端

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "TinyRPC/client"
    "TinyRPC/loadbalance"
    "TinyRPC/registry"
)

type HelloRequest struct { Name string `json:"name"` }
type HelloResponse struct { Message string `json:"message"` }

func main() {
    reg, _ := registry.NewEtcdRegistry([]string{"localhost:2379"})
    c := client.NewClient(
        client.WithRegistry(reg),
        client.WithBalancer(loadbalance.NewRandomBalancer()),
    )

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    req := &HelloRequest{Name: "TinyRPC"}
    resp := new(HelloResponse)
    if err := c.Call(ctx, "HelloService", "SayHello", req, resp); err != nil {
        log.Fatal(err)
    }
    fmt.Println(resp.Message)
}
```

## 目录结构

```
TinyRPC/
├── codec/              # 自定义二进制编解码
├── protocol/           # RPC 消息结构（Request / Response）
├── registry/           # Etcd 服务注册与发现
├── loadbalance/        # 负载均衡（随机、轮询、一致性哈希）
├── circuitbreaker/     # 熔断器（滑动窗口 + 三态状态机）
├── ratelimit/          # 限流器（Token Bucket）
├── server/             # TCP 服务端、服务注册表、反射调用
├── client/             # 客户端、连接池、集成 LB / 熔断 / 限流
└── demo/               # 服务端与客户端示例
```

## 许可证

MIT License
