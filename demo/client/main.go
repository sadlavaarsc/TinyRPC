// demo/client 是 TinyRPC 的客户端示例。
// 演示如何创建客户端、集成负载均衡与熔断，并发起同步 RPC 调用。
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

// HelloRequest 请求结构，需与服务端保持一致。
type HelloRequest struct {
	Name string `json:"name"`
}

// HelloResponse 响应结构，需与服务端保持一致。
type HelloResponse struct {
	Message string `json:"message"`
}

func main() {
	// 创建注册中心（示例使用 Etcd）
	reg, err := registry.NewEtcdRegistry([]string{"localhost:2379"})
	if err != nil {
		log.Printf("etcd registry not available: %v", err)
		reg = nil
	}

	// 创建客户端，配置随机负载均衡与令牌桶限流（QPS=50，突发=100）
	c := client.NewClient(
		client.WithRegistry(reg),
		client.WithBalancer(loadbalance.NewRandomBalancer()),
		client.WithRateLimiter(nil), // 如需限流可传入 ratelimit.NewTokenBucket(100, 50)
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &HelloRequest{Name: "TinyRPC"}
	resp := new(HelloResponse)
	if err := c.Call(ctx, "HelloService", "SayHello", req, resp); err != nil {
		fmt.Println("call error:", err)
		return
	}
	fmt.Println("response:", resp.Message)
}
