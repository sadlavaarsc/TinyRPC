// demo/client 是 TinyRPC 的客户端示例。
// 演示如何创建客户端、配置注册中心、负载均衡、限流器，并发起同步 RPC 调用。
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"TinyRPC/client"
	"TinyRPC/loadbalance"
	"TinyRPC/ratelimit"
	"TinyRPC/registry"
)

// HelloRequest 请求结构
type HelloRequest struct {
	Name string `json:"name"`
}

// HelloResponse 响应结构
type HelloResponse struct {
	Message string `json:"message"`
}

func main() {
	// 创建 Etcd 注册中心（实际使用需确保 Etcd 可用）
	reg, err := registry.NewEtcdRegistry([]string{"localhost:2379"})
	if err != nil {
		log.Printf("create registry failed: %v, using direct connection", err)
		reg = nil
	}

	// 创建负载均衡器（轮询策略）
	balancer := loadbalance.NewRoundRobinBalancer()

	// 创建客户端限流器（QPS=50，突发=100）
	limiter := ratelimit.NewTokenBucket(100, 50)

	// 创建客户端
	c := client.NewClient(
		client.WithRegistry(reg),
		client.WithBalancer(balancer),
		client.WithRateLimiter(limiter),
	)

	// 发起调用
	req := &HelloRequest{Name: "TinyRPC"}
	var resp HelloResponse

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Call(ctx, "HelloService", "SayHello", req, &resp); err != nil {
		log.Fatalf("call failed: %v", err)
	}

	fmt.Printf("Response: %s\n", resp.Message)
}
