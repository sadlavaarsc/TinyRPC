// demo/server 是 TinyRPC 的服务端示例。
// 演示如何注册一个 HelloService 并启动 TCP 服务，集成限流与拦截器。
package main

import (
	"context"
	"fmt"
	"log"

	"TinyRPC/protocol"
	"TinyRPC/ratelimit"
	"TinyRPC/server"
)

// HelloService 定义示例服务接口。
type HelloService interface {
	SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error)
}

// HelloRequest 请求结构。
type HelloRequest struct {
	Name string `json:"name"`
}

// HelloResponse 响应结构。
type HelloResponse struct {
	Message string `json:"message"`
}

// helloServiceImpl 实现 HelloService 接口。
type helloServiceImpl struct{}

func (s *helloServiceImpl) SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	return &HelloResponse{
		Message: fmt.Sprintf("Hello, %s! Welcome to TinyRPC.", req.Name),
	}, nil
}

func main() {
	// 创建服务端，配置限流器（QPS=100，突发=200）
	limiter := ratelimit.NewTokenBucket(200, 100)
	srv := server.NewServer(
		server.WithRateLimiter(limiter),
		server.WithInterceptor(logInterceptor),
	)

	// 注册 HelloService
	sd := &server.ServiceDesc{
		ServiceName: "HelloService",
		HandlerType: (*HelloService)(nil),
		Methods: []server.MethodDesc{
			{
				MethodName: "SayHello",
				Handler:    _HelloService_SayHello_Handler,
			},
		},
	}
	if err := srv.RegisterService(sd, &helloServiceImpl{}); err != nil {
		log.Fatalf("register service failed: %v", err)
	}

	// 可选：注册到 Etcd
	// reg, _ := registry.NewEtcdRegistry([]string{"localhost:2379"})
	// _ = reg.Register(context.Background(), &registry.ServiceInstance{
	// 	ID: "HelloService@127.0.0.1:8888", Name: "HelloService",
	// 	Host: "127.0.0.1", Port: 8888, Weight: 1,
	// })

	addr := ":8888"
	log.Printf("TinyRPC server starting on %s", addr)
	if err := srv.Serve(addr); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}

// _HelloService_SayHello_Handler 是 SayHello 方法的桩代码，模拟 protobuf 生成的 handler。
func _HelloService_SayHello_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor server.Interceptor) (interface{}, error) {
	in := new(HelloRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(HelloService).SayHello(ctx, in)
	}
	info := &protocol.Request{Service: "HelloService", Method: "SayHello"}
	return interceptor(ctx, info, func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(HelloService).SayHello(ctx, in)
	})
}

// logInterceptor 是一个简单的日志拦截器示例。
func logInterceptor(ctx context.Context, req interface{}, handler server.Handler) (interface{}, error) {
	log.Printf("[Interceptor] receive request: %+v", req)
	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("[Interceptor] handle error: %v", err)
	} else {
		log.Printf("[Interceptor] handle success")
	}
	return resp, err
}
