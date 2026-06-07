package main

import (
	"fmt"
	"github.com/sadlavaarsc/TinyRPC/server"
)

// HelloService 示例服务
type HelloService struct{}

// SayHello 打招呼方法
func (s *HelloService) SayHello() string {
	return "Hello, TinyRPC!"
}

func main() {
	srv := server.NewServer(":8000")
	srv.Register("HelloService", new(HelloService))
	fmt.Println("starting demo server on :8000")
	if err := srv.Serve(); err != nil {
		panic(err)
	}
}
