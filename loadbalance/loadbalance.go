// Package loadbalance 提供 TinyRPC 的负载均衡策略。
// 支持随机、轮询、一致性哈希等经典算法，所有策略均基于服务实例列表进行节点选择。
package loadbalance

import (
	"fmt"

	"TinyRPC/registry"
)

// Picker 定义负载均衡选择器接口。
// 实现该接口的策略可根据请求上下文从可用实例中选出目标节点。
type Picker interface {
	// Pick 从 instances 中选择一个服务实例。
	// key 为一致性哈希等策略所需的映射键（如请求标识、用户 ID 等），
	// 对于随机/轮询策略可传空字符串。
	Pick(instances []*registry.ServiceInstance, key string) (*registry.ServiceInstance, error)
	// Name 返回策略名称，用于日志与监控。
	Name() string
}

// ErrNoInstance 表示没有可用服务实例时的错误。
var ErrNoInstance = fmt.Errorf("loadbalance: no available instance")
