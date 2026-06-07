// Package loadbalance 提供 TinyRPC 的负载均衡策略。
// 支持随机、轮询、一致性哈希等经典算法，所有策略均基于服务实例列表进行节点选择。
package loadbalance

import (
	"errors"

	"TinyRPC/registry"
)

// ErrNoAvailableInstance 表示当前没有可用的服务实例。
var ErrNoAvailableInstance = errors.New("loadbalance: no available instance")

// Balancer 定义负载均衡器接口。
// 实现该接口的策略可动态更新实例列表，并根据 key 选择目标节点。
type Balancer interface {
	// Name 返回负载均衡器名称，用于日志与监控。
	Name() string
	// UpdateInstances 动态更新后端实例列表。
	UpdateInstances(instances []*registry.ServiceInstance)
	// Select 根据 key 选择一个后端实例；key 在不同策略中有不同语义（如一致性哈希用请求标识，随机/轮询可忽略）。
	Select(key string) (*registry.ServiceInstance, error)
}
