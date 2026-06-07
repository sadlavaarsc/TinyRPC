// Package loadbalance 提供 TinyRPC 的负载均衡策略实现。
// 包含随机、轮询、加权一致性哈希三种经典策略，支持动态节点变更。
package loadbalance

import (
	"errors"

	"TinyRPC/registry"
)

// ErrNoAvailableInstance 表示当前没有可用的服务实例
var ErrNoAvailableInstance = errors.New("loadbalance: no available instance")

// Balancer 定义负载均衡器接口
type Balancer interface {
	// Name 返回负载均衡器名称
	Name() string
	// UpdateInstances 动态更新后端实例列表
	UpdateInstances(instances []*registry.ServiceInstance)
	// Select 根据 key 选择一个后端实例；key 在不同策略中有不同语义
	Select(key string) (*registry.ServiceInstance, error)
}
