// Package loadbalance 提供 TinyRPC 的负载均衡策略实现。
// 包含随机、轮询、加权一致性哈希三种经典策略，支持动态节点变更。
package loadbalance

import (
	"sync"
	"sync/atomic"

	"TinyRPC/registry"
)

// RoundRobinBalancer 实现轮询负载均衡策略
type RoundRobinBalancer struct {
	mu        sync.RWMutex
	instances []*registry.ServiceInstance
	next      uint64
}

// NewRoundRobinBalancer 创建一个新的 RoundRobinBalancer
func NewRoundRobinBalancer() Balancer {
	return &RoundRobinBalancer{}
}

// Name 返回负载均衡器名称
func (b *RoundRobinBalancer) Name() string {
	return "roundrobin"
}

// UpdateInstances 动态更新后端实例列表
func (b *RoundRobinBalancer) UpdateInstances(instances []*registry.ServiceInstance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.instances = instances
}

// Select 从可用实例中按轮询顺序选择一个
func (b *RoundRobinBalancer) Select(key string) (*registry.ServiceInstance, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	n := atomic.AddUint64(&b.next, 1)
	idx := (n - 1) % uint64(len(b.instances))
	return b.instances[idx], nil
}
