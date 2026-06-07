// Package loadbalance 提供 TinyRPC 的负载均衡策略实现。
// 包含随机、轮询、加权一致性哈希三种经典策略，支持动态节点变更。
package loadbalance

import (
	"math/rand"
	"sync"

	"TinyRPC/registry"
)

// RandomBalancer 实现随机负载均衡策略
type RandomBalancer struct {
	mu        sync.RWMutex
	instances []*registry.ServiceInstance
	rnd       *rand.Rand
}

// NewRandomBalancer 创建一个新的 RandomBalancer
func NewRandomBalancer() Balancer {
	return &RandomBalancer{
		rnd: rand.New(rand.NewSource(rand.Int63())),
	}
}

// Name 返回负载均衡器名称
func (b *RandomBalancer) Name() string {
	return "random"
}

// UpdateInstances 动态更新后端实例列表
func (b *RandomBalancer) UpdateInstances(instances []*registry.ServiceInstance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.instances = instances
}

// Select 从可用实例中随机选择一个
func (b *RandomBalancer) Select(key string) (*registry.ServiceInstance, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	idx := b.rnd.Intn(len(b.instances))
	return b.instances[idx], nil
}
