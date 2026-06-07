package loadbalance

import (
	"math/rand"
	"sync"
	"time"

	"TinyRPC/registry"
)

// RandomBalancer 实现随机负载均衡策略。
// 从可用实例列表中按均匀分布随机选择一个节点，适用于各节点性能相近的场景。
type RandomBalancer struct {
	mu        sync.RWMutex
	instances []*registry.ServiceInstance
	rnd       *rand.Rand
}

// NewRandomBalancer 创建一个新的随机负载均衡器。
func NewRandomBalancer() Balancer {
	return &RandomBalancer{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Name 返回负载均衡器名称。
func (b *RandomBalancer) Name() string {
	return "random"
}

// UpdateInstances 动态更新后端实例列表。
func (b *RandomBalancer) UpdateInstances(instances []*registry.ServiceInstance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.instances = instances
}

// Select 从可用实例中随机选择一个。
func (b *RandomBalancer) Select(_ string) (*registry.ServiceInstance, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	idx := b.rnd.Intn(len(b.instances))
	return b.instances[idx], nil
}
