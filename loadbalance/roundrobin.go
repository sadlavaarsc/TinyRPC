package loadbalance

import (
	"sync"
	"sync/atomic"

	"TinyRPC/registry"
)

// RoundRobinBalancer 实现轮询（Round-Robin）负载均衡策略。
// 通过原子计数器顺序遍历实例列表，将请求均匀分发到每个节点，
// 适用于节点性能一致且需要公平调度的场景。
type RoundRobinBalancer struct {
	mu        sync.RWMutex
	instances []*registry.ServiceInstance
	next      uint64
}

// NewRoundRobinBalancer 创建一个新的轮询负载均衡器。
func NewRoundRobinBalancer() Balancer {
	return &RoundRobinBalancer{}
}

// Name 返回负载均衡器名称。
func (b *RoundRobinBalancer) Name() string {
	return "round_robin"
}

// UpdateInstances 动态更新后端实例列表。
func (b *RoundRobinBalancer) UpdateInstances(instances []*registry.ServiceInstance) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.instances = instances
}

// Select 从可用实例中按轮询顺序选择一个。
func (b *RoundRobinBalancer) Select(_ string) (*registry.ServiceInstance, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.instances) == 0 {
		return nil, ErrNoAvailableInstance
	}

	n := atomic.AddUint64(&b.next, 1)
	idx := (n - 1) % uint64(len(b.instances))
	return b.instances[idx], nil
}
