package loadbalance

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"

	"TinyRPC/registry"
)

// ConsistentHashBalancer 实现加权一致性哈希负载均衡策略。
// 每个实例根据权重在哈希环上生成多个虚拟节点，以实现更均匀的分布。
// 当实例增删时只有少量请求需要重新映射，适合有状态服务或缓存场景。
type ConsistentHashBalancer struct {
	mu        sync.RWMutex
	instances []*registry.ServiceInstance
	// replicas 为每个权重单位对应的虚拟节点数
	replicas int
	// ring 哈希环，有序存储所有虚拟节点的哈希值
	ring []uint32
	// nodes 哈希值到实例的映射
	nodes map[uint32]*registry.ServiceInstance
}

// NewConsistentHashBalancer 创建一个新的一致性哈希负载均衡器。
// replicas 为每个权重单位对应的虚拟节点数，建议 100-200。
func NewConsistentHashBalancer(replicas int) Balancer {
	if replicas <= 0 {
		replicas = 150
	}
	return &ConsistentHashBalancer{
		replicas: replicas,
		nodes:    make(map[uint32]*registry.ServiceInstance),
	}
}

// Name 返回负载均衡器名称。
func (b *ConsistentHashBalancer) Name() string {
	return "consistent_hash"
}

// UpdateInstances 动态更新后端实例列表，并重建哈希环。
func (b *ConsistentHashBalancer) UpdateInstances(instances []*registry.ServiceInstance) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.instances = instances
	b.ring = b.ring[:0]
	b.nodes = make(map[uint32]*registry.ServiceInstance, len(instances)*b.replicas)

	for _, inst := range instances {
		weight := inst.Weight
		if weight <= 0 {
			weight = 1
		}
		virtualCount := weight * b.replicas
		for i := 0; i < virtualCount; i++ {
			key := inst.ID + "#" + strconv.Itoa(i)
			hash := crc32.ChecksumIEEE([]byte(key))
			b.ring = append(b.ring, hash)
			b.nodes[hash] = inst
		}
	}

	sort.Slice(b.ring, func(i, j int) bool {
		return b.ring[i] < b.ring[j]
	})
}

// Select 根据 key 的哈希值在哈希环上顺时针查找最近的虚拟节点。
// 若 key 为空，则使用 "default" 作为兜底键。
func (b *ConsistentHashBalancer) Select(key string) (*registry.ServiceInstance, error) {
	if key == "" {
		key = "default"
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.ring) == 0 {
		return nil, ErrNoAvailableInstance
	}

	hash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(b.ring), func(i int) bool {
		return b.ring[i] >= hash
	})
	if idx == len(b.ring) {
		idx = 0
	}

	return b.nodes[b.ring[idx]], nil
}
