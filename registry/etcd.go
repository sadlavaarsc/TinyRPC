// Package registry 提供基于 Etcd 的服务注册与发现能力。
// 支持服务注册、心跳保活、服务发现、节点变更监听等核心功能。
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// DefaultTTL 默认服务节点租约 TTL（秒）
	DefaultTTL = 10
	// DefaultDialTimeout 连接 Etcd 的超时时间
	DefaultDialTimeout = 5 * time.Second
)

// ServiceInstance 描述一个服务实例的元数据
type ServiceInstance struct {
	// ID 实例唯一标识，通常格式为 {serviceName}@{host}:{port}
	ID string `json:"id"`
	// Name 服务名
	Name string `json:"name"`
	// Host 实例主机地址
	Host string `json:"host"`
	// Port 实例端口
	Port int `json:"port"`
	// Weight 权重，用于加权负载均衡
	Weight int `json:"weight"`
	// Metadata 扩展元数据
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Registry 定义服务注册发现接口
type Registry interface {
	// Register 注册一个服务实例到注册中心
	Register(ctx context.Context, instance *ServiceInstance) error
	// Deregister 注销服务实例
	Deregister(ctx context.Context, instance *ServiceInstance) error
	// Discover 发现指定服务的所有可用实例
	Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error)
	// Watch 监听指定服务的节点变更
	Watch(ctx context.Context, serviceName string) (chan []*ServiceInstance, error)
	// Close 关闭注册中心连接
	Close() error
}

// EtcdRegistry 基于 Etcd 的注册中心实现
type EtcdRegistry struct {
	client   *clientv3.Client
	leases   map[string]clientv3.LeaseID
	mu       sync.RWMutex
	watches  map[string]clientv3.WatchChan
	stopCh   chan struct{}
}

// NewEtcdRegistry 创建一个新的 EtcdRegistry 实例
// endpoints 为 Etcd 集群地址列表，如 []string{"localhost:2379"}
func NewEtcdRegistry(endpoints []string) (*EtcdRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DefaultDialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("registry: create etcd client failed: %w", err)
	}

	return &EtcdRegistry{
		client:  cli,
		leases:  make(map[string]clientv3.LeaseID),
		watches: make(map[string]clientv3.WatchChan),
		stopCh:  make(chan struct{}),
	}, nil
}

// serviceKey 生成 Etcd 中存储服务实例的 key
func serviceKey(serviceName, instanceID string) string {
	return fmt.Sprintf("/tinyrpc/%s/%s", serviceName, instanceID)
}

// prefixKey 生成服务名前缀 key，用于范围查询
func prefixKey(serviceName string) string {
	return fmt.Sprintf("/tinyrpc/%s/", serviceName)
}

// Register 将服务实例注册到 Etcd，并启动心跳保活协程。
// 使用 Etcd 的 Lease 机制实现自动过期与续期。
func (r *EtcdRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	if instance == nil {
		return fmt.Errorf("registry: nil instance")
	}

	// 创建租约
	resp, err := r.client.Grant(ctx, DefaultTTL)
	if err != nil {
		return fmt.Errorf("registry: grant lease failed: %w", err)
	}

	data, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("registry: marshal instance failed: %w", err)
	}

	key := serviceKey(instance.Name, instance.ID)
	_, err = r.client.Put(ctx, key, string(data), clientv3.WithLease(resp.ID))
	if err != nil {
		return fmt.Errorf("registry: put instance failed: %w", err)
	}

	r.mu.Lock()
	r.leases[key] = resp.ID
	r.mu.Unlock()

	// 启动后台心跳保活
	go r.keepAlive(key, resp.ID)

	return nil
}

// keepAlive 为指定租约启动自动续期，直到实例被注销或注册中心关闭。
func (r *EtcdRegistry) keepAlive(key string, leaseID clientv3.LeaseID) {
	ch, err := r.client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		return
	}

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-r.stopCh:
			return
		}
	}
}

// Deregister 从 Etcd 中注销服务实例，并释放对应租约。
func (r *EtcdRegistry) Deregister(ctx context.Context, instance *ServiceInstance) error {
	if instance == nil {
		return fmt.Errorf("registry: nil instance")
	}

	key := serviceKey(instance.Name, instance.ID)

	r.mu.Lock()
	leaseID, ok := r.leases[key]
	if ok {
		delete(r.leases, key)
	}
	r.mu.Unlock()

	if ok {
		_, err := r.client.Revoke(ctx, leaseID)
		if err != nil {
			return fmt.Errorf("registry: revoke lease failed: %w", err)
		}
	}

	_, err := r.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("registry: delete instance failed: %w", err)
	}

	return nil
}

// Discover 查询指定服务的所有可用实例。
func (r *EtcdRegistry) Discover(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	resp, err := r.client.Get(ctx, prefixKey(serviceName), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("registry: discover service failed: %w", err)
	}

	instances := make([]*ServiceInstance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var inst ServiceInstance
		if err := json.Unmarshal(kv.Value, &inst); err != nil {
			continue
		}
		instances = append(instances, &inst)
	}

	return instances, nil
}

// Watch 监听指定服务的节点变更事件，返回一个只读 channel。
// 当服务实例发生增删改时，channel 会收到最新的实例列表。
func (r *EtcdRegistry) Watch(ctx context.Context, serviceName string) (chan []*ServiceInstance, error) {
	watchCh := r.client.Watch(ctx, prefixKey(serviceName), clientv3.WithPrefix())

	resultCh := make(chan []*ServiceInstance, 1)

	go func() {
		defer close(resultCh)
		for wresp := range watchCh {
			if wresp.Err() != nil {
				continue
			}
			// 发生变更时，重新拉取全量列表
			instances, err := r.Discover(ctx, serviceName)
			if err != nil {
				continue
			}
			select {
			case resultCh <- instances:
			case <-ctx.Done():
				return
			case <-r.stopCh:
				return
			}
		}
	}()

	return resultCh, nil
}

// Close 关闭 Etcd 客户端连接，并停止所有后台协程。
func (r *EtcdRegistry) Close() error {
	close(r.stopCh)
	return r.client.Close()
}
