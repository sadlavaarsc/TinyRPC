// Package ratelimit 提供基于 Token Bucket 算法的限流器实现。
// 支持按固定速率填充令牌，突发流量可消耗桶内累积的令牌。
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrRateLimited 表示请求被限流拒绝
	ErrRateLimited = errors.New("ratelimit: rate limited")
)

// Limiter 定义限流器接口
type Limiter interface {
	// Allow 判断当前请求是否允许通过
	Allow() bool
	// AllowN 判断 n 个令牌是否可用
	AllowN(n int64) bool
	// Wait 阻塞等待直到获取一个令牌，或 context 取消
	Wait(ctx context.Context) error
}

// TokenBucket 实现令牌桶限流算法。
// 以固定速率向桶中填充令牌，请求需消耗令牌才能通过，
// 桶容量决定了最大突发流量。
type TokenBucket struct {
	mu sync.Mutex

	// capacity 桶容量（最大突发令牌数）
	capacity int64
	// tokens 当前可用令牌数（使用 float64 支持小数精度）
	tokens float64
	// rate 每秒填充速率
	rate float64
	// lastFill 上次填充令牌的时间戳
	lastFill time.Time
}

// NewTokenBucket 创建一个新的令牌桶限流器。
// capacity 为桶容量（最大突发请求数），rate 为每秒允许通过的请求数。
func NewTokenBucket(capacity int64, rate float64) *TokenBucket {
	if capacity <= 0 {
		capacity = 100
	}
	if rate <= 0 {
		rate = 10
	}
	return &TokenBucket{
		capacity: capacity,
		tokens:   float64(capacity),
		rate:     rate,
		lastFill: time.Now(),
	}
}

// Allow 尝试获取一个令牌，成功返回 true。
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN 尝试获取 n 个令牌，成功返回 true。
func (tb *TokenBucket) AllowN(n int64) bool {
	if n <= 0 {
		return true
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// Wait 阻塞等待直到获取到一个令牌，或 context 被取消/超时。
func (tb *TokenBucket) Wait(ctx context.Context) error {
	for {
		if tb.Allow() {
			return nil
		}

		tb.mu.Lock()
		tb.refill()
		need := 1.0 - tb.tokens
		waitTime := time.Duration(need / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		if waitTime <= 0 {
			waitTime = time.Millisecond
		}

		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			continue
		}
	}
}

// refill 根据时间差补充令牌，补充量受容量上限限制。
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastFill).Seconds()
	if elapsed > 0 {
		tb.tokens += elapsed * tb.rate
		if tb.tokens > float64(tb.capacity) {
			tb.tokens = float64(tb.capacity)
		}
		tb.lastFill = now
	}
}
