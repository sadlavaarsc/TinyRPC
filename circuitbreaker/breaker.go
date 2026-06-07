// Package circuitbreaker 提供熔断器实现，基于滑动窗口计数器。
// 当失败率达到阈值时自动熔断，经过冷却时间后进入半开状态试探恢复。
package circuitbreaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrCircuitOpen 表示熔断器处于开启状态，拒绝请求
	ErrCircuitOpen = errors.New("circuitbreaker: circuit is open")
	// ErrTooManyFailures 表示失败次数超过阈值
	ErrTooManyFailures = errors.New("circuitbreaker: too many failures")
)

// State 定义熔断器状态
type State int32

const (
	// StateClosed 关闭状态：正常放行请求
	StateClosed State = iota
	// StateOpen 开启状态：拒绝所有请求
	StateOpen
	// StateHalfOpen 半开状态：允许少量请求试探
	StateHalfOpen
)

// Config 熔断器配置
type Config struct {
	// WindowSize 滑动窗口大小
	WindowSize time.Duration
	// FailureThreshold 触发熔断的失败次数阈值
	FailureThreshold int64
	// SuccessThreshold 半开状态下恢复所需的连续成功次数
	SuccessThreshold int64
	// HalfOpenMaxRequests 半开状态下允许的最大请求数
	HalfOpenMaxRequests int64
	// OpenDuration 熔断开启后的冷却时间
	OpenDuration time.Duration
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		WindowSize:          10 * time.Second,
		FailureThreshold:    5,
		SuccessThreshold:    3,
		HalfOpenMaxRequests: 3,
		OpenDuration:        5 * time.Second,
	}
}

// Breaker 熔断器
type Breaker struct {
	name   string
	config *Config

	mu       sync.RWMutex
	state    int32 // atomic State
	window   *slidingWindow
	halfOpen int64 // atomic, 半开状态下已放行请求数

	lastFailureTime int64 // atomic, unix nano
}

// NewBreaker 创建一个新的熔断器
func NewBreaker(name string, config *Config) *Breaker {
	if config == nil {
		config = DefaultConfig()
	}
	return &Breaker{
		name:   name,
		config: config,
		window: newSlidingWindow(config.WindowSize),
	}
}

// Name 返回熔断器名称
func (b *Breaker) Name() string {
	return b.name
}

// State 返回当前熔断器状态
func (b *Breaker) State() State {
	return State(atomic.LoadInt32(&b.state))
}

// Allow 判断当前请求是否被允许通过
func (b *Breaker) Allow() error {
	state := b.currentState()
	switch state {
	case StateClosed:
		return nil
	case StateOpen:
		return ErrCircuitOpen
	case StateHalfOpen:
		if atomic.AddInt64(&b.halfOpen, 1) > b.config.HalfOpenMaxRequests {
			atomic.AddInt64(&b.halfOpen, -1)
			return ErrCircuitOpen
		}
		return nil
	}
	return nil
}

// RecordSuccess 记录一次成功调用
func (b *Breaker) RecordSuccess() {
	b.window.recordSuccess()
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.State() == StateHalfOpen {
		// 半开状态下连续成功达到阈值则关闭
		if b.window.successes() >= b.config.SuccessThreshold {
			atomic.StoreInt32(&b.state, int32(StateClosed))
			b.window.reset()
			atomic.StoreInt64(&b.halfOpen, 0)
		}
	}
}

// RecordFailure 记录一次失败调用
func (b *Breaker) RecordFailure() {
	b.window.recordFailure()
	atomic.StoreInt64(&b.lastFailureTime, time.Now().UnixNano())

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.State() == StateHalfOpen {
		// 半开状态下失败则重新开启
		atomic.StoreInt32(&b.state, int32(StateOpen))
		atomic.StoreInt64(&b.halfOpen, 0)
		return
	}

	if b.window.failures() >= b.config.FailureThreshold {
		atomic.StoreInt32(&b.state, int32(StateOpen))
	}
}

// currentState 计算当前应处的状态（含冷却时间判断）
func (b *Breaker) currentState() State {
	state := b.State()
	if state == StateOpen {
		last := atomic.LoadInt64(&b.lastFailureTime)
		if time.Since(time.Unix(0, last)) > b.config.OpenDuration {
			// 尝试进入半开状态
			if atomic.CompareAndSwapInt32(&b.state, int32(StateOpen), int32(StateHalfOpen)) {
				atomic.StoreInt64(&b.halfOpen, 0)
			}
			return StateHalfOpen
		}
	}
	return state
}

// slidingWindow 滑动窗口计数器
type slidingWindow struct {
	size      time.Duration
	mu        sync.RWMutex
	events    []event
	successCnt int64
	failureCnt int64
}

type event struct {
	time    time.Time
	success bool
}

func newSlidingWindow(size time.Duration) *slidingWindow {
	return &slidingWindow{size: size}
}

func (w *slidingWindow) recordSuccess() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trim(time.Now())
	w.events = append(w.events, event{time: time.Now(), success: true})
	w.successCnt++
}

func (w *slidingWindow) recordFailure() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trim(time.Now())
	w.events = append(w.events, event{time: time.Now(), success: false})
	w.failureCnt++
}

func (w *slidingWindow) successes() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.trim(time.Now())
	return w.successCnt
}

func (w *slidingWindow) failures() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	w.trim(time.Now())
	return w.failureCnt
}

func (w *slidingWindow) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = w.events[:0]
	w.successCnt = 0
	w.failureCnt = 0
}

// trim 清理窗口外的过期事件
func (w *slidingWindow) trim(now time.Time) {
	cutoff := now.Add(-w.size)
	idx := 0
	for i, e := range w.events {
		if e.time.After(cutoff) {
			idx = i
			break
		}
		if e.success {
			w.successCnt--
		} else {
			w.failureCnt--
		}
	}
	if idx > 0 {
		w.events = w.events[idx:]
	}
}
