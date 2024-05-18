package index_service

import (
	"math/rand"
	"sync/atomic"
)

type LoadBalancer interface {
	Take([]string) string
}

// RoundRobin 负载均衡算法--轮询法
type RoundRobin struct {
	acc int64 // 记录累计请求次数
}

// Take 选择一个Endpoint
func (b *RoundRobin) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	// Take()需要支持并发调用
	n := atomic.AddInt64(&b.acc, 1)
	index := int(n % int64(len(endpoints)))
	return endpoints[index]
}

// RandomSelect 负载均衡算法--随机法
type RandomSelect struct{}

// Take 选择一个Endpoint
func (b *RandomSelect) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	// 随机选择
	index := rand.Intn(len(endpoints))
	return endpoints[index]
}
