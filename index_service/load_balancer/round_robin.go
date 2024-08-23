package load_balancer

import "sync/atomic"

// RoundRobin 负载均衡算法：轮询法
// 轮询法确保每个请求轮流被分配到列表中的每个端点
type RoundRobin struct {
	acc int64 // 记录累计请求次数
}

// Take 选择一个Endpoint，根据轮询算法
func (b *RoundRobin) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	// 线程安全地增加请求次数
	n := atomic.AddInt64(&b.acc, 1)
	// 计算要选择的Endpoint的索引
	index := int(n % int64(len(endpoints)))
	return endpoints[index]
}
