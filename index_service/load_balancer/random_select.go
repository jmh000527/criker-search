package load_balancer

import "math/rand"

// RandomSelect 负载均衡算法：随机选择法
// 随机选择算法从列表中随机选择一个端点
type RandomSelect struct{}

// Take 选择一个Endpoint，根据随机选择算法
func (b *RandomSelect) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	// 从端点列表中随机选择一个索引
	index := rand.Intn(len(endpoints))
	return endpoints[index]
}
