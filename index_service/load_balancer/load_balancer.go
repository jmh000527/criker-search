package load_balancer

// LoadBalancer 负载均衡接口，定义选择Endpoint的方法
type LoadBalancer interface {
	// Take 从给定的端点列表中选择一个
	Take(endpoints []string) string
}
