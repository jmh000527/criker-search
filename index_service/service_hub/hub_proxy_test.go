package service_hub

import (
	"fmt"
	"testing"
	"time"
)

var (
	serviceName = "test_service"
	etcdServers = []string{"127.0.0.1:2379"} // etcd集群的地址
)

func TestGetServiceEndpointsByProxy(t *testing.T) {
	const qps = 10 // qps限制为10
	p := GetServiceHubProxy(etcdServers, 3, qps)

	endpoint := "127.0.0.1:5000"
	p.RegisterService(serviceName, endpoint, 0)
	defer p.UnregisterService(serviceName, endpoint)
	endpoints := p.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	endpoint = "127.0.0.2:5000"
	p.RegisterService(serviceName, endpoint, 0)
	defer p.UnregisterService(serviceName, endpoint)
	endpoints = p.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	endpoint = "127.0.0.3:5000"
	p.RegisterService(serviceName, endpoint, 0)
	defer p.UnregisterService(serviceName, endpoint)
	endpoints = p.GetServiceEndpoints(serviceName)
	fmt.Printf("endpoints %v\n", endpoints)

	time.Sleep(1 * time.Second)  // 暂停1秒钟，把令牌桶的容量打满
	for i := 0; i < qps+5; i++ { // 桶里面有10个令牌，从第11次开始就拒绝访问了
		endpoints = p.GetServiceEndpoints(serviceName)
		fmt.Printf("%d endpoints %v\n", i, endpoints)
	}

	time.Sleep(1 * time.Second)  // 暂停1秒钟，把令牌桶的容量打满
	for i := 0; i < qps+5; i++ { // 桶里面有10个令牌，从第11次开始就拒绝访问了
		endpoints = p.GetServiceEndpoints(serviceName)
		fmt.Printf("%d endpoints %v\n", i, endpoints)
	}
}
