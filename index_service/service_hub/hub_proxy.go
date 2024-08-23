package service_hub

import (
	"context"
	"criker-search/utils"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"strings"
	"sync"
	"time"
)

// HubProxy 代理模式，实现了ServiceHub接口。
// 该代理为ServiceHub实例提供了一层中间访问，增加了缓存和限流功能。
//
// 成员变量:
//   - EtcdServiceHub: 真实的ServiceHub实例，用于实际的服务发现和注册。
//   - endpointCache: 用于缓存服务端点的同步映射。
//   - limiter: 限流器，用于控制每秒请求的最大次数。
type HubProxy struct {
	*EtcdServiceHub               // 真实的ServiceHub实例
	endpointCache   sync.Map      // 缓存服务端点
	limiter         *rate.Limiter // 限流器
}

var (
	hubProxy  *HubProxy
	proxyOnce sync.Once
)

// GetServiceHubProxy HubProxy的构造函数，采用单例模式创建实例。
//
// 参数:
//   - etcdServers: etcd服务器的地址列表。
//   - heartbeatFrequency: 心跳频率，用于创建租约。
//   - qps: 每秒请求的最大次数，用于限流器的配置。
//
// 返回值:
//   - *HubProxy: 返回HubProxy的单例实例。
func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int) *HubProxy {
	if hubProxy == nil {
		proxyOnce.Do(func() {
			// 初始化HubProxy实例
			hubProxy = &HubProxy{
				EtcdServiceHub: GetServiceHub(etcdServers, heartbeatFrequency),
				endpointCache:  sync.Map{},
				// 配置限流器：每秒产生qps个令牌
				limiter: rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
			}

		})
	}
	return hubProxy
}

// 以下方法由EtcdServiceHub匿名变量提供

//// RegisterService 注册服务
//func (p *HubProxy) RegisterService(service, endpoint string, leaseId etcdv3.LeaseID) (etcdv3.LeaseID, error) {
//	return p.EtcdServiceHub.RegisterService(service, endpoint, leaseId)
//}
//
//// UnregisterService 注销服务
//func (p *HubProxy) UnregisterService(service, endpoint string) error {
//	return p.EtcdServiceHub.UnregisterService(service, endpoint)
//}
//
//// GetServiceEndpoint 根据负载均衡策略，从众多endpoint里选择一个
//func (p *HubProxy) GetServiceEndpoint(service string) string {
//	return p.EtcdServiceHub.GetServiceEndpoint(service)
//}

// GetServiceEndpoints 服务发现。把第一次查询etcd的结果缓存起来，然后安装一个Watcher，仅etcd数据变化时更新本地缓存，这样可以降低etcd的访问压力，同时加上限流保护。
//
// 参数:
//   - service: 需要获取端点的服务名称。
//
// 返回值:
//   - []string: 返回服务端点的列表。如果限流未通过或发生错误，则返回nil。
func (p *HubProxy) GetServiceEndpoints(service string) []string {
	// 限流检查：如果限流器不允许请求，则直接返回nil
	if !p.limiter.Allow() {
		return nil
	}

	// 更新服务端点缓存的Watcher
	p.watchEndpointsOfService(service)

	// 尝试从缓存中加载服务端点
	cachedEndpoints, ok := p.endpointCache.Load(service)
	if !ok {
		// 如果缓存中没有服务端点，查询etcd获取最新端点
		endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
		if len(endpoints) > 0 {
			// 如果查询到端点，将其存入缓存
			p.endpointCache.Store(service, endpoints)
		}
		return endpoints
	}
	// 如果缓存中已有服务端点，直接返回缓存结果。缓存的一致性由watchEndpointsOfService()函数保证。
	return cachedEndpoints.([]string)
}

// watchEndpointsOfService 监视服务端点的变化，确保本地缓存与etcd中的数据保持同步。
//
// 参数:
//   - service: 需要监视的服务名称。
//
// 该函数将设置一个Watcher来监听etcd中对应服务的变化，并在检测到变化时更新本地缓存。
func (p *HubProxy) watchEndpointsOfService(service string) {
	// 检查当前服务是否已经被监听
	_, ok := p.watched.LoadOrStore(service, true)
	if ok {
		// 如果已经监听过，直接返回
		return
	}

	// 构建服务的前缀路径
	prefix := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/"
	// 设置etcd Watcher，监视指定前缀的所有键值对的变化
	watchChan := p.EtcdServiceHub.client.Watch(context.Background(), prefix, etcdv3.WithPrefix())
	utils.Log.Printf("开始监视服务端点: %s", prefix)

	// 启动一个 goroutine 来异步处理 Watcher 事件
	go func() {
		for response := range watchChan {
			for _, event := range response.Events {
				// 记录事件类型（PUT或DELETE）
				utils.Log.Printf("etcd事件类型: %s", event.Type)

				// 提取服务名称
				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					// 从etcd中获取最新的服务端点列表
					endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
					if len(endpoints) > 0 {
						// 如果获取到服务端点，更新本地缓存
						p.endpointCache.Store(service, endpoints)
					} else {
						// 如果服务下没有端点，删除本地缓存
						p.endpointCache.Delete(service)
					}
				}
			}
		}
	}()
}
