package index_service

import (
	"context"
	"criker-search/utils"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"strings"
	"sync"
	"time"
)

// HubProxy 代理模式，实现了ServiceHub接口。对ServiceHub做一层代理，想访问endpoints时需要通过代理。代理提供了2个功能：缓存和限流保护
type HubProxy struct {
	*EtcdServiceHub               // 指向ServiceHub实例
	endpointCache   sync.Map      // 维护每一个service下的所有servers
	limiter         *rate.Limiter // 限流
}

var (
	hubProxy  *HubProxy
	proxyOnce sync.Once
)

// GetServiceHubProxy HubProxy的构造函数，单例模式。
// qps：一秒钟最多允许请求多少次
func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int) *HubProxy {
	if hubProxy == nil {
		proxyOnce.Do(func() {
			serviceHub := GetServiceHub(etcdServers, heartbeatFrequency)
			if serviceHub != nil {
				hubProxy = &HubProxy{
					EtcdServiceHub: serviceHub,
					endpointCache:  sync.Map{},
					// 每隔1E9/qps纳秒产生一个令牌，即一秒钟之内产生qps个令牌。令牌桶的容量为qps
					limiter: rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
				}
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

// GetServiceEndpoints 服务发现。把第一次查询etcd的结果缓存起来，然后安装一个Watcher，仅etcd数据变化时更新本地缓存，这样可以降低etcd的访问压力，同时加上限流保护
func (p *HubProxy) GetServiceEndpoints(service string) []string {
	// 不阻塞，如果桶中没有1个令牌，则函数直接返回空，即没有可用的endpoints
	if !p.limiter.Allow() {
		return nil
	}
	// 监听etcd的数据变化，及时更新本地缓存
	p.watchEndpointsOfService(service)
	cachedEndpoints, ok := p.endpointCache.Load(service)
	if !ok {
		// 如果本地没有缓存该服务的endpoints，从etcd上读出最新的所有endpoints
		endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
		if len(endpoints) > 0 {
			// 将查询etcd的结果放入本地缓存
			p.endpointCache.Store(service, endpoints)
		}
		return endpoints
	}
	// 本地有缓存该服务的endpoints，直接返回。缓存一致性由watchEndpointsOfService()函数保证。
	return cachedEndpoints.([]string)
}

func (p *HubProxy) watchEndpointsOfService(service string) {
	// 判断当前服务是否监听过
	_, ok := p.watched.LoadOrStore(service, true)
	if ok {
		// 已经监听过，直接返回
		return
	}
	// 获取关注的服务前缀
	prefix := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/"
	// 根据前缀监听，每一个修改都会放入管道watchChan
	watchChan := p.EtcdServiceHub.client.Watch(context.Background(), prefix, etcdv3.WithPrefix())
	utils.Log.Printf("Watch endpoints of " + prefix)
	// 遍历管道。这是个死循环，除非关闭管道
	go func() {
		for response := range watchChan {
			for _, event := range response.Events {
				utils.Log.Printf("etcd event type: %s", event.Type) // PUT或DELETE
				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					// 跟etcd进行一次全量同步，从etcd上读出最新的所有endpoints
					endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
					if len(endpoints) > 0 {
						// 将查询得到的etcd的结果放入本地缓存
						p.endpointCache.Store(service, endpoints)
					} else {
						// 该service下已经没有endpoint
						p.endpointCache.Delete(service)
					}
				}
			}
		}
	}()
}
