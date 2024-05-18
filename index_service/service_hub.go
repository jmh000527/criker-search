package index_service

import (
	"context"
	"criker-search/utils"
	"errors"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
	"time"
)

// ServiceHub 服务注册中心，应使用单例模式构造
type ServiceHub struct {
	client             *etcdv3.Client
	heartbeatFrequency int64        // server每隔几秒向Hub续约
	watched            sync.Map     // 监视的服务
	loadBalancer       LoadBalancer // 策略模式。完成同一个任务可以有多种不同的实现方案
}

const (
	ServiceRootPath = "/radic/index" // etcd key的前缀
)

var (
	serviceHub *ServiceHub // 该全局变量包外不可见，包外想使用时通过GetServiceHub()获得
	hubOnce    sync.Once   // 单例模式需要用到一个once
)

// GetServiceHub ServiceHub的构造函数，单例模式
func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *ServiceHub {
	if serviceHub == nil {
		hubOnce.Do(func() {
			if client, err := etcdv3.New(etcdv3.Config{
				Endpoints:   etcdServers,     // etcd 服务器的地址列表
				DialTimeout: 3 * time.Second, // 连接超时时间
			}); err != nil {
				// 发生log.Fatal时go进程会直接退出
				utils.Log.Fatal("Failed to connect to etcd:", err)
			} else {
				serviceHub = &ServiceHub{
					client:             client,
					heartbeatFrequency: heartbeatFrequency,
					loadBalancer:       &RoundRobin{}, // 采用Round-Robin负载均衡策略
				}
			}
		})
	}
	return serviceHub
}

// RegisterService 注册服务。第一次注册向etcd写一个key，后续注册仅仅是在续约。service 微服务的名称；endpoint 微服务server的地址；leaseID 租约ID,第一次注册时置为0
func (hub *ServiceHub) RegisterService(service string, endpoint string, leaseId etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	if leaseId <= 0 {
		// 创建一个租约，有效期为heartbeatFrequency秒
		leaseGrantResponse, err := hub.client.Grant(context.Background(), hub.heartbeatFrequency)
		if err != nil {
			utils.Log.Printf("Failed to create lease: %v", err)
			return 0, err
		}
		key := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/" + endpoint
		// 服务注册
		_, err = hub.client.Put(context.Background(), key, "", etcdv3.WithLease(leaseGrantResponse.ID))
		if err != nil {
			utils.Log.Printf("Failed to register service: %v", err)
			return leaseGrantResponse.ID, err
		}
		return leaseGrantResponse.ID, nil
	} else {
		// 续租
		_, err := hub.client.KeepAliveOnce(context.Background(), leaseId)
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// 找不到租约，重新注册（把leaseID置为0）
			utils.Log.Printf("Lease not found for service, registering with etcd")
			return hub.RegisterService(service, endpoint, 0)
		} else if err != nil {
			// 续租失败
			utils.Log.Printf("Failed to extend lease: %v", err)
			return 0, err
		}
		// 续租成功
		return leaseId, nil
	}
}

// UnregisterService 主动注销服务
func (hub *ServiceHub) UnregisterService(service string, endpoint string) error {
	key := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/" + endpoint
	_, err := hub.client.Delete(context.Background(), key)
	if err != nil {
		utils.Log.Printf("Failed to unregister service: %v", err)
		return err
	}
	utils.Log.Printf("Unregistered service: %v", service)
	return nil
}

// GetServiceEndpoints 服务发现。client每次进行RPC调用之前都查询etcd，获取server集合，然后采用负载均衡算法选择一台server。
// 或者也可以把负载均衡的功能放到注册中心，即放到getServiceEndpoints函数里，让它只返回一个server
func (hub *ServiceHub) GetServiceEndpoints(service string) []string {
	prefix := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/"
	// 按前缀获取key-value
	getResponse, err := hub.client.Get(context.Background(), prefix, etcdv3.WithPrefix())
	if err != nil {
		utils.Log.Printf("Get service endpoints from etcd failed: %v", err)
		return nil
	}
	// 构造返回结果
	endpoints := make([]string, 0, len(getResponse.Kvs))
	for _, kv := range getResponse.Kvs {
		// 只需要key，不需要value
		path := strings.Split(string(kv.Key), "")
		endpoints = append(endpoints, path[len(path)-1])
	}
	utils.Log.Printf("Get service endpoints: %v", endpoints)
	return endpoints
}

// GetServiceEndpoint 策略模式：根据负载均衡策略，从众多endpoint里选择一个
func (hub *ServiceHub) GetServiceEndpoint(service string) string {
	return hub.loadBalancer.Take(hub.GetServiceEndpoints(service))
}

// Close 关闭etcd客户端连接
func (hub *ServiceHub) Close() {
	err := hub.client.Close()
	if err != nil {
		utils.Log.Printf("Close etcd client connection failed: %v", err)
	}
}
