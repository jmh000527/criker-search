package service_hub

import (
	"context"
	"errors"
	"github.com/jmh000527/criker-search/index_service/load_balancer"
	"github.com/jmh000527/criker-search/utils"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
	"time"
)

// EtcdServiceHub 服务注册中心，使用单例模式构造。
// 该服务用于与etcd进行交互，管理服务的注册、注销以及心跳续约等功能。
type EtcdServiceHub struct {
	client             *etcdv3.Client             // etcd客户端，用于与etcd进行操作
	heartbeatFrequency int64                      // 服务续约的心跳频率，单位：秒
	watched            sync.Map                   // 存储已经监视的服务，以避免重复监视
	loadBalancer       load_balancer.LoadBalancer // 负载均衡策略的接口，支持多种负载均衡实现
}

const (
	ServiceRootPath = "/criker-search" // etcd key的前缀
)

var (
	etcdServiceHub *EtcdServiceHub // 该全局变量包外不可见，包外想使用时通过GetServiceHub()获得
	hubOnce        sync.Once       // 单例模式需要用到一个once
)

// GetServiceHub ServiceHub的构造函数，采用单例模式。
//
// 参数:
//   - etcdServers: 包含etcd服务器地址的字符串切片。
//   - heartbeatFrequency: 心跳频率，表示服务心跳的间隔时间（以秒为单位）。
//
// 返回值:
//   - *EtcdServiceHub: 返回一个初始化好的EtcdServiceHub实例。
func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *EtcdServiceHub {
	// 检查是否已经存在etcdServiceHub实例
	if etcdServiceHub == nil {
		// 使用sync.Once确保单例模式，hubOnce.Do中的代码块只会被执行一次
		hubOnce.Do(func() {
			// 创建一个新的etcd客户端，连接到指定的etcd服务器
			client, err := etcdv3.New(etcdv3.Config{
				Endpoints:   etcdServers,     // etcd 服务器的地址列表
				DialTimeout: 3 * time.Second, // 连接超时时间
			})
			if err != nil {
				// 如果连接etcd服务器失败，记录错误并终止程序
				utils.Log.Fatal("连接etcd失败:", err)
			}

			// 初始化一个新的EtcdServiceHub实例
			etcdServiceHub = &EtcdServiceHub{
				client:             client,                      // 设置etcd客户端
				heartbeatFrequency: heartbeatFrequency,          // 设置心跳频率
				loadBalancer:       &load_balancer.RoundRobin{}, // 使用Round-Robin负载均衡策略
			}
		})
	}

	// 返回已初始化的etcdServiceHub实例
	return etcdServiceHub
}

// RegisterService 注册服务。
// 第一次注册时，会向etcd写入一个key，并创建一个租约；后续注册仅进行续约。
//
// 参数:
//   - service: 微服务的名称。
//   - endpoint: 微服务服务器的地址。
//   - leaseId: 租约ID，第一次注册时应置为0。
//
// 返回值:
//   - etcdv3.LeaseID: 返回租约ID。
//   - error: 返回错误信息，如果操作成功则为nil。
func (hub *EtcdServiceHub) RegisterService(service, endpoint string, leaseId etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	// 检查是否为首次注册（租约ID是否小于等于0）
	if leaseId <= 0 {
		// 首次注册: 创建一个新的租约，租约的有效期为heartbeatFrequency秒
		leaseGrantResponse, err := hub.client.Grant(context.Background(), hub.heartbeatFrequency)
		if err != nil {
			// 如果创建租约失败，记录错误并返回
			utils.Log.Printf("创建租约失败: %v", err)
			return 0, err
		}
		// 构建服务在etcd中的key，路径形如: /{ServiceRootPath}/{service}/{endpoint}
		key := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/" + endpoint
		// 将服务注册到etcd中，并将租约与该服务绑定
		_, err = hub.client.Put(context.Background(), key, "", etcdv3.WithLease(leaseGrantResponse.ID))
		if err != nil {
			// 如果注册服务失败，记录错误并返回
			utils.Log.Printf("服务注册失败: %v", err)
			return leaseGrantResponse.ID, err
		}
		utils.Log.Printf("成功注册服务: %v", key)
		// 返回新的租约ID
		return leaseGrantResponse.ID, nil
	} else {
		// 续约: 通过租约ID进行续租操作
		_, err := hub.client.KeepAliveOnce(context.Background(), leaseId)
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// 如果续租时发现租约不存在，则重新注册服务，将leaseID置为0重新进行注册
			utils.Log.Printf("未找到租约，重新注册服务")
			return hub.RegisterService(service, endpoint, 0)
		} else if err != nil {
			// 如果续租过程中发生其他错误，记录错误并返回
			utils.Log.Printf("续租失败: %v", err)
			return 0, err
		}
		// 如果续租成功，则返回现有的租约ID
		return leaseId, nil
	}
}

// UnregisterService 主动注销服务。
// 从etcd中删除服务的注册信息。
//
// 参数:
//   - service: 微服务的名称。
//   - endpoint: 微服务服务器的地址。
//
// 返回值:
//   - error: 返回错误信息，如果操作成功则为nil。
func (hub *EtcdServiceHub) UnregisterService(service string, endpoint string) error {
	// 构建服务在etcd中的key，路径形如: /{ServiceRootPath}/{service}/{endpoint}
	key := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/" + endpoint

	// 从etcd中删除服务注册信息
	_, err := hub.client.Delete(context.Background(), key)
	if err != nil {
		// 如果删除操作失败，记录错误并返回
		utils.Log.Printf("注销服务失败: %v", err)
		return err
	}

	// 成功注销服务，记录日志
	utils.Log.Printf("成功注销服务: %v", key)
	return nil
}

// GetServiceEndpoints 服务发现。
// 从etcd中查询指定服务的所有endpoint，并返回这些endpoint的列表。
// 参数:
//   - service: 微服务的名称。
//
// 返回值:
//   - []string: 包含所有服务endpoint的列表。如果查询失败，则返回nil。
func (hub *EtcdServiceHub) GetServiceEndpoints(service string) []string {
	// 构造服务的key前缀，用于获取服务的所有endpoint
	prefix := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/"

	// 从etcd中获取以指定前缀为开头的所有key-value对
	getResponse, err := hub.client.Get(context.Background(), prefix, etcdv3.WithPrefix())
	if err != nil {
		// 如果获取服务endpoint失败，记录错误并返回nil
		utils.Log.Printf("从etcd获取服务端点失败: %v", err)
		return nil
	}

	// 构造返回的endpoint列表
	endpoints := make([]string, 0, len(getResponse.Kvs))
	for _, kv := range getResponse.Kvs {
		// 从key中提取endpoint
		path := strings.Split(string(kv.Key), "/")
		endpoints = append(endpoints, path[len(path)-1])
	}

	// 记录获取到的服务endpoint
	utils.Log.Printf("最新的服务端点: %v", endpoints)
	return endpoints
}

// GetServiceEndpoint 根据负载均衡策略从服务端点中选择一个。
// 通过调用负载均衡策略的Take方法，从获取的服务端点列表中选择一个。
//
// 参数:
//   - service: 微服务的名称。
//
// 返回值:
//   - string: 选择的服务端点地址。
func (hub *EtcdServiceHub) GetServiceEndpoint(service string) string {
	// 获取指定服务的所有端点
	endpoints := hub.GetServiceEndpoints(service)
	// 使用负载均衡策略选择一个端点
	return hub.loadBalancer.Take(endpoints)
}

// Close 关闭etcd客户端连接。
// 释放etcd客户端占用的资源，并记录关闭连接的状态。
//
// 返回值:
//   - 无
func (hub *EtcdServiceHub) Close() {
	// 尝试关闭etcd客户端连接
	err := hub.client.Close()
	if err != nil {
		// 如果关闭连接失败，记录错误日志
		utils.Log.Printf("关闭etcd客户端连接失败: %v", err)
	}
}
