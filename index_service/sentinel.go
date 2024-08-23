package index_service

import (
	"context"
	"criker-search/index_service/service_hub"
	"criker-search/types"
	"criker-search/utils"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel 哨兵前台，与外部系统对接的接口。
type Sentinel struct {
	hub      service_hub.ServiceHub // 从 Hub 中获取 IndexServiceWorker 的集合。可以直接访问 ServiceHub，也可能通过代理模式进行访问。
	connPool sync.Map               // 与各个 IndexServiceWorker 建立的 gRPC 连接池。缓存连接以避免每次请求都重新建立连接，提升效率。
}

// NewSentinel 创建并返回一个 Sentinel 实例。
//
// 参数:
//   - etcdServers: 一个字符串数组，包含了 etcd 服务器的地址。
//
// 返回值:
//   - *Sentinel: 一个新的 Sentinel 实例。
func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{
		// hub: GetServiceHub(etcdServers, 10), // 直接访问 ServiceHub
		hub:      service_hub.GetServiceHubProxy(etcdServers, 3, 100), // 使用代理模式访问 ServiceHub
		connPool: sync.Map{},                                          // 初始化 gRPC 连接池
	}
}

// GetGrpcConn 向指定的 endpoint 建立 gRPC 连接。
// 如果连接已经存在于缓存中且状态可用，则直接返回缓存的连接。
// 如果连接状态不可用或不存在，则重新建立连接并存储到缓存中。
//
// 参数:
//   - endpoint: 要连接的 gRPC 服务的地址。
//
// 返回值:
//   - *grpc.ClientConn: 返回与 endpoint 建立的 gRPC 连接，如果连接失败则返回 nil。
func (sentinel *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	v, exists := sentinel.connPool.Load(endpoint)
	// 连接缓存中存在
	if exists {
		conn := v.(*grpc.ClientConn)
		// 如果连接状态不可用，则从连接缓存中删除
		if conn.GetState() == connectivity.TransientFailure || conn.GetState() == connectivity.Shutdown {
			utils.Log.Printf("连接到 endpoint %s 的状态为 %s", endpoint, conn.GetState().String())
			conn.Close()
			sentinel.connPool.Delete(endpoint)
		} else {
			return conn
		}
	}

	// 连接到服务，控制连接超时
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// 获取 gRPC 连接
	// grpc.Dial 是异步连接，连接状态为正在连接。
	// 如果设置了 grpc.WithBlock 选项，则会阻塞等待（等待握手成功）。
	// 需要注意的是，当未设置 grpc.WithBlock 时，ctx 超时控制对其无任何效果。
	grpcConn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		utils.Log.Printf("连接到 %s 的 gRPC 失败，错误: %s", endpoint, err.Error())
		return nil
	}
	utils.Log.Printf("连接到 %s 的 gRPC 成功", endpoint)
	// 将 gRPC 连接缓存到连接池中
	sentinel.connPool.Store(endpoint, grpcConn)
	return grpcConn
}

// AddDoc 向集群中的 IndexService 添加文档。如果文档已存在，会先删除旧文档再添加新文档。
//
// 参数:
//   - doc: 要添加的文档，类型为 types.Document。
//
// 返回值:
//   - int: 成功添加的文档数量。
//   - error: 如果在添加文档时出现错误，返回相应的错误信息。
func (sentinel *Sentinel) AddDoc(doc types.Document) (int, error) {
	// 根据负载均衡策略，选择一个 IndexService 节点，将文档添加到该节点
	endpoint := sentinel.hub.GetServiceEndpoint(IndexService)
	if len(endpoint) == 0 {
		return 0, fmt.Errorf("未找到服务 %s 的有效节点", IndexService)
	}
	// 创建到该节点的 gRPC 连接
	grpcConn := sentinel.GetGrpcConn(endpoint)
	if grpcConn == nil {
		return 0, fmt.Errorf("连接到 %s 的 gRPC 失败", endpoint)
	}
	// 创建 gRPC 客户端并进行调用
	client := NewIndexServiceClient(grpcConn)
	affected, err := client.AddDoc(context.Background(), &doc)
	if err != nil {
		return 0, err
	}
	utils.Log.Printf("成功向 worker %s 添加 %d 个文档", endpoint, affected.Count)
	return int(affected.Count), nil
}

// DeleteDoc 从集群中删除与 docId 对应的文档，返回成功删除的文档数量（通常不会超过 1）。
//
// 参数:
//   - docId: 要删除的文档的唯一标识符。
//
// 返回值:
//   - int: 成功删除的文档数量。
func (sentinel *Sentinel) DeleteDoc(docId string) int {
	// 获取该服务的所有 endpoints
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}
	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		// 并行地向各个 IndexServiceWorker 删除对应的 docId 的文档。
		// 正常情况下，只有一个 worker 上有该文档。
		go func(endpoint string) {
			defer wg.Done()
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn == nil {
				utils.Log.Printf("连接到 %s 的 gRPC 失败", endpoint)
				return
			}
			client := NewIndexServiceClient(grpcConn)
			affected, err := client.DeleteDoc(context.Background(), &DocId{docId})
			if err != nil {
				utils.Log.Printf("从 worker %s 删除文档 %s 失败，错误: %s", endpoint, docId, err)
				return
			}
			if affected.Count > 0 {
				atomic.AddInt32(&n, affected.Count)
				utils.Log.Printf("从 worker %s 删除文档 %s 成功", endpoint, docId)
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}

// Search 执行检索操作，并返回文档列表。
//
// 参数:
//   - query: 指定的检索查询条件，类型为 *types.TermQuery。
//   - onFlag: 开启的标志位，类型为 uint64。
//   - offFlag: 关闭的标志位，类型为 uint64。
//   - orFlags: OR 标志位的切片，类型为 []uint64。
//
// 返回值:
//   - []*types.Document: 经过检索的文档列表，可能为空。
//
// 详细描述:
//  1. 从服务中心获取所有的 endpoints。
//  2. 使用 goroutines 并行地对每个 endpoint 执行检索操作。
//  3. 将每个检索结果发送到 resultChan 通道中。
//  4. 在另一个 goroutine 中，从 resultChan 通道中读取结果，并将其存储在 docs 切片中。
//  5. 等待所有的检索操作完成后，关闭 resultChan，并等待从 resultChan 中读取完所有结果。
//  6. 返回存储的文档列表。
func (sentinel *Sentinel) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	// 获取该服务所有的 endpoints
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return nil
	}

	// 用于存储检索结果的切片和通道
	docs := make([]*types.Document, 0, 1000)
	resultChan := make(chan *types.Document, 1000)

	// 使用 WaitGroup 并行开启协程去每个 endpoint 执行检索操作
	var wg sync.WaitGroup
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()

			// 获取 gRPC 连接
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn == nil {
				utils.Log.Printf("连接到 %s 的 gRPC 连接失败", endpoint)
				return
			}
			client := NewIndexServiceClient(grpcConn)

			// 执行检索请求
			searchResult, err := client.Search(context.Background(), &SearchRequest{
				Query:   query,
				OnFlag:  onFlag,
				OffFlag: offFlag,
				OrFlags: orFlags,
			})
			if err != nil {
				utils.Log.Printf("向 worker %s 执行查询 %s 失败，错误: %s", endpoint, query, err)
				return
			}
			if len(searchResult.Results) > 0 {
				utils.Log.Printf("向 worker %s 执行查询 %s 成功，获取到 %v 个文档", endpoint, query, len(searchResult.Results))
				for _, result := range searchResult.Results {
					resultChan <- result
				}
			}
		}(endpoint)
	}

	// 启动另一个 goroutine 从 resultChan 中获取结果
	signalChan := make(chan struct{})
	go func() {
		for doc := range resultChan {
			docs = append(docs, doc)
		}
		// 读取完成，通知主 goroutine
		signalChan <- struct{}{}
	}()

	// 等待所有检索操作完成
	wg.Wait()
	// 关闭 resultChan 通道
	close(resultChan)
	// 等待结果读取完毕
	<-signalChan

	return docs
}

// Count 获取所有服务中的搜索条目数量。
//
// 参数:
//   - 无参数。
//
// 返回值:
//   - int: 所有服务中的文档总数量。
//
// 详细描述:
//  1. 从服务中心获取所有的 endpoints。
//  2. 使用 goroutines 并行地对每个 endpoint 执行计数操作。
//  3. 将每个 worker 中的文档数量累加到总计数中。
//  4. 等待所有计数操作完成后，返回文档总数量。
func (sentinel *Sentinel) Count() int {
	var n int32
	// 获取所有服务的 endpoints
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}

	var wg sync.WaitGroup
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			// 获取 gRPC 连接
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn != nil {
				client := NewIndexServiceClient(grpcConn)
				// 执行计数请求
				affected, err := client.Count(context.Background(), new(CountRequest))
				if err != nil {
					utils.Log.Printf("从 worker %s 获取文档数量失败: %s", endpoint, err)
				}
				if affected.Count > 0 {
					// 累加计数
					atomic.AddInt32(&n, affected.Count)
					utils.Log.Printf("worker %s 共有 %d 个文档", endpoint, affected.Count)
				}
			}
		}(endpoint)
	}
	// 等待所有计数操作完成
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}

// Close 关闭各个grpc client连接，关闭etcd client连接
func (sentinel *Sentinel) Close() (err error) {
	sentinel.connPool.Range(func(key, value any) bool {
		conn := value.(*grpc.ClientConn)
		err = conn.Close()
		return true
	})
	sentinel.hub.Close()
	return
}
