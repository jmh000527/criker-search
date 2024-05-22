package index_service

import (
	"context"
	indexservice "criker-search/index_service/interface"
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

// Sentinel 哨兵前台，与外部对接
type Sentinel struct {
	hub      indexservice.ServiceHub // 从Hub上获取IndexServiceWorker集合。可能是直接访问ServiceHub，也可能是走代理
	connPool sync.Map                // 与各个IndexServiceWorker建立的gRPC连接。把连接缓存起来，避免每次都重建连接
}

func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{
		// hub: GetServiceHub(etcdServers, 10), //直接访问ServiceHub
		hub:      GetServiceHubProxy(etcdServers, 3, 100), // 使用代理模式
		connPool: sync.Map{},
	}
}

// GetGrpcConn 向endpoint建立gRPC连接
func (sentinel *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	v, exists := sentinel.connPool.Load(endpoint)
	// 连接缓存中存在
	if exists {
		conn := v.(*grpc.ClientConn)
		// 如果连接状态不可用，则从连接缓存中删除
		if conn.GetState() == connectivity.TransientFailure || conn.GetState() == connectivity.Shutdown {
			utils.Log.Printf("connection status to endpoint %s is %s", endpoint, conn.GetState().String())
			conn.Close()
			sentinel.connPool.Delete(endpoint)
		} else {
			return conn
		}
	}
	// 连接到服务，控制连接超时
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	// 获取gRPC连接
	// grpc.Dial是异步连接的，连接状态为正在连接。
	// 但如果设置了 grpc.WithBlock 选项，就会阻塞等待（等待握手成功）。
	// 另外需要注意，当未设置 grpc.WithBlock 时，ctx 超时控制对其无任何效果。
	grpcConn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		utils.Log.Printf("grpc connnection to %s failed, err: %s", endpoint, err.Error())
		return nil
	}
	utils.Log.Printf("grpc connnection to %s success", endpoint)
	// 将gRPC连接和缓存到连接池
	sentinel.connPool.Store(endpoint, grpcConn)
	return grpcConn
}

// AddDoc 向集群中添加文档（如果已存在，会先删除）
func (sentinel *Sentinel) AddDoc(doc types.Document) (int, error) {
	// 根据负载均衡策略，选择一台index worker，把doc添加到它上面去
	endpoint := sentinel.hub.GetServiceEndpoint(IndexService)
	if len(endpoint) == 0 {
		return 0, fmt.Errorf("no endpoint found for service %s", IndexService)
	}
	// 创建到该endpoint的gRPC连接
	grpcConn := sentinel.GetGrpcConn(endpoint)
	if grpcConn == nil {
		return 0, fmt.Errorf("grpc connnection to %s failed", endpoint)
	}
	// 创建给RPC客户端进行gRPC调用
	client := NewIndexServiceClient(grpcConn)
	affected, err := client.AddDoc(context.Background(), &doc)
	if err != nil {
		return 0, err
	}
	utils.Log.Printf("add %d doc to worker %s", affected.Count, endpoint)
	return int(affected.Count), nil
}

// DeleteDoc 从集群上删除docId对应的doc，返回成功删除的doc数（正常情况下不会超过1）
func (sentinel *Sentinel) DeleteDoc(docId string) int {
	// 获取该服务所有的endpoints
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}
	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		// 并行到各个IndexServiceWorker上把docId对应文档删除。正常情况下只有一个worker上有该doc
		go func(endpoint string) {
			defer wg.Done()
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn == nil {
				utils.Log.Printf("grpc connnection to %s failed", endpoint)
				return
			}
			client := NewIndexServiceClient(grpcConn)
			affected, err := client.DeleteDoc(context.Background(), &DocId{docId})
			if err != nil {
				utils.Log.Printf("delete doc %s from worker %s failed, err: %s", docId, endpoint, err)
				return
			}
			if affected.Count > 0 {
				atomic.AddInt32(&n, affected.Count)
				utils.Log.Printf("delete doc %s from worker %s success", docId, endpoint)
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}

// Search 检索，返回文档列表
func (sentinel *Sentinel) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	// 获取该服务所有的endpoints
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return nil
	}
	// 用于获取结果的切片和管道
	docs := make([]*types.Document, 0, 1000)
	resultChan := make(chan *types.Document, 1000)
	// 并行开启协程去每个endpoint搜索
	var wg sync.WaitGroup
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn == nil {
				utils.Log.Printf("grpc connnection to %s failed", endpoint)
				return
			}
			client := NewIndexServiceClient(grpcConn)
			searchResult, err := client.Search(context.Background(), &SearchRequest{
				Query:   query,
				OnFlag:  onFlag,
				OffFlag: offFlag,
				OrFlags: orFlags,
			})
			if err != nil {
				utils.Log.Printf("search query %s to worker %s failed, err: %s", query, endpoint, err)
				return
			}
			if len(searchResult.Results) > 0 {
				utils.Log.Printf("search query %s to worker %s success, get %v doc(s)", query, endpoint, len(searchResult.Results))
				for _, result := range searchResult.Results {
					resultChan <- result
				}
			}
		}(endpoint)
	}
	// 开启另一个携程从管道resultChan内获取结果
	signalChan := make(chan struct{})
	go func() {
		for {
			doc, ok := <-resultChan
			// 通道已经关闭，并且通道中的数据已经被全部读取完毕
			if !ok {
				break
			}
			docs = append(docs, doc)
		}
		// 向无缓冲管道写入数据会阻塞
		signalChan <- struct{}{}
	}()
	wg.Wait()
	// 写入完成之后关闭resultChan
	close(resultChan)
	<-signalChan
	return docs
}

// Count 获取搜索条目数量
func (sentinel *Sentinel) Count() int {
	var n int32
	endpoints := sentinel.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			grpcConn := sentinel.GetGrpcConn(endpoint)
			if grpcConn != nil {
				client := NewIndexServiceClient(grpcConn)
				affected, err := client.Count(context.Background(), new(CountRequest))
				if err != nil {
					utils.Log.Printf("get doc count from worker %s failed: %s", endpoint, err)
				}
				if affected.Count > 0 {
					atomic.AddInt32(&n, affected.Count)
					utils.Log.Printf("worker %s have %d documents", endpoint, affected.Count)
				}
			}
		}(endpoint)
	}
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
