package index_service

import (
	"context"
	"criker-search/types"
	"criker-search/utils"
	"fmt"
	"strconv"
	"time"
)

const (
	IndexService = "index_service"
)

// IndexServiceWorker 一个grpc server
type IndexServiceWorker struct {
	Indexer  *Indexer        // 把正排和倒排索引放到一起
	hub      *EtcdServiceHub // 服务注册相关的配置
	selfAddr string
}

// Init 初始化索引。若传入的etcdServers不为空，则需要创建hub，向注册中心注册自己；否则为单机模式
func (w *IndexServiceWorker) Init(docNumEstimate int, dbType int, dataDir string, etcdServers []string, servicePort int) error {
	w.Indexer = new(Indexer)
	err := w.Indexer.Init(docNumEstimate, dbType, dataDir)
	if err != nil {
		panic(err)
	}
	// 如果Init()传入的etcdServers不为空，需要创建hub，向注册中心注册自己
	if len(etcdServers) > 0 {
		if servicePort <= 1024 {
			return fmt.Errorf("invalid service port %d, service port must be larger than 1024", servicePort)
		}
		localIP, err := utils.GetLocalIP()
		if err != nil {
			panic(err)
		}
		// 单机模拟分布式时，本地IP写死为127.0.0.1
		localIP = "127.0.0.1"
		w.selfAddr = localIP + ":" + strconv.Itoa(servicePort)
		var heartbeatFrequency int64 = 3
		// 单例模式获取EtcdServiceHub
		hub := GetServiceHub(etcdServers, heartbeatFrequency)
		// 注册服务
		leaseID, err := hub.RegisterService(IndexService, w.selfAddr, 0)
		if err != nil {
			panic(err)
		}
		w.hub = hub
		// 开启心跳协程，周期性向etcd注册自己
		go func() {
			for {
				_, err := hub.RegisterService(IndexService, w.selfAddr, leaseID)
				if err != nil {
					utils.Log.Printf("register lease id, %v failed", leaseID)
				}
				// 比心跳最大超时时间稍短
				time.Sleep(time.Duration(heartbeatFrequency)*time.Second - 100*time.Millisecond)
			}
		}()
	}
	return nil
}

// LoadFromIndexFile 系统重启时，直接从索引文件里加载数据
func (w *IndexServiceWorker) LoadFromIndexFile() int {
	return w.Indexer.LoadFromIndexFile()
}

// Close 关闭索引
func (w *IndexServiceWorker) Close() error {
	// 如果Init()传入的etcdServers不为空，即创建了hub，需要向etcd注销服务
	if w.hub != nil {
		err := w.hub.UnregisterService(IndexService, w.selfAddr)
		if err != nil {
			utils.Log.Printf("unregister service %v failed", w.selfAddr)
			return err
		}
		utils.Log.Printf("unregister service %v success", w.selfAddr)
	}
	// Init()传入的etcdServers为空，只需要关闭索引
	return w.Indexer.Close()
}

// DeleteDoc 从索引上删除文档
func (w *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	return &AffectedCount{
		Count: int32(w.Indexer.DeleteDoc(docId.DocId)),
	}, nil
}

// AddDoc 向索引中添加文档（如果已存在，会先删除）
func (w *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	n, err := w.Indexer.AddDoc(*doc)
	return &AffectedCount{
		Count: int32(n),
	}, err
}

// Search 检索，返回文档列表
func (w *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResult, error) {
	result := w.Indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResult{
		Results: result,
	}, nil
}

// Count 索引里有几个文档
func (w *IndexServiceWorker) Count(ctx context.Context, request *CountRequest) (*AffectedCount, error) {
	return &AffectedCount{
		Count: int32(w.Indexer.Count()),
	}, nil
}
