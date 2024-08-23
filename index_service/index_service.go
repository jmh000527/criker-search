package index_service

import (
	"context"
	"fmt"
	"github.com/jmh000527/criker-search/index_service/service_hub"
	"github.com/jmh000527/criker-search/types"
	"github.com/jmh000527/criker-search/utils"
	"strconv"
	"time"
)

const (
	IndexService = "index_service"
)

// IndexServiceWorker 代表一个gRPC服务器，负责处理索引相关的服务请求。
// 它包括正排索引和倒排索引的管理，以及与服务注册中心的交互。
type IndexServiceWorker struct {
	Indexer  *LocalIndexer          // 正排索引和倒排索引的组合，用于处理文档的索引和搜索
	hub      service_hub.ServiceHub // 服务注册和发现相关的配置，负责服务的注册、注销和发现
	selfAddr string                 // 当前服务实例的地址，用于注册到服务中心和服务发现
}

// Init 初始化索引服务。
// 该方法负责初始化IndexServiceWorker的索引管理器，并设置相关的数据库类型和数据目录。
//
// 参数:
//   - DocNumEstimate: 预计文档数量，用于初始化倒排索引。
//   - dbtype: 数据库类型，决定使用哪种数据库存储索引数据。
//   - DataDir: 数据目录，数据库文件存放的路径。
//
// 返回值:
//   - error: 如果初始化过程中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) Init(DocNumEstimate int, dbtype int, DataDir string) error {
	// 创建一个新的Indexer实例
	w.Indexer = new(LocalIndexer)
	// 初始化Indexer实例，并传递文档数量估计、数据库类型和数据目录
	return w.Indexer.Init(DocNumEstimate, dbtype, DataDir)
}

// RegisterService 注册服务到etcd。如果提供了etcdServers，则创建EtcdServiceHub并注册服务。
// 如果etcdServers为空，则表示使用单机模式，不进行服务注册。
//
// 参数:
//   - etcdServers: etcd服务器地址列表。如果为空，则表示不进行服务注册。
//   - servicePort: 服务端口号。必须大于1024。
//
// 返回值:
//   - error: 如果传入的端口号无效或服务注册过程中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) RegisterService(etcdServers []string, servicePort int) error {
	// 检查是否需要注册服务到etcd
	if len(etcdServers) > 0 {
		// 验证服务端口号是否合法
		if servicePort <= 1024 {
			return fmt.Errorf("无效的服务端口号 %d，服务端口必须大于1024", servicePort)
		}

		// 获取本地IP地址
		localIP, err := utils.GetLocalIP()
		if err != nil {
			return fmt.Errorf("获取本地IP地址失败: %v", err)
		}

		// 单机模式下，将本地IP写死为127.0.0.1
		localIP = "127.0.0.1"
		w.selfAddr = localIP + ":" + strconv.Itoa(servicePort)

		// 设置心跳频率
		var heartbeatFrequency int64 = 3

		// 获取EtcdServiceHub实例（单例模式）
		hub := service_hub.GetServiceHub(etcdServers, heartbeatFrequency)

		// 注册服务到etcd，初始时租约ID为0
		leaseID, err := hub.RegisterService(IndexService, w.selfAddr, 0)
		if err != nil {
			return fmt.Errorf("服务注册失败: %v", err)
		}

		// 设置hub
		w.hub = hub

		// 启动一个协程，定期续约服务租约
		go func() {
			for {
				_, err := hub.RegisterService(IndexService, w.selfAddr, leaseID)
				if err != nil {
					utils.Log.Printf("续约服务租约失败，租约ID: %v, 错误: %v", leaseID, err)
				}
				// 心跳间隔时间稍短于最大超时时间
				time.Sleep(time.Duration(heartbeatFrequency)*time.Second - 100*time.Millisecond)
			}
		}()
	}
	return nil
}

// LoadFromIndexFile 从索引文件中加载数据。在系统重启后，可以通过此方法从持久化的索引文件中恢复数据。
//
// 返回值:
//   - int: 加载成功的文档数量。如果加载过程中发生错误，则返回0。
func (w *IndexServiceWorker) LoadFromIndexFile() int {
	return w.Indexer.LoadFromIndexFile()
}

// Close 关闭索引服务。如果服务在etcd中注册过，则需要注销服务；否则只需要关闭索引。
//
// 返回值:
//   - error: 如果在注销服务或关闭索引过程中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) Close() error {
	// 检查是否需要注销服务
	if w.hub != nil {
		// 注销服务
		err := w.hub.UnregisterService(IndexService, w.selfAddr)
		if err != nil {
			utils.Log.Printf("注销服务失败，服务地址: %v, 错误: %v", w.selfAddr, err)
			return err
		}
		utils.Log.Printf("注销服务成功，服务地址: %v", w.selfAddr)
	}

	// 关闭索引
	return w.Indexer.Close()
}

// DeleteDoc 从索引中删除文档。根据提供的文档ID删除对应的文档。
//
// 参数:
//   - ctx: 上下文，用于处理请求的生命周期和取消操作。
//   - docId: 包含要删除的文档ID。
//
// 返回值:
//   - *AffectedCount: 删除操作影响的文档数量。
//   - error: 如果删除操作中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	// 调用Indexer的DeleteDoc方法删除文档，并返回影响的文档数量
	return &AffectedCount{
		Count: int32(w.Indexer.DeleteDoc(docId.DocId)),
	}, nil
}

// AddDoc 向索引中添加文档。如果文档已经存在，会先删除旧文档再添加新文档。
//
// 参数:
//   - ctx: 上下文，用于处理请求的生命周期和取消操作。
//   - doc: 要添加的文档对象。
//
// 返回值:
//   - *AffectedCount: 添加操作影响的文档数量。
//   - error: 如果添加操作中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	// 调用Indexer的AddDoc方法添加文档，并返回影响的文档数量
	n, err := w.Indexer.AddDoc(*doc)
	return &AffectedCount{
		Count: int32(n),
	}, err
}

// Search 执行检索操作，返回符合查询条件的文档列表。
//
// 参数:
//   - ctx: 上下文，用于处理请求的生命周期和取消操作。
//   - request: 包含检索查询的请求对象。
//
// 返回值:
//   - *SearchResult: 包含检索结果的文档列表。
//   - error: 如果检索操作中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResult, error) {
	// 调用Indexer的Search方法进行检索，并返回检索结果
	result := w.Indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResult{
		Results: result,
	}, nil
}

// Count 返回索引中当前文档的数量。
//
// 参数:
//   - ctx: 上下文，用于处理请求的生命周期和取消操作。
//   - request: 包含计数请求的对象。
//
// 返回值:
//   - *AffectedCount: 当前索引中的文档数量。
//   - error: 如果计数操作中发生错误，则返回相应的错误。
func (w *IndexServiceWorker) Count(ctx context.Context, request *CountRequest) (*AffectedCount, error) {
	// 调用Indexer的Count方法获取文档数量，并返回结果
	return &AffectedCount{
		Count: int32(w.Indexer.Count()),
	}, nil
}
