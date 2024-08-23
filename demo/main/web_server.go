package main

import (
	"github.com/jmh000527/criker-search/index_service"
	"os"
	"os/signal"
	"syscall"

	"github.com/jmh000527/criker-search/demo"
	"github.com/jmh000527/criker-search/demo/handler"
)

// WebServerInit 初始化 Web 服务器，根据传入的模式选择不同的索引初始化方式
//
// mode: 初始化模式，1 表示单机索引，3 表示分布式索引
func WebServerInit(mode int) {
	switch mode {
	case 1:
		// 模式 1：单机索引
		// 创建一个新的索引器实例
		standaloneIndexer := new(index_service.LocalIndexer)

		// 初始化索引，参数为估计的文档数量，数据库类型，和数据库路径
		if err := standaloneIndexer.Init(50000, dbType, *dbPath); err != nil {
			// 初始化失败，终止程序并报告错误
			panic(err)
		}

		if *rebuildIndex {
			// 如果指定重建索引，从 CSV 文件重建索引
			demo.BuildIndexFromFile(csvFile, standaloneIndexer, 0, 0)
		} else {
			// 否则从正排索引文件加载索引
			standaloneIndexer.LoadFromIndexFile()
		}

		// 将索引器实例分配给处理程序，以便处理请求时使用
		handler.Indexer = standaloneIndexer

	case 3:
		// 模式 3：分布式索引
		// 创建一个新的 Sentinel 实例作为分布式索引器
		handler.Indexer = index_service.NewSentinel(etcdServers)

	default:
		// 如果传入的模式无效，终止程序并报告错误
		panic("invalid mode")
	}
}

// WebServerTeardown 在收到终止信号时优雅地关闭Web服务器。
func WebServerTeardown() {
	// 创建一个通道用于接收操作系统信号。
	sigCh := make(chan os.Signal, 1)
	// 当接收到中断（SIGINT）或终止（SIGTERM）信号时，通知该通道。
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	// 阻塞等待接收信号。
	<-sigCh
	// 当收到终止信号时关闭索引器，确保干净地关闭。
	handler.Indexer.Close()
	// 以状态码0退出程序，表示成功终止。
	os.Exit(0)
}

// WebServerMain 启动 Web 服务器的主函数
func WebServerMain(mode int) {
	// 异步执行服务器关闭处理
	go WebServerTeardown()
	// 初始化 Web 服务器
	WebServerInit(mode)
}
