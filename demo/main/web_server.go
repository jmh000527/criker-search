package main

import (
	"os"
	"os/signal"
	"syscall"

	"criker-search/demo"
	"criker-search/demo/handler"
	"criker-search/index_service"
)

// WebServerInit 初始化 Web 服务器，根据模式选择不同的索引初始化方式
func WebServerInit(mode int) {
	switch mode {
	case 1:
		// 模式 1：单机索引
		standaloneIndexer := new(index_service.Indexer)
		// 初始化索引
		if err := standaloneIndexer.Init(50000, dbType, *dbPath); err != nil {
			panic(err)
		}
		if *rebuildIndex {
			// 从 CSV 文件重建索引
			demo.BuildIndexFromFile(csvFile, standaloneIndexer, 0, 0)
		} else {
			// 从正排索引文件加载索引
			standaloneIndexer.LoadFromIndexFile()
		}
		// 将索引器分配给处理程序
		handler.Indexer = standaloneIndexer
	case 3:
		// 模式 3：分布式索引
		handler.Indexer = index_service.NewSentinel(etcdServers)
	default:
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
