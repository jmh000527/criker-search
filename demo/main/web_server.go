package main

import (
	"os"
	"os/signal"
	"syscall"

	"criker-search/demo"
	"criker-search/demo/handler"
	"criker-search/index_service"
)

func WebServerInit(mode int) {
	switch mode {
	case 1:
		// 新建单机索引
		standaloneIndexer := new(index_service.Indexer)
		// 初始化索引
		if err := standaloneIndexer.Init(50000, dbType, *dbPath); err != nil {
			panic(err)
		}
		if *rebuildIndex {
			// 从csv文件重建索引
			demo.BuildIndexFromFile(csvFile, standaloneIndexer, 0, 0)
		} else {
			// 直接从正排索引文件里加载
			standaloneIndexer.LoadFromIndexFile()
		}
		handler.Indexer = standaloneIndexer
	case 3:
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

func WebServerMain(mode int) {
	go WebServerTeardown()
	WebServerInit(mode)
}
