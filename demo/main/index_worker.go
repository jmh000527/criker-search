package main

import (
	"criker-search/demo"
	"criker-search/index_service"
	"criker-search/utils"
	"fmt"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

var service *index_service.IndexServiceWorker // IndexWorker 是一个 gRPC 服务器

// GrpcIndexerInit 初始化 gRPC 索引服务
func GrpcIndexerInit() {
	// 监听本地端口
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(*port))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	service = new(index_service.IndexServiceWorker)

	// 初始化索引
	err = service.Init(50000, dbType, *dbPath+"_part"+strconv.Itoa(*workerIndex))
	if err != nil {
		panic(err)
	}
	// 是否重建索引
	if *rebuildIndex {
		utils.Log.Printf("totalWorkers=%d, workerIndex=%d", *totalWorkers, *workerIndex)
		// 重建索引
		demo.BuildIndexFromFile(csvFile, service.Indexer, *totalWorkers, *workerIndex)
	} else {
		// 从正排索引文件加载
		service.Indexer.LoadFromIndexFile()
	}
	// 注册服务实现
	index_service.RegisterIndexServiceServer(server, service)
	// 启动服务
	utils.Log.Printf("start grpc server on port %d\n", *port)
	// 向注册中心注册服务并周期性续期
	err = service.RegisterService(etcdServers, *port)
	if err != nil {
		panic(err)
	}
	err = server.Serve(listener)
	if err != nil {
		service.Close()
		fmt.Printf("start grpc server on port %d failed, err: %s\n", *port, err)
	}

}

// GrpcIndexerTeardown 处理服务终止信号
func GrpcIndexerTeardown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	service.Close() // 接收到终止信号时关闭索引
	os.Exit(0)      // 退出程序
}

// GrpcIndexerMain 启动 gRPC 服务并处理终止信号
func GrpcIndexerMain() {
	go GrpcIndexerTeardown() // 启动协程处理终止信号
	GrpcIndexerInit()        // 初始化并启动 gRPC 服务
}
