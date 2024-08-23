package main

import (
	"criker-search/demo/handler"
	"criker-search/index/kv_db"
	"flag"
	"net/http"
	"strconv"

	"criker-search/utils"
	"github.com/gin-gonic/gin"
)

var (
	mode         = flag.Int("mode", 1, "启动哪类服务。1-standalone web server, 2-grpc index server, 3-distributed web server")
	rebuildIndex = flag.Bool("index", false, "server启动时是否需要重建索引")
	port         = flag.Int("port", 0, "server的工作端口")
	dbPath       = flag.String("dbPath", "", "正排索引数据的存放路径")
	totalWorkers = flag.Int("totalWorkers", 0, "分布式环境中一共有几台index worker")
	workerIndex  = flag.Int("workerIndex", 0, "本机是第几台index worker(从0开始编号)")
)

var (
	dbType      = kv_db.BOLT                                  // 正排索引使用哪种KV数据库
	csvFile     = utils.RootPath + "demo/data/bili_video.csv" // 原始的数据文件，由它来创建索引
	etcdServers = []string{"127.0.0.1:2379"}                  // etcd集群的地址
)

// StartGin 启动 Gin Web 服务器
func StartGin() {
	// 创建默认的 Gin 引擎
	engine := gin.Default()
	// 设置 Gin 运行模式为 Release 模式
	gin.SetMode(gin.ReleaseMode)
	// 设置静态文件路径
	engine.Static("/js", "demo/views/js")
	engine.Static("/css", "demo/views/css")
	engine.Static("/img", "demo/views/img")
	// 加载 HTML 文件
	engine.LoadHTMLFiles("demo/views/search.html", "demo/views/up_search.html")
	// 使用全局中间件
	engine.Use(handler.GetUserInfo)
	// 定义视频分类数组
	classes := [...]string{
		"资讯", "社会", "热点", "生活", "知识", "环球", "游戏", "综合", "日常", "影视", "科技", "编程",
	}
	// 设置路由和处理函数
	engine.GET("/", func(ctx *gin.Context) {
		ctx.HTML(http.StatusOK, "search.html", classes)
	})
	engine.GET("/up", func(ctx *gin.Context) {
		ctx.HTML(http.StatusOK, "up_search.html", classes)
	})
	// 设置 POST 请求路由
	engine.POST("/search", handler.SearchAll)
	engine.POST("/up_search", handler.SearchByAuthor)
	// 启动服务器，监听指定端口
	engine.Run("127.0.0.1:" + strconv.Itoa(*port))
}

// main 程序入口函数
func main() {
	flag.Parse()

	switch *mode {
	case 1, 3:
		// 1：单机模式，索引功能嵌套在 Web 服务器内部。
		// 3：分布式模式，Web 服务器内持有一个哨兵，通过哨兵访问各个 gRPC Index 服务器。
		WebServerMain(*mode)
		StartGin()
	case 2:
		// 2：以 gRPC 服务器的方式启动索引服务 IndexWorker
		GrpcIndexerMain()
	}
}

// go run ./demo/main -mode=1 -index=true -port=5678 -dbPath=data/local_db/video_bolt
// go run ./demo/main -mode=2 -index=true -port=5600 -dbPath=data/local_db/video_bolt -totalWorkers=2 -workerIndex=0
// go run ./demo/main -mode=2 -index=true -port=5601 -dbPath=data/local_db/video_bolt -totalWorkers=2 -workerIndex=1
// go run ./demo/main -mode=3 -index=true -port=5678
