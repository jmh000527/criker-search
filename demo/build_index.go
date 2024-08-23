package demo

import (
	indexer "criker-search/index_service"
	"criker-search/types"
	"criker-search/utils"
	"encoding/csv"
	"github.com/gogo/protobuf/proto"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// BuildIndexFromFile 将CSV文件中的视频信息写入索引。
//
// 参数:
//   - csvFile: CSV文件的路径。
//   - indexer: 索引接口，用于添加文档到索引中。
//   - totalWorkers: 分布式环境中的总worker数量。如果是单机模式，设为0。
//   - workerIndex: 当前worker的索引，从0开始编号。单机模式下不使用此参数。
//
// 返回值: 无返回值
// 注意事项: 如果使用分布式模式，每个worker只处理一部分数据。
func BuildIndexFromFile(csvFile string, indexer indexer.Indexer, totalWorkers, workerIndex int) {
	file, err := os.Open(csvFile)
	if err != nil {
		utils.Log.Printf("打开CSV文件 %v 失败，错误: %v", csvFile, err)
		return
	}
	defer file.Close()

	location, _ := time.LoadLocation("Asia/Shanghai")
	reader := csv.NewReader(file)
	progress := 0
	for {
		// 读取CSV文件的一行
		record, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				utils.Log.Printf("无法读取CSV文件: %v", err)
			}
			break
		}
		// 如果记录的字段少于10个，跳过该行
		if len(record) < 10 {
			continue
		}

		// 获取视频ID（业务侧ID）
		docId := strings.TrimPrefix(record[0], "https://www.bilibili.com/video/")
		// 在分布式模式下，每个worker只处理特定的视频数据
		if totalWorkers > 0 && int(farmhash.Hash32WithSeed([]byte(docId), 0))%totalWorkers != workerIndex {
			continue
		}

		// 构建BiliVideo实体
		video := &BiliVideo{
			Id:     strings.TrimPrefix(record[0], "https://www.bilibili.com/video/"),
			Title:  record[1],
			Author: record[3],
		}

		// 解析发布日期
		if len(record[2]) > 4 {
			t, err := time.ParseInLocation("2006/1/2 15:4", record[2], location)
			if err != nil {
				utils.Log.Printf("解析时间 %s 失败: %s", record[2], err)
			} else {
				video.PostTime = t.Unix()
			}
		}

		// 解析视频的其他属性
		n, _ := strconv.Atoi(record[4])
		video.View = int32(n)
		n, _ = strconv.Atoi(record[5])
		video.Like = int32(n)
		n, _ = strconv.Atoi(record[6])
		video.Coin = int32(n)
		n, _ = strconv.Atoi(record[7])
		video.Favorite = int32(n)
		n, _ = strconv.Atoi(record[8])
		video.Share = int32(n)

		// 解析关键字
		keywords := strings.Split(record[9], ",")
		if len(keywords) > 0 {
			for _, word := range keywords {
				word = strings.TrimSpace(word)
				if len(word) > 0 {
					video.Keywords = append(video.Keywords, strings.ToLower(word))
				}
			}
		}

		// 将视频信息添加到索引中
		AddVideo2Index(video, indexer)
		progress++

		// 每处理100条记录，输出进度
		if progress%100 == 0 {
			utils.Log.Printf("索引进度: %d\n", progress)
		}
	}

	utils.Log.Printf("索引构建完成，共添加了 %d 个文档", progress)
}

// AddVideo2Index 将视频信息添加或更新至索引。
//
// 参数:
// - video: 包含视频信息的BiliVideo对象。
// - indexer: 实现了IIndexer接口的索引器实例。
func AddVideo2Index(video *BiliVideo, indexer indexer.Indexer) {
	// 构建Document对象，将视频ID赋值给文档ID
	doc := types.Document{
		Id: video.Id,
	}

	// 将BiliVideo对象序列化为字节数组
	docBytes, err := proto.Marshal(video)
	if err != nil {
		utils.Log.Printf("序列化视频信息失败: %v", err)
		return
	}
	doc.Bytes = docBytes

	// 构建关键词列表
	keywords := make([]*types.Keyword, 0, len(video.Keywords))
	// 遍历视频关键词，将每个关键词添加到关键词列表中
	for _, word := range video.Keywords {
		keywords = append(keywords, &types.Keyword{
			Field: "content",
			Word:  strings.ToLower(word),
		})
	}
	if len(video.Author) > 0 {
		keywords = append(keywords, &types.Keyword{
			Field: "author",
			Word:  strings.ToLower(strings.TrimSpace(video.Author)),
		})
	}
	doc.Keywords = keywords

	// 计算视频的特征位
	doc.BitsFeature = GetClassBits(video.Keywords)

	// 将文档添加或更新到索引中
	_, err = indexer.AddDoc(doc)
	if err != nil {
		utils.Log.Printf("无法添加文档, 错误: %v", err)
	}
}
