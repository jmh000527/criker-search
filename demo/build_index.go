package demo

import (
	index_service "criker-search/index_service/interface"
	"criker-search/types"
	"criker-search/utils"
	"encoding/csv"
	"github.com/gogo/protobuf/proto"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// BuildIndexFromFile 把CSV文件中的视频信息全部写入索引。
// totalWorkers: 分布式环境中一共有几台index worker，workerIndex本机是第几台worker(从0开始编号)。单机模式下把totalWorkers置0即可
func BuildIndexFromFile(csvFile string, indexer index_service.IIndexer, totalWorkers, workerIndex int) {
	file, err := os.Open(csvFile)
	if err != nil {
		utils.Log.Printf("open csv file %v failed, err: %v", csvFile, err)
		return
	}
	defer file.Close()

	location, _ := time.LoadLocation("Asia/Shanghai")
	reader := csv.NewReader(file)
	progress := 0
	for {
		// 读取CSV文件的一行，record是个切片
		record, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				utils.Log.Printf("Can not read CSV file: %v", err)
			}
			break
		}
		if len(record) < 10 {
			continue
		}
		video := &BiliVideo{
			Id:     strings.TrimPrefix(record[0], "https://www.bilibili.com/video/"),
			Title:  record[1],
			Author: record[3],
		}
		if len(record[2]) > 4 {
			t, err := time.ParseInLocation("2006/1/2 15:4", record[2], location)
			if err != nil {
				utils.Log.Printf("parse time %s failed: %s", record[2], err)
			} else {
				video.PostTime = t.Unix()
			}
		}
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
		keywords := strings.Split(record[9], ",")
		if len(keywords) > 0 {
			for _, word := range keywords {
				word = strings.TrimSpace(word)
				if len(word) > 0 {
					video.Keywords = append(video.Keywords, strings.ToLower(word))
				}
			}
		}
		AddVideo2Index(video, indexer) // 构建好BiliVideo实体，写入索引
		progress++
		// 输出构建索引的进度
		if progress%100 == 0 {
			utils.Log.Printf("indexing progress=%d\n", progress)
		}
	}
	utils.Log.Printf("indexing finished, added %d documents", progress)
}

// AddVideo2Index 把一条视频信息写入索引（可能是create，也可能是update）。实时更新索引时可调该函数
func AddVideo2Index(video *BiliVideo, indexer index_service.IIndexer) {
	doc := types.Document{
		Id: video.Id,
	}
	docBytes, err := proto.Marshal(video)
	if err != nil {
		utils.Log.Printf("marshal video failed: %v", err)
		return
	}
	doc.Bytes = docBytes
	keywords := make([]*types.Keyword, 0, len(video.Keywords))
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
	doc.BitsFeature = GetClassBits(video.Keywords)
	_, err = indexer.AddDoc(doc)
	if err != nil {
		utils.Log.Printf("Can not add document, err: %v", err)
	}
}
