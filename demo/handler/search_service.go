package handler

import (
	"criker-search/demo"
	index_service "criker-search/index_service/interface"
	"criker-search/types"
	"criker-search/utils"
	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"log"
	"net/http"
	"strings"
)

var Indexer index_service.IIndexer

// cleanKeywords 接收一个字符串切片，并返回一个清理后的字符串切片。
// 清理过程包括去除每个字符串的前后空白字符，将其转换为小写，并排除空字符串。
func cleanKeywords(words []string) []string {
	// 创建一个新的字符串切片，用于存储清理后的关键词。初始容量设置为输入切片的长度。
	keywords := make([]string, 0, len(words))
	for _, w := range words {
		// 去除字符串前后的空白字符，并将其转换为小写。
		word := strings.TrimSpace(strings.ToLower(w))
		// 如果字符串长度大于0（非空字符串），则将其添加到关键词切片中。
		if len(word) > 0 {
			keywords = append(keywords, word)
		}
	}
	return keywords
}

// Search 搜索接口
func Search(ctx *gin.Context) {
	var request demo.SearchRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		log.Printf("bind request parameter failed: %s", err)
		ctx.String(http.StatusBadRequest, "invalid json")
		return
	}

	keywords := cleanKeywords(request.Keywords)
	if len(keywords) == 0 && len(request.Author) == 0 {
		ctx.String(http.StatusBadRequest, "invalid keywords, keywords and author can not be empty both")
		return
	}
	// 构建搜索条件
	query := new(types.TermQuery)
	// 满足关键词
	if len(keywords) > 0 {
		for _, word := range keywords {
			query = query.And(types.NewTermQuery("content", word))
		}
	}
	// 满足作者
	if len(request.Author) > 0 {
		query = query.And(types.NewTermQuery("author", strings.ToLower(request.Author)))
	}
	// 满足类别
	orFlags := []uint64{demo.GetClassBits(request.Classes)}
	// 执行搜索
	docs := Indexer.Search(query, 0, 0, orFlags)
	videos := make([]demo.BiliVideo, 0, len(docs))
	for _, doc := range docs {
		var video demo.BiliVideo
		if err := proto.Unmarshal(doc.Bytes, &video); err == nil {
			// 满足播放量的区间范围
			if video.View >= int32(request.ViewFrom) && (request.ViewTo <= 0 || video.View <= int32(request.ViewTo)) {
				videos = append(videos, video)
			}
		}
	}
	utils.Log.Printf("returning %d videos", len(videos))
	// 把搜索结果以json形式返回给前端
	ctx.JSON(http.StatusOK, videos)
}

//// SearchAll 搜索全站视频
//func SearchAll(ctx *gin.Context) {
//	var request demo.SearchRequest
//	if err := ctx.ShouldBindJSON(&request); err != nil {
//		log.Printf("bind request parameter failed: %s", err)
//		ctx.String(http.StatusBadRequest, "invalid json")
//		return
//	}
//
//	request.Keywords = cleanKeywords(request.Keywords)
//	if len(request.Keywords) == 0 && len(request.Author) == 0 {
//		ctx.String(http.StatusBadRequest, "关键词和作者不能同时为空")
//		return
//	}
//
//	searchCtx := &common.VideoSearchContext{
//		Ctx:     context.Background(),
//		Request: &request,
//		Indexer: Indexer,
//	}
//	searcher := video_search.NewAllVideoSearcher()
//	videos := searcher.Search(searchCtx)
//
//	ctx.JSON(http.StatusOK, videos) // 把搜索结果以json形式返回给前端
//}

//
//// SearchByAuthor up主在后台搜索自己的视频
//func SearchByAuthor(ctx *gin.Context) {
//	var request demo.SearchRequest
//	if err := ctx.ShouldBindJSON(&request); err != nil {
//		log.Printf("bind request parameter failed: %s", err)
//		ctx.String(http.StatusBadRequest, "invalid json")
//		return
//	}
//
//	request.Keywords = cleanKeywords(request.Keywords)
//	if len(request.Keywords) == 0 {
//		ctx.String(http.StatusBadRequest, "关键词不能为空")
//		return
//	}
//
//	userName, ok := ctx.Value("user_name").(string) //从gin.Context里取得userName
//	if !ok || len(userName) == 0 {
//		ctx.String(http.StatusBadRequest, "获取不到登录用户名")
//		return
//	}
//	searchCtx := &common.VideoSearchContext{
//		Ctx:     context.WithValue(context.Background(), common.UN("user_name"), userName), //把userName放到context里
//		Request: &request,
//		Indexer: Indexer,
//	}
//	searcher := video_search.NewUpVideoSearcher()
//	videos := searcher.Search(searchCtx)
//
//	ctx.JSON(http.StatusOK, videos) //把搜索结果以json形式返回给前端
//}
