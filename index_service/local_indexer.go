package index_service

import (
	"bytes"
	"encoding/gob"
	"fmt"
	invertedIndex "github.com/jmh000527/criker-search/index/inverted_index"
	kvDb "github.com/jmh000527/criker-search/index/kv_db"
	"github.com/jmh000527/criker-search/types"
	"github.com/jmh000527/criker-search/utils"
	"strings"
	"sync/atomic"
)

// LocalIndexer 使用外观模式将正排索引和倒排索引两个子系统封装在一起。
// 这个结构体提供了一个统一的接口来操作这两个子系统，从而简化了索引操作的复杂性。
//
// 字段:
//   - forwardIndex: 正排索引的数据库实例，类型为 kvDb.KeyValueDB。
//     这个数据库用于存储和检索文档的原始数据。
//   - reverseIndex: 倒排索引的实例，类型为 invertedIndex.InvertedIndexer。
//     这个索引用于实现关键词到文档ID的映射，支持高效的文档检索。
//   - maxIntId: 当前最大文档ID，类型为 uint64。
//     这个值用于跟踪已分配的最大文档ID，以便生成新的唯一ID。
type LocalIndexer struct {
	forwardIndex kvDb.KeyValueDB               // 正排索引数据库实例
	reverseIndex invertedIndex.InvertedIndexer // 倒排索引实例
	maxIntId     uint64                        // 当前最大文档ID
}

// Init 初始化索引器，包括正排索引和倒排索引。
// 该方法会创建或打开数据库实例，并初始化倒排索引。
//
// 参数:
//   - docNumEstimate: 预估的文档数量，用于初始化倒排索引的容量。
//   - dbType: 数据库类型，用于选择和创建相应的数据库实例。
//   - dataDir: 数据存储目录，指定数据库文件的位置。
//
// 返回值:
//   - error: 如果在创建数据库或初始化索引时发生错误，则返回相应的错误。
func (indexer *LocalIndexer) Init(docNumEstimate, dbType int, dataDir string) error {
	// 调用 GetKvDB 工厂方法创建或打开数据库实例
	db, err := kvDb.GetKvDB(dbType, dataDir)
	if err != nil {
		return err
	}

	// 设置正排索引数据库实例
	indexer.forwardIndex = db

	// 初始化倒排索引
	indexer.reverseIndex = invertedIndex.NewSkipListInvertedIndexer(docNumEstimate)

	return nil
}

// Close 关闭索引器，释放所有相关资源。
// 该方法会关闭正排索引数据库实例。
//
// 返回值:
//   - error: 如果在关闭正排索引数据库时发生错误，则返回相应的错误。
func (indexer *LocalIndexer) Close() error {
	// 关闭正排索引数据库实例
	return indexer.forwardIndex.Close()
}

// AddDoc 向索引中添加文档（如果文档已存在，会先删除再覆盖）。
//
// 参数:
//   - doc: 需要添加到索引中的文档，包含业务侧ID和其他相关信息。
//
// 返回值:
//   - int: 成功添加的文档数量，正常情况下应为 1。
//   - error: 如果添加过程中发生错误，返回相应的错误。
func (indexer *LocalIndexer) AddDoc(doc types.Document) (int, error) {
	// 获取并修剪文档的业务侧ID（docId）
	docId := strings.TrimSpace(doc.Id)
	// 如果文档ID为空，返回错误
	if len(docId) == 0 {
		return 0, fmt.Errorf("业务侧ID不能为空")
	}

	// 将文档ID从正排索引和倒排索引中删除（如果已存在）
	indexer.DeleteDoc(docId)

	// 为新文档自动生成一个唯一的IntId
	doc.IntId = atomic.AddUint64(&indexer.maxIntId, 1)

	// 将文档写入正排索引
	var value bytes.Buffer
	encoder := gob.NewEncoder(&value)
	// 对文档进行编码并存储到正排索引中
	if err := encoder.Encode(doc); err == nil {
		err := indexer.forwardIndex.Set([]byte(docId), value.Bytes())
		if err != nil {
			return 0, err
		}
	} else {
		// 如果编码失败，返回错误
		return 0, err
	}

	// 将文档添加到倒排索引
	indexer.reverseIndex.Add(doc)

	// 返回成功添加的文档数量
	return 1, nil
}

// DeleteDoc 从索引中删除文档，接受业务侧文档ID（docId）作为参数。
//
// 参数:
//   - docId: 业务侧文档ID，表示要删除的文档。
//
// 返回值:
//   - int: 成功删除的文档数量，正常情况下应为 1。
func (indexer *LocalIndexer) DeleteDoc(docId string) int {
	if len(docId) == 0 {
		// 如果docId为空，直接返回0
		utils.Log.Printf("无效的文档ID: %s\n", docId)
		return 0
	}

	forwardKey := []byte(docId)

	// 从正排索引中读取文档的bytes数据
	docBytes, err := indexer.forwardIndex.Get(forwardKey)
	if err != nil {
		// 如果发生读取错误，记录日志并返回0
		utils.Log.Printf("读取文档失败: %s, 错误: %v\n", docId, err)
		return 0
	}

	// 如果正排索引中不存在该文档，直接返回0
	if len(docBytes) == 0 {
		utils.Log.Printf("文档不存在于索引中: %s\n", docId)
		return 0
	}

	// 将bytes数据解码成文档结构
	reader := bytes.NewReader(docBytes)
	var doc types.Document
	if err := gob.NewDecoder(reader).Decode(&doc); err != nil {
		// 解码失败时记录日志
		utils.Log.Printf("解码文档失败: %s, 错误: %v\n", docId, err)
		return 0
	}

	// 遍历文档中的每一个Keyword，从倒排索引中删除
	for _, keyword := range doc.Keywords {
		indexer.reverseIndex.Delete(keyword, doc.IntId)
	}

	// 从正排索引中删除文档的正排记录
	if err := indexer.forwardIndex.Delete(forwardKey); err != nil {
		// 删除失败时记录日志
		utils.Log.Printf("删除文档失败: %s, 错误: %v\n", docId, err)
		return 0
	}

	// 返回成功删除的文档数量
	return 1
}

// LoadFromIndexFile 系统重启时，直接从索引文件里加载数据
//
// 返回值:
//   - int: 成功加载的文档数量
func (indexer *LocalIndexer) LoadFromIndexFile() int {
	// 创建一个bytes读取器
	reader := bytes.NewReader([]byte{})

	// 遍历正排索引数据库中的所有记录
	n, err := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		// 重置读取器的内容
		reader.Reset(v)
		// 创建解码器
		decoder := gob.NewDecoder(reader)
		var doc types.Document

		// 解码bytes数据为文档结构
		err := decoder.Decode(&doc)
		if err != nil {
			// 解码失败，记录错误日志（中文输出）
			utils.Log.Printf("解码文档出错: %v", err)
			return nil
		}

		// 将文档添加到倒排索引中
		indexer.reverseIndex.Add(doc)
		return err
	})

	// 如果加载过程中出现错误，返回0
	if err != nil {
		return 0
	}

	// 记录成功加载的文档数量（中文输出）
	utils.Log.Printf("从正排索引中加载了 %d 个文档", n)
	return int(n)
}

// Search 检索，返回文档列表
//
// 参数:
//   - query: *types.TermQuery，表示要检索的查询条件。
//   - onFlag: uint64，表示需要匹配的位特征。
//   - offFlag: uint64，表示需要排除的位特征。
//   - orFlags: []uint64，表示需要至少命中一个bit的位特征集合。
//
// 返回值:
//   - []*types.Document: 符合查询条件的文档列表。
func (indexer *LocalIndexer) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	// 从倒排索引中获取符合条件的业务侧ID集合
	docIds := indexer.reverseIndex.Search(query, onFlag, offFlag, orFlags)
	if len(docIds) == 0 {
		return nil
	}

	// 构建正排索引的关键字集合，用于批量获取文档
	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}

	// 批量获取文档的二进制数据
	docBytes, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		// 批量获取正排索引中的文档失败，记录错误日志（中文输出）
		utils.Log.Printf("从正排索引批量获取文档出错: %v", err)
		return nil
	}

	// 解码每个文档的二进制数据，构造返回结果
	result := make([]*types.Document, 0, len(docIds))
	reader := bytes.NewReader([]byte{}) // 用于读取二进制数据的字节读取器
	for _, docByte := range docBytes {
		reader.Reset(docByte)             // 重置读取器
		decoder := gob.NewDecoder(reader) // 创建Gob解码器
		var doc types.Document
		err = decoder.Decode(&doc) // 解码文档
		if err == nil {
			result = append(result, &doc) // 将解码后的文档添加到结果集中
		}
	}
	return result
}

// Count 索引里有几个document
//
// 返回值:
//   - int: 索引中文档的数量。
func (indexer *LocalIndexer) Count() int {
	// 通过遍历正排索引中的键来统计文档数量
	n, err := indexer.forwardIndex.IterKey(func(k []byte) error {
		// 遍历时不需要具体操作，只需要返回 nil 表示继续
		return nil
	})
	if err != nil {
		// 如果遍历过程中出现错误，记录错误日志（中文输出）
		utils.Log.Printf("遍历键时出错: %v", err)
		return 0
	}
	// 返回文档的数量
	return int(n)
}
