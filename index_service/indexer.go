package index_service

import (
	"bytes"
	_interface "criker-search/internal/interface"
	invertedIndex "criker-search/internal/inverted_index"
	kvDb "criker-search/internal/kv_db"
	"criker-search/types"
	"criker-search/utils"
	"encoding/gob"
	"fmt"
	"strings"
	"sync/atomic"
)

// Indexer Facade外观模式。把正排索引和倒排索引两个子系统封装到了一起
type Indexer struct {
	forwardIndex _interface.KeyValueDB
	reverseIndex _interface.InvertedIndexer
	maxIntId     uint64 // 当前最大文档ID，即IntId
}

// Init 初始化索引
func (indexer *Indexer) Init(docNumEstimate, dbType int, dataDir string) error {
	db, err := kvDb.GetKvDB(dbType, dataDir)
	if err != nil {
		return err
	}
	indexer.forwardIndex = db
	indexer.reverseIndex = invertedIndex.NewSkipListInvertedIndexer(docNumEstimate)
	return nil
}

// Close 关闭索引
func (indexer *Indexer) Close() error {
	return indexer.forwardIndex.Close()
}

// AddDoc 向索引中添加文档（如果已存在，会覆盖）
func (indexer *Indexer) AddDoc(doc types.Document) (int, error) {
	// 业务侧ID作为docId
	docId := strings.TrimSpace(doc.Id)
	if len(docId) == 0 {
		return 0, fmt.Errorf("doc id cannot be blank")
	}
	// 将docId从正排索引和倒排索引上删除
	indexer.DeleteDoc(docId)
	// 写入索引时自动为文档生成IntId
	doc.IntId = atomic.AddUint64(&indexer.maxIntId, 1)
	// 写入正排索引
	var value bytes.Buffer
	encoder := gob.NewEncoder(&value)
	if err := encoder.Encode(doc); err == nil {
		err := indexer.forwardIndex.Set([]byte(docId), value.Bytes())
		if err != nil {
			return 0, err
		}
	} else {
		return 0, err
	}
	// 写入倒排索引
	indexer.reverseIndex.Add(doc)
	return 1, nil
}

// DeleteDoc 从索引上删除文档，接受业务侧文档ID作为参数
func (indexer *Indexer) DeleteDoc(docId string) int {
	forwardKey := []byte(docId)
	// 先读取正排索引，获取文档bytes后解码，获取IntId和Keywords，然后从倒排索引上删除
	docBytes, err := indexer.forwardIndex.Get(forwardKey)
	if err == nil {
		reader := bytes.NewReader([]byte{})
		if len(docBytes) > 0 {
			reader.Reset(docBytes)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err = decoder.Decode(&doc)
			if err == nil {
				// 遍历每一个Keyword，从倒排索引删除
				for _, keyword := range doc.Keywords {
					indexer.reverseIndex.Delete(keyword, doc.IntId)
				}
			}
		}
	}
	// 从正排索引上删除
	err = indexer.forwardIndex.Delete(forwardKey)
	if err != nil {
		utils.Log.Printf("Error deleting doc: %s\n", docId)
		return 0
	}
	return 1
}

// LoadFromIndexFile 系统重启时，直接从索引文件里加载数据
func (indexer *Indexer) LoadFromIndexFile() int {
	reader := bytes.NewReader([]byte{})
	n, err := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		reader.Reset(v)
		decoder := gob.NewDecoder(reader)
		var doc types.Document
		err := decoder.Decode(&doc)
		if err != nil {
			utils.Log.Printf("error decoding document: %v", err)
			return nil
		}
		indexer.reverseIndex.Add(doc)
		return err
	})
	if err != nil {
		return 0
	}
	utils.Log.Printf("loaded %d documents from forward index", n)
	return int(n)
}

// Search 检索，返回文档列表
func (indexer *Indexer) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	// 从倒排索引上获取符合条件的业务侧ID集合
	docIds := indexer.reverseIndex.Search(query, onFlag, offFlag, orFlags)
	if len(docIds) == 0 {
		return nil
	}
	// 从业务侧ID集合构建正排索引关键字集合，获取所有文档切片
	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}
	docBytes, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		utils.Log.Printf("error batch get documents from forward index: %v", err)
		return nil
	}
	// 解码每个文档切片，构造返回结果
	result := make([]*types.Document, 0, len(docIds))
	reader := bytes.NewReader([]byte{})
	for _, docByte := range docBytes {
		reader.Reset(docByte)
		decoder := gob.NewDecoder(reader)
		var doc types.Document
		err = decoder.Decode(&doc)
		if err == nil {
			result = append(result, &doc)
		}
	}
	return result
}

// Count 索引里有几个document
func (indexer *Indexer) Count() int {
	n, err := indexer.forwardIndex.IterKey(func(k []byte) error {
		return nil
	})
	if err != nil {
		utils.Log.Printf("error iterating keys: %v", err)
		return 0
	}
	return int(n)
}
