package kv_db

// 几种常见的基于LSM-tree算法实现的KV数据库
const (
	BOLT = iota
	BADGER
)

// KeyValueDB k-v数据库接口
type KeyValueDB interface {
	Open() error                                      // 初始化DB
	GetDbPath() string                                // 获取存储数据的目录
	Set(k, v []byte) error                            // 写入<key, value>
	BatchSet(keys, values [][]byte) error             // 批量写入<key, value>
	Get(k []byte) ([]byte, error)                     // 读取key对应的value
	BatchGet(keys [][]byte) ([][]byte, error)         // 批量读取，注意不保证顺序
	Delete(k []byte) error                            // 删除
	BatchDelete(keys [][]byte) error                  // 批量删除
	Has(k []byte) bool                                // 判断某个key是否存在
	IterDB(fn func(k, v []byte) error) (int64, error) // 遍历数据库，返回数据的条数
	IterKey(fn func(k []byte) error) (int64, error)   // 遍历所有key，返回数据的条数
	Close() error                                     // 把内存中的数据flush到磁盘，同时释放文件锁
}
