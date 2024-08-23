package kv_db

import (
	"errors"
	bolt "go.etcd.io/bbolt"
	"sync/atomic"
)

var NoDataError = errors.New("no data found")

// Bolt 存储结构
type Bolt struct {
	db     *bolt.DB // 数据库实例
	path   string   // 本地存储目录
	bucket []byte   // 表的名称
}

// Open 初始化数据库
func (b *Bolt) Open() error {
	// 获取数据库文件的路径
	dataDir := b.GetDbPath()
	// 打开 BoltDB 数据库文件
	db, err := bolt.Open(dataDir, 0600, bolt.DefaultOptions)
	if err != nil {
		return err
	}
	// 使用 Update 事务来确保指定的 Bucket 存在
	err = db.Update(func(tx *bolt.Tx) error {
		// 创建指定的 Bucket（如果不存在）
		_, err := tx.CreateBucketIfNotExists(b.bucket)
		return err
	})
	if err != nil {
		db.Close()
		return err
	} else {
		b.db = db
		return nil
	}
}

// GetDbPath 获取数据库文件的路径
func (b *Bolt) GetDbPath() string {
	return b.path
}

// Set 写入<key, value>
func (b *Bolt) Set(k, v []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(b.bucket).Put(k, v)
	})
	return err
}

// BatchSet 批量写入<key, value>
func (b *Bolt) BatchSet(keys, values [][]byte) error {
	// 检查键和值的集合长度是否一致
	if len(keys) != len(values) {
		return errors.New("keys and values do not match")
	}
	// 开启 BoltDB 的批处理事务
	err := b.db.Batch(func(tx *bolt.Tx) error {
		for i, key := range keys {
			value := values[i]
			// 写入键值对到指定的 Bucket 中
			if err := tx.Bucket(b.bucket).Put(key, value); err != nil {
				return err
			}
		}
		// 批处理事务执行成功，返回 nil
		return nil
	})
	return err
}

// Get 读取key对应的value
func (b *Bolt) Get(k []byte) ([]byte, error) {
	var v []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v = tx.Bucket(b.bucket).Get(k)
		return nil
	})
	if len(v) == 0 {
		return nil, NoDataError
	}
	return v, err
}

// BatchGet 批量读取，注意不保证顺序
func (b *Bolt) BatchGet(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))
	err := b.db.Batch(func(tx *bolt.Tx) error {
		for i, key := range keys {
			values[i] = tx.Bucket(b.bucket).Get(key)
		}
		return nil
	})
	return values, err
}

// Delete 删除
func (b *Bolt) Delete(k []byte) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(b.bucket).Delete(k)
	})
	return err
}

// BatchDelete 批量删除
func (b *Bolt) BatchDelete(keys [][]byte) error {
	err := b.db.Batch(func(tx *bolt.Tx) error {
		for _, key := range keys {
			if err := tx.Bucket(b.bucket).Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Has 判断某个key是否存在
func (b *Bolt) Has(k []byte) bool {
	var v []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v = tx.Bucket(b.bucket).Get(k)
		return nil
	})
	if err != nil || string(v) == "" {
		return false
	}
	return true
}

// IterDB 遍历数据库，返回数据的条数
func (b *Bolt) IterDB(fn func(k []byte, v []byte) error) (int64, error) {
	var count int64
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		// 迭代器模式
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := fn(k, v); err != nil {
				return err
			} else {
				atomic.AddInt64(&count, 1)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return atomic.LoadInt64(&count), nil
}

// IterKey 遍历所有key，返回数据的条数
func (b *Bolt) IterKey(fn func(k []byte) error) (int64, error) {
	var count int64
	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := fn(k); err != nil {
				return err
			} else {
				atomic.AddInt64(&count, 1)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return atomic.LoadInt64(&count), nil
}

// Close 关闭数据库，把内存中的数据flush到磁盘，同时释放文件锁
func (b *Bolt) Close() error {
	return b.db.Close()
}

// WithDataPath 方法设置 Bolt 结构的本地存储目录路径。
func (b *Bolt) WithDataPath(path string) *Bolt {
	b.path = path
	return b
}

// WithBucket 方法设置 Bolt 结构的表名（Bucket）。
func (b *Bolt) WithBucket(bucket string) *Bolt {
	b.bucket = []byte(bucket)
	return b
}
