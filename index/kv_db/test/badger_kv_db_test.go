package test

import (
	"github.com/jmh000527/criker-search/index/kv_db"
	"github.com/jmh000527/criker-search/utils"
	"testing"
)

func TestBadger(t *testing.T) {
	setup = func() {
		var err error
		db, err = kv_db.GetKvDB(kv_db.BADGER, utils.RootPath+"data/badger_db")
		if err != nil {
			panic(err)
		}
	}

	// 子测试
	t.Run("badger_test", testPipeline)
}
