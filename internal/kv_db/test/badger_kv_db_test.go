package test

import (
	kv_db "criker-search/internal/kv_db/interface"
	"criker-search/utils"
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
