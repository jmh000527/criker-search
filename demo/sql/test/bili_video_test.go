package sql

import (
	"fmt"
	"github.com/jmh000527/criker-search/demo/sql"
	"github.com/jmh000527/criker-search/utils"
	"testing"
)

var csvFile = utils.RootPath + "demo/data/bili_video.csv"

func TestDumpDataFromFile2DB1(t *testing.T) {
	sql.DumpDataFromFile2DB1(csvFile) // DumpDataFromFile2DB1 use time 117240 ms
	/*
		select count(*) from bili_video;
		delete from bili_video;
	*/
}

func TestDumpDataFromFile2DB2(t *testing.T) {
	sql.DumpDataFromFile2DB2(csvFile) // DumpDataFromFile2DB2 use time 7955 ms
	/*
		select count(*) from bili_video;
		delete from bili_video;
	*/
}

func TestDumpDataFromFile2DB3(t *testing.T) {
	sql.DumpDataFromFile2DB3(csvFile) // DumpDataFromFile2DB3 use time 107 ms
	/*
		select count(*) from bili_video;
		delete from bili_video;
	*/
}

func testReadAllTable(f func(ch chan<- sql.BiliVideo)) {
	ch := make(chan sql.BiliVideo, 100)
	go f(ch)
	idMap := make(map[string]struct{}, 40000)
	for {
		video, ok := <-ch
		if !ok {
			break
		}
		idMap[video.Id] = struct{}{}
	}
	fmt.Println(len(idMap))
	fmt.Println(idMap)
}

func TestReadAllTable1(t *testing.T) {
	testReadAllTable(sql.ReadAllTable1) // ReadAllTable1 use time 29 ms
}

func TestReadAllTable2(t *testing.T) {
	testReadAllTable(sql.ReadAllTable2) // ReadAllTable2 use time 42 ms
}

func TestReadAllTable3(t *testing.T) {
	testReadAllTable(sql.ReadAllTable3) // ReadAllTable3 use time 36 ms
}

// go test -v ./course/sql/test -run=^TestDumpDataFromFile2DB1$ -count=1
// go test -v ./course/sql/test -run=^TestDumpDataFromFile2DB2$ -count=1
// go test -v ./course/sql/test -run=^TestDumpDataFromFile2DB3$ -count=1
// go test -v ./course/sql/test -run=^TestReadAllTable1$ -count=1 -timeout=30m
// go test -v ./course/sql/test -run=^TestReadAllTable2$ -count=1 -timeout=30m
// go test -v ./course/sql/test -run=^TestReadAllTable3$ -count=1 -timeout=30m
