package tests

import (
	"reflect"
	"testing"
)

func TestBuildInvertedIndex1(t *testing.T) {
	type args struct {
		documents []*Document
	}
	tests := []struct {
		name string
		args args
		want map[string][]int
	}{
		// TODO: Add test cases.
		{
			name: "test",
			args: args{
				documents: []*Document{
					{
						Id:       1,
						Keywords: []string{"go", "数据结构"},
					},
					{
						Id:       2,
						Keywords: []string{"go", "数据库"},
					},
					{
						Id:       3,
						Keywords: []string{"C++", "数据结构"},
					},
					{
						Id:       4,
						Keywords: []string{"C++", "数据库"},
					},
				},
			},
			want: map[string][]int{
				"go":   {1, 2},
				"C++":  {3, 4},
				"数据库":  {2, 4},
				"数据结构": {1, 3},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BuildInvertedIndex(tt.args.documents); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BuildInvertedIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
