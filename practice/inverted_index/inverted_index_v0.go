package tests

type Document struct {
	Id       int
	Keywords []string
}

func BuildInvertedIndex(documents []*Document) map[string][]int {
	index := make(map[string][]int, 100)
	for _, document := range documents {
		for _, keyword := range document.Keywords {
			index[keyword] = append(index[keyword], document.Id)
		}
	}
	return index
}
