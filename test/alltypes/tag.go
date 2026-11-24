package alltypes

// go=Tag sql=tags proto
type tag struct {
	id    int // sql{inc}
	name  string
	owner int
	meta  map[string]int
}
