package examples

// go sql ee
type order struct {
	id       int // sql{inc}
	sender   int
	receiver int
	sum      int64
}
