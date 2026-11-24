package examples

// go ch=events
type event struct {
	id      uint64 // ch{primary}
	country string // ch{low}
	ip      string // ch{type=ip}
	path    string
}
