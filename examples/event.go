package examples

// go ch=events
type event struct {
	id      uint64 // ch{primary}
	country string // ch{low}
	ip      string // ch{type=ip}
	path    string
	tags    []string
}

// own clickhouse queries of EventCQL
type eventCQL interface {
	// where country = @country order by id desc limit 10
	Top(country string) ([]Event, error)
}
