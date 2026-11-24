package examples

// go ch=events engine=ReplacingMergeTree(id) partition=intDiv(id,1000000)
type event struct {
	id      uint64 // ch{primary order}
	country string // ch{low order}
	ip      string // ch{ip}
	path    string
	tags    []string
}

// own clickhouse queries of EventCQL
type eventCQL interface {
	// where country = @country order by id desc limit 10
	Top(country string) ([]Event, error)
}
