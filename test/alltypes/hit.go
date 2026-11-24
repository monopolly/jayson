package alltypes

import "time"

// go=Hit ch=hits engine=ReplacingMergeTree(views) partition=toYYYYMM(created)
type hit struct {
	id      int    // ch{primary order}
	user    int
	country string // ch{low order}
	ip      string // ch{ip}
	ip6     string // ch{ip6}
	path    string
	ok      bool
	views   int64
	created time.Time
	tags    []string
	codes   []int
	sizes   []uint
	attrs   map[string]string
	counts  map[string]int
	from    string // reserved word: column ifrom
}

// own clickhouse functions of HitCQL
type hitCQL interface {
	// where country = @country order by id
	ByCountry(country string) ([]Hit, error)

	// where path = @path
	ByPath(path string) (Hit, error) // !404

	// select path from hits where id = @id
	Path(id int) (string, error)

	// select views from hits where id = @id
	Views(id int) (int64, error) // !404

	// select id from hits where country = @country order by id
	IDs(country string) ([]int64, error)

	// select tags from hits where id = @id
	Tags(id int) ([]string, error)

	// alter table hits delete where country = @country settings mutations_sync = 1
	DropCountry(country string) error
}
