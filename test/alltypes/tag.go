package alltypes

// go=Tag sql=tags proto ee
type tag struct {
	id    int // sql{inc}
	name  string
	owner int
	meta  map[string]int
}

// own sql functions of TagSQL
type tagSQL interface {
	// where owner = @owner order by id
	ByOwner(owner int) ([]Tag, error)

	// where name = @name
	ByName(name string) (Tag, error) // !404

	// select name from tags where id = @id
	Name(id int) (string, error)

	// select owner from tags where id = @id
	Owner(id int) (int, error) // !404

	// select id from tags where owner = any(@owners) order by id
	IDs(owners []int) ([]int, error)

	// update tags set name = @name where owner = @owner
	Rename(owner int, name string) (int64, error)

	// delete from tags where id = $1
	Drop(id int) error
}
