package examples

// go sql
type account struct {
	id       int               // sql{inc}
	email    string            // sql{unique="email"}
	name     string            // sql{index}
	balance  int64
	tags     []string
	scores   []int
	meta     map[string]string
	settings map[string]int
	items    []Item
}

type accountSQL interface {
	// where balance > @min order by balance desc
	Rich(min int64) ([]Account, error)

	// where email = @email
	ByEmail(email string) (Account, error) // !404

	// select balance from account where id = @id
	Balance(id int) (int64, error)

	// select id from account where name = any(@names)
	IDs(names []string) ([]int, error)

	// update account set balance = 0 where balance < @min
	Reset(min int64) (int64, error)

	// delete from account where id = @id
	Drop(id int) error
}
