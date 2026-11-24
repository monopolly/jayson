package news

import (
	"time"

	"github.com/google/uuid"
	"github.com/monopolly/jayson/test/role"
)

// go sql
type angel struct {
	id      int       // sql{primary}  #readonly
	role    role.Role //sql{unique="css"}
	key     uuid.UUID //sql{unique="css"}
	created time.Time
	smalls  uint8
	name    string
	about   []string
	music   []int
	links   map[string]string
	stats   map[string]int
}

type angelSQL interface {
	// where role = @role and key = @key limit 1
	Creds(role role.Role, key uuid.UUID) (Angel, error)

	// select role from angels where id = @id
	Role(id int) (role.Role, error)

	// select music from angels where id = @id
	Music(id int) ([]int, error)

	// update angels set created = now() where id = @id
	Touch(id int) error
}
