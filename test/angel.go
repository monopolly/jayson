package news

import (
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/monopolly/jayson/test/role"
)

// go sql ch engine=ReplacingMergeTree(role) partition=toYYYYMM(created)
type angel struct {
	id      int       // sql{primary}  #readonly
	role    role.Role // sql{unique="css"}
	key     uuid.UUID // sql{unique="css"}
	created time.Time
	ip      net.IP
	smalls  uint8
	name    string
	about   []string
	ipes    []net.IP
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

type angelCQL interface {
	// where role = @role and key = @key limit 1
	Creds(role role.Role, key uuid.UUID) (Angel, error)

	// select role from angels where id = @id
	Role(id int) (role.Role, error)

	// select music from angels where id = @id
	Music(id int) ([]int, error)

	// update angels set created = now() where id = @id
	Touch(id int) error
}
