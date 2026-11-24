package alltypes

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/monopolly/jayson/test/role"
	"github.com/shopspring/decimal"
)

type Status int16
type Provider string

// go=All sql=alltypes
type all struct {
	id int // sql{inc}

	b   bool
	s   string
	i   int
	i8  int8
	i16 int16
	i32 int32
	i64 int64
	u   uint
	u8  uint8
	u16 uint16
	u32 uint32
	u64 uint64
	f32 float32
	f64 float64
	r   rune
	by  byte

	bytes  []byte
	fixed  [16]byte
	uid    uuid.UUID
	puid   pgtype.UUID
	raw    json.RawMessage
	mapany map[string]any
	maps   map[string]string
	conf   Config
	confs  []Config
	anyv   any

	t    time.Time
	dur  time.Duration
	date time.Time // sql{type="date"}
	pd   pgtype.Date
	pts  pgtype.Timestamp
	piv  pgtype.Interval

	lb   []bool
	ls   []string
	li   []int
	li8  []int8
	li16 []int16
	li32 []int32
	lu16 []uint16
	lu32 []uint32
	lu64 []uint64
	lf32 []float32
	lt   []time.Time
	lu   []uuid.UUID
	ls2  [][]string
	li2  [][]int64
	lbb  [][]byte
	lm   []map[string]any
	lst  []Status
	lrl  []role.Role

	ip   netip.Addr
	pfx  netip.Prefix
	nip  net.IP
	nnet net.IPNet
	num  pgtype.Numeric
	dec  decimal.Decimal
	rng  pgtype.Range[pgtype.Int4]
	mrng pgtype.Multirange[pgtype.Range[pgtype.Int8]]
	pt   pgtype.Point
	bits pgtype.Bits

	ps  *string
	pi  *int
	pb  *bool
	pf  *float64
	pt2 *time.Time
	pip *netip.Addr
	ns  sql.NullString
	ni  sql.NullInt64
	nb  sql.NullBool
	nf  sql.NullFloat64
	nt  sql.NullTime
	ptx pgtype.Text
	pi8 pgtype.Int8
	pbo pgtype.Bool

	st   Status
	prov Provider
	rl   role.Role
	nn   string // sql{notnull}
}
