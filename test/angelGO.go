package news

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/jayson/test/role"
)

/*
	Auto-generate. Do not change!
	Help to auto-generated correct models for golang structures.
	Sergey Keplin (c) 2026
	a@senthy.com, lava.mobi@gmail.com, https://t.me/martinprestone
	github.com/monopolly/jayson
	Ex: // go=News sql=news ch=news proto test

	STRUCT:
	!omit                         No omit tag for json
	ch=views                      Set clickhouse sql table name. Ex: ch=views
	debug                         Debug mode
	ee                            Errors of sql methods are errors.E of github.com/monopolly/errors: errors.PGX(err)
	engine=MergeTree              Clickhouse engine, MergeTree by default. Ex: engine=ReplacingMergeTree(updated)
	go=News                       Set golang struct names. Ex: go=News1 > type News1 struct{}
	gotiny                        Create gotiny marshal/unmarshal
	lock                          Lock model for generation. Can't change model.
	msgp                          Create message pack marshal/unmarshal
	noinit                        No New() init function for struct
	optimize                      Try to create golang padding optimized struct
	partition=toYYYYMM(created)   Clickhouse partition by
	proto=NewsProto               Generate proto file and compile to current package
	sql=news                      Set sql table name. Ex: sql=accounts
	test                          Generate go tests for generated model
	ttl="created + interval 30 day" Clickhouse ttl: rows are removed after the time

	Field:
	desc{}                        Add custom desc for index. desc{This field for success}
	js{name=""}                   Replace json field name. Ex: js{name="sid"}
	title{}                       Add custom title for index. title{Nice & Sweet}

	GO:
	#                             Add lists for fields. Ex: id int //#readonly #must...
	@                             Add custom structs for fields. Ex: id int //@public @team...
	base=""                       Underlying type for custom type (detected automatically). Ex: role role.Role // go{base="int"}
	desc=""                       Add custom desc for index
	desc{}                        Add field user title desc{Use it nice}
	must                          Create one validation function for all must fields
	name=""                       Replace golang struct name. Ex: name="NewsList"
	title=""                      Add custom title for index
	title{}                       Add field user title title{Nice}
	type=""                       Replace golang type. Ex: type="[]*News"
	up                            Make uppercase for functions

	SQL:
	add="primary key"             Append sql for field. Ex: replace="primary key"
	altertable                    Add field line Alter table to SQL file with current time comment
	defaults                      Add default value based on field type
	idx                           Add index fields by group name. Ex: idx="nameIndex" idx="credsIndex"
	index                         Create simple default index or gin for jsonb and arrays
	noinsert                      Do use field for insert function
	notnull                       Add not null to column
	primary                       Primary key. One field: asset bigint primary key. Several fields: primary key (asset, user)
	renames="oldname"             Create an alter table record in SQL file. Rename table column.
	replace="bigint primary key"  Rewrite sql for field. Ex: replace="bigint primary key"
	search                        Add tsvector index by group. Ex: search="tsv": tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || brand)). For search: SELECT brand, title FROM assets WHERE search @@ to_tsquery('english', 'f8');
	skip                          Skip field for sql queries
	type="date"                   Explicit postgres type: date, timestamp, time, interval, varchar(64), numeric(18,2), json, vector(3)...
	type="jsonb"                  Rewrite sql type for field. Ex: type="jsonb"
	unique="groupname"            Add unique fields constrains by group (you need set group name, then generator join fields). Ex: unique="group1" unique="group2"
	unix                          Add default value: extract(epoch from now())
	ver="v4"                      Create an alter table record in SQL file. Add new column. With new version in comment. Ex: ver="2"

	Clickhouse:
	low                           ch{low} Wrap field type in LowCardinality(...)
	primary"                      ch{primary} Make primary key
	type=ip                       ch{type=ip} Use ClickHouse IPv4 type for IP address fields

	PROTO:
	name="field"                  Rewrite proto field name
	skip                          Skip field for proto
	type="string"                 Rewrite proto type for field. Ex: type="google.protobuf.Value"

	SQL FUNCTIONS: methods of interface become methods of AngelSQL
	type angelSQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (Angel, error)

		// select role from angel where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from angel where id = @id
		Music(id int) ([]int, error)

		// select id from angel where role = @role
		IDs(role role.Role) ([]int, error)

		// update angel set created = now() where id = @id
		Touch(id int) error

		// delete from angel where created < @before
		Clean(before time.Time) (int64, error) // count of rows
	}

	CLICKHOUSE FUNCTIONS: methods of interface become methods of AngelCQL
	type angelCQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (Angel, error)

		// select role from angel where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from angel where id = @id
		Music(id int) ([]int, error)

		// select id from angel where role = @role
		IDs(role role.Role) ([]int64, error)

		// alter table angel delete where created < @before
		Clean(before time.Time) error
	}

	CLICKHOUSE ENGINES: engine=ReplacingMergeTree(updated) partition=toYYYYMM(created) ttl="created + interval 30 day"
	MergeTree                                   default: rows sorted by key, for most tables
	ReplacingMergeTree(ver)                     removes duplicates of key on merge, keeps the row with max ver (or the last one)
	SummingMergeTree(a, b)                      sums number columns of rows with the same key
	AggregatingMergeTree                        merges states of aggregate functions, for materialized views
	CollapsingMergeTree(sign)                   pair of rows with sign 1 and -1 is removed: change of row without update
	VersionedCollapsingMergeTree(sign, ver)     the same for inserts in any order
	GraphiteMergeTree('config')                 rollup of graphite metrics
	ReplicatedMergeTree('/path', '{replica}')   any MergeTree engine with replication: Replicated + name
	Log, TinyLog, StripeLog                     small tables without key and index, written once
	Memory                                      rows in memory, lost on restart
	Null                                        rows are dropped, source for materialized views
*/

// init struct
func NewAngel() (a *Angel) {
	a = new(Angel)
	a.Links = make(map[string]string)
	a.Stats = make(map[string]int)
	return
}

// 104 bytes (go padding)
type Angel struct {
	ID      int               `json:"id,omitempty"`      // sql{primary} ch{primary}  #readonly
	Role    role.Role         `json:"role,omitempty"`    // sql{unique="css"} ch{primary}
	Key     uuid.UUID         `json:"key,omitempty"`     // sql{unique="css"}
	Created time.Time         `json:"created,omitempty"` //
	IP      net.IP            `json:"ip,omitempty"`      //
	Smalls  uint8             `json:"smalls,omitempty"`  //
	Name    string            `json:"name,omitempty"`    //
	About   []string          `json:"about,omitempty"`   //
	Ipes    []net.IP          `json:"ipes,omitempty"`    //
	Music   []int             `json:"music,omitempty"`   //
	Links   map[string]string `json:"links,omitempty"`   //
	Stats   map[string]int    `json:"stats,omitempty"`   //
}

// angelSQLDB runs queries: pool or transaction
type angelSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// AngelSQL sql queries of table angel
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type AngelSQL struct {
	pool *pgxpool.Pool
	db   angelSQLDB // pool, or transaction after TX
}

// NewAngelSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewAngelSQL(pool *pgxpool.Pool) *AngelSQL {
	a := &AngelSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *AngelSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of AngelSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum angel")
//		return err
//	})
func (a *AngelSQL) Conn(f func(conn *pgxpool.Conn) error) error {
	conn, err := a.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()
	return f(conn)
}

// Begin runs f in transaction: commit on nil, rollback on error or panic.
// TX(tx) of every model runs its queries in this transaction
//
//	err := news.Begin(func(tx pgx.Tx) error {
//		if err := news.TX(tx).Insert(&item); err != nil {
//			return err
//		}
//		return users.TX(tx).Update(id, UserIndexNews, item.ID)
//	})
//
// Begin of TX(tx) makes a savepoint
func (a *AngelSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *AngelSQL) TX(tx pgx.Tx) *AngelSQL {
	return &AngelSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *AngelSQL) TableName() string {
	return "angel"
}

// Fields returns all sql columns
func (a *AngelSQL) Fields() []AngelIndexTyped {
	return slices.Clone(angelSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var angelSQLFields = []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexIP, AngelIndexSmalls, AngelIndexName, AngelIndexAbout, AngelIndexIpes, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

// columns of insert
var angelSQLInsertColumns = []string{"id", "role", "key", "created", "ip", "smalls", "name", "about", "ipes", "music", "links", "stats"}

// sql column names
var angelSQLColumns = [...]string{
	AngelIndexID:      "id",
	AngelIndexRole:    "role",
	AngelIndexKey:     "key",
	AngelIndexCreated: "created",
	AngelIndexIP:      "ip",
	AngelIndexSmalls:  "smalls",
	AngelIndexName:    "name",
	AngelIndexAbout:   "about",
	AngelIndexIpes:    "ipes",
	AngelIndexMusic:   "music",
	AngelIndexLinks:   "links",
	AngelIndexStats:   "stats",
}

// select expressions: null of scalar column is zero value
var angelSQLSelects = [...]string{
	AngelIndexID:      "coalesce(id, 0)",
	AngelIndexRole:    "coalesce(role, 0)",
	AngelIndexKey:     "key",
	AngelIndexCreated: "coalesce(created, '0001-01-01 00:00:00+00')",
	AngelIndexIP:      "ip",
	AngelIndexSmalls:  "coalesce(smalls, 0)",
	AngelIndexName:    "coalesce(name, '')",
	AngelIndexAbout:   "about",
	AngelIndexIpes:    "ipes",
	AngelIndexMusic:   "music",
	AngelIndexLinks:   "links",
	AngelIndexStats:   "stats",
}

// sql column types
var angelSQLTypes = [...]string{
	AngelIndexID:      "bigint",
	AngelIndexRole:    "bigint",
	AngelIndexKey:     "uuid",
	AngelIndexCreated: "timestamptz",
	AngelIndexIP:      "inet",
	AngelIndexSmalls:  "smallint",
	AngelIndexName:    "text",
	AngelIndexAbout:   "text[]",
	AngelIndexIpes:    "inet[]",
	AngelIndexMusic:   "bigint[]",
	AngelIndexLinks:   "jsonb",
	AngelIndexStats:   "jsonb",
}

const angelSQLSelectAll = "coalesce(id, 0), coalesce(role, 0), key, coalesce(created, '0001-01-01 00:00:00+00'), ip, coalesce(smalls, 0), coalesce(name, ''), about, ipes, music, links, stats"

// angelSQLColumn returns sql column name of field
func angelSQLColumn(f AngelIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(angelSQLColumns) || angelSQLColumns[f] == "" {
		return "", fmt.Errorf("angel: field %d is not a sql column", int(f))
	}
	return angelSQLColumns[f], nil
}

// angelSQLSelect returns select list of fields, all columns when empty
func angelSQLSelect(fields []AngelIndexTyped) (string, []AngelIndexTyped, error) {
	if len(fields) == 0 {
		return angelSQLSelectAll, angelSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := angelSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(angelSQLSelects[f])
	}
	return b.String(), fields, nil
}

// angelSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func angelSQLSet(v map[AngelIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("angel: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := angelSQLColumn(f)
		if err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
		b.WriteString(" = $")
		b.WriteString(strconv.Itoa(start + i))
		args = append(args, v[f])
	}
	return b.String(), args, nil
}

// angelSQLAffected returns pgx.ErrNoRows when nothing was changed
func angelSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// angelSQLScanBytes scans bytea to byte array
type angelSQLScanBytes func(b []byte)

func (f angelSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Angel) sqlDest(fields []AngelIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case AngelIndexID:
			dest[i] = &a.ID
		case AngelIndexRole:
			dest[i] = &a.Role
		case AngelIndexKey:
			dest[i] = &a.Key
		case AngelIndexCreated:
			dest[i] = &a.Created
		case AngelIndexIP:
			dest[i] = &a.IP
		case AngelIndexSmalls:
			dest[i] = &a.Smalls
		case AngelIndexName:
			dest[i] = &a.Name
		case AngelIndexAbout:
			dest[i] = &a.About
		case AngelIndexIpes:
			dest[i] = &a.Ipes
		case AngelIndexMusic:
			dest[i] = &a.Music
		case AngelIndexLinks:
			dest[i] = &a.Links
		case AngelIndexStats:
			dest[i] = &a.Stats
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Angel) sqlValue(f AngelIndexTyped) any {
	switch f {
	case AngelIndexID:
		return a.ID
	case AngelIndexRole:
		return int(a.Role)
	case AngelIndexKey:
		return a.Key
	case AngelIndexCreated:
		return a.Created
	case AngelIndexIP:
		return a.IP
	case AngelIndexSmalls:
		return a.Smalls
	case AngelIndexName:
		return a.Name
	case AngelIndexAbout:
		return a.About
	case AngelIndexIpes:
		return a.Ipes
	case AngelIndexMusic:
		return a.Music
	case AngelIndexLinks:
		return a.Links
	case AngelIndexStats:
		return a.Stats
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Angel) sqlInsertArgs() []any {
	return []any{
		a.ID,
		int(a.Role),
		a.Key,
		a.Created,
		a.IP,
		a.Smalls,
		a.Name,
		a.About,
		a.Ipes,
		a.Music,
		a.Links,
		a.Stats,
	}
}

// sqlAllArgs returns values of all columns
func (a *Angel) sqlAllArgs() []any {
	return []any{
		a.ID,
		int(a.Role),
		a.Key,
		a.Created,
		a.IP,
		a.Smalls,
		a.Name,
		a.About,
		a.Ipes,
		a.Music,
		a.Links,
		a.Stats,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *AngelSQL) query(fields []AngelIndexTyped, tail string, args ...any) ([]Angel, error) {
	cols, fields, err := angelSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from angel"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Angel, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *AngelSQL) queryRow(fields []AngelIndexTyped, tail string, args ...any) (v Angel, err error) {
	cols, fields, err := angelSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from angel"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by id
func (a *AngelSQL) Get(id int, fields ...AngelIndexTyped) (Angel, error) {
	return a.queryRow(fields, " where id = $1", id)
}

// Exists checks row by id
func (a *AngelSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from angel where id = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *AngelSQL) GetBy(field AngelIndexTyped, v any, fields ...AngelIndexTyped) (res Angel, err error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *AngelSQL) GetWhere(fields []AngelIndexTyped, where string, args ...any) (Angel, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *AngelSQL) Has(field AngelIndexTyped, v any) (has bool, err error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from angel where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *AngelSQL) All(fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *AngelSQL) List(limit, offset int, fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *AngelSQL) Select(fields []AngelIndexTyped, where string, args ...any) ([]Angel, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *AngelSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from angel").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *AngelSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from angel where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const angelSQLInsert = "insert into angel (id, role, key, created, ip, smalls, name, about, ipes, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"

const angelSQLInsertFull = "insert into angel (id, role, key, created, ip, smalls, name, about, ipes, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"

// Insert adds row
func (a *AngelSQL) Insert(v *Angel) error {
	_, err := a.db.Exec(context.Background(), angelSQLInsert, v.sqlInsertArgs()...)
	return err
}

// InsertNoConflict adds row, false when row exists
func (a *AngelSQL) InsertNoConflict(v *Angel) (inserted bool, err error) {
	tag, err := a.db.Exec(context.Background(), angelSQLInsert+" on conflict do nothing", v.sqlInsertArgs()...)
	return tag.RowsAffected() > 0, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *AngelSQL) InsertFull(v *Angel) error {
	_, err := a.db.Exec(context.Background(), angelSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *AngelSQL) Upsert(v *Angel) error {
	_, err := a.db.Exec(context.Background(), "insert into angel (id, role, key, created, ip, smalls, name, about, ipes, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) on conflict (id) do update set role = excluded.role, key = excluded.key, created = excluded.created, ip = excluded.ip, smalls = excluded.smalls, name = excluded.name, about = excluded.about, ipes = excluded.ipes, music = excluded.music, links = excluded.links, stats = excluded.stats", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *AngelSQL) InsertMany(list []Angel) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"angel"}, angelSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip
func (a *AngelSQL) InsertBatch(list []*Angel) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(angelSQLInsert, v.sqlInsertArgs()...)
	}
	res := a.db.SendBatch(context.Background(), b)
	for _, v := range list {
		_ = v
		if _, err := res.Exec(); err != nil {
			res.Close()
			return err
		}
	}
	return res.Close()
}

// ---------------------------------------------------------------------------
// update, delete
// ---------------------------------------------------------------------------

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *AngelSQL) Update(id int, field AngelIndexTyped, v any) error {
	col, err := angelSQLColumn(field)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = $1 where id = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *AngelSQL) Updates(id int, v map[AngelIndexTyped]any) error {
	set, args, err := angelSQLSet(v, 1)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *AngelSQL) Save(v *Angel, fields ...AngelIndexTyped) error {
	if len(fields) == 0 {
		fields = angelSQLFields
	}
	values := make(map[AngelIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != AngelIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := angelSQLSet(values, 1)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(AngelIndexID))...))
}

// Inc adds v to number field: views = views + 1
func (a *AngelSQL) Inc(id int, field AngelIndexTyped, v any) error {
	col, err := angelSQLColumn(field)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

// Delete removes row by id
func (a *AngelSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from angel where id = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *AngelSQL) UpdateWhere(field AngelIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update angel set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *AngelSQL) UpdatesWhere(v map[AngelIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := angelSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update angel set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *AngelSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from angel where "+where, args...)
	return tag.RowsAffected(), err
}

// AppendAbout adds values to array about
func (a *AngelSQL) AppendAbout(id int, v ...string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set about = coalesce(about, '{}') || $1::text[] where id = $2", v, id))
}

// RemoveAbout removes all entries of v from array about
func (a *AngelSQL) RemoveAbout(id int, v string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set about = array_remove(about, $1::text) where id = $2", v, id))
}

// AppendIpes adds values to array ipes
func (a *AngelSQL) AppendIpes(id int, v ...net.IP) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set ipes = coalesce(ipes, '{}') || $1::inet[] where id = $2", v, id))
}

// RemoveIpes removes all entries of v from array ipes
func (a *AngelSQL) RemoveIpes(id int, v net.IP) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set ipes = array_remove(ipes, $1::inet) where id = $2", v, id))
}

// AppendMusic adds values to array music
func (a *AngelSQL) AppendMusic(id int, v ...int) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set music = coalesce(music, '{}') || $1::bigint[] where id = $2", v, id))
}

// RemoveMusic removes all entries of v from array music
func (a *AngelSQL) RemoveMusic(id int, v int) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set music = array_remove(music, $1::bigint) where id = $2", v, id))
}

// UpdateLinks sets key of jsonb links
func (a *AngelSQL) UpdateLinks(id int, k string, v string) error {
	return a.jsonSet(id, "links", fmt.Sprint(k), v)
}

// MergeLinks adds keys to jsonb links: existing keys are replaced
func (a *AngelSQL) MergeLinks(id int, v map[string]string) error {
	return a.jsonMerge(id, "links", "{}", v)
}

// DeleteLinks removes key of jsonb links
func (a *AngelSQL) DeleteLinks(id int, k string) error {
	return a.jsonDelete(id, "links", fmt.Sprint(k))
}

// RenameLinks renames key of jsonb links, pgx.ErrNoRows when key not found
func (a *AngelSQL) RenameLinks(id int, k, newK string) error {
	return a.jsonRename(id, "links", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateStats sets key of jsonb stats
func (a *AngelSQL) UpdateStats(id int, k string, v int) error {
	return a.jsonSet(id, "stats", fmt.Sprint(k), v)
}

// MergeStats adds keys to jsonb stats: existing keys are replaced
func (a *AngelSQL) MergeStats(id int, v map[string]int) error {
	return a.jsonMerge(id, "stats", "{}", v)
}

// DeleteStats removes key of jsonb stats
func (a *AngelSQL) DeleteStats(id int, k string) error {
	return a.jsonDelete(id, "stats", fmt.Sprint(k))
}

// RenameStats renames key of jsonb stats, pgx.ErrNoRows when key not found
func (a *AngelSQL) RenameStats(id int, k, newK string) error {
	return a.jsonRename(id, "stats", fmt.Sprint(k), fmt.Sprint(newK))
}

// jsonSet sets key of jsonb column
func (a *AngelSQL) jsonSet(id int, col, k string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = jsonb_set(coalesce("+col+", '{}'::jsonb), array[$1::text], $2::jsonb, true) where id = $3", k, b, id))
}

// jsonMerge concats json to jsonb column: object keys or array values
func (a *AngelSQL) jsonMerge(id int, col, empty string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = coalesce("+col+", '"+empty+"'::jsonb) || $1::jsonb where id = $2", b, id))
}

// jsonDelete removes key of jsonb column
func (a *AngelSQL) jsonDelete(id int, col, k string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = "+col+" - $1::text where id = $2", k, id))
}

// jsonRename renames key of jsonb column
func (a *AngelSQL) jsonRename(id int, col, k, newK string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = ("+col+" - $1::text) || jsonb_build_object($2::text, "+col+" -> $1::text) where id = $3 and "+col+" ? $1::text", k, newK, id))
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

// Creds: where role = @role and key = @key limit 1
func (a *AngelSQL) Creds(role role.Role, key uuid.UUID) (res Angel, err error) {
	res, err = a.queryRow(nil, " where role = $1 and key = $2 limit 1", role, key)
	return
}

// Role: select role from angels where id = @id
func (a *AngelSQL) Role(id int) (res role.Role, err error) {
	err = a.db.QueryRow(context.Background(), "select role from angels where id = $1", id).Scan(&res)
	return
}

// Music: select music from angels where id = @id
func (a *AngelSQL) Music(id int) (res []int, err error) {
	err = a.db.QueryRow(context.Background(), "select music from angels where id = $1", id).Scan(&res)
	return
}

// Touch: update angels set created = now() where id = @id
func (a *AngelSQL) Touch(id int) (err error) {
	_, err = a.db.Exec(context.Background(), "update angels set created = now() where id = $1", id)
	return
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// AngelQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type AngelQuery struct {
	Fields []string            `json:"fields,omitempty"` // columns to select, all when empty
	EQ     map[string]any      `json:"eq,omitempty"`     // field = value, empty value is skipped
	NOT    map[string][]any    `json:"not,omitempty"`    // field != values
	GT     map[string]any      `json:"gt,omitempty"`     // field > value
	GTE    map[string]any      `json:"gte,omitempty"`    // field >= value
	LT     map[string]any      `json:"lt,omitempty"`     // field < value
	LTE    map[string]any      `json:"lte,omitempty"`    // field <= value
	Like   map[string]string   `json:"like,omitempty"`   // field ilike %value%
	IN     map[string][]int    `json:"in,omitempty"`     // field in numbers
	INS    map[string][]string `json:"ins,omitempty"`    // field in strings
	Sort   string              `json:"sort,omitempty"`   // order by field
	Desc   bool                `json:"desc,omitempty"`
	Limit  int                 `json:"limit,omitempty"`
	Offset int                 `json:"offset,omitempty"`

	Where string `json:"-"` // trusted sql condition with placeholders $1, $2... for Args
	Args  []any  `json:"-"`
}

// NewAngelQuery creates query with empty maps
func NewAngelQuery() *AngelQuery {
	return &AngelQuery{
		EQ:   map[string]any{},
		NOT:  map[string][]any{},
		GT:   map[string]any{},
		GTE:  map[string]any{},
		LT:   map[string]any{},
		LTE:  map[string]any{},
		Like: map[string]string{},
		IN:   map[string][]int{},
		INS:  map[string][]string{},
	}
}

// column returns sql column by key
func (q *AngelQuery) column(k string) (string, AngelIndexTyped, error) {
	if !AngelValidKey(k) {
		return "", 0, fmt.Errorf("angel: unknown field %q", k)
	}
	f := AngelKeyIndex(k)
	col, err := angelSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *AngelQuery) Render() (tail string, fields []AngelIndexTyped, args []any, err error) {

	for _, k := range q.Fields {
		_, f, err := q.column(k)
		if err != nil {
			return "", nil, nil, err
		}
		fields = append(fields, f)
	}

	args = append(args, q.Args...)
	var and []string
	if q.Where != "" {
		and = append(and, "("+q.Where+")")
	}

	// field op $n
	add := func(k, op string, v any) error {
		col, _, err := q.column(k)
		if err != nil {
			return err
		}
		args = append(args, v)
		and = append(and, fmt.Sprintf(op, col, len(args)))
		return nil
	}

	for _, k := range slices.Sorted(maps.Keys(q.EQ)) {
		v := q.EQ[k]
		if v == nil || v == "" {
			continue
		}
		op := "%s = $%d"
		if _, f, err := q.column(k); err == nil && f.Type() == "bool" {
			op = "coalesce(%s, false) = $%d" // false is null too
		}
		if err = add(k, op, v); err != nil {
			return
		}
	}
	for _, k := range slices.Sorted(maps.Keys(q.NOT)) {
		for _, v := range q.NOT[k] {
			if err = add(k, "%s is distinct from $%d", v); err != nil {
				return
			}
		}
	}
	for _, x := range []struct {
		op     string
		values map[string]any
	}{
		{"%s > $%d", q.GT}, {"%s >= $%d", q.GTE}, {"%s < $%d", q.LT}, {"%s <= $%d", q.LTE},
	} {
		for _, k := range slices.Sorted(maps.Keys(x.values)) {
			v := x.values[k]
			if v == nil || v == "" {
				continue
			}
			if err = add(k, x.op, v); err != nil {
				return
			}
		}
	}
	for _, k := range slices.Sorted(maps.Keys(q.Like)) {
		if q.Like[k] == "" {
			continue
		}
		if err = add(k, "%s ilike $%d", "%"+q.Like[k]+"%"); err != nil {
			return
		}
	}
	for _, k := range slices.Sorted(maps.Keys(q.IN)) {
		if len(q.IN[k]) == 0 {
			continue
		}
		if err = add(k, "%s = any($%d)", q.IN[k]); err != nil {
			return
		}
	}
	for _, k := range slices.Sorted(maps.Keys(q.INS)) {
		if len(q.INS[k]) == 0 {
			continue
		}
		if err = add(k, "%s = any($%d)", q.INS[k]); err != nil {
			return
		}
	}

	var b strings.Builder
	if len(and) > 0 {
		b.WriteString(" where ")
		b.WriteString(strings.Join(and, " and "))
	}
	if q.Sort != "" {
		col, _, err := q.column(q.Sort)
		if err != nil {
			return "", nil, nil, err
		}
		b.WriteString(" order by ")
		b.WriteString(col)
		if q.Desc {
			b.WriteString(" desc")
		}
	}
	if q.Limit > 0 {
		args = append(args, q.Limit)
		b.WriteString(" limit $")
		b.WriteString(strconv.Itoa(len(args)))
	}
	if q.Offset > 0 {
		args = append(args, q.Offset)
		b.WriteString(" offset $")
		b.WriteString(strconv.Itoa(len(args)))
	}

	return b.String(), fields, args, nil
}

// Search returns rows by query
func (a *AngelSQL) Search(q *AngelQuery) ([]Angel, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const angelSQLCreateTable = `create table if not exists angel (
	id          bigint primary key,
	role        bigint,
	key         uuid,
	created     timestamptz,
	ip          inet,
	smalls      smallint,
	name        text,
	about       text[] default '{}',
	ipes        inet[] default '{}',
	music       bigint[] default '{}',
	links       jsonb default '{}'::jsonb,
	stats       jsonb default '{}'::jsonb,
	unique (role, key)
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var angelSQLIndexes = []string{}

// CreateTable creates table if not exists
func (a *AngelSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), angelSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *AngelSQL) CreateIndexes() error {
	for _, q := range angelSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// field type
type AngelIndexTyped int

// int index
const (
	AngelIndexID = AngelIndexTyped(iota)
	AngelIndexRole
	AngelIndexKey
	AngelIndexCreated
	AngelIndexIP
	AngelIndexSmalls
	AngelIndexName
	AngelIndexAbout
	AngelIndexIpes
	AngelIndexMusic
	AngelIndexLinks
	AngelIndexStats
)

// string index
const (
	AngelFieldID      = "id"      // int sql{primary} ch{primary}  #readonly
	AngelFieldRole    = "role"    // role.Role sql{unique="css"} ch{primary}
	AngelFieldKey     = "key"     // uuid.UUID sql{unique="css"}
	AngelFieldCreated = "created" // time.Time
	AngelFieldIP      = "ip"      // net.IP
	AngelFieldSmalls  = "smalls"  // uint8
	AngelFieldName    = "name"    // string
	AngelFieldAbout   = "about"   // []string
	AngelFieldIpes    = "ipes"    // []net.IP
	AngelFieldMusic   = "music"   // []int
	AngelFieldLinks   = "links"   // map[string]string
	AngelFieldStats   = "stats"   // map[string]int
)

// index func
func AngelIndexes() []AngelIndexTyped {
	return []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexIP, AngelIndexSmalls, AngelIndexName, AngelIndexAbout, AngelIndexIpes, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

}

func AngelReadonlyList() []AngelIndexTyped {
	return []AngelIndexTyped{AngelIndexID}
}

func (a AngelIndexTyped) Readonly() bool {
	switch a {
	case AngelIndexID:
		return true
	default:
		return false
	}
}

// key index string
func (a AngelIndexTyped) String() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexIP:
		return "ip"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
	case AngelIndexIpes:
		return "ipes"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index string
func (a AngelIndexTyped) SQLName() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexIP:
		return "ip"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
	case AngelIndexIpes:
		return "ipes"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index clickhouse string
func (a AngelIndexTyped) ClickhouseName() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexIP:
		return "ip"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
	case AngelIndexIpes:
		return "ipes"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index type
func (a AngelIndexTyped) Type() string {
	switch a {
	case AngelIndexID:
		return "int"
	case AngelIndexRole:
		return "role.Role"
	case AngelIndexKey:
		return "uuid.UUID"
	case AngelIndexCreated:
		return "time.Time"
	case AngelIndexIP:
		return "net.IP"
	case AngelIndexSmalls:
		return "uint8"
	case AngelIndexName:
		return "string"
	case AngelIndexAbout:
		return "[]string"
	case AngelIndexIpes:
		return "[]net.IP"
	case AngelIndexMusic:
		return "[]int"
	case AngelIndexLinks:
		return "map[string]string"
	case AngelIndexStats:
		return "map[string]int"
	default:
		return ""
	}
}

// custom title
func (a AngelIndexTyped) Title() string {
	switch a {
	case AngelIndexID:
		return "ID"
	case AngelIndexRole:
		return "Role"
	case AngelIndexKey:
		return "Key"
	case AngelIndexCreated:
		return "Created"
	case AngelIndexIP:
		return "IP"
	case AngelIndexSmalls:
		return "Smalls"
	case AngelIndexName:
		return "Name"
	case AngelIndexAbout:
		return "About"
	case AngelIndexIpes:
		return "Ipes"
	case AngelIndexMusic:
		return "Music"
	case AngelIndexLinks:
		return "Links"
	case AngelIndexStats:
		return "Stats"
	default:
		return ""
	}
}

// custom desc
func (a AngelIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func AngelKeyIndex(key string) AngelIndexTyped {
	switch key {
	case "id":
		return AngelIndexID
	case "role":
		return AngelIndexRole
	case "key":
		return AngelIndexKey
	case "created":
		return AngelIndexCreated
	case "ip":
		return AngelIndexIP
	case "smalls":
		return AngelIndexSmalls
	case "name":
		return AngelIndexName
	case "about":
		return AngelIndexAbout
	case "ipes":
		return AngelIndexIpes
	case "music":
		return AngelIndexMusic
	case "links":
		return AngelIndexLinks
	case "stats":
		return AngelIndexStats
	default:
		return 0
	}
}

// valid struct key check
func AngelValidKey(key string) bool {
	switch key {
	case "id", "role", "key", "created", "ip", "smalls", "name", "about", "ipes", "music", "links", "stats":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Angel) Tuple() (r []any) {
	return []any{a.ID, a.Role, a.Key, a.Created, a.IP, a.Smalls, a.Name, a.About, a.Ipes, a.Music, a.Links, a.Stats}
}

// get struct value with function
func (a *Angel) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "role":
		return a.Role //role.Role
	case "key":
		return a.Key //uuid.UUID
	case "created":
		return a.Created //time.Time
	case "ip":
		return a.IP //net.IP
	case "smalls":
		return a.Smalls //uint8
	case "name":
		return a.Name //string
	case "about":
		return a.About //[]string
	case "ipes":
		return a.Ipes //[]net.IP
	case "music":
		return a.Music //[]int
	case "links":
		return a.Links //map[string]string
	case "stats":
		return a.Stats //map[string]int
	}
	return
}

// get any struct value as string
func (a *Angel) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "role":
		return fmt.Sprint(a.Role) //role.Role
	case "key":
		return fmt.Sprint(a.Key) //uuid.UUID
	case "created":
		return fmt.Sprint(a.Created) //time.Time
	case "ip":
		return fmt.Sprint(a.IP) //net.IP
	case "smalls":
		return fmt.Sprint(a.Smalls) //uint8
	case "name":
		return fmt.Sprint(a.Name) //string
	case "about":
		return fmt.Sprint(a.About) //[]string
	case "ipes":
		return fmt.Sprint(a.Ipes) //[]net.IP
	case "music":
		return fmt.Sprint(a.Music) //[]int
	case "links":
		return fmt.Sprint(a.Links) //map[string]string
	case "stats":
		return fmt.Sprint(a.Stats) //map[string]int
	}
	return
}

// struct to map
func (a *Angel) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"role":    a.Role,
		"key":     a.Key,
		"created": a.Created,
		"ip":      a.IP,
		"smalls":  a.Smalls,
		"name":    a.Name,
		"about":   a.About,
		"ipes":    a.Ipes,
		"music":   a.Music,
		"links":   a.Links,
		"stats":   a.Stats,
	}
}

// struct to map
func (a *Angel) Iterate(f func(k AngelIndexTyped, v any)) {
	for _, x := range AngelIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Angel) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseAngel(v []byte) (a Angel, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// Tuple create an array from struct for clickhouse
func (a *Angel) clickhouseTuple() (r []any) {
	return []any{a.ID, int(a.Role), a.Key, a.Created, a.IP, a.Smalls, a.Name, a.About, a.Ipes, a.Music, a.Links, func() map[string]int64 {
		r := make(map[string]int64, len(a.Stats))
		for k, v := range a.Stats {
			r[string(k)] = int64(v)
		}
		return r
	}()}
}

// AngelCQL clickhouse queries of table angel
//
// Rules:
//   - safe for concurrent use
//   - not found row is sql.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "country = $1 and views > $2"
//   - rows are only inserted, selected and deleted
type AngelCQL struct {
	conn driver.Conn
	mu   sync.Mutex
	list []Angel // buffer of Add
}

// NewAngelCQL creates queries on connection and table if not exists.
// Rows of Add are written every 10 minutes and by Push.
// Error of table is not returned: call CreateTable to see it
func NewAngelCQL(conn driver.Conn) *AngelCQL {
	a := &AngelCQL{conn: conn}
	_ = a.CreateTable()
	go a.flush()
	return a
}

// Conn returns connection, for own queries
func (a *AngelCQL) Conn() driver.Conn {
	return a.conn
}

// TableName returns table name
func (a *AngelCQL) TableName() string {
	return "angel"
}

// Fields returns all columns
func (a *AngelCQL) Fields() []AngelIndexTyped {
	return slices.Clone(angelCHFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all columns
var angelCHFields = []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexIP, AngelIndexSmalls, AngelIndexName, AngelIndexAbout, AngelIndexIpes, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

// column names
var angelCHColumns = [...]string{
	AngelIndexID:      "id",
	AngelIndexRole:    "role",
	AngelIndexKey:     "key",
	AngelIndexCreated: "created",
	AngelIndexIP:      "ip",
	AngelIndexSmalls:  "smalls",
	AngelIndexName:    "name",
	AngelIndexAbout:   "about",
	AngelIndexIpes:    "ipes",
	AngelIndexMusic:   "music",
	AngelIndexLinks:   "links",
	AngelIndexStats:   "stats",
}

const angelCHSelectAll = "id, role, key, created, ip, smalls, name, about, ipes, music, links, stats"

// mutations wait until the data is changed
const angelCHSync = " settings mutations_sync = 1"

// angelCHColumn returns column name of field
func angelCHColumn(f AngelIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(angelCHColumns) || angelCHColumns[f] == "" {
		return "", fmt.Errorf("angel: field %d is not a clickhouse column", int(f))
	}
	return angelCHColumns[f], nil
}

// angelCHSelect returns select list of fields, all columns when empty
func angelCHSelect(fields []AngelIndexTyped) (string, []AngelIndexTyped, error) {
	if len(fields) == 0 {
		return angelCHSelectAll, angelCHFields, nil
	}
	list := make([]string, 0, len(fields))
	for _, f := range fields {
		col, err := angelCHColumn(f)
		if err != nil {
			return "", nil, err
		}
		list = append(list, col)
	}
	return strings.Join(list, ", "), fields, nil
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

// Add puts rows to buffer: they are written every 10 minutes and by Push
func (a *AngelCQL) Add(v ...Angel) {
	a.mu.Lock()
	a.list = append(a.list, v...)
	a.mu.Unlock()
}

// Len returns count of rows in buffer
func (a *AngelCQL) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.list)
}

// Push writes rows of buffer. Rows stay in buffer when the write fails
func (a *AngelCQL) Push() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.list) == 0 {
		return nil
	}
	if err := a.send(a.list); err != nil {
		return err
	}
	a.list = a.list[:0]
	return nil
}

// Insert writes rows now, without buffer
func (a *AngelCQL) Insert(v ...Angel) error {
	if len(v) == 0 {
		return nil
	}
	return a.send(v)
}

// send writes rows by one batch
func (a *AngelCQL) send(list []Angel) error {
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into angel (id, role, key, created, ip, smalls, name, about, ipes, music, links, stats)")
	if err != nil {
		return err
	}
	for i := range list {
		if err = batch.Append(list[i].clickhouseTuple()...); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

// flush writes buffer every 10 minutes
func (a *AngelCQL) flush() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		_ = a.Push()
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *AngelCQL) query(fields []AngelIndexTyped, tail string, args ...any) (res []Angel, err error) {
	cols, fields, err := angelCHSelect(fields)
	if err != nil {
		return
	}
	rows, err := a.conn.Query(context.Background(), "select "+cols+" from angel"+tail, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	// scan targets by column types: clickhouse needs exact types
	types := rows.ColumnTypes()
	for rows.Next() {
		scan := make([]any, len(types))
		for i, t := range types {
			scan[i] = reflect.New(t.ScanType()).Interface()
		}
		if err = rows.Scan(scan...); err != nil {
			return
		}
		var v Angel
		for i, f := range fields {
			v.clickhouseSet(f, scan[i])
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// queryRow selects one row
func (a *AngelCQL) queryRow(fields []AngelIndexTyped, tail string, args ...any) (v Angel, err error) {
	list, err := a.query(fields, tail, args...)
	if err != nil {
		return
	}
	if len(list) == 0 {
		return v, sql.ErrNoRows
	}
	return list[0], nil
}

// Get returns row by id
func (a *AngelCQL) Get(id int, fields ...AngelIndexTyped) (Angel, error) {
	return a.queryRow(fields, " where id = $1 limit 1", id)
}

// Exists checks row by id
func (a *AngelCQL) Exists(id int) (bool, error) {
	n, err := a.CountWhere("id = $1", id)
	return n > 0, err
}

// GetBy returns first row with field = v
func (a *AngelCQL) GetBy(field AngelIndexTyped, v any, fields ...AngelIndexTyped) (res Angel, err error) {
	col, err := angelCHColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "country = $1 order by created desc", "DE")
func (a *AngelCQL) GetWhere(fields []AngelIndexTyped, where string, args ...any) (Angel, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *AngelCQL) Has(field AngelIndexTyped, v any) (bool, error) {
	col, err := angelCHColumn(field)
	if err != nil {
		return false, err
	}
	n, err := a.CountWhere(col+" = $1", v)
	return n > 0, err
}

// All returns all rows
func (a *AngelCQL) All(fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *AngelCQL) List(limit, offset int, fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *AngelCQL) Select(fields []AngelIndexTyped, where string, args ...any) ([]Angel, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *AngelCQL) Count() (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from angel").Scan(&n)
	return int(n), err
}

// CountWhere returns count of rows by condition
func (a *AngelCQL) CountWhere(where string, args ...any) (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from angel where "+where, args...).Scan(&n)
	return int(n), err
}

// Stat returns count of rows by values of field: {"DE": 425, "US": 134}
func (a *AngelCQL) Stat(field AngelIndexTyped) (map[string]int, error) {
	col, err := angelCHColumn(field)
	if err != nil {
		return nil, err
	}
	rows, err := a.conn.Query(context.Background(), "select toString("+col+"), count() from angel group by "+col)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string]int{}
	for rows.Next() {
		var k string
		var n uint64
		if err = rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		res[k] = int(n)
	}
	return res, rows.Err()
}

// ---------------------------------------------------------------------------
// delete: mutations, they wait until the data is removed
// ---------------------------------------------------------------------------

// Delete removes row by id
func (a *AngelCQL) Delete(id int) error {
	return a.conn.Exec(context.Background(), "alter table angel delete where id = $1"+angelCHSync, id)
}

// DeleteWhere removes rows by condition
func (a *AngelCQL) DeleteWhere(where string, args ...any) error {
	return a.conn.Exec(context.Background(), "alter table angel delete where "+where+angelCHSync, args...)
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

// Creds: where role = @role and key = @key limit 1
func (a *AngelCQL) Creds(role role.Role, key uuid.UUID) (res Angel, err error) {
	res, err = a.queryRow(nil, " where role = $1 and key = $2 limit 1", role, key)
	return
}

// Role: select role from angels where id = @id
func (a *AngelCQL) Role(id int) (res role.Role, err error) {
	err = a.conn.QueryRow(context.Background(), "select role from angels where id = $1", id).Scan(&res)
	return
}

// Music: select music from angels where id = @id
func (a *AngelCQL) Music(id int) (res []int, err error) {
	err = a.conn.QueryRow(context.Background(), "select music from angels where id = $1", id).Scan(&res)
	return
}

// Touch: update angels set created = now() where id = @id
func (a *AngelCQL) Touch(id int) (err error) {
	return a.conn.Exec(context.Background(), "update angels set created = now() where id = $1", id)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const angelCHCreateTable = `create table if not exists angel (
	id          Int64,
	role        Int64,
	key         UUID,
	created     DateTime64(3),
	ip          IPv4,
	smalls      UInt8,
	name        String,
	about       Array(String),
	ipes        JSON,
	music       Array(Int64),
	links       Map(String, String),
	stats       Map(String, Int64)
)
engine = MergeTree()
order by (id, role)
primary key (id, role)`

// CreateTable creates table if not exists
func (a *AngelCQL) CreateTable() error {
	return a.conn.Exec(context.Background(), angelCHCreateTable)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// clickhouseSet sets field from value of clickhouse row
func (a *Angel) clickhouseSet(f AngelIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case AngelIndexID:
		angelCHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case AngelIndexRole:
		angelCHAssign(reflect.ValueOf(&a.Role).Elem(), reflect.ValueOf(v))
	case AngelIndexKey:
		angelCHAssign(reflect.ValueOf(&a.Key).Elem(), reflect.ValueOf(v))
	case AngelIndexCreated:
		angelCHAssign(reflect.ValueOf(&a.Created).Elem(), reflect.ValueOf(v))
	case AngelIndexIP:
		angelCHAssign(reflect.ValueOf(&a.IP).Elem(), reflect.ValueOf(v))
	case AngelIndexSmalls:
		angelCHAssign(reflect.ValueOf(&a.Smalls).Elem(), reflect.ValueOf(v))
	case AngelIndexName:
		angelCHAssign(reflect.ValueOf(&a.Name).Elem(), reflect.ValueOf(v))
	case AngelIndexAbout:
		angelCHAssign(reflect.ValueOf(&a.About).Elem(), reflect.ValueOf(v))
	case AngelIndexIpes:
		angelCHAssign(reflect.ValueOf(&a.Ipes).Elem(), reflect.ValueOf(v))
	case AngelIndexMusic:
		angelCHAssign(reflect.ValueOf(&a.Music).Elem(), reflect.ValueOf(v))
	case AngelIndexLinks:
		angelCHAssign(reflect.ValueOf(&a.Links).Elem(), reflect.ValueOf(v))
	case AngelIndexStats:
		angelCHAssign(reflect.ValueOf(&a.Stats).Elem(), reflect.ValueOf(v))
	}
}

// angelCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func angelCHAssign(d, s reflect.Value) {
	for s.Kind() == reflect.Pointer || s.Kind() == reflect.Interface {
		if s.IsNil() {
			return
		}
		s = s.Elem()
	}

	number := func(k reflect.Kind) bool {
		return k >= reflect.Int && k <= reflect.Float64 && k != reflect.Uintptr
	}

	switch {
	case s.Type().AssignableTo(d.Type()):
		d.Set(s)

	// *T
	case d.Kind() == reflect.Pointer:
		p := reflect.New(d.Type().Elem())
		angelCHAssign(p.Elem(), s)
		d.Set(p)

	// IPv4, UUID, DateTime > string
	case d.Kind() == reflect.String:
		switch v := s.Interface().(type) {
		case fmt.Stringer:
			d.SetString(v.String())
		case []byte:
			d.SetString(string(v))
		default:
			d.SetString(fmt.Sprint(v))
		}

	case d.Kind() == reflect.Bool:
		d.SetBool(!s.IsZero())

	case number(d.Kind()):
		switch v := s.Interface().(type) {
		// DateTime > unix time
		case time.Time:
			if !v.IsZero() {
				d.Set(reflect.ValueOf(v.Unix()).Convert(d.Type()))
			}
		case bool:
			if v {
				d.Set(reflect.ValueOf(1).Convert(d.Type()))
			}
		case string:
			if n, err := strconv.ParseFloat(v, 64); err == nil {
				d.Set(reflect.ValueOf(n).Convert(d.Type()))
			}
		default:
			if number(s.Kind()) {
				d.Set(s.Convert(d.Type()))
			}
		}

	// Array(UInt64) > []int
	case d.Kind() == reflect.Slice && s.Kind() == reflect.Slice:
		r := reflect.MakeSlice(d.Type(), s.Len(), s.Len())
		for i := range s.Len() {
			angelCHAssign(r.Index(i), s.Index(i))
		}
		d.Set(r)

	// Map(String, Int64) > map[string]int
	case d.Kind() == reflect.Map && s.Kind() == reflect.Map:
		r := reflect.MakeMapWithSize(d.Type(), s.Len())
		for it := s.MapRange(); it.Next(); {
			k := reflect.New(d.Type().Key()).Elem()
			v := reflect.New(d.Type().Elem()).Elem()
			angelCHAssign(k, it.Key())
			angelCHAssign(v, it.Value())
			r.SetMapIndex(k, v)
		}
		d.Set(r)

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}
