package news

import (
	"context"
	stderrors "errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/errors"
	"github.com/monopolly/jayson/test/role"
)

/*
	Auto-generate. Do not change!
	Help to auto-generated correct models for golang structures.
	Sergey Keplin (c) 2026
	a@senthy.com, lava.mobi@gmail.com, https://t.me/martinprestone
	github.com/monopolly/jsons
	Ex: //! [] go=News js=NewsJson sql=news noinit up...

	STRUCT:
	!omit                         No omit tag for json
	ch=views                      Set clickhouse sql table name. Ex: ch=views
	chengine=MergeTree            Optional, mergeTree by default
	debug                         Debug mode
	ee                            Errors of sql methods are errors.E of github.com/monopolly/errors: errors.PGX(err)
	go=News                       Set golang struct names. Ex: go=News1 > type News1 struct{}
	gotiny                        Create gotiny marshal/unmarshal
	lock                          Lock model for generation. Can't change model.
	msgp                          Create message pack marshal/unmarshal
	noinit                        No New() init function for struct
	optimize                      Try to create golang padding optimized struct
	proto=NewsProto               Generate proto file and compile to current package
	sql=news                      Set sql table name. Ex: sql=accounts
	test                          Generate go tests for generated model

	Field:
	desc{}                        Add custom desc for index. desc{This field for success}
	js{name=""}                   Replace json field name. Ex: js{name="sid"}
	title{}                       Add custom title for index. title{Nice & Sweet}

	GO:
	#                             Add lists for fields. Ex: id int //#readonly #must...
	@                             Add custom structs for fields. Ex: id int //@public @team...
	base=""                       Underlying type for custom type (detected automatically). Ex: role role.Role // go{base="int"}
	desc=""                       Add custom desc for index
	desc{}                        Add field user title desc{Use it nice}
	must                          Create one validation function for all must fields
	name=""                       Replace golang struct name. Ex: name="NewsList"
	title=""                      Add custom title for index
	title{}                       Add field user title title{Nice}
	type=""                       Replace golang type. Ex: type="[]*News"
	up                            Make uppercase for functions

	SQL:
	add="primary key"             Append sql for field. Ex: replace="primary key"
	altertable                    Add field line Alter table to SQL file with current time comment
	defaults                      Add default value based on field type
	idx                           Add index fields by group name. Ex: idx="nameIndex" idx="credsIndex"
	index                         Create simple default index or gin for jsonb and arrays
	noinsert                      Do use field for insert function
	notnull                       Add not null to column
	primary                       Primary key. One field: asset bigint primary key. Several fields: primary key (asset, user)
	renames="oldname"             Create an alter table record in SQL file. Rename table column.
	replace="bigint primary key"  Rewrite sql for field. Ex: replace="bigint primary key"
	search                        Add tsvector index by group. Ex: search="tsv": tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || brand)). For search: SELECT brand, title FROM assets WHERE search @@ to_tsquery('english', 'f8');
	skip                          Skip field for sql queries
	type="date"                   Explicit postgres type: date, timestamp, time, interval, varchar(64), numeric(18,2), json, vector(3)...
	type="jsonb"                  Rewrite sql type for field. Ex: type="jsonb"
	unique="groupname"            Add unique fields constrains by group (you need set group name, then generator join fields). Ex: unique="group1" unique="group2"
	unix                          Add default value: extract(epoch from now())
	ver="v4"                      Create an alter table record in SQL file. Add new column. With new version in comment. Ex: ver="2"

	Clickhouse:
	low                           ch{low} Wrap field type in LowCardinality(...)
	primary"                      ch{primary} Make primary key
	type=ip                       ch{type=ip} Use ClickHouse IPv4 type for IP address fields

	PROTO:
	name="field"                  Rewrite proto field name
	skip                          Skip field for proto
	type="string"                 Rewrite proto type for field. Ex: type="google.protobuf.Value"
*/

// field type
type AngelIndexTyped int

// int index
const (
	AngelIndexID = AngelIndexTyped(iota)
	AngelIndexRole
	AngelIndexKey
	AngelIndexCreated
	AngelIndexSmalls
	AngelIndexName
	AngelIndexAbout
	AngelIndexMusic
	AngelIndexLinks
	AngelIndexStats
)

// string index
const (
	AngelFieldID      = "id"      // int sql{primary}  #readonly
	AngelFieldRole    = "role"    // role.Role sql{unique="css"}
	AngelFieldKey     = "key"     // uuid.UUID sql{unique="css"}
	AngelFieldCreated = "created" // time.Time
	AngelFieldSmalls  = "smalls"  // uint8
	AngelFieldName    = "name"    // string
	AngelFieldAbout   = "about"   // []string
	AngelFieldMusic   = "music"   // []int
	AngelFieldLinks   = "links"   // map[string]string
	AngelFieldStats   = "stats"   // map[string]int
)

// index func
func AngelIndexes() []AngelIndexTyped {
	return []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexSmalls, AngelIndexName, AngelIndexAbout, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

}

// 88 bytes (go padding)
//
//easyjson:json
type Angel struct {
	ID      int               `json:"id,omitempty"`      // sql{primary}  #readonly
	Role    role.Role         `json:"role,omitempty"`    // sql{unique="css"}
	Key     uuid.UUID         `json:"key,omitempty"`     // sql{unique="css"}
	Created time.Time         `json:"created,omitempty"` //
	Smalls  uint8             `json:"smalls,omitempty"`  //
	Name    string            `json:"name,omitempty"`    //
	About   []string          `json:"about,omitempty"`   //
	Music   []int             `json:"music,omitempty"`   //
	Links   map[string]string `json:"links,omitempty"`   //
	Stats   map[string]int    `json:"stats,omitempty"`   //
}

// init struct
func NewAngel() (a *Angel) {
	a = new(Angel)
	a.Links = make(map[string]string)
	a.Stats = make(map[string]int)
	return
}

// Tuple create an array from struct
func (a *Angel) Tuple() (r []any) {
	return []any{a.ID, a.Role, a.Key, a.Created, a.Smalls, a.Name, a.About, a.Music, a.Links, a.Stats}
}

// Tuple create an array from struct for clickhouse
func (a *Angel) clickhouseTuple() (r []any) {
	return []any{a.ID, int(a.Role), a.Key, a.Created, a.Smalls, a.Name, a.About, a.Music, a.Links, a.Stats}
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
	case "smalls":
		return a.Smalls //uint8
	case "name":
		return a.Name //string
	case "about":
		return a.About //[]string
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
	case "smalls":
		return fmt.Sprint(a.Smalls) //uint8
	case "name":
		return fmt.Sprint(a.Name) //string
	case "about":
		return fmt.Sprint(a.About) //[]string
	case "music":
		return fmt.Sprint(a.Music) //[]int
	case "links":
		return fmt.Sprint(a.Links) //map[string]string
	case "stats":
		return fmt.Sprint(a.Stats) //map[string]int
	}
	return
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
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
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
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
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
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexName:
		return "name"
	case AngelIndexAbout:
		return "about"
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
	case AngelIndexSmalls:
		return "uint8"
	case AngelIndexName:
		return "string"
	case AngelIndexAbout:
		return "[]string"
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
	case AngelIndexSmalls:
		return "Smalls"
	case AngelIndexName:
		return "Name"
	case AngelIndexAbout:
		return "About"
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
	case "smalls":
		return AngelIndexSmalls
	case "name":
		return AngelIndexName
	case "about":
		return AngelIndexAbout
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
	case "id", "role", "key", "created", "smalls", "name", "about", "music", "links", "stats":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Angel) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"role":    a.Role,
		"key":     a.Key,
		"created": a.Created,
		"smalls":  a.Smalls,
		"name":    a.Name,
		"about":   a.About,
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

// NewAngelSQL creates queries on pool, table and indexes if not exist
func NewAngelSQL(pool *pgxpool.Pool) (*AngelSQL, errors.E) {
	a := &AngelSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err != nil {
		return nil, err
	}
	return a, a.CreateIndexes()
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
func (a *AngelSQL) Conn(f func(conn *pgxpool.Conn) errors.E) errors.E {
	conn, err := a.pool.Acquire(context.Background())
	if err != nil {
		return errors.PGX(err)
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
func (a *AngelSQL) Begin(f func(tx pgx.Tx) errors.E) errors.E {
	return angelSQLErr(pgx.BeginFunc(context.Background(), a.db, func(tx pgx.Tx) error {
		if err := f(tx); err != nil {
			return err
		}
		return nil
	}))
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
var angelSQLFields = []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexSmalls, AngelIndexName, AngelIndexAbout, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

// columns of insert
var angelSQLInsertColumns = []string{"id", "role", "key", "created", "smalls", "name", "about", "music", "links", "stats"}

// sql column names
var angelSQLColumns = [...]string{
	AngelIndexID:      "id",
	AngelIndexRole:    "role",
	AngelIndexKey:     "key",
	AngelIndexCreated: "created",
	AngelIndexSmalls:  "smalls",
	AngelIndexName:    "name",
	AngelIndexAbout:   "about",
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
	AngelIndexSmalls:  "coalesce(smalls, 0)",
	AngelIndexName:    "coalesce(name, '')",
	AngelIndexAbout:   "about",
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
	AngelIndexSmalls:  "smallint",
	AngelIndexName:    "text",
	AngelIndexAbout:   "text[]",
	AngelIndexMusic:   "bigint[]",
	AngelIndexLinks:   "jsonb",
	AngelIndexStats:   "jsonb",
}

const angelSQLSelectAll = "coalesce(id, 0), coalesce(role, 0), key, coalesce(created, '0001-01-01 00:00:00+00'), coalesce(smalls, 0), coalesce(name, ''), about, music, links, stats"

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
		return "", nil, stderrors.New("angel: no values")
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
		case AngelIndexSmalls:
			dest[i] = &a.Smalls
		case AngelIndexName:
			dest[i] = &a.Name
		case AngelIndexAbout:
			dest[i] = &a.About
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
	case AngelIndexSmalls:
		return a.Smalls
	case AngelIndexName:
		return a.Name
	case AngelIndexAbout:
		return a.About
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
		a.Smalls,
		a.Name,
		a.About,
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
		a.Smalls,
		a.Name,
		a.About,
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

func (a *AngelSQL) stdGet(id int, fields ...AngelIndexTyped) (Angel, error) {
	return a.queryRow(fields, " where id = $1", id)
}

func (a *AngelSQL) stdExists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from angel where id = $1)", id).Scan(&has)
	return
}

func (a *AngelSQL) stdGetBy(field AngelIndexTyped, v any, fields ...AngelIndexTyped) (res Angel, err error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

func (a *AngelSQL) stdGetWhere(fields []AngelIndexTyped, where string, args ...any) (Angel, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

func (a *AngelSQL) stdHas(field AngelIndexTyped, v any) (has bool, err error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from angel where "+col+" = $1)", v).Scan(&has)
	return
}

func (a *AngelSQL) stdAll(fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id")
}

func (a *AngelSQL) stdList(limit, offset int, fields ...AngelIndexTyped) ([]Angel, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

func (a *AngelSQL) stdSelect(fields []AngelIndexTyped, where string, args ...any) ([]Angel, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

func (a *AngelSQL) stdCount() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from angel").Scan(&count)
	return
}

func (a *AngelSQL) stdCountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from angel where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const angelSQLInsert = "insert into angel (id, role, key, created, smalls, name, about, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

const angelSQLInsertFull = "insert into angel (id, role, key, created, smalls, name, about, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

func (a *AngelSQL) stdInsert(v *Angel) error {
	_, err := a.db.Exec(context.Background(), angelSQLInsert, v.sqlInsertArgs()...)
	return err
}

func (a *AngelSQL) stdInsertNoConflict(v *Angel) (inserted bool, err error) {
	tag, err := a.db.Exec(context.Background(), angelSQLInsert+" on conflict do nothing", v.sqlInsertArgs()...)
	return tag.RowsAffected() > 0, err
}

func (a *AngelSQL) stdInsertFull(v *Angel) error {
	_, err := a.db.Exec(context.Background(), angelSQLInsertFull, v.sqlAllArgs()...)
	return err
}

func (a *AngelSQL) stdUpsert(v *Angel) error {
	_, err := a.db.Exec(context.Background(), "insert into angel (id, role, key, created, smalls, name, about, music, links, stats) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) on conflict (id) do update set role = excluded.role, key = excluded.key, created = excluded.created, smalls = excluded.smalls, name = excluded.name, about = excluded.about, music = excluded.music, links = excluded.links, stats = excluded.stats", v.sqlAllArgs()...)
	return err
}

func (a *AngelSQL) stdInsertMany(list []Angel) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"angel"}, angelSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

func (a *AngelSQL) stdInsertBatch(list []*Angel) error {
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

func (a *AngelSQL) stdUpdate(id int, field AngelIndexTyped, v any) error {
	col, err := angelSQLColumn(field)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = $1 where id = $2", v, id))
}

func (a *AngelSQL) stdUpdates(id int, v map[AngelIndexTyped]any) error {
	set, args, err := angelSQLSet(v, 1)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

func (a *AngelSQL) stdSave(v *Angel, fields ...AngelIndexTyped) error {
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

func (a *AngelSQL) stdInc(id int, field AngelIndexTyped, v any) error {
	col, err := angelSQLColumn(field)
	if err != nil {
		return err
	}
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

func (a *AngelSQL) stdDelete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from angel where id = $1", id)
	return err
}

func (a *AngelSQL) stdUpdateWhere(field AngelIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := angelSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update angel set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

func (a *AngelSQL) stdUpdatesWhere(v map[AngelIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := angelSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update angel set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

func (a *AngelSQL) stdDeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from angel where "+where, args...)
	return tag.RowsAffected(), err
}

func (a *AngelSQL) stdAppendAbout(id int, v ...string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set about = coalesce(about, '{}') || $1::text[] where id = $2", v, id))
}

func (a *AngelSQL) stdRemoveAbout(id int, v string) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set about = array_remove(about, $1::text) where id = $2", v, id))
}

func (a *AngelSQL) stdAppendMusic(id int, v ...int) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set music = coalesce(music, '{}') || $1::bigint[] where id = $2", v, id))
}

func (a *AngelSQL) stdRemoveMusic(id int, v int) error {
	return angelSQLAffected(a.db.Exec(context.Background(), "update angel set music = array_remove(music, $1::bigint) where id = $2", v, id))
}

func (a *AngelSQL) stdUpdateLinks(id int, k string, v string) error {
	return a.jsonSet(id, "links", fmt.Sprint(k), v)
}

func (a *AngelSQL) stdMergeLinks(id int, v map[string]string) error {
	return a.jsonMerge(id, "links", "{}", v)
}

func (a *AngelSQL) stdDeleteLinks(id int, k string) error {
	return a.jsonDelete(id, "links", fmt.Sprint(k))
}

func (a *AngelSQL) stdRenameLinks(id int, k, newK string) error {
	return a.jsonRename(id, "links", fmt.Sprint(k), fmt.Sprint(newK))
}

func (a *AngelSQL) stdUpdateStats(id int, k string, v int) error {
	return a.jsonSet(id, "stats", fmt.Sprint(k), v)
}

func (a *AngelSQL) stdMergeStats(id int, v map[string]int) error {
	return a.jsonMerge(id, "stats", "{}", v)
}

func (a *AngelSQL) stdDeleteStats(id int, k string) error {
	return a.jsonDelete(id, "stats", fmt.Sprint(k))
}

func (a *AngelSQL) stdRenameStats(id int, k, newK string) error {
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

func (a *AngelSQL) stdCreds(role role.Role, key uuid.UUID) (res Angel, err error) {
	res, err = a.queryRow(nil, " where role = $1 and key = $2 limit 1", role, key)
	return
}

func (a *AngelSQL) stdRole(id int) (res role.Role, err error) {
	err = a.db.QueryRow(context.Background(), "select role from angels where id = $1", id).Scan(&res)
	return
}

func (a *AngelSQL) stdMusic(id int) (res []int, err error) {
	err = a.db.QueryRow(context.Background(), "select music from angels where id = $1", id).Scan(&res)
	return
}

func (a *AngelSQL) stdTouch(id int) (err error) {
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

func (a *AngelSQL) stdSearch(q *AngelQuery) ([]Angel, error) {
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
	smalls      smallint,
	name        text,
	about       text[] default '{}',
	music       bigint[] default '{}',
	links       jsonb default '{}'::jsonb,
	stats       jsonb default '{}'::jsonb,
	unique (role, key)
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var angelSQLIndexes = []string{}

func (a *AngelSQL) stdCreateTable() error {
	_, err := a.db.Exec(context.Background(), angelSQLCreateTable)
	return err
}

func (a *AngelSQL) stdCreateIndexes() error {
	for _, q := range angelSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// angelSQLErr converts error of postgres to errors.E
func angelSQLErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.PGX(err)
}

// Get returns row by id
func (a *AngelSQL) Get(id int, fields ...AngelIndexTyped) (Angel, errors.E) {
	res, err := a.stdGet(id, fields...)
	return res, angelSQLErr(err)
}

// Exists checks row by id
func (a *AngelSQL) Exists(id int) (bool, errors.E) {
	res, err := a.stdExists(id)
	return res, angelSQLErr(err)
}

// GetBy returns first row with field = v
func (a *AngelSQL) GetBy(field AngelIndexTyped, v any, fields ...AngelIndexTyped) (Angel, errors.E) {
	res, err := a.stdGetBy(field, v, fields...)
	return res, angelSQLErr(err)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *AngelSQL) GetWhere(fields []AngelIndexTyped, where string, args ...any) (Angel, errors.E) {
	res, err := a.stdGetWhere(fields, where, args...)
	return res, angelSQLErr(err)
}

// Has checks row with field = v
func (a *AngelSQL) Has(field AngelIndexTyped, v any) (bool, errors.E) {
	res, err := a.stdHas(field, v)
	return res, angelSQLErr(err)
}

// All returns all rows
func (a *AngelSQL) All(fields ...AngelIndexTyped) ([]Angel, errors.E) {
	res, err := a.stdAll(fields...)
	return res, angelSQLErr(err)
}

// List returns page of rows
func (a *AngelSQL) List(limit, offset int, fields ...AngelIndexTyped) ([]Angel, errors.E) {
	res, err := a.stdList(limit, offset, fields...)
	return res, angelSQLErr(err)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *AngelSQL) Select(fields []AngelIndexTyped, where string, args ...any) ([]Angel, errors.E) {
	res, err := a.stdSelect(fields, where, args...)
	return res, angelSQLErr(err)
}

// Count returns count of rows
func (a *AngelSQL) Count() (int, errors.E) {
	res, err := a.stdCount()
	return res, angelSQLErr(err)
}

// CountWhere returns count of rows by condition
func (a *AngelSQL) CountWhere(where string, args ...any) (int, errors.E) {
	res, err := a.stdCountWhere(where, args...)
	return res, angelSQLErr(err)
}

// Insert adds row
func (a *AngelSQL) Insert(v *Angel) errors.E {
	return angelSQLErr(a.stdInsert(v))
}

// InsertNoConflict adds row, false when row exists
func (a *AngelSQL) InsertNoConflict(v *Angel) (bool, errors.E) {
	res, err := a.stdInsertNoConflict(v)
	return res, angelSQLErr(err)
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *AngelSQL) InsertFull(v *Angel) errors.E {
	return angelSQLErr(a.stdInsertFull(v))
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *AngelSQL) Upsert(v *Angel) errors.E {
	return angelSQLErr(a.stdUpsert(v))
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *AngelSQL) InsertMany(list []Angel) (int64, errors.E) {
	res, err := a.stdInsertMany(list)
	return res, angelSQLErr(err)
}

// InsertBatch adds rows in one round trip
func (a *AngelSQL) InsertBatch(list []*Angel) errors.E {
	return angelSQLErr(a.stdInsertBatch(list))
}

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *AngelSQL) Update(id int, field AngelIndexTyped, v any) errors.E {
	return angelSQLErr(a.stdUpdate(id, field, v))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *AngelSQL) Updates(id int, v map[AngelIndexTyped]any) errors.E {
	return angelSQLErr(a.stdUpdates(id, v))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *AngelSQL) Save(v *Angel, fields ...AngelIndexTyped) errors.E {
	return angelSQLErr(a.stdSave(v, fields...))
}

// Inc adds v to number field: views = views + 1
func (a *AngelSQL) Inc(id int, field AngelIndexTyped, v any) errors.E {
	return angelSQLErr(a.stdInc(id, field, v))
}

// Delete removes row by id
func (a *AngelSQL) Delete(id int) errors.E {
	return angelSQLErr(a.stdDelete(id))
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *AngelSQL) UpdateWhere(field AngelIndexTyped, v any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdateWhere(field, v, where, args...)
	return res, angelSQLErr(err)
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *AngelSQL) UpdatesWhere(v map[AngelIndexTyped]any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdatesWhere(v, where, args...)
	return res, angelSQLErr(err)
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *AngelSQL) DeleteWhere(where string, args ...any) (int64, errors.E) {
	res, err := a.stdDeleteWhere(where, args...)
	return res, angelSQLErr(err)
}

// AppendAbout adds values to array about
func (a *AngelSQL) AppendAbout(id int, v ...string) errors.E {
	return angelSQLErr(a.stdAppendAbout(id, v...))
}

// RemoveAbout removes all entries of v from array about
func (a *AngelSQL) RemoveAbout(id int, v string) errors.E {
	return angelSQLErr(a.stdRemoveAbout(id, v))
}

// AppendMusic adds values to array music
func (a *AngelSQL) AppendMusic(id int, v ...int) errors.E {
	return angelSQLErr(a.stdAppendMusic(id, v...))
}

// RemoveMusic removes all entries of v from array music
func (a *AngelSQL) RemoveMusic(id int, v int) errors.E {
	return angelSQLErr(a.stdRemoveMusic(id, v))
}

// UpdateLinks sets key of jsonb links
func (a *AngelSQL) UpdateLinks(id int, k string, v string) errors.E {
	return angelSQLErr(a.stdUpdateLinks(id, k, v))
}

// MergeLinks adds keys to jsonb links: existing keys are replaced
func (a *AngelSQL) MergeLinks(id int, v map[string]string) errors.E {
	return angelSQLErr(a.stdMergeLinks(id, v))
}

// DeleteLinks removes key of jsonb links
func (a *AngelSQL) DeleteLinks(id int, k string) errors.E {
	return angelSQLErr(a.stdDeleteLinks(id, k))
}

// RenameLinks renames key of jsonb links, pgx.ErrNoRows when key not found
func (a *AngelSQL) RenameLinks(id int, k, newK string) errors.E {
	return angelSQLErr(a.stdRenameLinks(id, k, newK))
}

// UpdateStats sets key of jsonb stats
func (a *AngelSQL) UpdateStats(id int, k string, v int) errors.E {
	return angelSQLErr(a.stdUpdateStats(id, k, v))
}

// MergeStats adds keys to jsonb stats: existing keys are replaced
func (a *AngelSQL) MergeStats(id int, v map[string]int) errors.E {
	return angelSQLErr(a.stdMergeStats(id, v))
}

// DeleteStats removes key of jsonb stats
func (a *AngelSQL) DeleteStats(id int, k string) errors.E {
	return angelSQLErr(a.stdDeleteStats(id, k))
}

// RenameStats renames key of jsonb stats, pgx.ErrNoRows when key not found
func (a *AngelSQL) RenameStats(id int, k, newK string) errors.E {
	return angelSQLErr(a.stdRenameStats(id, k, newK))
}

// Creds: where role = @role and key = @key limit 1
func (a *AngelSQL) Creds(role role.Role, key uuid.UUID) (Angel, errors.E) {
	res, err := a.stdCreds(role, key)
	return res, angelSQLErr(err)
}

// Role: select role from angels where id = @id
func (a *AngelSQL) Role(id int) (role.Role, errors.E) {
	res, err := a.stdRole(id)
	return res, angelSQLErr(err)
}

// Music: select music from angels where id = @id
func (a *AngelSQL) Music(id int) ([]int, errors.E) {
	res, err := a.stdMusic(id)
	return res, angelSQLErr(err)
}

// Touch: update angels set created = now() where id = @id
func (a *AngelSQL) Touch(id int) errors.E {
	return angelSQLErr(a.stdTouch(id))
}

// Search returns rows by query
func (a *AngelSQL) Search(q *AngelQuery) ([]Angel, errors.E) {
	res, err := a.stdSearch(q)
	return res, angelSQLErr(err)
}

// CreateTable creates table if not exists
func (a *AngelSQL) CreateTable() errors.E {
	return angelSQLErr(a.stdCreateTable())
}

// CreateIndexes creates indexes if not exist
func (a *AngelSQL) CreateIndexes() errors.E {
	return angelSQLErr(a.stdCreateIndexes())
}
