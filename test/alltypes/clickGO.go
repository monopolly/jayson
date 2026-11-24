package alltypes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	gojson "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/errors"
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
	ip                            ch{ip} IPv4 column for ip address
	ip6                           ch{ip6} IPv6 column for ip address
	low                           ch{low} Wrap field type in LowCardinality(...)
	name=""                       ch{name="col"} Name of column
	order                         ch{order} Sorting key: order by (name). No order by without the tag
	primary                       ch{primary} Primary key, several fields make a composite key
	skip                          ch{skip} No column for the field
	ttl=30d                       ch{ttl=30d} Rows are removed after the time from this time field: 30s, 10m, 1h, 1d, 2w, 6M, 1y
	type=""                       ch{type="UInt8"} Clickhouse type of column
	unix                          ch{unix} DateTime column for number field with unix time

	PROTO:
	name="field"                  Rewrite proto field name
	skip                          Skip field for proto
	type="string"                 Rewrite proto type for field. Ex: type="google.protobuf.Value"

	SQL FUNCTIONS: methods of interface become methods of ClickSQL
	type clickSQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (Click, error)

		// select role from clicks where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from clicks where id = @id
		Music(id int) ([]int, error)

		// select id from clicks where role = @role
		IDs(role role.Role) ([]int, error)

		// update clicks set created = now() where id = @id
		Touch(id int) error

		// delete from clicks where created < @before
		Clean(before time.Time) (int64, error) // count of rows
	}

	CLICKHOUSE FUNCTIONS: methods of interface become methods of ClickCQL
	type clickCQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (Click, error)

		// select role from clicks where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from clicks where id = @id
		Music(id int) ([]int, error)

		// select id from clicks where role = @role
		IDs(role role.Role) ([]int64, error)

		// alter table clicks delete where created < @before
		Clean(before time.Time) error
	}

	CLICKHOUSE ENGINES: engine=ReplacingMergeTree(updated) partition=toYYYYMM(created). Key is required: ch{primary} or ch{order}
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

// 32 bytes (go padding)
type Click struct {
	ID   int      `json:"id,omitempty"`   // sql{inc} ch{primary}
	Link string   `json:"link,omitempty"` //
	Tags []string `json:"tags,omitempty"` //
}

// clickSQLDB runs queries: pool or transaction
type clickSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// ClickSQL sql queries of table clicks
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type ClickSQL struct {
	pool *pgxpool.Pool
	db   clickSQLDB // pool, or transaction after TX
}

// NewClickSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewClickSQL(pool *pgxpool.Pool) *ClickSQL {
	a := &ClickSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *ClickSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of ClickSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum clicks")
//		return err
//	})
func (a *ClickSQL) Conn(f func(conn *pgxpool.Conn) errors.E) errors.E {
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
func (a *ClickSQL) Begin(f func(tx pgx.Tx) errors.E) errors.E {
	return clickSQLErr(pgx.BeginFunc(context.Background(), a.db, func(tx pgx.Tx) error {
		if err := f(tx); err != nil {
			return err
		}
		return nil
	}))
}

// TX returns queries in transaction tx
func (a *ClickSQL) TX(tx pgx.Tx) *ClickSQL {
	return &ClickSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *ClickSQL) TableName() string {
	return "clicks"
}

// Fields returns all sql columns
func (a *ClickSQL) Fields() []ClickIndexTyped {
	return slices.Clone(clickSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var clickSQLFields = []ClickIndexTyped{ClickIndexID, ClickIndexLink, ClickIndexTags}

// columns of insert
var clickSQLInsertColumns = []string{"link", "tags"}

// sql column names
var clickSQLColumns = [...]string{
	ClickIndexID:   "id",
	ClickIndexLink: "link",
	ClickIndexTags: "tags",
}

// select expressions: null of scalar column is zero value
var clickSQLSelects = [...]string{
	ClickIndexID:   "coalesce(id, 0)",
	ClickIndexLink: "coalesce(link, '')",
	ClickIndexTags: "tags",
}

// sql column types
var clickSQLTypes = [...]string{
	ClickIndexID:   "bigint",
	ClickIndexLink: "text",
	ClickIndexTags: "text[]",
}

const clickSQLSelectAll = "coalesce(id, 0), coalesce(link, ''), tags"

// clickSQLColumn returns sql column name of field
func clickSQLColumn(f ClickIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(clickSQLColumns) || clickSQLColumns[f] == "" {
		return "", fmt.Errorf("clicks: field %d is not a sql column", int(f))
	}
	return clickSQLColumns[f], nil
}

// clickSQLSelect returns select list of fields, all columns when empty
func clickSQLSelect(fields []ClickIndexTyped) (string, []ClickIndexTyped, error) {
	if len(fields) == 0 {
		return clickSQLSelectAll, clickSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := clickSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(clickSQLSelects[f])
	}
	return b.String(), fields, nil
}

// clickSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func clickSQLSet(v map[ClickIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, stderrors.New("clicks: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := clickSQLColumn(f)
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

// clickSQLAffected returns pgx.ErrNoRows when nothing was changed
func clickSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// clickSQLScanBytes scans bytea to byte array
type clickSQLScanBytes func(b []byte)

func (f clickSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Click) sqlDest(fields []ClickIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case ClickIndexID:
			dest[i] = &a.ID
		case ClickIndexLink:
			dest[i] = &a.Link
		case ClickIndexTags:
			dest[i] = &a.Tags
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Click) sqlValue(f ClickIndexTyped) any {
	switch f {
	case ClickIndexID:
		return a.ID
	case ClickIndexLink:
		return a.Link
	case ClickIndexTags:
		return a.Tags
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Click) sqlInsertArgs() []any {
	return []any{
		a.Link,
		a.Tags,
	}
}

// sqlAllArgs returns values of all columns
func (a *Click) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Link,
		a.Tags,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *ClickSQL) query(fields []ClickIndexTyped, tail string, args ...any) ([]Click, error) {
	cols, fields, err := clickSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from clicks"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Click, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *ClickSQL) queryRow(fields []ClickIndexTyped, tail string, args ...any) (v Click, err error) {
	cols, fields, err := clickSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from clicks"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

func (a *ClickSQL) stdGet(id int, fields ...ClickIndexTyped) (Click, error) {
	return a.queryRow(fields, " where id = $1", id)
}

func (a *ClickSQL) stdExists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from clicks where id = $1)", id).Scan(&has)
	return
}

func (a *ClickSQL) stdGetBy(field ClickIndexTyped, v any, fields ...ClickIndexTyped) (res Click, err error) {
	col, err := clickSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

func (a *ClickSQL) stdGetWhere(fields []ClickIndexTyped, where string, args ...any) (Click, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

func (a *ClickSQL) stdHas(field ClickIndexTyped, v any) (has bool, err error) {
	col, err := clickSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from clicks where "+col+" = $1)", v).Scan(&has)
	return
}

func (a *ClickSQL) stdAll(fields ...ClickIndexTyped) ([]Click, error) {
	return a.query(fields, " order by id")
}

func (a *ClickSQL) stdList(limit, offset int, fields ...ClickIndexTyped) ([]Click, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

func (a *ClickSQL) stdSelect(fields []ClickIndexTyped, where string, args ...any) ([]Click, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

func (a *ClickSQL) stdCount() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from clicks").Scan(&count)
	return
}

func (a *ClickSQL) stdCountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from clicks where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const clickSQLInsert = "insert into clicks (link, tags) values ($1, $2)"

const clickSQLInsertFull = "insert into clicks (id, link, tags) values ($1, $2, $3)"

func (a *ClickSQL) stdInsert(v Click) (id int, err error) {
	err = a.db.QueryRow(context.Background(), clickSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&id)
	return
}

func (a *ClickSQL) stdInsertNoConflict(v Click) (id int, inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), clickSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&id)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return id, false, nil
	}
	return id, err == nil, err
}

func (a *ClickSQL) stdInsertFull(v Click) error {
	_, err := a.db.Exec(context.Background(), clickSQLInsertFull, v.sqlAllArgs()...)
	return err
}

func (a *ClickSQL) stdUpsert(v Click) error {
	_, err := a.db.Exec(context.Background(), "insert into clicks (id, link, tags) values ($1, $2, $3) on conflict (id) do update set link = excluded.link, tags = excluded.tags", v.sqlAllArgs()...)
	return err
}

func (a *ClickSQL) stdInsertMany(list []Click) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"clicks"}, clickSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

func (a *ClickSQL) stdInsertBatch(list []Click) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for i := range list {
		b.Queue(clickSQLInsert+" returning id", list[i].sqlInsertArgs()...)
	}
	res := a.db.SendBatch(context.Background(), b)
	for i := range list {
		if err := res.QueryRow().Scan(&list[i].ID); err != nil {
			res.Close()
			return err
		}
	}
	return res.Close()
}

// ---------------------------------------------------------------------------
// update, delete
// ---------------------------------------------------------------------------

func (a *ClickSQL) stdUpdate(id int, field ClickIndexTyped, v any) error {
	col, err := clickSQLColumn(field)
	if err != nil {
		return err
	}
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set "+col+" = $1 where id = $2", v, id))
}

func (a *ClickSQL) stdUpdates(id int, v map[ClickIndexTyped]any) error {
	set, args, err := clickSQLSet(v, 1)
	if err != nil {
		return err
	}
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

func (a *ClickSQL) stdSave(v Click, fields ...ClickIndexTyped) error {
	if len(fields) == 0 {
		fields = clickSQLFields
	}
	values := make(map[ClickIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != ClickIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := clickSQLSet(values, 1)
	if err != nil {
		return err
	}
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(ClickIndexID))...))
}

func (a *ClickSQL) stdInc(id int, field ClickIndexTyped, v any) error {
	col, err := clickSQLColumn(field)
	if err != nil {
		return err
	}
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

func (a *ClickSQL) stdDelete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from clicks where id = $1", id)
	return err
}

func (a *ClickSQL) stdUpdateWhere(field ClickIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := clickSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update clicks set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

func (a *ClickSQL) stdUpdatesWhere(v map[ClickIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := clickSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update clicks set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

func (a *ClickSQL) stdDeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from clicks where "+where, args...)
	return tag.RowsAffected(), err
}

func (a *ClickSQL) stdAppendTags(id int, v ...string) error {
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set tags = coalesce(tags, '{}') || $1::text[] where id = $2", v, id))
}

func (a *ClickSQL) stdRemoveTags(id int, v string) error {
	return clickSQLAffected(a.db.Exec(context.Background(), "update clicks set tags = array_remove(tags, $1::text) where id = $2", v, id))
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// ClickQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type ClickQuery struct {
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

// NewClickQuery creates query with empty maps
func NewClickQuery() *ClickQuery {
	return &ClickQuery{
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
func (q *ClickQuery) column(k string) (string, ClickIndexTyped, error) {
	if !ClickValidKey(k) {
		return "", 0, fmt.Errorf("clicks: unknown field %q", k)
	}
	f := ClickKeyIndex(k)
	col, err := clickSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *ClickQuery) Render() (tail string, fields []ClickIndexTyped, args []any, err error) {

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

func (a *ClickSQL) stdSearch(q *ClickQuery) ([]Click, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const clickSQLCreateTable = `create table if not exists clicks (
	id       bigint generated by default as identity primary key,
	link     text,
	tags     text[] default '{}'
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var clickSQLIndexes = []string{}

func (a *ClickSQL) stdCreateTable() error {
	_, err := a.db.Exec(context.Background(), clickSQLCreateTable)
	return err
}

func (a *ClickSQL) stdCreateIndexes() error {
	for _, q := range clickSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

func (a *ClickSQL) stdResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('clicks', 'id'), coalesce((select max(id) from clicks), 0) + $1, false)", plus)
	return err
}

func (a *ClickSQL) stdSetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('clicks', 'id'), $1, false)", v)
	return err
}

// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// clickSQLErr converts error of database to errors.E
func clickSQLErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.PGX(err)
}

// Get returns row by id
func (a *ClickSQL) Get(id int, fields ...ClickIndexTyped) (Click, errors.E) {
	r1, err := a.stdGet(id, fields...)
	return r1, clickSQLErr(err)
}

// Exists checks row by id
func (a *ClickSQL) Exists(id int) (bool, errors.E) {
	r1, err := a.stdExists(id)
	return r1, clickSQLErr(err)
}

// GetBy returns first row with field = v
func (a *ClickSQL) GetBy(field ClickIndexTyped, v any, fields ...ClickIndexTyped) (Click, errors.E) {
	r1, err := a.stdGetBy(field, v, fields...)
	return r1, clickSQLErr(err)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *ClickSQL) GetWhere(fields []ClickIndexTyped, where string, args ...any) (Click, errors.E) {
	r1, err := a.stdGetWhere(fields, where, args...)
	return r1, clickSQLErr(err)
}

// Has checks row with field = v
func (a *ClickSQL) Has(field ClickIndexTyped, v any) (bool, errors.E) {
	r1, err := a.stdHas(field, v)
	return r1, clickSQLErr(err)
}

// All returns all rows
func (a *ClickSQL) All(fields ...ClickIndexTyped) ([]Click, errors.E) {
	r1, err := a.stdAll(fields...)
	return r1, clickSQLErr(err)
}

// List returns page of rows
func (a *ClickSQL) List(limit, offset int, fields ...ClickIndexTyped) ([]Click, errors.E) {
	r1, err := a.stdList(limit, offset, fields...)
	return r1, clickSQLErr(err)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *ClickSQL) Select(fields []ClickIndexTyped, where string, args ...any) ([]Click, errors.E) {
	r1, err := a.stdSelect(fields, where, args...)
	return r1, clickSQLErr(err)
}

// Count returns count of rows
func (a *ClickSQL) Count() (int, errors.E) {
	r1, err := a.stdCount()
	return r1, clickSQLErr(err)
}

// CountWhere returns count of rows by condition
func (a *ClickSQL) CountWhere(where string, args ...any) (int, errors.E) {
	r1, err := a.stdCountWhere(where, args...)
	return r1, clickSQLErr(err)
}

// Insert adds row and returns generated id
func (a *ClickSQL) Insert(v Click) (int, errors.E) {
	r1, err := a.stdInsert(v)
	return r1, clickSQLErr(err)
}

// InsertNoConflict adds row and returns generated id, false when row exists
func (a *ClickSQL) InsertNoConflict(v Click) (int, bool, errors.E) {
	r1, r2, err := a.stdInsertNoConflict(v)
	return r1, r2, clickSQLErr(err)
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *ClickSQL) InsertFull(v Click) errors.E {
	return clickSQLErr(a.stdInsertFull(v))
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *ClickSQL) Upsert(v Click) errors.E {
	return clickSQLErr(a.stdUpsert(v))
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *ClickSQL) InsertMany(list []Click) (int64, errors.E) {
	r1, err := a.stdInsertMany(list)
	return r1, clickSQLErr(err)
}

// InsertBatch adds rows in one round trip and sets generated ID of list items
func (a *ClickSQL) InsertBatch(list []Click) errors.E {
	return clickSQLErr(a.stdInsertBatch(list))
}

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *ClickSQL) Update(id int, field ClickIndexTyped, v any) errors.E {
	return clickSQLErr(a.stdUpdate(id, field, v))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *ClickSQL) Updates(id int, v map[ClickIndexTyped]any) errors.E {
	return clickSQLErr(a.stdUpdates(id, v))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *ClickSQL) Save(v Click, fields ...ClickIndexTyped) errors.E {
	return clickSQLErr(a.stdSave(v, fields...))
}

// Inc adds v to number field: views = views + 1
func (a *ClickSQL) Inc(id int, field ClickIndexTyped, v any) errors.E {
	return clickSQLErr(a.stdInc(id, field, v))
}

// Delete removes row by id
func (a *ClickSQL) Delete(id int) errors.E {
	return clickSQLErr(a.stdDelete(id))
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *ClickSQL) UpdateWhere(field ClickIndexTyped, v any, where string, args ...any) (int64, errors.E) {
	r1, err := a.stdUpdateWhere(field, v, where, args...)
	return r1, clickSQLErr(err)
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *ClickSQL) UpdatesWhere(v map[ClickIndexTyped]any, where string, args ...any) (int64, errors.E) {
	r1, err := a.stdUpdatesWhere(v, where, args...)
	return r1, clickSQLErr(err)
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *ClickSQL) DeleteWhere(where string, args ...any) (int64, errors.E) {
	r1, err := a.stdDeleteWhere(where, args...)
	return r1, clickSQLErr(err)
}

// AppendTags adds values to array tags
func (a *ClickSQL) AppendTags(id int, v ...string) errors.E {
	return clickSQLErr(a.stdAppendTags(id, v...))
}

// RemoveTags removes all entries of v from array tags
func (a *ClickSQL) RemoveTags(id int, v string) errors.E {
	return clickSQLErr(a.stdRemoveTags(id, v))
}

// Search returns rows by query
func (a *ClickSQL) Search(q *ClickQuery) ([]Click, errors.E) {
	r1, err := a.stdSearch(q)
	return r1, clickSQLErr(err)
}

// CreateTable creates table if not exists
func (a *ClickSQL) CreateTable() errors.E {
	return clickSQLErr(a.stdCreateTable())
}

// CreateIndexes creates indexes if not exist
func (a *ClickSQL) CreateIndexes() errors.E {
	return clickSQLErr(a.stdCreateIndexes())
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *ClickSQL) ResetSequence(add ...int64) errors.E {
	return clickSQLErr(a.stdResetSequence(add...))
}

// SetSequence sets next id
func (a *ClickSQL) SetSequence(v int64) errors.E {
	return clickSQLErr(a.stdSetSequence(v))
}

// field type
type ClickIndexTyped int

// int index
const (
	ClickIndexID = ClickIndexTyped(iota)
	ClickIndexLink
	ClickIndexTags
)

// string index
const (
	ClickFieldID   = "id"   // int sql{inc} ch{primary}
	ClickFieldLink = "link" // string
	ClickFieldTags = "tags" // []string
)

// index func
func ClickIndexes() []ClickIndexTyped {
	return []ClickIndexTyped{ClickIndexID, ClickIndexLink, ClickIndexTags}

}

// key index string
func (a ClickIndexTyped) String() string {
	switch a {
	case ClickIndexID:
		return "id"
	case ClickIndexLink:
		return "link"
	case ClickIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index string
func (a ClickIndexTyped) SQLName() string {
	switch a {
	case ClickIndexID:
		return "id"
	case ClickIndexLink:
		return "link"
	case ClickIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index clickhouse string
func (a ClickIndexTyped) ClickhouseName() string {
	switch a {
	case ClickIndexID:
		return "id"
	case ClickIndexLink:
		return "link"
	case ClickIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index type
func (a ClickIndexTyped) Type() string {
	switch a {
	case ClickIndexID:
		return "int"
	case ClickIndexLink:
		return "string"
	case ClickIndexTags:
		return "[]string"
	default:
		return ""
	}
}

// custom title
func (a ClickIndexTyped) Title() string {
	switch a {
	case ClickIndexID:
		return "ID"
	case ClickIndexLink:
		return "Link"
	case ClickIndexTags:
		return "Tags"
	default:
		return ""
	}
}

// custom desc
func (a ClickIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func ClickKeyIndex(key string) ClickIndexTyped {
	switch key {
	case "id":
		return ClickIndexID
	case "link":
		return ClickIndexLink
	case "tags":
		return ClickIndexTags
	default:
		return 0
	}
}

// valid struct key check
func ClickValidKey(key string) bool {
	switch key {
	case "id", "link", "tags":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Click) Tuple() (r []any) {
	return []any{a.ID, a.Link, a.Tags}
}

// get struct value with function
func (a *Click) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "link":
		return a.Link //string
	case "tags":
		return a.Tags //[]string
	}
	return
}

// get any struct value as string
func (a *Click) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "link":
		return fmt.Sprint(a.Link) //string
	case "tags":
		return fmt.Sprint(a.Tags) //[]string
	}
	return
}

// struct to map
func (a *Click) Map() map[string]any {
	return map[string]any{
		"id":   a.ID,
		"link": a.Link,
		"tags": a.Tags,
	}
}

// struct to map
func (a *Click) Iterate(f func(k ClickIndexTyped, v any)) {
	for _, x := range ClickIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Click) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseClick(v []byte) (a Click, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// Tuple create an array from struct for clickhouse
func (a *Click) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Link, a.Tags}
}

// ClickCQL clickhouse queries of table clicks
//
// Rules:
//   - safe for concurrent use
//   - not found row is sql.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "country = $1 and views > $2"
//   - rows are only inserted, selected and deleted
type ClickCQL struct {
	conn driver.Conn
	mu   sync.Mutex
	list []Click // buffer of Add
}

// NewClickCQL creates queries on connection and table if not exists.
// Rows of Add are written every 10 minutes and by Push.
// Error of table is not returned: call CreateTable to see it
func NewClickCQL(conn driver.Conn) *ClickCQL {
	a := &ClickCQL{conn: conn}
	_ = a.CreateTable()
	go a.flush()
	return a
}

// Conn returns connection, for own queries
func (a *ClickCQL) Conn() driver.Conn {
	return a.conn
}

// TableName returns table name
func (a *ClickCQL) TableName() string {
	return "clicks"
}

// Fields returns all columns
func (a *ClickCQL) Fields() []ClickIndexTyped {
	return slices.Clone(clickCHFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all columns
var clickCHFields = []ClickIndexTyped{ClickIndexID, ClickIndexLink, ClickIndexTags}

// column names
var clickCHColumns = [...]string{
	ClickIndexID:   "id",
	ClickIndexLink: "link",
	ClickIndexTags: "tags",
}

const clickCHSelectAll = "id, link, tags"

// mutations wait until the data is changed
const clickCHSync = " settings mutations_sync = 1"

// clickCHColumn returns column name of field
func clickCHColumn(f ClickIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(clickCHColumns) || clickCHColumns[f] == "" {
		return "", fmt.Errorf("clicks: field %d is not a clickhouse column", int(f))
	}
	return clickCHColumns[f], nil
}

// clickCHSelect returns select list of fields, all columns when empty
func clickCHSelect(fields []ClickIndexTyped) (string, []ClickIndexTyped, error) {
	if len(fields) == 0 {
		return clickCHSelectAll, clickCHFields, nil
	}
	list := make([]string, 0, len(fields))
	for _, f := range fields {
		col, err := clickCHColumn(f)
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
func (a *ClickCQL) Add(v ...Click) {
	a.mu.Lock()
	a.list = append(a.list, v...)
	a.mu.Unlock()
}

// Len returns count of rows in buffer
func (a *ClickCQL) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.list)
}

func (a *ClickCQL) stdPush() error {
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

func (a *ClickCQL) stdInsert(v ...Click) error {
	if len(v) == 0 {
		return nil
	}
	return a.send(v)
}

// send writes rows by one batch
func (a *ClickCQL) send(list []Click) error {
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into clicks (id, link, tags)")
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
func (a *ClickCQL) flush() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		_ = a.stdPush()
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *ClickCQL) query(fields []ClickIndexTyped, tail string, args ...any) (res []Click, err error) {
	cols, fields, err := clickCHSelect(fields)
	if err != nil {
		return
	}
	rows, err := a.conn.Query(context.Background(), "select "+cols+" from clicks"+tail, args...)
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
		var v Click
		for i, f := range fields {
			v.clickhouseSet(f, scan[i])
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// queryRow selects one row
func (a *ClickCQL) queryRow(fields []ClickIndexTyped, tail string, args ...any) (v Click, err error) {
	list, err := a.query(fields, tail, args...)
	if err != nil {
		return
	}
	if len(list) == 0 {
		return v, sql.ErrNoRows
	}
	return list[0], nil
}

func (a *ClickCQL) stdGet(id int, fields ...ClickIndexTyped) (Click, error) {
	return a.queryRow(fields, " where id = $1 limit 1", id)
}

func (a *ClickCQL) stdExists(id int) (bool, error) {
	n, err := a.stdCountWhere("id = $1", id)
	return n > 0, err
}

func (a *ClickCQL) stdGetBy(field ClickIndexTyped, v any, fields ...ClickIndexTyped) (res Click, err error) {
	col, err := clickCHColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

func (a *ClickCQL) stdGetWhere(fields []ClickIndexTyped, where string, args ...any) (Click, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

func (a *ClickCQL) stdHas(field ClickIndexTyped, v any) (bool, error) {
	col, err := clickCHColumn(field)
	if err != nil {
		return false, err
	}
	n, err := a.stdCountWhere(col+" = $1", v)
	return n > 0, err
}

func (a *ClickCQL) stdAll(fields ...ClickIndexTyped) ([]Click, error) {
	return a.query(fields, " order by id")
}

func (a *ClickCQL) stdList(limit, offset int, fields ...ClickIndexTyped) ([]Click, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

func (a *ClickCQL) stdSelect(fields []ClickIndexTyped, where string, args ...any) ([]Click, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

func (a *ClickCQL) stdCount() (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from clicks").Scan(&n)
	return int(n), err
}

func (a *ClickCQL) stdCountWhere(where string, args ...any) (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from clicks where "+where, args...).Scan(&n)
	return int(n), err
}

func (a *ClickCQL) stdStat(field ClickIndexTyped) (map[string]int, error) {
	col, err := clickCHColumn(field)
	if err != nil {
		return nil, err
	}
	rows, err := a.conn.Query(context.Background(), "select toString("+col+"), count() from clicks group by "+col)
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

func (a *ClickCQL) stdDelete(id int) error {
	return a.conn.Exec(context.Background(), "alter table clicks delete where id = $1"+clickCHSync, id)
}

func (a *ClickCQL) stdDeleteWhere(where string, args ...any) error {
	return a.conn.Exec(context.Background(), "alter table clicks delete where "+where+clickCHSync, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const clickCHCreateTable = `create table if not exists clicks (
	id       Int64,
	link     String,
	tags     Array(String)
)
engine = MergeTree()
primary key (id)`

func (a *ClickCQL) stdCreateTable() error {
	return a.conn.Exec(context.Background(), clickCHCreateTable)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// clickhouseSet sets field from value of clickhouse row
func (a *Click) clickhouseSet(f ClickIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case ClickIndexID:
		clickCHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case ClickIndexLink:
		clickCHAssign(reflect.ValueOf(&a.Link).Elem(), reflect.ValueOf(v))
	case ClickIndexTags:
		clickCHAssign(reflect.ValueOf(&a.Tags).Elem(), reflect.ValueOf(v))
	}
}

// clickCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func clickCHAssign(d, s reflect.Value) {
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
		clickCHAssign(p.Elem(), s)
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
			clickCHAssign(r.Index(i), s.Index(i))
		}
		d.Set(r)

	// Map(String, Int64) > map[string]int
	case d.Kind() == reflect.Map && s.Kind() == reflect.Map:
		r := reflect.MakeMapWithSize(d.Type(), s.Len())
		for it := s.MapRange(); it.Next(); {
			k := reflect.New(d.Type().Key()).Elem()
			v := reflect.New(d.Type().Elem()).Elem()
			clickCHAssign(k, it.Key())
			clickCHAssign(v, it.Value())
			r.SetMapIndex(k, v)
		}
		d.Set(r)

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}

// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// clickCHErr converts error of database to errors.E
func clickCHErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.ClickHouse(err)
}

// Push writes rows of buffer. Rows stay in buffer when the write fails
func (a *ClickCQL) Push() errors.E {
	return clickCHErr(a.stdPush())
}

// Insert writes rows now, without buffer
func (a *ClickCQL) Insert(v ...Click) errors.E {
	return clickCHErr(a.stdInsert(v...))
}

// Get returns row by id
func (a *ClickCQL) Get(id int, fields ...ClickIndexTyped) (Click, errors.E) {
	r1, err := a.stdGet(id, fields...)
	return r1, clickCHErr(err)
}

// Exists checks row by id
func (a *ClickCQL) Exists(id int) (bool, errors.E) {
	r1, err := a.stdExists(id)
	return r1, clickCHErr(err)
}

// GetBy returns first row with field = v
func (a *ClickCQL) GetBy(field ClickIndexTyped, v any, fields ...ClickIndexTyped) (Click, errors.E) {
	r1, err := a.stdGetBy(field, v, fields...)
	return r1, clickCHErr(err)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "country = $1 order by created desc", "DE")
func (a *ClickCQL) GetWhere(fields []ClickIndexTyped, where string, args ...any) (Click, errors.E) {
	r1, err := a.stdGetWhere(fields, where, args...)
	return r1, clickCHErr(err)
}

// Has checks row with field = v
func (a *ClickCQL) Has(field ClickIndexTyped, v any) (bool, errors.E) {
	r1, err := a.stdHas(field, v)
	return r1, clickCHErr(err)
}

// All returns all rows
func (a *ClickCQL) All(fields ...ClickIndexTyped) ([]Click, errors.E) {
	r1, err := a.stdAll(fields...)
	return r1, clickCHErr(err)
}

// List returns page of rows
func (a *ClickCQL) List(limit, offset int, fields ...ClickIndexTyped) ([]Click, errors.E) {
	r1, err := a.stdList(limit, offset, fields...)
	return r1, clickCHErr(err)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *ClickCQL) Select(fields []ClickIndexTyped, where string, args ...any) ([]Click, errors.E) {
	r1, err := a.stdSelect(fields, where, args...)
	return r1, clickCHErr(err)
}

// Count returns count of rows
func (a *ClickCQL) Count() (int, errors.E) {
	r1, err := a.stdCount()
	return r1, clickCHErr(err)
}

// CountWhere returns count of rows by condition
func (a *ClickCQL) CountWhere(where string, args ...any) (int, errors.E) {
	r1, err := a.stdCountWhere(where, args...)
	return r1, clickCHErr(err)
}

// Stat returns count of rows by values of field: {"DE": 425, "US": 134}
func (a *ClickCQL) Stat(field ClickIndexTyped) (map[string]int, errors.E) {
	r1, err := a.stdStat(field)
	return r1, clickCHErr(err)
}

// Delete removes row by id
func (a *ClickCQL) Delete(id int) errors.E {
	return clickCHErr(a.stdDelete(id))
}

// DeleteWhere removes rows by condition
func (a *ClickCQL) DeleteWhere(where string, args ...any) errors.E {
	return clickCHErr(a.stdDeleteWhere(where, args...))
}

// CreateTable creates table if not exists
func (a *ClickCQL) CreateTable() errors.E {
	return clickCHErr(a.stdCreateTable())
}
