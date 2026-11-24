package alltypes

import (
	"context"
	stderrors "errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	gojson "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/errors"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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
type TagIndexTyped int

// int index
const (
	TagIndexID = TagIndexTyped(iota)
	TagIndexName
	TagIndexOwner
	TagIndexMeta
	TagIndexFrom
	TagIndexTo
)

// string index
const (
	TagFieldID    = "id"    // int sql{inc}
	TagFieldName  = "name"  // string
	TagFieldOwner = "owner" // int
	TagFieldMeta  = "meta"  // map[string]int
	TagFieldFrom  = "from"  // int reserved sql word: column froms
	TagFieldTo    = "to"    // int reserved sql word: column tos
)

// index func
func TagIndexes() []TagIndexTyped {
	return []TagIndexTyped{TagIndexID, TagIndexName, TagIndexOwner, TagIndexMeta, TagIndexFrom, TagIndexTo}

}

// 56 bytes (go padding)
//
//easyjson:json
type Tag struct {
	ID    int            `json:"id,omitempty"`    // sql{inc}
	Name  string         `json:"name,omitempty"`  //
	Owner int            `json:"owner,omitempty"` //
	Meta  map[string]int `json:"meta,omitempty"`  //
	From  int            `json:"from,omitempty"`  // reserved sql word: column froms
	To    int            `json:"to,omitempty"`    // reserved sql word: column tos
}

// init struct
func NewTag() (a *Tag) {
	a = new(Tag)
	a.Meta = make(map[string]int)
	return
}

// Tuple create an array from struct
func (a *Tag) Tuple() (r []any) {
	return []any{a.ID, a.Name, a.Owner, a.Meta, a.From, a.To}
}

// Tuple create an array from struct for clickhouse
func (a *Tag) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Name, a.Owner, a.Meta, a.From, a.To}
}

// get struct value with function
func (a *Tag) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "name":
		return a.Name //string
	case "owner":
		return a.Owner //int
	case "meta":
		return a.Meta //map[string]int
	case "from":
		return a.From //int
	case "to":
		return a.To //int
	}
	return
}

// get any struct value as string
func (a *Tag) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "name":
		return fmt.Sprint(a.Name) //string
	case "owner":
		return fmt.Sprint(a.Owner) //int
	case "meta":
		return fmt.Sprint(a.Meta) //map[string]int
	case "from":
		return fmt.Sprint(a.From) //int
	case "to":
		return fmt.Sprint(a.To) //int
	}
	return
}

// key index string
func (a TagIndexTyped) String() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	case TagIndexFrom:
		return "from"
	case TagIndexTo:
		return "to"
	default:
		return ""
	}
}

// key index string
func (a TagIndexTyped) SQLName() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	case TagIndexFrom:
		return "froms"
	case TagIndexTo:
		return "tos"
	default:
		return ""
	}
}

// key index clickhouse string
func (a TagIndexTyped) ClickhouseName() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	case TagIndexFrom:
		return "from"
	case TagIndexTo:
		return "to"
	default:
		return ""
	}
}

// key index type
func (a TagIndexTyped) Type() string {
	switch a {
	case TagIndexID:
		return "int"
	case TagIndexName:
		return "string"
	case TagIndexOwner:
		return "int"
	case TagIndexMeta:
		return "map[string]int"
	case TagIndexFrom:
		return "int"
	case TagIndexTo:
		return "int"
	default:
		return ""
	}
}

// custom title
func (a TagIndexTyped) Title() string {
	switch a {
	case TagIndexID:
		return "ID"
	case TagIndexName:
		return "Name"
	case TagIndexOwner:
		return "Owner"
	case TagIndexMeta:
		return "Meta"
	case TagIndexFrom:
		return "From"
	case TagIndexTo:
		return "To"
	default:
		return ""
	}
}

// custom desc
func (a TagIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func TagKeyIndex(key string) TagIndexTyped {
	switch key {
	case "id":
		return TagIndexID
	case "name":
		return TagIndexName
	case "owner":
		return TagIndexOwner
	case "meta":
		return TagIndexMeta
	case "from":
		return TagIndexFrom
	case "to":
		return TagIndexTo
	default:
		return 0
	}
}

// valid struct key check
func TagValidKey(key string) bool {
	switch key {
	case "id", "name", "owner", "meta", "from", "to":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Tag) Map() map[string]any {
	return map[string]any{
		"id":    a.ID,
		"name":  a.Name,
		"owner": a.Owner,
		"meta":  a.Meta,
		"from":  a.From,
		"to":    a.To,
	}
}

// struct to map
func (a *Tag) Iterate(f func(k TagIndexTyped, v any)) {
	for _, x := range TagIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Tag) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseTag(v []byte) (a Tag, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// tagSQLDB runs queries: pool or transaction
type tagSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// TagSQL sql queries of table tags
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type TagSQL struct {
	pool *pgxpool.Pool
	db   tagSQLDB // pool, or transaction after TX
}

// NewTagSQL creates queries on pool, table and indexes if not exist
func NewTagSQL(pool *pgxpool.Pool) (*TagSQL, errors.E) {
	a := &TagSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err != nil {
		return nil, err
	}
	return a, a.CreateIndexes()
}

// Pool returns pool
func (a *TagSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of TagSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum tags")
//		return err
//	})
func (a *TagSQL) Conn(f func(conn *pgxpool.Conn) errors.E) errors.E {
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
func (a *TagSQL) Begin(f func(tx pgx.Tx) errors.E) errors.E {
	return tagSQLErr(pgx.BeginFunc(context.Background(), a.db, func(tx pgx.Tx) error {
		if err := f(tx); err != nil {
			return err
		}
		return nil
	}))
}

// TX returns queries in transaction tx
func (a *TagSQL) TX(tx pgx.Tx) *TagSQL {
	return &TagSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *TagSQL) TableName() string {
	return "tags"
}

// Fields returns all sql columns
func (a *TagSQL) Fields() []TagIndexTyped {
	return slices.Clone(tagSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var tagSQLFields = []TagIndexTyped{TagIndexID, TagIndexName, TagIndexOwner, TagIndexMeta, TagIndexFrom, TagIndexTo}

// columns of insert
var tagSQLInsertColumns = []string{"name", "owner", "meta", "froms", "tos"}

// sql column names
var tagSQLColumns = [...]string{
	TagIndexID:    "id",
	TagIndexName:  "name",
	TagIndexOwner: "owner",
	TagIndexMeta:  "meta",
	TagIndexFrom:  "froms",
	TagIndexTo:    "tos",
}

// select expressions: null of scalar column is zero value
var tagSQLSelects = [...]string{
	TagIndexID:    "coalesce(id, 0)",
	TagIndexName:  "coalesce(name, '')",
	TagIndexOwner: "coalesce(owner, 0)",
	TagIndexMeta:  "meta",
	TagIndexFrom:  "coalesce(froms, 0)",
	TagIndexTo:    "coalesce(tos, 0)",
}

// sql column types
var tagSQLTypes = [...]string{
	TagIndexID:    "bigint",
	TagIndexName:  "text",
	TagIndexOwner: "bigint",
	TagIndexMeta:  "jsonb",
	TagIndexFrom:  "bigint",
	TagIndexTo:    "bigint",
}

const tagSQLSelectAll = "coalesce(id, 0), coalesce(name, ''), coalesce(owner, 0), meta, coalesce(froms, 0), coalesce(tos, 0)"

// tagSQLColumn returns sql column name of field
func tagSQLColumn(f TagIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(tagSQLColumns) || tagSQLColumns[f] == "" {
		return "", fmt.Errorf("tags: field %d is not a sql column", int(f))
	}
	return tagSQLColumns[f], nil
}

// tagSQLSelect returns select list of fields, all columns when empty
func tagSQLSelect(fields []TagIndexTyped) (string, []TagIndexTyped, error) {
	if len(fields) == 0 {
		return tagSQLSelectAll, tagSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := tagSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tagSQLSelects[f])
	}
	return b.String(), fields, nil
}

// tagSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func tagSQLSet(v map[TagIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, stderrors.New("tags: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := tagSQLColumn(f)
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

// tagSQLAffected returns pgx.ErrNoRows when nothing was changed
func tagSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// tagSQLScanBytes scans bytea to byte array
type tagSQLScanBytes func(b []byte)

func (f tagSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Tag) sqlDest(fields []TagIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case TagIndexID:
			dest[i] = &a.ID
		case TagIndexName:
			dest[i] = &a.Name
		case TagIndexOwner:
			dest[i] = &a.Owner
		case TagIndexMeta:
			dest[i] = &a.Meta
		case TagIndexFrom:
			dest[i] = &a.From
		case TagIndexTo:
			dest[i] = &a.To
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Tag) sqlValue(f TagIndexTyped) any {
	switch f {
	case TagIndexID:
		return a.ID
	case TagIndexName:
		return a.Name
	case TagIndexOwner:
		return a.Owner
	case TagIndexMeta:
		return a.Meta
	case TagIndexFrom:
		return a.From
	case TagIndexTo:
		return a.To
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Tag) sqlInsertArgs() []any {
	return []any{
		a.Name,
		a.Owner,
		a.Meta,
		a.From,
		a.To,
	}
}

// sqlAllArgs returns values of all columns
func (a *Tag) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Name,
		a.Owner,
		a.Meta,
		a.From,
		a.To,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *TagSQL) query(fields []TagIndexTyped, tail string, args ...any) ([]Tag, error) {
	cols, fields, err := tagSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from tags"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Tag, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *TagSQL) queryRow(fields []TagIndexTyped, tail string, args ...any) (v Tag, err error) {
	cols, fields, err := tagSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from tags"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

func (a *TagSQL) stdGet(id int, fields ...TagIndexTyped) (Tag, error) {
	return a.queryRow(fields, " where id = $1", id)
}

func (a *TagSQL) stdExists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from tags where id = $1)", id).Scan(&has)
	return
}

func (a *TagSQL) stdGetBy(field TagIndexTyped, v any, fields ...TagIndexTyped) (res Tag, err error) {
	col, err := tagSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

func (a *TagSQL) stdGetWhere(fields []TagIndexTyped, where string, args ...any) (Tag, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

func (a *TagSQL) stdHas(field TagIndexTyped, v any) (has bool, err error) {
	col, err := tagSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from tags where "+col+" = $1)", v).Scan(&has)
	return
}

func (a *TagSQL) stdAll(fields ...TagIndexTyped) ([]Tag, error) {
	return a.query(fields, " order by id")
}

func (a *TagSQL) stdList(limit, offset int, fields ...TagIndexTyped) ([]Tag, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

func (a *TagSQL) stdSelect(fields []TagIndexTyped, where string, args ...any) ([]Tag, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

func (a *TagSQL) stdCount() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from tags").Scan(&count)
	return
}

func (a *TagSQL) stdCountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from tags where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const tagSQLInsert = "insert into tags (name, owner, meta, froms, tos) values ($1, $2, $3, $4, $5)"

const tagSQLInsertFull = "insert into tags (id, name, owner, meta, froms, tos) values ($1, $2, $3, $4, $5, $6)"

func (a *TagSQL) stdInsert(v *Tag) error {
	return a.db.QueryRow(context.Background(), tagSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

func (a *TagSQL) stdInsertNoConflict(v *Tag) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), tagSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (a *TagSQL) stdInsertFull(v *Tag) error {
	_, err := a.db.Exec(context.Background(), tagSQLInsertFull, v.sqlAllArgs()...)
	return err
}

func (a *TagSQL) stdUpsert(v *Tag) error {
	_, err := a.db.Exec(context.Background(), "insert into tags (id, name, owner, meta, froms, tos) values ($1, $2, $3, $4, $5, $6) on conflict (id) do update set name = excluded.name, owner = excluded.owner, meta = excluded.meta, froms = excluded.froms, tos = excluded.tos", v.sqlAllArgs()...)
	return err
}

func (a *TagSQL) stdInsertMany(list []Tag) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"tags"}, tagSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

func (a *TagSQL) stdInsertBatch(list []*Tag) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(tagSQLInsert+" returning id", v.sqlInsertArgs()...)
	}
	res := a.db.SendBatch(context.Background(), b)
	for _, v := range list {
		if err := res.QueryRow().Scan(&v.ID); err != nil {
			res.Close()
			return err
		}
	}
	return res.Close()
}

// ---------------------------------------------------------------------------
// update, delete
// ---------------------------------------------------------------------------

func (a *TagSQL) stdUpdate(id int, field TagIndexTyped, v any) error {
	col, err := tagSQLColumn(field)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = $1 where id = $2", v, id))
}

func (a *TagSQL) stdUpdates(id int, v map[TagIndexTyped]any) error {
	set, args, err := tagSQLSet(v, 1)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

func (a *TagSQL) stdSave(v *Tag, fields ...TagIndexTyped) error {
	if len(fields) == 0 {
		fields = tagSQLFields
	}
	values := make(map[TagIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != TagIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := tagSQLSet(values, 1)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(TagIndexID))...))
}

func (a *TagSQL) stdInc(id int, field TagIndexTyped, v any) error {
	col, err := tagSQLColumn(field)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

func (a *TagSQL) stdDelete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from tags where id = $1", id)
	return err
}

func (a *TagSQL) stdUpdateWhere(field TagIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := tagSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update tags set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

func (a *TagSQL) stdUpdatesWhere(v map[TagIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := tagSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update tags set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

func (a *TagSQL) stdDeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from tags where "+where, args...)
	return tag.RowsAffected(), err
}

func (a *TagSQL) stdUpdateMeta(id int, k string, v int) error {
	return a.jsonSet(id, "meta", fmt.Sprint(k), v)
}

func (a *TagSQL) stdMergeMeta(id int, v map[string]int) error {
	return a.jsonMerge(id, "meta", "{}", v)
}

func (a *TagSQL) stdDeleteMeta(id int, k string) error {
	return a.jsonDelete(id, "meta", fmt.Sprint(k))
}

func (a *TagSQL) stdRenameMeta(id int, k, newK string) error {
	return a.jsonRename(id, "meta", fmt.Sprint(k), fmt.Sprint(newK))
}

// jsonSet sets key of jsonb column
func (a *TagSQL) jsonSet(id int, col, k string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = jsonb_set(coalesce("+col+", '{}'::jsonb), array[$1::text], $2::jsonb, true) where id = $3", k, b, id))
}

// jsonMerge concats json to jsonb column: object keys or array values
func (a *TagSQL) jsonMerge(id int, col, empty string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = coalesce("+col+", '"+empty+"'::jsonb) || $1::jsonb where id = $2", b, id))
}

// jsonDelete removes key of jsonb column
func (a *TagSQL) jsonDelete(id int, col, k string) error {
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = "+col+" - $1::text where id = $2", k, id))
}

// jsonRename renames key of jsonb column
func (a *TagSQL) jsonRename(id int, col, k, newK string) error {
	return tagSQLAffected(a.db.Exec(context.Background(), "update tags set "+col+" = ("+col+" - $1::text) || jsonb_build_object($2::text, "+col+" -> $1::text) where id = $3 and "+col+" ? $1::text", k, newK, id))
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

func (a *TagSQL) stdByOwner(owner int) (res []Tag, err error) {
	return a.query(nil, " where owner = $1 order by id", owner)
}

func (a *TagSQL) stdByName(name string) (res Tag, err error) {
	res, err = a.queryRow(nil, " where name = $1", name)
	if stderrors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}

func (a *TagSQL) stdName(id int) (res string, err error) {
	err = a.db.QueryRow(context.Background(), "select name from tags where id = $1", id).Scan(&res)
	return
}

func (a *TagSQL) stdOwner(id int) (res int, err error) {
	err = a.db.QueryRow(context.Background(), "select owner from tags where id = $1", id).Scan(&res)
	if stderrors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}

func (a *TagSQL) stdIDs(owners []int) (res []int, err error) {
	rows, err := a.db.Query(context.Background(), "select id from tags where owner = any($1) order by id", owners)
	if err != nil {
		return
	}
	return pgx.CollectRows(rows, pgx.RowTo[int])
}

func (a *TagSQL) stdRename(owner int, name string) (res int64, err error) {
	tag, err := a.db.Exec(context.Background(), "update tags set name = $2 where owner = $1", owner, name)
	return tag.RowsAffected(), err
}

func (a *TagSQL) stdDrop(id int) (err error) {
	_, err = a.db.Exec(context.Background(), "delete from tags where id = $1", id)
	return
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// TagQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type TagQuery struct {
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

// NewTagQuery creates query with empty maps
func NewTagQuery() *TagQuery {
	return &TagQuery{
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
func (q *TagQuery) column(k string) (string, TagIndexTyped, error) {
	if !TagValidKey(k) {
		return "", 0, fmt.Errorf("tags: unknown field %q", k)
	}
	f := TagKeyIndex(k)
	col, err := tagSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *TagQuery) Render() (tail string, fields []TagIndexTyped, args []any, err error) {

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

func (a *TagSQL) stdSearch(q *TagQuery) ([]Tag, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const tagSQLCreateTable = `create table if not exists tags (
	id        bigint generated by default as identity primary key,
	name      text,
	owner     bigint,
	meta      jsonb default '{}'::jsonb,
	froms     bigint,
	tos       bigint
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var tagSQLIndexes = []string{}

func (a *TagSQL) stdCreateTable() error {
	_, err := a.db.Exec(context.Background(), tagSQLCreateTable)
	return err
}

func (a *TagSQL) stdCreateIndexes() error {
	for _, q := range tagSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

func (a *TagSQL) stdResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('tags', 'id'), coalesce((select max(id) from tags), 0) + $1, false)", plus)
	return err
}

func (a *TagSQL) stdSetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('tags', 'id'), $1, false)", v)
	return err
}

// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// tagSQLErr converts error of postgres to errors.E
func tagSQLErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.PGX(err)
}

// Get returns row by id
func (a *TagSQL) Get(id int, fields ...TagIndexTyped) (Tag, errors.E) {
	res, err := a.stdGet(id, fields...)
	return res, tagSQLErr(err)
}

// Exists checks row by id
func (a *TagSQL) Exists(id int) (bool, errors.E) {
	res, err := a.stdExists(id)
	return res, tagSQLErr(err)
}

// GetBy returns first row with field = v
func (a *TagSQL) GetBy(field TagIndexTyped, v any, fields ...TagIndexTyped) (Tag, errors.E) {
	res, err := a.stdGetBy(field, v, fields...)
	return res, tagSQLErr(err)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *TagSQL) GetWhere(fields []TagIndexTyped, where string, args ...any) (Tag, errors.E) {
	res, err := a.stdGetWhere(fields, where, args...)
	return res, tagSQLErr(err)
}

// Has checks row with field = v
func (a *TagSQL) Has(field TagIndexTyped, v any) (bool, errors.E) {
	res, err := a.stdHas(field, v)
	return res, tagSQLErr(err)
}

// All returns all rows
func (a *TagSQL) All(fields ...TagIndexTyped) ([]Tag, errors.E) {
	res, err := a.stdAll(fields...)
	return res, tagSQLErr(err)
}

// List returns page of rows
func (a *TagSQL) List(limit, offset int, fields ...TagIndexTyped) ([]Tag, errors.E) {
	res, err := a.stdList(limit, offset, fields...)
	return res, tagSQLErr(err)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *TagSQL) Select(fields []TagIndexTyped, where string, args ...any) ([]Tag, errors.E) {
	res, err := a.stdSelect(fields, where, args...)
	return res, tagSQLErr(err)
}

// Count returns count of rows
func (a *TagSQL) Count() (int, errors.E) {
	res, err := a.stdCount()
	return res, tagSQLErr(err)
}

// CountWhere returns count of rows by condition
func (a *TagSQL) CountWhere(where string, args ...any) (int, errors.E) {
	res, err := a.stdCountWhere(where, args...)
	return res, tagSQLErr(err)
}

// Insert adds row and sets generated v.ID
func (a *TagSQL) Insert(v *Tag) errors.E {
	return tagSQLErr(a.stdInsert(v))
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *TagSQL) InsertNoConflict(v *Tag) (bool, errors.E) {
	res, err := a.stdInsertNoConflict(v)
	return res, tagSQLErr(err)
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *TagSQL) InsertFull(v *Tag) errors.E {
	return tagSQLErr(a.stdInsertFull(v))
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *TagSQL) Upsert(v *Tag) errors.E {
	return tagSQLErr(a.stdUpsert(v))
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *TagSQL) InsertMany(list []Tag) (int64, errors.E) {
	res, err := a.stdInsertMany(list)
	return res, tagSQLErr(err)
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *TagSQL) InsertBatch(list []*Tag) errors.E {
	return tagSQLErr(a.stdInsertBatch(list))
}

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *TagSQL) Update(id int, field TagIndexTyped, v any) errors.E {
	return tagSQLErr(a.stdUpdate(id, field, v))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *TagSQL) Updates(id int, v map[TagIndexTyped]any) errors.E {
	return tagSQLErr(a.stdUpdates(id, v))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *TagSQL) Save(v *Tag, fields ...TagIndexTyped) errors.E {
	return tagSQLErr(a.stdSave(v, fields...))
}

// Inc adds v to number field: views = views + 1
func (a *TagSQL) Inc(id int, field TagIndexTyped, v any) errors.E {
	return tagSQLErr(a.stdInc(id, field, v))
}

// Delete removes row by id
func (a *TagSQL) Delete(id int) errors.E {
	return tagSQLErr(a.stdDelete(id))
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *TagSQL) UpdateWhere(field TagIndexTyped, v any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdateWhere(field, v, where, args...)
	return res, tagSQLErr(err)
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *TagSQL) UpdatesWhere(v map[TagIndexTyped]any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdatesWhere(v, where, args...)
	return res, tagSQLErr(err)
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *TagSQL) DeleteWhere(where string, args ...any) (int64, errors.E) {
	res, err := a.stdDeleteWhere(where, args...)
	return res, tagSQLErr(err)
}

// UpdateMeta sets key of jsonb meta
func (a *TagSQL) UpdateMeta(id int, k string, v int) errors.E {
	return tagSQLErr(a.stdUpdateMeta(id, k, v))
}

// MergeMeta adds keys to jsonb meta: existing keys are replaced
func (a *TagSQL) MergeMeta(id int, v map[string]int) errors.E {
	return tagSQLErr(a.stdMergeMeta(id, v))
}

// DeleteMeta removes key of jsonb meta
func (a *TagSQL) DeleteMeta(id int, k string) errors.E {
	return tagSQLErr(a.stdDeleteMeta(id, k))
}

// RenameMeta renames key of jsonb meta, pgx.ErrNoRows when key not found
func (a *TagSQL) RenameMeta(id int, k, newK string) errors.E {
	return tagSQLErr(a.stdRenameMeta(id, k, newK))
}

// ByOwner: where owner = @owner order by id
func (a *TagSQL) ByOwner(owner int) ([]Tag, errors.E) {
	res, err := a.stdByOwner(owner)
	return res, tagSQLErr(err)
}

// ByName: where name = @name
func (a *TagSQL) ByName(name string) (Tag, errors.E) {
	res, err := a.stdByName(name)
	return res, tagSQLErr(err)
}

// Name: select name from tags where id = @id
func (a *TagSQL) Name(id int) (string, errors.E) {
	res, err := a.stdName(id)
	return res, tagSQLErr(err)
}

// Owner: select owner from tags where id = @id
func (a *TagSQL) Owner(id int) (int, errors.E) {
	res, err := a.stdOwner(id)
	return res, tagSQLErr(err)
}

// IDs: select id from tags where owner = any(@owners) order by id
func (a *TagSQL) IDs(owners []int) ([]int, errors.E) {
	res, err := a.stdIDs(owners)
	return res, tagSQLErr(err)
}

// Rename: update tags set name = @name where owner = @owner
func (a *TagSQL) Rename(owner int, name string) (int64, errors.E) {
	res, err := a.stdRename(owner, name)
	return res, tagSQLErr(err)
}

// Drop: delete from tags where id = $1
func (a *TagSQL) Drop(id int) errors.E {
	return tagSQLErr(a.stdDrop(id))
}

// Search returns rows by query
func (a *TagSQL) Search(q *TagQuery) ([]Tag, errors.E) {
	res, err := a.stdSearch(q)
	return res, tagSQLErr(err)
}

// CreateTable creates table if not exists
func (a *TagSQL) CreateTable() errors.E {
	return tagSQLErr(a.stdCreateTable())
}

// CreateIndexes creates indexes if not exist
func (a *TagSQL) CreateIndexes() errors.E {
	return tagSQLErr(a.stdCreateIndexes())
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *TagSQL) ResetSequence(add ...int64) errors.E {
	return tagSQLErr(a.stdResetSequence(add...))
}

// SetSequence sets next id
func (a *TagSQL) SetSequence(v int64) errors.E {
	return tagSQLErr(a.stdSetSequence(v))
}

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TagProto struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            int64                  `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Owner         int64                  `protobuf:"varint,3,opt,name=owner,proto3" json:"owner,omitempty"`
	Meta          map[string]int64       `protobuf:"bytes,4,rep,name=meta,proto3" json:"meta,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	From          int64                  `protobuf:"varint,5,opt,name=from,proto3" json:"from,omitempty"`
	To            int64                  `protobuf:"varint,6,opt,name=to,proto3" json:"to,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TagProto) Reset() {
	*x = TagProto{}
	mi := &file_tag_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TagProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagProto) ProtoMessage() {}

func (x *TagProto) ProtoReflect() protoreflect.Message {
	mi := &file_tag_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagProto.ProtoReflect.Descriptor instead.
func (*TagProto) Descriptor() ([]byte, []int) {
	return file_tag_proto_rawDescGZIP(), []int{0}
}

func (x *TagProto) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *TagProto) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TagProto) GetOwner() int64 {
	if x != nil {
		return x.Owner
	}
	return 0
}

func (x *TagProto) GetMeta() map[string]int64 {
	if x != nil {
		return x.Meta
	}
	return nil
}

func (x *TagProto) GetFrom() int64 {
	if x != nil {
		return x.From
	}
	return 0
}

func (x *TagProto) GetTo() int64 {
	if x != nil {
		return x.To
	}
	return 0
}

var File_tag_proto protoreflect.FileDescriptor

const file_tag_proto_rawDesc = "" +
	"\n" +
	"\ttag.proto\x12\balltypes\"\xd3\x01\n" +
	"\bTagProto\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x03R\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12\x14\n" +
	"\x05owner\x18\x03 \x01(\x03R\x05owner\x120\n" +
	"\x04meta\x18\x04 \x03(\v2\x1c.alltypes.TagProto.MetaEntryR\x04meta\x12\x12\n" +
	"\x04from\x18\x05 \x01(\x03R\x04from\x12\x0e\n" +
	"\x02to\x18\x06 \x01(\x03R\x02to\x1a7\n" +
	"\tMetaEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01B\rZ\v./;alltypesb\x06proto3"

var (
	file_tag_proto_rawDescOnce sync.Once
	file_tag_proto_rawDescData []byte
)

func file_tag_proto_rawDescGZIP() []byte {
	file_tag_proto_rawDescOnce.Do(func() {
		file_tag_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_tag_proto_rawDesc), len(file_tag_proto_rawDesc)))
	})
	return file_tag_proto_rawDescData
}

var file_tag_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_tag_proto_goTypes = []any{
	(*TagProto)(nil), // 0: alltypes.TagProto
	nil,              // 1: alltypes.TagProto.MetaEntry
}
var file_tag_proto_depIdxs = []int32{
	1, // 0: alltypes.TagProto.meta:type_name -> alltypes.TagProto.MetaEntry
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

var _ = initTagProto()

func initTagProto() struct{} {
	file_tag_proto_init()
	return struct{}{}
}
func file_tag_proto_init() {
	if File_tag_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_tag_proto_rawDesc), len(file_tag_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tag_proto_goTypes,
		DependencyIndexes: file_tag_proto_depIdxs,
		MessageInfos:      file_tag_proto_msgTypes,
	}.Build()
	File_tag_proto = out.File
	file_tag_proto_goTypes = nil
	file_tag_proto_depIdxs = nil
}
