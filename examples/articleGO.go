package examples

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	gojson "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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

// 40 bytes (go padding)
type Article struct {
	ID    int    `json:"id,omitempty"`    // sql{inc}
	Title string `json:"title,omitempty"` // sql{search="text"}
	Body  string `json:"body,omitempty"`  // sql{search="text"}
}

// articleSQLDB runs queries: pool or transaction
type articleSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// ArticleSQL sql queries of table article
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type ArticleSQL struct {
	pool *pgxpool.Pool
	db   articleSQLDB // pool, or transaction after TX
}

// NewArticleSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewArticleSQL(pool *pgxpool.Pool) *ArticleSQL {
	a := &ArticleSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *ArticleSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of ArticleSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum article")
//		return err
//	})
func (a *ArticleSQL) Conn(f func(conn *pgxpool.Conn) error) error {
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
func (a *ArticleSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *ArticleSQL) TX(tx pgx.Tx) *ArticleSQL {
	return &ArticleSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *ArticleSQL) TableName() string {
	return "article"
}

// Fields returns all sql columns
func (a *ArticleSQL) Fields() []ArticleIndexTyped {
	return slices.Clone(articleSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var articleSQLFields = []ArticleIndexTyped{ArticleIndexID, ArticleIndexTitle, ArticleIndexBody}

// columns of insert
var articleSQLInsertColumns = []string{"title", "body"}

// sql column names
var articleSQLColumns = [...]string{
	ArticleIndexID:    "id",
	ArticleIndexTitle: "title",
	ArticleIndexBody:  "body",
}

// select expressions: null of scalar column is zero value
var articleSQLSelects = [...]string{
	ArticleIndexID:    "coalesce(id, 0)",
	ArticleIndexTitle: "coalesce(title, '')",
	ArticleIndexBody:  "coalesce(body, '')",
}

// sql column types
var articleSQLTypes = [...]string{
	ArticleIndexID:    "bigint",
	ArticleIndexTitle: "text",
	ArticleIndexBody:  "text",
}

const articleSQLSelectAll = "coalesce(id, 0), coalesce(title, ''), coalesce(body, '')"

// articleSQLColumn returns sql column name of field
func articleSQLColumn(f ArticleIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(articleSQLColumns) || articleSQLColumns[f] == "" {
		return "", fmt.Errorf("article: field %d is not a sql column", int(f))
	}
	return articleSQLColumns[f], nil
}

// articleSQLSelect returns select list of fields, all columns when empty
func articleSQLSelect(fields []ArticleIndexTyped) (string, []ArticleIndexTyped, error) {
	if len(fields) == 0 {
		return articleSQLSelectAll, articleSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := articleSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(articleSQLSelects[f])
	}
	return b.String(), fields, nil
}

// articleSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func articleSQLSet(v map[ArticleIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("article: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := articleSQLColumn(f)
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

// articleSQLAffected returns pgx.ErrNoRows when nothing was changed
func articleSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// articleSQLScanBytes scans bytea to byte array
type articleSQLScanBytes func(b []byte)

func (f articleSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Article) sqlDest(fields []ArticleIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case ArticleIndexID:
			dest[i] = &a.ID
		case ArticleIndexTitle:
			dest[i] = &a.Title
		case ArticleIndexBody:
			dest[i] = &a.Body
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Article) sqlValue(f ArticleIndexTyped) any {
	switch f {
	case ArticleIndexID:
		return a.ID
	case ArticleIndexTitle:
		return a.Title
	case ArticleIndexBody:
		return a.Body
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Article) sqlInsertArgs() []any {
	return []any{
		a.Title,
		a.Body,
	}
}

// sqlAllArgs returns values of all columns
func (a *Article) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Title,
		a.Body,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *ArticleSQL) query(fields []ArticleIndexTyped, tail string, args ...any) ([]Article, error) {
	cols, fields, err := articleSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from article"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Article, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *ArticleSQL) queryRow(fields []ArticleIndexTyped, tail string, args ...any) (v Article, err error) {
	cols, fields, err := articleSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from article"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by id
func (a *ArticleSQL) Get(id int, fields ...ArticleIndexTyped) (Article, error) {
	return a.queryRow(fields, " where id = $1", id)
}

// Exists checks row by id
func (a *ArticleSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from article where id = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *ArticleSQL) GetBy(field ArticleIndexTyped, v any, fields ...ArticleIndexTyped) (res Article, err error) {
	col, err := articleSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *ArticleSQL) GetWhere(fields []ArticleIndexTyped, where string, args ...any) (Article, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *ArticleSQL) Has(field ArticleIndexTyped, v any) (has bool, err error) {
	col, err := articleSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from article where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *ArticleSQL) All(fields ...ArticleIndexTyped) ([]Article, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *ArticleSQL) List(limit, offset int, fields ...ArticleIndexTyped) ([]Article, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *ArticleSQL) Select(fields []ArticleIndexTyped, where string, args ...any) ([]Article, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *ArticleSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from article").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *ArticleSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from article where "+where, args...).Scan(&count)
	return
}

// FullTextText full text search in text, best match first
//
//	db.FullTextText("red car -old", 20)
func (a *ArticleSQL) FullTextText(text string, limit int, fields ...ArticleIndexTyped) ([]Article, error) {
	return a.query(fields, " where text @@ websearch_to_tsquery('simple', $1) order by ts_rank(text, websearch_to_tsquery('simple', $1)) desc limit $2", text, limit)
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const articleSQLInsert = "insert into article (title, body) values ($1, $2)"

const articleSQLInsertFull = "insert into article (id, title, body) values ($1, $2, $3)"

// Insert adds row and sets generated v.ID
func (a *ArticleSQL) Insert(v *Article) error {
	return a.db.QueryRow(context.Background(), articleSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *ArticleSQL) InsertNoConflict(v *Article) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), articleSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *ArticleSQL) InsertFull(v *Article) error {
	_, err := a.db.Exec(context.Background(), articleSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *ArticleSQL) Upsert(v *Article) error {
	_, err := a.db.Exec(context.Background(), "insert into article (id, title, body) values ($1, $2, $3) on conflict (id) do update set title = excluded.title, body = excluded.body", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *ArticleSQL) InsertMany(list []Article) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"article"}, articleSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *ArticleSQL) InsertBatch(list []*Article) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(articleSQLInsert+" returning id", v.sqlInsertArgs()...)
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

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *ArticleSQL) Update(id int, field ArticleIndexTyped, v any) error {
	col, err := articleSQLColumn(field)
	if err != nil {
		return err
	}
	return articleSQLAffected(a.db.Exec(context.Background(), "update article set "+col+" = $1 where id = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *ArticleSQL) Updates(id int, v map[ArticleIndexTyped]any) error {
	set, args, err := articleSQLSet(v, 1)
	if err != nil {
		return err
	}
	return articleSQLAffected(a.db.Exec(context.Background(), "update article set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *ArticleSQL) Save(v *Article, fields ...ArticleIndexTyped) error {
	if len(fields) == 0 {
		fields = articleSQLFields
	}
	values := make(map[ArticleIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != ArticleIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := articleSQLSet(values, 1)
	if err != nil {
		return err
	}
	return articleSQLAffected(a.db.Exec(context.Background(), "update article set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(ArticleIndexID))...))
}

// Inc adds v to number field: views = views + 1
func (a *ArticleSQL) Inc(id int, field ArticleIndexTyped, v any) error {
	col, err := articleSQLColumn(field)
	if err != nil {
		return err
	}
	return articleSQLAffected(a.db.Exec(context.Background(), "update article set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

// Delete removes row by id
func (a *ArticleSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from article where id = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *ArticleSQL) UpdateWhere(field ArticleIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := articleSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update article set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *ArticleSQL) UpdatesWhere(v map[ArticleIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := articleSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update article set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *ArticleSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from article where "+where, args...)
	return tag.RowsAffected(), err
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// ArticleQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type ArticleQuery struct {
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

// NewArticleQuery creates query with empty maps
func NewArticleQuery() *ArticleQuery {
	return &ArticleQuery{
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
func (q *ArticleQuery) column(k string) (string, ArticleIndexTyped, error) {
	if !ArticleValidKey(k) {
		return "", 0, fmt.Errorf("article: unknown field %q", k)
	}
	f := ArticleKeyIndex(k)
	col, err := articleSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *ArticleQuery) Render() (tail string, fields []ArticleIndexTyped, args []any, err error) {

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
func (a *ArticleSQL) Search(q *ArticleQuery) ([]Article, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const articleSQLCreateTable = `create table if not exists article (
	id        bigint generated by default as identity primary key,
	title     text,
	body      text,
	text      tsvector generated always as (to_tsvector('simple', coalesce(title::text, '') || ' ' || coalesce(body::text, ''))) stored
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var articleSQLIndexes = []string{
	"create index if not exists gin_article_text on article using gin(text)",
}

// CreateTable creates table if not exists
func (a *ArticleSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), articleSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *ArticleSQL) CreateIndexes() error {
	for _, q := range articleSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *ArticleSQL) ResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('article', 'id'), coalesce((select max(id) from article), 0) + $1, false)", plus)
	return err
}

// SetSequence sets next id
func (a *ArticleSQL) SetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('article', 'id'), $1, false)", v)
	return err
}

// field type
type ArticleIndexTyped int

// int index
const (
	ArticleIndexID = ArticleIndexTyped(iota)
	ArticleIndexTitle
	ArticleIndexBody
)

// string index
const (
	ArticleFieldID    = "id"    // int sql{inc}
	ArticleFieldTitle = "title" // string sql{search="text"}
	ArticleFieldBody  = "body"  // string sql{search="text"}
)

// index func
func ArticleIndexes() []ArticleIndexTyped {
	return []ArticleIndexTyped{ArticleIndexID, ArticleIndexTitle, ArticleIndexBody}

}

// key index string
func (a ArticleIndexTyped) String() string {
	switch a {
	case ArticleIndexID:
		return "id"
	case ArticleIndexTitle:
		return "title"
	case ArticleIndexBody:
		return "body"
	default:
		return ""
	}
}

// key index string
func (a ArticleIndexTyped) SQLName() string {
	switch a {
	case ArticleIndexID:
		return "id"
	case ArticleIndexTitle:
		return "title"
	case ArticleIndexBody:
		return "body"
	default:
		return ""
	}
}

// key index clickhouse string
func (a ArticleIndexTyped) ClickhouseName() string {
	switch a {
	case ArticleIndexID:
		return "id"
	case ArticleIndexTitle:
		return "title"
	case ArticleIndexBody:
		return "body"
	default:
		return ""
	}
}

// key index type
func (a ArticleIndexTyped) Type() string {
	switch a {
	case ArticleIndexID:
		return "int"
	case ArticleIndexTitle:
		return "string"
	case ArticleIndexBody:
		return "string"
	default:
		return ""
	}
}

// custom title
func (a ArticleIndexTyped) Title() string {
	switch a {
	case ArticleIndexID:
		return "ID"
	case ArticleIndexTitle:
		return "Title"
	case ArticleIndexBody:
		return "Body"
	default:
		return ""
	}
}

// custom desc
func (a ArticleIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func ArticleKeyIndex(key string) ArticleIndexTyped {
	switch key {
	case "id":
		return ArticleIndexID
	case "title":
		return ArticleIndexTitle
	case "body":
		return ArticleIndexBody
	default:
		return 0
	}
}

// valid struct key check
func ArticleValidKey(key string) bool {
	switch key {
	case "id", "title", "body":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Article) Tuple() (r []any) {
	return []any{a.ID, a.Title, a.Body}
}

// get struct value with function
func (a *Article) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "title":
		return a.Title //string
	case "body":
		return a.Body //string
	}
	return
}

// get any struct value as string
func (a *Article) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "title":
		return fmt.Sprint(a.Title) //string
	case "body":
		return fmt.Sprint(a.Body) //string
	}
	return
}

// struct to map
func (a *Article) Map() map[string]any {
	return map[string]any{
		"id":    a.ID,
		"title": a.Title,
		"body":  a.Body,
	}
}

// struct to map
func (a *Article) Iterate(f func(k ArticleIndexTyped, v any)) {
	for _, x := range ArticleIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Article) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseArticle(v []byte) (a Article, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}
