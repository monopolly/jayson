package examples

import (
	"context"
	stderrors "errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

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

// 32 bytes (go padding)
type Order struct {
	ID       int   `json:"id,omitempty"`       // sql{inc}
	Sender   int   `json:"sender,omitempty"`   //
	Receiver int   `json:"receiver,omitempty"` //
	Sum      int64 `json:"sum,omitempty"`      //
}

// orderSQLDB runs queries: pool or transaction
type orderSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// OrderSQL sql queries of table order
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type OrderSQL struct {
	pool *pgxpool.Pool
	db   orderSQLDB // pool, or transaction after TX
}

// NewOrderSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewOrderSQL(pool *pgxpool.Pool) *OrderSQL {
	a := &OrderSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *OrderSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of OrderSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum order")
//		return err
//	})
func (a *OrderSQL) Conn(f func(conn *pgxpool.Conn) errors.E) errors.E {
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
func (a *OrderSQL) Begin(f func(tx pgx.Tx) errors.E) errors.E {
	return orderSQLErr(pgx.BeginFunc(context.Background(), a.db, func(tx pgx.Tx) error {
		if err := f(tx); err != nil {
			return err
		}
		return nil
	}))
}

// TX returns queries in transaction tx
func (a *OrderSQL) TX(tx pgx.Tx) *OrderSQL {
	return &OrderSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *OrderSQL) TableName() string {
	return "order"
}

// Fields returns all sql columns
func (a *OrderSQL) Fields() []OrderIndexTyped {
	return slices.Clone(orderSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var orderSQLFields = []OrderIndexTyped{OrderIndexID, OrderIndexSender, OrderIndexReceiver, OrderIndexSum}

// columns of insert
var orderSQLInsertColumns = []string{"sender", "receiver", "sum"}

// sql column names
var orderSQLColumns = [...]string{
	OrderIndexID:       "id",
	OrderIndexSender:   "sender",
	OrderIndexReceiver: "receiver",
	OrderIndexSum:      "sum",
}

// select expressions: null of scalar column is zero value
var orderSQLSelects = [...]string{
	OrderIndexID:       "coalesce(id, 0)",
	OrderIndexSender:   "coalesce(sender, 0)",
	OrderIndexReceiver: "coalesce(receiver, 0)",
	OrderIndexSum:      "coalesce(sum, 0)",
}

// sql column types
var orderSQLTypes = [...]string{
	OrderIndexID:       "bigint",
	OrderIndexSender:   "bigint",
	OrderIndexReceiver: "bigint",
	OrderIndexSum:      "bigint",
}

const orderSQLSelectAll = "coalesce(id, 0), coalesce(sender, 0), coalesce(receiver, 0), coalesce(sum, 0)"

// orderSQLColumn returns sql column name of field
func orderSQLColumn(f OrderIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(orderSQLColumns) || orderSQLColumns[f] == "" {
		return "", fmt.Errorf("order: field %d is not a sql column", int(f))
	}
	return orderSQLColumns[f], nil
}

// orderSQLSelect returns select list of fields, all columns when empty
func orderSQLSelect(fields []OrderIndexTyped) (string, []OrderIndexTyped, error) {
	if len(fields) == 0 {
		return orderSQLSelectAll, orderSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := orderSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(orderSQLSelects[f])
	}
	return b.String(), fields, nil
}

// orderSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func orderSQLSet(v map[OrderIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, stderrors.New("order: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := orderSQLColumn(f)
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

// orderSQLAffected returns pgx.ErrNoRows when nothing was changed
func orderSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// orderSQLScanBytes scans bytea to byte array
type orderSQLScanBytes func(b []byte)

func (f orderSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Order) sqlDest(fields []OrderIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case OrderIndexID:
			dest[i] = &a.ID
		case OrderIndexSender:
			dest[i] = &a.Sender
		case OrderIndexReceiver:
			dest[i] = &a.Receiver
		case OrderIndexSum:
			dest[i] = &a.Sum
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Order) sqlValue(f OrderIndexTyped) any {
	switch f {
	case OrderIndexID:
		return a.ID
	case OrderIndexSender:
		return a.Sender
	case OrderIndexReceiver:
		return a.Receiver
	case OrderIndexSum:
		return a.Sum
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Order) sqlInsertArgs() []any {
	return []any{
		a.Sender,
		a.Receiver,
		a.Sum,
	}
}

// sqlAllArgs returns values of all columns
func (a *Order) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Sender,
		a.Receiver,
		a.Sum,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *OrderSQL) query(fields []OrderIndexTyped, tail string, args ...any) ([]Order, error) {
	cols, fields, err := orderSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from order"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Order, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *OrderSQL) queryRow(fields []OrderIndexTyped, tail string, args ...any) (v Order, err error) {
	cols, fields, err := orderSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from order"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

func (a *OrderSQL) stdGet(id int, fields ...OrderIndexTyped) (Order, error) {
	return a.queryRow(fields, " where id = $1", id)
}

func (a *OrderSQL) stdExists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from order where id = $1)", id).Scan(&has)
	return
}

func (a *OrderSQL) stdGetBy(field OrderIndexTyped, v any, fields ...OrderIndexTyped) (res Order, err error) {
	col, err := orderSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

func (a *OrderSQL) stdGetWhere(fields []OrderIndexTyped, where string, args ...any) (Order, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

func (a *OrderSQL) stdHas(field OrderIndexTyped, v any) (has bool, err error) {
	col, err := orderSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from order where "+col+" = $1)", v).Scan(&has)
	return
}

func (a *OrderSQL) stdAll(fields ...OrderIndexTyped) ([]Order, error) {
	return a.query(fields, " order by id")
}

func (a *OrderSQL) stdList(limit, offset int, fields ...OrderIndexTyped) ([]Order, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

func (a *OrderSQL) stdSelect(fields []OrderIndexTyped, where string, args ...any) ([]Order, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

func (a *OrderSQL) stdCount() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from order").Scan(&count)
	return
}

func (a *OrderSQL) stdCountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from order where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const orderSQLInsert = "insert into order (sender, receiver, sum) values ($1, $2, $3)"

const orderSQLInsertFull = "insert into order (id, sender, receiver, sum) values ($1, $2, $3, $4)"

func (a *OrderSQL) stdInsert(v *Order) error {
	return a.db.QueryRow(context.Background(), orderSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

func (a *OrderSQL) stdInsertNoConflict(v *Order) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), orderSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if stderrors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (a *OrderSQL) stdInsertFull(v *Order) error {
	_, err := a.db.Exec(context.Background(), orderSQLInsertFull, v.sqlAllArgs()...)
	return err
}

func (a *OrderSQL) stdUpsert(v *Order) error {
	_, err := a.db.Exec(context.Background(), "insert into order (id, sender, receiver, sum) values ($1, $2, $3, $4) on conflict (id) do update set sender = excluded.sender, receiver = excluded.receiver, sum = excluded.sum", v.sqlAllArgs()...)
	return err
}

func (a *OrderSQL) stdInsertMany(list []Order) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"order"}, orderSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

func (a *OrderSQL) stdInsertBatch(list []*Order) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(orderSQLInsert+" returning id", v.sqlInsertArgs()...)
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

func (a *OrderSQL) stdUpdate(id int, field OrderIndexTyped, v any) error {
	col, err := orderSQLColumn(field)
	if err != nil {
		return err
	}
	return orderSQLAffected(a.db.Exec(context.Background(), "update order set "+col+" = $1 where id = $2", v, id))
}

func (a *OrderSQL) stdUpdates(id int, v map[OrderIndexTyped]any) error {
	set, args, err := orderSQLSet(v, 1)
	if err != nil {
		return err
	}
	return orderSQLAffected(a.db.Exec(context.Background(), "update order set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

func (a *OrderSQL) stdSave(v *Order, fields ...OrderIndexTyped) error {
	if len(fields) == 0 {
		fields = orderSQLFields
	}
	values := make(map[OrderIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != OrderIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := orderSQLSet(values, 1)
	if err != nil {
		return err
	}
	return orderSQLAffected(a.db.Exec(context.Background(), "update order set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(OrderIndexID))...))
}

func (a *OrderSQL) stdInc(id int, field OrderIndexTyped, v any) error {
	col, err := orderSQLColumn(field)
	if err != nil {
		return err
	}
	return orderSQLAffected(a.db.Exec(context.Background(), "update order set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

func (a *OrderSQL) stdDelete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from order where id = $1", id)
	return err
}

func (a *OrderSQL) stdUpdateWhere(field OrderIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := orderSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update order set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

func (a *OrderSQL) stdUpdatesWhere(v map[OrderIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := orderSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update order set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

func (a *OrderSQL) stdDeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from order where "+where, args...)
	return tag.RowsAffected(), err
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// OrderQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type OrderQuery struct {
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

// NewOrderQuery creates query with empty maps
func NewOrderQuery() *OrderQuery {
	return &OrderQuery{
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
func (q *OrderQuery) column(k string) (string, OrderIndexTyped, error) {
	if !OrderValidKey(k) {
		return "", 0, fmt.Errorf("order: unknown field %q", k)
	}
	f := OrderKeyIndex(k)
	col, err := orderSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *OrderQuery) Render() (tail string, fields []OrderIndexTyped, args []any, err error) {

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

func (a *OrderSQL) stdSearch(q *OrderQuery) ([]Order, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const orderSQLCreateTable = `create table if not exists order (
	id           bigint generated by default as identity primary key,
	sender       bigint,
	receiver     bigint,
	sum          bigint
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var orderSQLIndexes = []string{}

func (a *OrderSQL) stdCreateTable() error {
	_, err := a.db.Exec(context.Background(), orderSQLCreateTable)
	return err
}

func (a *OrderSQL) stdCreateIndexes() error {
	for _, q := range orderSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

func (a *OrderSQL) stdResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('order', 'id'), coalesce((select max(id) from order), 0) + $1, false)", plus)
	return err
}

func (a *OrderSQL) stdSetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('order', 'id'), $1, false)", v)
	return err
}

// ---------------------------------------------------------------------------
// errors: every error of sql method is errors.E
// ---------------------------------------------------------------------------

// orderSQLErr converts error of database to errors.E
func orderSQLErr(err error) errors.E {
	if err == nil {
		return nil
	}
	if e, ok := err.(errors.E); ok {
		return e
	}
	return errors.PGX(err)
}

// Get returns row by id
func (a *OrderSQL) Get(id int, fields ...OrderIndexTyped) (Order, errors.E) {
	res, err := a.stdGet(id, fields...)
	return res, orderSQLErr(err)
}

// Exists checks row by id
func (a *OrderSQL) Exists(id int) (bool, errors.E) {
	res, err := a.stdExists(id)
	return res, orderSQLErr(err)
}

// GetBy returns first row with field = v
func (a *OrderSQL) GetBy(field OrderIndexTyped, v any, fields ...OrderIndexTyped) (Order, errors.E) {
	res, err := a.stdGetBy(field, v, fields...)
	return res, orderSQLErr(err)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *OrderSQL) GetWhere(fields []OrderIndexTyped, where string, args ...any) (Order, errors.E) {
	res, err := a.stdGetWhere(fields, where, args...)
	return res, orderSQLErr(err)
}

// Has checks row with field = v
func (a *OrderSQL) Has(field OrderIndexTyped, v any) (bool, errors.E) {
	res, err := a.stdHas(field, v)
	return res, orderSQLErr(err)
}

// All returns all rows
func (a *OrderSQL) All(fields ...OrderIndexTyped) ([]Order, errors.E) {
	res, err := a.stdAll(fields...)
	return res, orderSQLErr(err)
}

// List returns page of rows
func (a *OrderSQL) List(limit, offset int, fields ...OrderIndexTyped) ([]Order, errors.E) {
	res, err := a.stdList(limit, offset, fields...)
	return res, orderSQLErr(err)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *OrderSQL) Select(fields []OrderIndexTyped, where string, args ...any) ([]Order, errors.E) {
	res, err := a.stdSelect(fields, where, args...)
	return res, orderSQLErr(err)
}

// Count returns count of rows
func (a *OrderSQL) Count() (int, errors.E) {
	res, err := a.stdCount()
	return res, orderSQLErr(err)
}

// CountWhere returns count of rows by condition
func (a *OrderSQL) CountWhere(where string, args ...any) (int, errors.E) {
	res, err := a.stdCountWhere(where, args...)
	return res, orderSQLErr(err)
}

// Insert adds row and sets generated v.ID
func (a *OrderSQL) Insert(v *Order) errors.E {
	return orderSQLErr(a.stdInsert(v))
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *OrderSQL) InsertNoConflict(v *Order) (bool, errors.E) {
	res, err := a.stdInsertNoConflict(v)
	return res, orderSQLErr(err)
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *OrderSQL) InsertFull(v *Order) errors.E {
	return orderSQLErr(a.stdInsertFull(v))
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *OrderSQL) Upsert(v *Order) errors.E {
	return orderSQLErr(a.stdUpsert(v))
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *OrderSQL) InsertMany(list []Order) (int64, errors.E) {
	res, err := a.stdInsertMany(list)
	return res, orderSQLErr(err)
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *OrderSQL) InsertBatch(list []*Order) errors.E {
	return orderSQLErr(a.stdInsertBatch(list))
}

// Update sets field of row, pgx.ErrNoRows when row not found
func (a *OrderSQL) Update(id int, field OrderIndexTyped, v any) errors.E {
	return orderSQLErr(a.stdUpdate(id, field, v))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *OrderSQL) Updates(id int, v map[OrderIndexTyped]any) errors.E {
	return orderSQLErr(a.stdUpdates(id, v))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *OrderSQL) Save(v *Order, fields ...OrderIndexTyped) errors.E {
	return orderSQLErr(a.stdSave(v, fields...))
}

// Inc adds v to number field: views = views + 1
func (a *OrderSQL) Inc(id int, field OrderIndexTyped, v any) errors.E {
	return orderSQLErr(a.stdInc(id, field, v))
}

// Delete removes row by id
func (a *OrderSQL) Delete(id int) errors.E {
	return orderSQLErr(a.stdDelete(id))
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *OrderSQL) UpdateWhere(field OrderIndexTyped, v any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdateWhere(field, v, where, args...)
	return res, orderSQLErr(err)
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *OrderSQL) UpdatesWhere(v map[OrderIndexTyped]any, where string, args ...any) (int64, errors.E) {
	res, err := a.stdUpdatesWhere(v, where, args...)
	return res, orderSQLErr(err)
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *OrderSQL) DeleteWhere(where string, args ...any) (int64, errors.E) {
	res, err := a.stdDeleteWhere(where, args...)
	return res, orderSQLErr(err)
}

// Search returns rows by query
func (a *OrderSQL) Search(q *OrderQuery) ([]Order, errors.E) {
	res, err := a.stdSearch(q)
	return res, orderSQLErr(err)
}

// CreateTable creates table if not exists
func (a *OrderSQL) CreateTable() errors.E {
	return orderSQLErr(a.stdCreateTable())
}

// CreateIndexes creates indexes if not exist
func (a *OrderSQL) CreateIndexes() errors.E {
	return orderSQLErr(a.stdCreateIndexes())
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *OrderSQL) ResetSequence(add ...int64) errors.E {
	return orderSQLErr(a.stdResetSequence(add...))
}

// SetSequence sets next id
func (a *OrderSQL) SetSequence(v int64) errors.E {
	return orderSQLErr(a.stdSetSequence(v))
}

// field type
type OrderIndexTyped int

// int index
const (
	OrderIndexID = OrderIndexTyped(iota)
	OrderIndexSender
	OrderIndexReceiver
	OrderIndexSum
)

// string index
const (
	OrderFieldID       = "id"       // int sql{inc}
	OrderFieldSender   = "sender"   // int
	OrderFieldReceiver = "receiver" // int
	OrderFieldSum      = "sum"      // int64
)

// index func
func OrderIndexes() []OrderIndexTyped {
	return []OrderIndexTyped{OrderIndexID, OrderIndexSender, OrderIndexReceiver, OrderIndexSum}

}

// key index string
func (a OrderIndexTyped) String() string {
	switch a {
	case OrderIndexID:
		return "id"
	case OrderIndexSender:
		return "sender"
	case OrderIndexReceiver:
		return "receiver"
	case OrderIndexSum:
		return "sum"
	default:
		return ""
	}
}

// key index string
func (a OrderIndexTyped) SQLName() string {
	switch a {
	case OrderIndexID:
		return "id"
	case OrderIndexSender:
		return "sender"
	case OrderIndexReceiver:
		return "receiver"
	case OrderIndexSum:
		return "sum"
	default:
		return ""
	}
}

// key index clickhouse string
func (a OrderIndexTyped) ClickhouseName() string {
	switch a {
	case OrderIndexID:
		return "id"
	case OrderIndexSender:
		return "sender"
	case OrderIndexReceiver:
		return "receiver"
	case OrderIndexSum:
		return "sum"
	default:
		return ""
	}
}

// key index type
func (a OrderIndexTyped) Type() string {
	switch a {
	case OrderIndexID:
		return "int"
	case OrderIndexSender:
		return "int"
	case OrderIndexReceiver:
		return "int"
	case OrderIndexSum:
		return "int64"
	default:
		return ""
	}
}

// custom title
func (a OrderIndexTyped) Title() string {
	switch a {
	case OrderIndexID:
		return "ID"
	case OrderIndexSender:
		return "Sender"
	case OrderIndexReceiver:
		return "Receiver"
	case OrderIndexSum:
		return "Sum"
	default:
		return ""
	}
}

// custom desc
func (a OrderIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func OrderKeyIndex(key string) OrderIndexTyped {
	switch key {
	case "id":
		return OrderIndexID
	case "sender":
		return OrderIndexSender
	case "receiver":
		return OrderIndexReceiver
	case "sum":
		return OrderIndexSum
	default:
		return 0
	}
}

// valid struct key check
func OrderValidKey(key string) bool {
	switch key {
	case "id", "sender", "receiver", "sum":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Order) Tuple() (r []any) {
	return []any{a.ID, a.Sender, a.Receiver, a.Sum}
}

// get struct value with function
func (a *Order) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "sender":
		return a.Sender //int
	case "receiver":
		return a.Receiver //int
	case "sum":
		return a.Sum //int64
	}
	return
}

// get any struct value as string
func (a *Order) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "sender":
		return fmt.Sprint(a.Sender) //int
	case "receiver":
		return fmt.Sprint(a.Receiver) //int
	case "sum":
		return fmt.Sprint(a.Sum) //int64
	}
	return
}

// struct to map
func (a *Order) Map() map[string]any {
	return map[string]any{
		"id":       a.ID,
		"sender":   a.Sender,
		"receiver": a.Receiver,
		"sum":      a.Sum,
	}
}

// struct to map
func (a *Order) Iterate(f func(k OrderIndexTyped, v any)) {
	for _, x := range OrderIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Order) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseOrder(v []byte) (a Order, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}
