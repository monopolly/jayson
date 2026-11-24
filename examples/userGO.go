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

	SQL FUNCTIONS: methods of interface become methods of UserSQL
	type userSQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (User, error)

		// select role from user where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from user where id = @id
		Music(id int) ([]int, error)

		// select id from user where role = @role
		IDs(role role.Role) ([]int, error)

		// update user set created = now() where id = @id
		Touch(id int) error

		// delete from user where created < @before
		Clean(before time.Time) (int64, error) // count of rows
	}
*/

// 40 bytes (go padding)
type User struct {
	ID       int    `json:"id,omitempty"`       // sql{inc} @public #readonly
	Name     string `json:"name,omitempty"`     // @public
	Password string `json:"password,omitempty"` //
}

// userSQLDB runs queries: pool or transaction
type userSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// UserSQL sql queries of table user
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type UserSQL struct {
	pool *pgxpool.Pool
	db   userSQLDB // pool, or transaction after TX
}

// NewUserSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewUserSQL(pool *pgxpool.Pool) *UserSQL {
	a := &UserSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *UserSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of UserSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum user")
//		return err
//	})
func (a *UserSQL) Conn(f func(conn *pgxpool.Conn) error) error {
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
func (a *UserSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *UserSQL) TX(tx pgx.Tx) *UserSQL {
	return &UserSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *UserSQL) TableName() string {
	return "user"
}

// Fields returns all sql columns
func (a *UserSQL) Fields() []UserIndexTyped {
	return slices.Clone(userSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var userSQLFields = []UserIndexTyped{UserIndexID, UserIndexName, UserIndexPassword}

// columns of insert
var userSQLInsertColumns = []string{"name", "password"}

// sql column names
var userSQLColumns = [...]string{
	UserIndexID:       "id",
	UserIndexName:     "name",
	UserIndexPassword: "password",
}

// select expressions: null of scalar column is zero value
var userSQLSelects = [...]string{
	UserIndexID:       "coalesce(id, 0)",
	UserIndexName:     "coalesce(name, '')",
	UserIndexPassword: "coalesce(password, '')",
}

// sql column types
var userSQLTypes = [...]string{
	UserIndexID:       "bigint",
	UserIndexName:     "text",
	UserIndexPassword: "text",
}

const userSQLSelectAll = "coalesce(id, 0), coalesce(name, ''), coalesce(password, '')"

// userSQLColumn returns sql column name of field
func userSQLColumn(f UserIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(userSQLColumns) || userSQLColumns[f] == "" {
		return "", fmt.Errorf("user: field %d is not a sql column", int(f))
	}
	return userSQLColumns[f], nil
}

// userSQLSelect returns select list of fields, all columns when empty
func userSQLSelect(fields []UserIndexTyped) (string, []UserIndexTyped, error) {
	if len(fields) == 0 {
		return userSQLSelectAll, userSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := userSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(userSQLSelects[f])
	}
	return b.String(), fields, nil
}

// userSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func userSQLSet(v map[UserIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("user: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := userSQLColumn(f)
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

// userSQLAffected returns pgx.ErrNoRows when nothing was changed
func userSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// userSQLScanBytes scans bytea to byte array
type userSQLScanBytes func(b []byte)

func (f userSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *User) sqlDest(fields []UserIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case UserIndexID:
			dest[i] = &a.ID
		case UserIndexName:
			dest[i] = &a.Name
		case UserIndexPassword:
			dest[i] = &a.Password
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *User) sqlValue(f UserIndexTyped) any {
	switch f {
	case UserIndexID:
		return a.ID
	case UserIndexName:
		return a.Name
	case UserIndexPassword:
		return a.Password
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *User) sqlInsertArgs() []any {
	return []any{
		a.Name,
		a.Password,
	}
}

// sqlAllArgs returns values of all columns
func (a *User) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Name,
		a.Password,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *UserSQL) query(fields []UserIndexTyped, tail string, args ...any) ([]User, error) {
	cols, fields, err := userSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from user"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v User, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *UserSQL) queryRow(fields []UserIndexTyped, tail string, args ...any) (v User, err error) {
	cols, fields, err := userSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from user"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by id
func (a *UserSQL) Get(id int, fields ...UserIndexTyped) (User, error) {
	return a.queryRow(fields, " where id = $1", id)
}

// GetPublic returns UserPublic by id
func (a *UserSQL) GetPublic(id int) (res UserPublic, err error) {
	v, err := a.Get(id, UserPublicList()...)
	if err != nil {
		return
	}
	return *v.UserPublic(), nil
}

// Exists checks row by id
func (a *UserSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from user where id = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *UserSQL) GetBy(field UserIndexTyped, v any, fields ...UserIndexTyped) (res User, err error) {
	col, err := userSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *UserSQL) GetWhere(fields []UserIndexTyped, where string, args ...any) (User, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *UserSQL) Has(field UserIndexTyped, v any) (has bool, err error) {
	col, err := userSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from user where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *UserSQL) All(fields ...UserIndexTyped) ([]User, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *UserSQL) List(limit, offset int, fields ...UserIndexTyped) ([]User, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *UserSQL) Select(fields []UserIndexTyped, where string, args ...any) ([]User, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *UserSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from user").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *UserSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from user where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const userSQLInsert = "insert into user (name, password) values ($1, $2)"

const userSQLInsertFull = "insert into user (id, name, password) values ($1, $2, $3)"

// Insert adds row and sets generated v.ID
func (a *UserSQL) Insert(v *User) error {
	return a.db.QueryRow(context.Background(), userSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *UserSQL) InsertNoConflict(v *User) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), userSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *UserSQL) InsertFull(v *User) error {
	_, err := a.db.Exec(context.Background(), userSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *UserSQL) Upsert(v *User) error {
	_, err := a.db.Exec(context.Background(), "insert into user (id, name, password) values ($1, $2, $3) on conflict (id) do update set name = excluded.name, password = excluded.password", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *UserSQL) InsertMany(list []User) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"user"}, userSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *UserSQL) InsertBatch(list []*User) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(userSQLInsert+" returning id", v.sqlInsertArgs()...)
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
func (a *UserSQL) Update(id int, field UserIndexTyped, v any) error {
	col, err := userSQLColumn(field)
	if err != nil {
		return err
	}
	return userSQLAffected(a.db.Exec(context.Background(), "update user set "+col+" = $1 where id = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *UserSQL) Updates(id int, v map[UserIndexTyped]any) error {
	set, args, err := userSQLSet(v, 1)
	if err != nil {
		return err
	}
	return userSQLAffected(a.db.Exec(context.Background(), "update user set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *UserSQL) Save(v *User, fields ...UserIndexTyped) error {
	if len(fields) == 0 {
		fields = userSQLFields
	}
	values := make(map[UserIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != UserIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := userSQLSet(values, 1)
	if err != nil {
		return err
	}
	return userSQLAffected(a.db.Exec(context.Background(), "update user set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(UserIndexID))...))
}

// Inc adds v to number field: views = views + 1
func (a *UserSQL) Inc(id int, field UserIndexTyped, v any) error {
	col, err := userSQLColumn(field)
	if err != nil {
		return err
	}
	return userSQLAffected(a.db.Exec(context.Background(), "update user set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

// Delete removes row by id
func (a *UserSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from user where id = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *UserSQL) UpdateWhere(field UserIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := userSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update user set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *UserSQL) UpdatesWhere(v map[UserIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := userSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update user set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *UserSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from user where "+where, args...)
	return tag.RowsAffected(), err
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// UserQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type UserQuery struct {
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

// NewUserQuery creates query with empty maps
func NewUserQuery() *UserQuery {
	return &UserQuery{
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
func (q *UserQuery) column(k string) (string, UserIndexTyped, error) {
	if !UserValidKey(k) {
		return "", 0, fmt.Errorf("user: unknown field %q", k)
	}
	f := UserKeyIndex(k)
	col, err := userSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *UserQuery) Render() (tail string, fields []UserIndexTyped, args []any, err error) {

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
func (a *UserSQL) Search(q *UserQuery) ([]User, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const userSQLCreateTable = `create table if not exists user (
	id           bigint generated by default as identity primary key,
	name         text,
	password     text
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var userSQLIndexes = []string{}

// CreateTable creates table if not exists
func (a *UserSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), userSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *UserSQL) CreateIndexes() error {
	for _, q := range userSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *UserSQL) ResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('user', 'id'), coalesce((select max(id) from user), 0) + $1, false)", plus)
	return err
}

// SetSequence sets next id
func (a *UserSQL) SetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('user', 'id'), $1, false)", v)
	return err
}

// field type
type UserIndexTyped int

// int index
const (
	UserIndexID = UserIndexTyped(iota)
	UserIndexName
	UserIndexPassword
)

// string index
const (
	UserFieldID       = "id"       // int sql{inc} @public #readonly
	UserFieldName     = "name"     // string @public
	UserFieldPassword = "password" // string
)

// index func
func UserIndexes() []UserIndexTyped {
	return []UserIndexTyped{UserIndexID, UserIndexName, UserIndexPassword}

}

func UserPublicList() []UserIndexTyped {
	return []UserIndexTyped{UserIndexID, UserIndexName}
}

func UserReadonlyList() []UserIndexTyped {
	return []UserIndexTyped{UserIndexID}
}

func (a UserIndexTyped) Public() bool {
	switch a {
	case UserIndexID, UserIndexName:
		return true
	default:
		return false
	}
}
func (a UserIndexTyped) Readonly() bool {
	switch a {
	case UserIndexID:
		return true
	default:
		return false
	}
}

// key index string
func (a UserIndexTyped) String() string {
	switch a {
	case UserIndexID:
		return "id"
	case UserIndexName:
		return "name"
	case UserIndexPassword:
		return "password"
	default:
		return ""
	}
}

// key index string
func (a UserIndexTyped) SQLName() string {
	switch a {
	case UserIndexID:
		return "id"
	case UserIndexName:
		return "name"
	case UserIndexPassword:
		return "password"
	default:
		return ""
	}
}

// key index clickhouse string
func (a UserIndexTyped) ClickhouseName() string {
	switch a {
	case UserIndexID:
		return "id"
	case UserIndexName:
		return "name"
	case UserIndexPassword:
		return "password"
	default:
		return ""
	}
}

// key index type
func (a UserIndexTyped) Type() string {
	switch a {
	case UserIndexID:
		return "int"
	case UserIndexName:
		return "string"
	case UserIndexPassword:
		return "string"
	default:
		return ""
	}
}

// custom title
func (a UserIndexTyped) Title() string {
	switch a {
	case UserIndexID:
		return "ID"
	case UserIndexName:
		return "Name"
	case UserIndexPassword:
		return "Password"
	default:
		return ""
	}
}

// custom desc
func (a UserIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func UserKeyIndex(key string) UserIndexTyped {
	switch key {
	case "id":
		return UserIndexID
	case "name":
		return UserIndexName
	case "password":
		return UserIndexPassword
	default:
		return 0
	}
}

// valid struct key check
func UserValidKey(key string) bool {
	switch key {
	case "id", "name", "password":
		return true
	default:
		return false
	}
}

type UserPublic struct {
	ID   int    `json:"id,omitempty"`   // sql{inc} @public #readonly
	Name string `json:"name,omitempty"` // @public
}

func (a *User) UserPublic() (res *UserPublic) {
	return &UserPublic{
		ID:   a.ID,
		Name: a.Name,
	}
}

func (a *UserPublic) User() (res *User) {
	return &User{
		ID:   a.ID,
		Name: a.Name,
	}
}

func (a *UserPublic) Pack() (res []byte) {
	res, _ = gojson.Marshal(a)
	return
}

// Tuple create an array from struct
func (a *User) Tuple() (r []any) {
	return []any{a.ID, a.Name, a.Password}
}

// get struct value with function
func (a *User) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "name":
		return a.Name //string
	case "password":
		return a.Password //string
	}
	return
}

// get any struct value as string
func (a *User) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "name":
		return fmt.Sprint(a.Name) //string
	case "password":
		return fmt.Sprint(a.Password) //string
	}
	return
}

// struct to map
func (a *User) Map() map[string]any {
	return map[string]any{
		"id":       a.ID,
		"name":     a.Name,
		"password": a.Password,
	}
}

// struct to map
func (a *User) Iterate(f func(k UserIndexTyped, v any)) {
	for _, x := range UserIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *User) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseUser(v []byte) (a User, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}
