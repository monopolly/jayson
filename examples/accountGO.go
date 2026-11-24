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

// field type
type AccountIndexTyped int

// int index
const (
	AccountIndexID = AccountIndexTyped(iota)
	AccountIndexEmail
	AccountIndexName
	AccountIndexBalance
	AccountIndexTags
	AccountIndexScores
	AccountIndexMeta
	AccountIndexSettings
	AccountIndexItems
)

// string index
const (
	AccountFieldID       = "id"       // int sql{inc}
	AccountFieldEmail    = "email"    // string sql{unique="email"}
	AccountFieldName     = "name"     // string sql{index}
	AccountFieldBalance  = "balance"  // int64
	AccountFieldTags     = "tags"     // []string
	AccountFieldScores   = "scores"   // []int
	AccountFieldMeta     = "meta"     // map[string]string
	AccountFieldSettings = "settings" // map[string]int
	AccountFieldItems    = "items"    // []Item
)

// index func
func AccountIndexes() []AccountIndexTyped {
	return []AccountIndexTyped{AccountIndexID, AccountIndexEmail, AccountIndexName, AccountIndexBalance, AccountIndexTags, AccountIndexScores, AccountIndexMeta, AccountIndexSettings, AccountIndexItems}

}

// 88 bytes (go padding)
//
//easyjson:json
type Account struct {
	ID       int               `json:"id,omitempty"`       // sql{inc}
	Email    string            `json:"email,omitempty"`    // sql{unique="email"}
	Name     string            `json:"name,omitempty"`     // sql{index}
	Balance  int64             `json:"balance,omitempty"`  //
	Tags     []string          `json:"tags,omitempty"`     //
	Scores   []int             `json:"scores,omitempty"`   //
	Meta     map[string]string `json:"meta,omitempty"`     //
	Settings map[string]int    `json:"settings,omitempty"` //
	Items    []Item            `json:"items,omitempty"`    //
}

// init struct
func NewAccount() (a *Account) {
	a = new(Account)
	a.Meta = make(map[string]string)
	a.Settings = make(map[string]int)
	return
}

// Tuple create an array from struct
func (a *Account) Tuple() (r []any) {
	return []any{a.ID, a.Email, a.Name, a.Balance, a.Tags, a.Scores, a.Meta, a.Settings, a.Items}
}

// Tuple create an array from struct for clickhouse
func (a *Account) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Email, a.Name, a.Balance, a.Tags, a.Scores, a.Meta, a.Settings, a.Items}
}

// get struct value with function
func (a *Account) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "email":
		return a.Email //string
	case "name":
		return a.Name //string
	case "balance":
		return a.Balance //int64
	case "tags":
		return a.Tags //[]string
	case "scores":
		return a.Scores //[]int
	case "meta":
		return a.Meta //map[string]string
	case "settings":
		return a.Settings //map[string]int
	case "items":
		return a.Items //[]Item
	}
	return
}

// get any struct value as string
func (a *Account) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "email":
		return fmt.Sprint(a.Email) //string
	case "name":
		return fmt.Sprint(a.Name) //string
	case "balance":
		return fmt.Sprint(a.Balance) //int64
	case "tags":
		return fmt.Sprint(a.Tags) //[]string
	case "scores":
		return fmt.Sprint(a.Scores) //[]int
	case "meta":
		return fmt.Sprint(a.Meta) //map[string]string
	case "settings":
		return fmt.Sprint(a.Settings) //map[string]int
	case "items":
		return fmt.Sprint(a.Items) //[]Item
	}
	return
}

// key index string
func (a AccountIndexTyped) String() string {
	switch a {
	case AccountIndexID:
		return "id"
	case AccountIndexEmail:
		return "email"
	case AccountIndexName:
		return "name"
	case AccountIndexBalance:
		return "balance"
	case AccountIndexTags:
		return "tags"
	case AccountIndexScores:
		return "scores"
	case AccountIndexMeta:
		return "meta"
	case AccountIndexSettings:
		return "settings"
	case AccountIndexItems:
		return "items"
	default:
		return ""
	}
}

// key index string
func (a AccountIndexTyped) SQLName() string {
	switch a {
	case AccountIndexID:
		return "id"
	case AccountIndexEmail:
		return "email"
	case AccountIndexName:
		return "name"
	case AccountIndexBalance:
		return "balance"
	case AccountIndexTags:
		return "tags"
	case AccountIndexScores:
		return "scores"
	case AccountIndexMeta:
		return "meta"
	case AccountIndexSettings:
		return "settings"
	case AccountIndexItems:
		return "items"
	default:
		return ""
	}
}

// key index clickhouse string
func (a AccountIndexTyped) ClickhouseName() string {
	switch a {
	case AccountIndexID:
		return "id"
	case AccountIndexEmail:
		return "email"
	case AccountIndexName:
		return "name"
	case AccountIndexBalance:
		return "balance"
	case AccountIndexTags:
		return "tags"
	case AccountIndexScores:
		return "scores"
	case AccountIndexMeta:
		return "meta"
	case AccountIndexSettings:
		return "settings"
	case AccountIndexItems:
		return "items"
	default:
		return ""
	}
}

// key index type
func (a AccountIndexTyped) Type() string {
	switch a {
	case AccountIndexID:
		return "int"
	case AccountIndexEmail:
		return "string"
	case AccountIndexName:
		return "string"
	case AccountIndexBalance:
		return "int64"
	case AccountIndexTags:
		return "[]string"
	case AccountIndexScores:
		return "[]int"
	case AccountIndexMeta:
		return "map[string]string"
	case AccountIndexSettings:
		return "map[string]int"
	case AccountIndexItems:
		return "[]Item"
	default:
		return ""
	}
}

// custom title
func (a AccountIndexTyped) Title() string {
	switch a {
	case AccountIndexID:
		return "ID"
	case AccountIndexEmail:
		return "Email"
	case AccountIndexName:
		return "Name"
	case AccountIndexBalance:
		return "Balance"
	case AccountIndexTags:
		return "Tags"
	case AccountIndexScores:
		return "Scores"
	case AccountIndexMeta:
		return "Meta"
	case AccountIndexSettings:
		return "Settings"
	case AccountIndexItems:
		return "Items"
	default:
		return ""
	}
}

// custom desc
func (a AccountIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func AccountKeyIndex(key string) AccountIndexTyped {
	switch key {
	case "id":
		return AccountIndexID
	case "email":
		return AccountIndexEmail
	case "name":
		return AccountIndexName
	case "balance":
		return AccountIndexBalance
	case "tags":
		return AccountIndexTags
	case "scores":
		return AccountIndexScores
	case "meta":
		return AccountIndexMeta
	case "settings":
		return AccountIndexSettings
	case "items":
		return AccountIndexItems
	default:
		return 0
	}
}

// valid struct key check
func AccountValidKey(key string) bool {
	switch key {
	case "id", "email", "name", "balance", "tags", "scores", "meta", "settings", "items":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Account) Map() map[string]any {
	return map[string]any{
		"id":       a.ID,
		"email":    a.Email,
		"name":     a.Name,
		"balance":  a.Balance,
		"tags":     a.Tags,
		"scores":   a.Scores,
		"meta":     a.Meta,
		"settings": a.Settings,
		"items":    a.Items,
	}
}

// struct to map
func (a *Account) Iterate(f func(k AccountIndexTyped, v any)) {
	for _, x := range AccountIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Account) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseAccount(v []byte) (a Account, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// accountSQLDB runs queries: pool or transaction
type accountSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// AccountSQL sql queries of table account
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type AccountSQL struct {
	pool *pgxpool.Pool
	db   accountSQLDB // pool, or transaction after TX
}

// NewAccountSQL creates queries on pool, table and indexes if not exist
func NewAccountSQL(pool *pgxpool.Pool) (*AccountSQL, error) {
	a := &AccountSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err != nil {
		return nil, err
	}
	return a, a.CreateIndexes()
}

// Pool returns pool
func (a *AccountSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of AccountSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum account")
//		return err
//	})
func (a *AccountSQL) Conn(f func(conn *pgxpool.Conn) error) error {
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
func (a *AccountSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *AccountSQL) TX(tx pgx.Tx) *AccountSQL {
	return &AccountSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *AccountSQL) TableName() string {
	return "account"
}

// Fields returns all sql columns
func (a *AccountSQL) Fields() []AccountIndexTyped {
	return slices.Clone(accountSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var accountSQLFields = []AccountIndexTyped{AccountIndexID, AccountIndexEmail, AccountIndexName, AccountIndexBalance, AccountIndexTags, AccountIndexScores, AccountIndexMeta, AccountIndexSettings, AccountIndexItems}

// columns of insert
var accountSQLInsertColumns = []string{"email", "name", "balance", "tags", "scores", "meta", "settings", "items"}

// sql column names
var accountSQLColumns = [...]string{
	AccountIndexID:       "id",
	AccountIndexEmail:    "email",
	AccountIndexName:     "name",
	AccountIndexBalance:  "balance",
	AccountIndexTags:     "tags",
	AccountIndexScores:   "scores",
	AccountIndexMeta:     "meta",
	AccountIndexSettings: "settings",
	AccountIndexItems:    "items",
}

// select expressions: null of scalar column is zero value
var accountSQLSelects = [...]string{
	AccountIndexID:       "coalesce(id, 0)",
	AccountIndexEmail:    "coalesce(email, '')",
	AccountIndexName:     "coalesce(name, '')",
	AccountIndexBalance:  "coalesce(balance, 0)",
	AccountIndexTags:     "tags",
	AccountIndexScores:   "scores",
	AccountIndexMeta:     "meta",
	AccountIndexSettings: "settings",
	AccountIndexItems:    "items",
}

// sql column types
var accountSQLTypes = [...]string{
	AccountIndexID:       "bigint",
	AccountIndexEmail:    "text",
	AccountIndexName:     "text",
	AccountIndexBalance:  "bigint",
	AccountIndexTags:     "text[]",
	AccountIndexScores:   "bigint[]",
	AccountIndexMeta:     "jsonb",
	AccountIndexSettings: "jsonb",
	AccountIndexItems:    "jsonb",
}

const accountSQLSelectAll = "coalesce(id, 0), coalesce(email, ''), coalesce(name, ''), coalesce(balance, 0), tags, scores, meta, settings, items"

// accountSQLColumn returns sql column name of field
func accountSQLColumn(f AccountIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(accountSQLColumns) || accountSQLColumns[f] == "" {
		return "", fmt.Errorf("account: field %d is not a sql column", int(f))
	}
	return accountSQLColumns[f], nil
}

// accountSQLSelect returns select list of fields, all columns when empty
func accountSQLSelect(fields []AccountIndexTyped) (string, []AccountIndexTyped, error) {
	if len(fields) == 0 {
		return accountSQLSelectAll, accountSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := accountSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(accountSQLSelects[f])
	}
	return b.String(), fields, nil
}

// accountSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func accountSQLSet(v map[AccountIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("account: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := accountSQLColumn(f)
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

// accountSQLAffected returns pgx.ErrNoRows when nothing was changed
func accountSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// accountSQLScanBytes scans bytea to byte array
type accountSQLScanBytes func(b []byte)

func (f accountSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *Account) sqlDest(fields []AccountIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case AccountIndexID:
			dest[i] = &a.ID
		case AccountIndexEmail:
			dest[i] = &a.Email
		case AccountIndexName:
			dest[i] = &a.Name
		case AccountIndexBalance:
			dest[i] = &a.Balance
		case AccountIndexTags:
			dest[i] = &a.Tags
		case AccountIndexScores:
			dest[i] = &a.Scores
		case AccountIndexMeta:
			dest[i] = &a.Meta
		case AccountIndexSettings:
			dest[i] = &a.Settings
		case AccountIndexItems:
			dest[i] = &a.Items
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *Account) sqlValue(f AccountIndexTyped) any {
	switch f {
	case AccountIndexID:
		return a.ID
	case AccountIndexEmail:
		return a.Email
	case AccountIndexName:
		return a.Name
	case AccountIndexBalance:
		return a.Balance
	case AccountIndexTags:
		return a.Tags
	case AccountIndexScores:
		return a.Scores
	case AccountIndexMeta:
		return a.Meta
	case AccountIndexSettings:
		return a.Settings
	case AccountIndexItems:
		return a.Items
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *Account) sqlInsertArgs() []any {
	return []any{
		a.Email,
		a.Name,
		a.Balance,
		a.Tags,
		a.Scores,
		a.Meta,
		a.Settings,
		a.Items,
	}
}

// sqlAllArgs returns values of all columns
func (a *Account) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.Email,
		a.Name,
		a.Balance,
		a.Tags,
		a.Scores,
		a.Meta,
		a.Settings,
		a.Items,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *AccountSQL) query(fields []AccountIndexTyped, tail string, args ...any) ([]Account, error) {
	cols, fields, err := accountSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from account"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v Account, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *AccountSQL) queryRow(fields []AccountIndexTyped, tail string, args ...any) (v Account, err error) {
	cols, fields, err := accountSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from account"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by id
func (a *AccountSQL) Get(id int, fields ...AccountIndexTyped) (Account, error) {
	return a.queryRow(fields, " where id = $1", id)
}

// Exists checks row by id
func (a *AccountSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from account where id = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *AccountSQL) GetBy(field AccountIndexTyped, v any, fields ...AccountIndexTyped) (res Account, err error) {
	col, err := accountSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *AccountSQL) GetWhere(fields []AccountIndexTyped, where string, args ...any) (Account, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *AccountSQL) Has(field AccountIndexTyped, v any) (has bool, err error) {
	col, err := accountSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from account where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *AccountSQL) All(fields ...AccountIndexTyped) ([]Account, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *AccountSQL) List(limit, offset int, fields ...AccountIndexTyped) ([]Account, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *AccountSQL) Select(fields []AccountIndexTyped, where string, args ...any) ([]Account, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *AccountSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from account").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *AccountSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from account where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const accountSQLInsert = "insert into account (email, name, balance, tags, scores, meta, settings, items) values ($1, $2, $3, $4, $5, $6, $7, $8)"

const accountSQLInsertFull = "insert into account (id, email, name, balance, tags, scores, meta, settings, items) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

// Insert adds row and sets generated v.ID
func (a *AccountSQL) Insert(v *Account) error {
	return a.db.QueryRow(context.Background(), accountSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *AccountSQL) InsertNoConflict(v *Account) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), accountSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *AccountSQL) InsertFull(v *Account) error {
	_, err := a.db.Exec(context.Background(), accountSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *AccountSQL) Upsert(v *Account) error {
	_, err := a.db.Exec(context.Background(), "insert into account (id, email, name, balance, tags, scores, meta, settings, items) values ($1, $2, $3, $4, $5, $6, $7, $8, $9) on conflict (id) do update set email = excluded.email, name = excluded.name, balance = excluded.balance, tags = excluded.tags, scores = excluded.scores, meta = excluded.meta, settings = excluded.settings, items = excluded.items", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *AccountSQL) InsertMany(list []Account) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"account"}, accountSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *AccountSQL) InsertBatch(list []*Account) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(accountSQLInsert+" returning id", v.sqlInsertArgs()...)
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
func (a *AccountSQL) Update(id int, field AccountIndexTyped, v any) error {
	col, err := accountSQLColumn(field)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = $1 where id = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *AccountSQL) Updates(id int, v map[AccountIndexTyped]any) error {
	set, args, err := accountSQLSet(v, 1)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *AccountSQL) Save(v *Account, fields ...AccountIndexTyped) error {
	if len(fields) == 0 {
		fields = accountSQLFields
	}
	values := make(map[AccountIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != AccountIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := accountSQLSet(values, 1)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(AccountIndexID))...))
}

// Inc adds v to number field: views = views + 1
func (a *AccountSQL) Inc(id int, field AccountIndexTyped, v any) error {
	col, err := accountSQLColumn(field)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

// Delete removes row by id
func (a *AccountSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from account where id = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *AccountSQL) UpdateWhere(field AccountIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := accountSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update account set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *AccountSQL) UpdatesWhere(v map[AccountIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := accountSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update account set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *AccountSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from account where "+where, args...)
	return tag.RowsAffected(), err
}

// TagsAppend adds values to array tags
func (a *AccountSQL) TagsAppend(id int, v ...string) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set tags = coalesce(tags, '{}') || $1::text[] where id = $2", v, id))
}

// TagsRemove removes all entries of v from array tags
func (a *AccountSQL) TagsRemove(id int, v string) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set tags = array_remove(tags, $1::text) where id = $2", v, id))
}

// ScoresAppend adds values to array scores
func (a *AccountSQL) ScoresAppend(id int, v ...int) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set scores = coalesce(scores, '{}') || $1::bigint[] where id = $2", v, id))
}

// ScoresRemove removes all entries of v from array scores
func (a *AccountSQL) ScoresRemove(id int, v int) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set scores = array_remove(scores, $1::bigint) where id = $2", v, id))
}

// MetaUpdate sets key of jsonb meta
func (a *AccountSQL) MetaUpdate(id int, k string, v string) error {
	return a.jsonSet(id, "meta", fmt.Sprint(k), v)
}

// MetaMerge adds keys to jsonb meta: existing keys are replaced
func (a *AccountSQL) MetaMerge(id int, v map[string]string) error {
	return a.jsonMerge(id, "meta", "{}", v)
}

// MetaDelete removes key of jsonb meta
func (a *AccountSQL) MetaDelete(id int, k string) error {
	return a.jsonDelete(id, "meta", fmt.Sprint(k))
}

// MetaRename renames key of jsonb meta, pgx.ErrNoRows when key not found
func (a *AccountSQL) MetaRename(id int, k, newK string) error {
	return a.jsonRename(id, "meta", fmt.Sprint(k), fmt.Sprint(newK))
}

// SettingsUpdate sets key of jsonb settings
func (a *AccountSQL) SettingsUpdate(id int, k string, v int) error {
	return a.jsonSet(id, "settings", fmt.Sprint(k), v)
}

// SettingsMerge adds keys to jsonb settings: existing keys are replaced
func (a *AccountSQL) SettingsMerge(id int, v map[string]int) error {
	return a.jsonMerge(id, "settings", "{}", v)
}

// SettingsDelete removes key of jsonb settings
func (a *AccountSQL) SettingsDelete(id int, k string) error {
	return a.jsonDelete(id, "settings", fmt.Sprint(k))
}

// SettingsRename renames key of jsonb settings, pgx.ErrNoRows when key not found
func (a *AccountSQL) SettingsRename(id int, k, newK string) error {
	return a.jsonRename(id, "settings", fmt.Sprint(k), fmt.Sprint(newK))
}

// ItemsAppend adds values to jsonb array items
func (a *AccountSQL) ItemsAppend(id int, v ...Item) error {
	return a.jsonMerge(id, "items", "[]", v)
}

// jsonSet sets key of jsonb column
func (a *AccountSQL) jsonSet(id int, col, k string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = jsonb_set(coalesce("+col+", '{}'::jsonb), array[$1::text], $2::jsonb, true) where id = $3", k, b, id))
}

// jsonMerge concats json to jsonb column: object keys or array values
func (a *AccountSQL) jsonMerge(id int, col, empty string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = coalesce("+col+", '"+empty+"'::jsonb) || $1::jsonb where id = $2", b, id))
}

// jsonDelete removes key of jsonb column
func (a *AccountSQL) jsonDelete(id int, col, k string) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = "+col+" - $1::text where id = $2", k, id))
}

// jsonRename renames key of jsonb column
func (a *AccountSQL) jsonRename(id int, col, k, newK string) error {
	return accountSQLAffected(a.db.Exec(context.Background(), "update account set "+col+" = ("+col+" - $1::text) || jsonb_build_object($2::text, "+col+" -> $1::text) where id = $3 and "+col+" ? $1::text", k, newK, id))
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

// Rich: where balance > @min order by balance desc
func (a *AccountSQL) Rich(min int64) (res []Account, err error) {
	return a.query(nil, " where balance > $1 order by balance desc", min)
}

// ByEmail: where email = @email
func (a *AccountSQL) ByEmail(email string) (res Account, err error) {
	res, err = a.queryRow(nil, " where email = $1", email)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}

// Balance: select balance from account where id = @id
func (a *AccountSQL) Balance(id int) (res int64, err error) {
	err = a.db.QueryRow(context.Background(), "select balance from account where id = $1", id).Scan(&res)
	return
}

// IDs: select id from account where name = any(@names)
func (a *AccountSQL) IDs(names []string) (res []int, err error) {
	rows, err := a.db.Query(context.Background(), "select id from account where name = any($1)", names)
	if err != nil {
		return
	}
	return pgx.CollectRows(rows, pgx.RowTo[int])
}

// Reset: update account set balance = 0 where balance < @min
func (a *AccountSQL) Reset(min int64) (res int64, err error) {
	tag, err := a.db.Exec(context.Background(), "update account set balance = 0 where balance < $1", min)
	return tag.RowsAffected(), err
}

// Drop: delete from account where id = @id
func (a *AccountSQL) Drop(id int) (err error) {
	_, err = a.db.Exec(context.Background(), "delete from account where id = $1", id)
	return
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// AccountQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type AccountQuery struct {
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

// NewAccountQuery creates query with empty maps
func NewAccountQuery() *AccountQuery {
	return &AccountQuery{
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
func (q *AccountQuery) column(k string) (string, AccountIndexTyped, error) {
	if !AccountValidKey(k) {
		return "", 0, fmt.Errorf("account: unknown field %q", k)
	}
	f := AccountKeyIndex(k)
	col, err := accountSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *AccountQuery) Render() (tail string, fields []AccountIndexTyped, args []any, err error) {

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
func (a *AccountSQL) Search(q *AccountQuery) ([]Account, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const accountSQLCreateTable = `create table if not exists account (
	id           bigint generated by default as identity primary key,
	email        text,
	name         text,
	balance      bigint,
	tags         text[] default '{}',
	scores       bigint[] default '{}',
	meta         jsonb default '{}'::jsonb,
	settings     jsonb default '{}'::jsonb,
	items        jsonb default '[]'::jsonb,
	unique (email)
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var accountSQLIndexes = []string{
	"create index if not exists idx_account_name on account (name)",
}

// CreateTable creates table if not exists
func (a *AccountSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), accountSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *AccountSQL) CreateIndexes() error {
	for _, q := range accountSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *AccountSQL) ResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('account', 'id'), coalesce((select max(id) from account), 0) + $1, false)", plus)
	return err
}

// SetSequence sets next id
func (a *AccountSQL) SetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('account', 'id'), $1, false)", v)
	return err
}
