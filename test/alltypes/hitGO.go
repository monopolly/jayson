package alltypes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	gojson "github.com/goccy/go-json"
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

// init struct
func NewHit() (a *Hit) {
	a = new(Hit)
	a.Attrs = make(map[string]string)
	a.Counts = make(map[string]int)
	return
}

// 144 bytes (go padding)
type Hit struct {
	ID      int               `json:"id,omitempty"`      // ch{primary}
	User    int               `json:"user,omitempty"`    //
	Country string            `json:"country,omitempty"` // ch{low}
	IP      string            `json:"ip,omitempty"`      //
	Path    string            `json:"path,omitempty"`    //
	Ok      bool              `json:"ok,omitempty"`      //
	Views   int64             `json:"views,omitempty"`   //
	Created time.Time         `json:"created,omitempty"` //
	Tags    []string          `json:"tags,omitempty"`    //
	Codes   []int             `json:"codes,omitempty"`   //
	Sizes   []uint            `json:"sizes,omitempty"`   //
	Attrs   map[string]string `json:"attrs,omitempty"`   //
	Counts  map[string]int    `json:"counts,omitempty"`  //
	From    string            `json:"from,omitempty"`    // reserved word: column ifrom
}

// field type
type HitIndexTyped int

// int index
const (
	HitIndexID = HitIndexTyped(iota)
	HitIndexUser
	HitIndexCountry
	HitIndexIP
	HitIndexPath
	HitIndexOk
	HitIndexViews
	HitIndexCreated
	HitIndexTags
	HitIndexCodes
	HitIndexSizes
	HitIndexAttrs
	HitIndexCounts
	HitIndexFrom
)

// string index
const (
	HitFieldID      = "id"      // int ch{primary}
	HitFieldUser    = "user"    // int
	HitFieldCountry = "country" // string ch{low}
	HitFieldIP      = "ip"      // string
	HitFieldPath    = "path"    // string
	HitFieldOk      = "ok"      // bool
	HitFieldViews   = "views"   // int64
	HitFieldCreated = "created" // time.Time
	HitFieldTags    = "tags"    // []string
	HitFieldCodes   = "codes"   // []int
	HitFieldSizes   = "sizes"   // []uint
	HitFieldAttrs   = "attrs"   // map[string]string
	HitFieldCounts  = "counts"  // map[string]int
	HitFieldFrom    = "from"    // string reserved word: column ifrom
)

// index func
func HitIndexes() []HitIndexTyped {
	return []HitIndexTyped{HitIndexID, HitIndexUser, HitIndexCountry, HitIndexIP, HitIndexPath, HitIndexOk, HitIndexViews, HitIndexCreated, HitIndexTags, HitIndexCodes, HitIndexSizes, HitIndexAttrs, HitIndexCounts, HitIndexFrom}

}

// key index string
func (a HitIndexTyped) String() string {
	switch a {
	case HitIndexID:
		return "id"
	case HitIndexUser:
		return "user"
	case HitIndexCountry:
		return "country"
	case HitIndexIP:
		return "ip"
	case HitIndexPath:
		return "path"
	case HitIndexOk:
		return "ok"
	case HitIndexViews:
		return "views"
	case HitIndexCreated:
		return "created"
	case HitIndexTags:
		return "tags"
	case HitIndexCodes:
		return "codes"
	case HitIndexSizes:
		return "sizes"
	case HitIndexAttrs:
		return "attrs"
	case HitIndexCounts:
		return "counts"
	case HitIndexFrom:
		return "from"
	default:
		return ""
	}
}

// key index string
func (a HitIndexTyped) SQLName() string {
	switch a {
	case HitIndexID:
		return "id"
	case HitIndexUser:
		return "user"
	case HitIndexCountry:
		return "country"
	case HitIndexIP:
		return "ip"
	case HitIndexPath:
		return "path"
	case HitIndexOk:
		return "ok"
	case HitIndexViews:
		return "views"
	case HitIndexCreated:
		return "created"
	case HitIndexTags:
		return "tags"
	case HitIndexCodes:
		return "codes"
	case HitIndexSizes:
		return "sizes"
	case HitIndexAttrs:
		return "attrs"
	case HitIndexCounts:
		return "counts"
	case HitIndexFrom:
		return "from"
	default:
		return ""
	}
}

// key index clickhouse string
func (a HitIndexTyped) ClickhouseName() string {
	switch a {
	case HitIndexID:
		return "id"
	case HitIndexUser:
		return "iuser"
	case HitIndexCountry:
		return "country"
	case HitIndexIP:
		return "ip"
	case HitIndexPath:
		return "path"
	case HitIndexOk:
		return "ok"
	case HitIndexViews:
		return "views"
	case HitIndexCreated:
		return "created"
	case HitIndexTags:
		return "tags"
	case HitIndexCodes:
		return "codes"
	case HitIndexSizes:
		return "sizes"
	case HitIndexAttrs:
		return "attrs"
	case HitIndexCounts:
		return "counts"
	case HitIndexFrom:
		return "ifrom"
	default:
		return ""
	}
}

// key index type
func (a HitIndexTyped) Type() string {
	switch a {
	case HitIndexID:
		return "int"
	case HitIndexUser:
		return "int"
	case HitIndexCountry:
		return "string"
	case HitIndexIP:
		return "string"
	case HitIndexPath:
		return "string"
	case HitIndexOk:
		return "bool"
	case HitIndexViews:
		return "int64"
	case HitIndexCreated:
		return "time.Time"
	case HitIndexTags:
		return "[]string"
	case HitIndexCodes:
		return "[]int"
	case HitIndexSizes:
		return "[]uint"
	case HitIndexAttrs:
		return "map[string]string"
	case HitIndexCounts:
		return "map[string]int"
	case HitIndexFrom:
		return "string"
	default:
		return ""
	}
}

// custom title
func (a HitIndexTyped) Title() string {
	switch a {
	case HitIndexID:
		return "ID"
	case HitIndexUser:
		return "User"
	case HitIndexCountry:
		return "Country"
	case HitIndexIP:
		return "IP"
	case HitIndexPath:
		return "Path"
	case HitIndexOk:
		return "Ok"
	case HitIndexViews:
		return "Views"
	case HitIndexCreated:
		return "Created"
	case HitIndexTags:
		return "Tags"
	case HitIndexCodes:
		return "Codes"
	case HitIndexSizes:
		return "Sizes"
	case HitIndexAttrs:
		return "Attrs"
	case HitIndexCounts:
		return "Counts"
	case HitIndexFrom:
		return "From"
	default:
		return ""
	}
}

// custom desc
func (a HitIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func HitKeyIndex(key string) HitIndexTyped {
	switch key {
	case "id":
		return HitIndexID
	case "user":
		return HitIndexUser
	case "country":
		return HitIndexCountry
	case "ip":
		return HitIndexIP
	case "path":
		return HitIndexPath
	case "ok":
		return HitIndexOk
	case "views":
		return HitIndexViews
	case "created":
		return HitIndexCreated
	case "tags":
		return HitIndexTags
	case "codes":
		return HitIndexCodes
	case "sizes":
		return HitIndexSizes
	case "attrs":
		return HitIndexAttrs
	case "counts":
		return HitIndexCounts
	case "from":
		return HitIndexFrom
	default:
		return 0
	}
}

// valid struct key check
func HitValidKey(key string) bool {
	switch key {
	case "id", "user", "country", "ip", "path", "ok", "views", "created", "tags", "codes", "sizes", "attrs", "counts", "from":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Hit) Tuple() (r []any) {
	return []any{a.ID, a.User, a.Country, a.IP, a.Path, a.Ok, a.Views, a.Created, a.Tags, a.Codes, a.Sizes, a.Attrs, a.Counts, a.From}
}

// get struct value with function
func (a *Hit) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "user":
		return a.User //int
	case "country":
		return a.Country //string
	case "ip":
		return a.IP //string
	case "path":
		return a.Path //string
	case "ok":
		return a.Ok //bool
	case "views":
		return a.Views //int64
	case "created":
		return a.Created //time.Time
	case "tags":
		return a.Tags //[]string
	case "codes":
		return a.Codes //[]int
	case "sizes":
		return a.Sizes //[]uint
	case "attrs":
		return a.Attrs //map[string]string
	case "counts":
		return a.Counts //map[string]int
	case "from":
		return a.From //string
	}
	return
}

// get any struct value as string
func (a *Hit) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "user":
		return fmt.Sprint(a.User) //int
	case "country":
		return fmt.Sprint(a.Country) //string
	case "ip":
		return fmt.Sprint(a.IP) //string
	case "path":
		return fmt.Sprint(a.Path) //string
	case "ok":
		return fmt.Sprint(a.Ok) //bool
	case "views":
		return fmt.Sprint(a.Views) //int64
	case "created":
		return fmt.Sprint(a.Created) //time.Time
	case "tags":
		return fmt.Sprint(a.Tags) //[]string
	case "codes":
		return fmt.Sprint(a.Codes) //[]int
	case "sizes":
		return fmt.Sprint(a.Sizes) //[]uint
	case "attrs":
		return fmt.Sprint(a.Attrs) //map[string]string
	case "counts":
		return fmt.Sprint(a.Counts) //map[string]int
	case "from":
		return fmt.Sprint(a.From) //string
	}
	return
}

// struct to map
func (a *Hit) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"user":    a.User,
		"country": a.Country,
		"ip":      a.IP,
		"path":    a.Path,
		"ok":      a.Ok,
		"views":   a.Views,
		"created": a.Created,
		"tags":    a.Tags,
		"codes":   a.Codes,
		"sizes":   a.Sizes,
		"attrs":   a.Attrs,
		"counts":  a.Counts,
		"from":    a.From,
	}
}

// struct to map
func (a *Hit) Iterate(f func(k HitIndexTyped, v any)) {
	for _, x := range HitIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Hit) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseHit(v []byte) (a Hit, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// Tuple create an array from struct for clickhouse
func (a *Hit) clickhouseTuple() (r []any) {
	return []any{a.ID, a.User, a.Country, net.ParseIP(a.IP), a.Path, a.Ok, a.Views, a.Created, a.Tags, a.Codes, a.Sizes, a.Attrs, func() map[string]int64 {
		r := make(map[string]int64, len(a.Counts))
		for k, v := range a.Counts {
			r[string(k)] = int64(v)
		}
		return r
	}(), a.From}
}

// HitCQL clickhouse queries of table hits
//
// Rules:
//   - safe for concurrent use
//   - not found row is sql.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "country = $1 and views > $2"
//   - rows are only inserted, selected and deleted
type HitCQL struct {
	conn driver.Conn
	mu   sync.Mutex
	list []Hit // buffer of Add
}

// NewHitCQL creates queries on connection and table if not exists.
// Rows of Add are written every 10 minutes and by Push.
// Error of table is not returned: call CreateTable to see it
func NewHitCQL(conn driver.Conn) *HitCQL {
	a := &HitCQL{conn: conn}
	_ = a.CreateTable()
	go a.flush()
	return a
}

// Conn returns connection, for own queries
func (a *HitCQL) Conn() driver.Conn {
	return a.conn
}

// TableName returns table name
func (a *HitCQL) TableName() string {
	return "hits"
}

// Fields returns all columns
func (a *HitCQL) Fields() []HitIndexTyped {
	return slices.Clone(hitCHFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all columns
var hitCHFields = []HitIndexTyped{HitIndexID, HitIndexUser, HitIndexCountry, HitIndexIP, HitIndexPath, HitIndexOk, HitIndexViews, HitIndexCreated, HitIndexTags, HitIndexCodes, HitIndexSizes, HitIndexAttrs, HitIndexCounts, HitIndexFrom}

// column names
var hitCHColumns = [...]string{
	HitIndexID:      "id",
	HitIndexUser:    "iuser",
	HitIndexCountry: "country",
	HitIndexIP:      "ip",
	HitIndexPath:    "path",
	HitIndexOk:      "ok",
	HitIndexViews:   "views",
	HitIndexCreated: "created",
	HitIndexTags:    "tags",
	HitIndexCodes:   "codes",
	HitIndexSizes:   "sizes",
	HitIndexAttrs:   "attrs",
	HitIndexCounts:  "counts",
	HitIndexFrom:    "ifrom",
}

const hitCHSelectAll = "id, iuser, country, ip, path, ok, views, created, tags, codes, sizes, attrs, counts, ifrom"

// mutations wait until the data is changed
const hitCHSync = " settings mutations_sync = 1"

// hitCHColumn returns column name of field
func hitCHColumn(f HitIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(hitCHColumns) || hitCHColumns[f] == "" {
		return "", fmt.Errorf("hits: field %d is not a clickhouse column", int(f))
	}
	return hitCHColumns[f], nil
}

// hitCHSelect returns select list of fields, all columns when empty
func hitCHSelect(fields []HitIndexTyped) (string, []HitIndexTyped, error) {
	if len(fields) == 0 {
		return hitCHSelectAll, hitCHFields, nil
	}
	list := make([]string, 0, len(fields))
	for _, f := range fields {
		col, err := hitCHColumn(f)
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
func (a *HitCQL) Add(v ...Hit) {
	a.mu.Lock()
	a.list = append(a.list, v...)
	a.mu.Unlock()
}

// Len returns count of rows in buffer
func (a *HitCQL) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.list)
}

// Push writes rows of buffer. Rows stay in buffer when the write fails
func (a *HitCQL) Push() error {
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
func (a *HitCQL) Insert(v ...Hit) error {
	if len(v) == 0 {
		return nil
	}
	return a.send(v)
}

// send writes rows by one batch
func (a *HitCQL) send(list []Hit) error {
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into hits (id, iuser, country, ip, path, ok, views, created, tags, codes, sizes, attrs, counts, ifrom)")
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
func (a *HitCQL) flush() {
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
func (a *HitCQL) query(fields []HitIndexTyped, tail string, args ...any) (res []Hit, err error) {
	cols, fields, err := hitCHSelect(fields)
	if err != nil {
		return
	}
	rows, err := a.conn.Query(context.Background(), "select "+cols+" from hits"+tail, args...)
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
		var v Hit
		for i, f := range fields {
			v.clickhouseSet(f, scan[i])
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// queryRow selects one row
func (a *HitCQL) queryRow(fields []HitIndexTyped, tail string, args ...any) (v Hit, err error) {
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
func (a *HitCQL) Get(id int, fields ...HitIndexTyped) (Hit, error) {
	return a.queryRow(fields, " where id = $1 limit 1", id)
}

// Exists checks row by id
func (a *HitCQL) Exists(id int) (bool, error) {
	n, err := a.CountWhere("id = $1", id)
	return n > 0, err
}

// GetBy returns first row with field = v
func (a *HitCQL) GetBy(field HitIndexTyped, v any, fields ...HitIndexTyped) (res Hit, err error) {
	col, err := hitCHColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "country = $1 order by created desc", "DE")
func (a *HitCQL) GetWhere(fields []HitIndexTyped, where string, args ...any) (Hit, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *HitCQL) Has(field HitIndexTyped, v any) (bool, error) {
	col, err := hitCHColumn(field)
	if err != nil {
		return false, err
	}
	n, err := a.CountWhere(col+" = $1", v)
	return n > 0, err
}

// All returns all rows
func (a *HitCQL) All(fields ...HitIndexTyped) ([]Hit, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *HitCQL) List(limit, offset int, fields ...HitIndexTyped) ([]Hit, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *HitCQL) Select(fields []HitIndexTyped, where string, args ...any) ([]Hit, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *HitCQL) Count() (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from hits").Scan(&n)
	return int(n), err
}

// CountWhere returns count of rows by condition
func (a *HitCQL) CountWhere(where string, args ...any) (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from hits where "+where, args...).Scan(&n)
	return int(n), err
}

// Stat returns count of rows by values of field: {"DE": 425, "US": 134}
func (a *HitCQL) Stat(field HitIndexTyped) (map[string]int, error) {
	col, err := hitCHColumn(field)
	if err != nil {
		return nil, err
	}
	rows, err := a.conn.Query(context.Background(), "select toString("+col+"), count() from hits group by "+col)
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
func (a *HitCQL) Delete(id int) error {
	return a.conn.Exec(context.Background(), "alter table hits delete where id = $1"+hitCHSync, id)
}

// DeleteWhere removes rows by condition
func (a *HitCQL) DeleteWhere(where string, args ...any) error {
	return a.conn.Exec(context.Background(), "alter table hits delete where "+where+hitCHSync, args...)
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

// ByCountry: where country = @country order by id
func (a *HitCQL) ByCountry(country string) (res []Hit, err error) {
	return a.query(nil, " where country = $1 order by id", country)
}

// ByPath: where path = @path
func (a *HitCQL) ByPath(path string) (res Hit, err error) {
	res, err = a.queryRow(nil, " where path = $1", path)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

// Path: select path from hits where id = @id
func (a *HitCQL) Path(id int) (res string, err error) {
	err = a.conn.QueryRow(context.Background(), "select path from hits where id = $1", id).Scan(&res)
	return
}

// Views: select views from hits where id = @id
func (a *HitCQL) Views(id int) (res int64, err error) {
	err = a.conn.QueryRow(context.Background(), "select views from hits where id = $1", id).Scan(&res)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

// IDs: select id from hits where country = @country order by id
func (a *HitCQL) IDs(country string) (res []int64, err error) {
	rows, err := a.conn.Query(context.Background(), "select id from hits where country = $1 order by id", country)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var v int64
		if err = rows.Scan(&v); err != nil {
			return
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// Tags: select tags from hits where id = @id
func (a *HitCQL) Tags(id int) (res []string, err error) {
	err = a.conn.QueryRow(context.Background(), "select tags from hits where id = $1", id).Scan(&res)
	return
}

// DropCountry: alter table hits delete where country = @country settings mutations_sync = 1
func (a *HitCQL) DropCountry(country string) (err error) {
	return a.conn.Exec(context.Background(), "alter table hits delete where country = $1 settings mutations_sync = 1", country)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const hitCHCreateTable = `create table if not exists hits (
	id          Int64,
	iuser       Int64,
	country     LowCardinality(String),
	ip          IPv4,
	path        String,
	ok          Bool,
	views       Int64,
	created     DateTime64(3),
	tags        Array(String),
	codes       Array(Int64),
	sizes       Array(UInt64),
	attrs       Map(String, String),
	counts      Map(String, Int64),
	ifrom       String
)
engine = MergeTree()
order by (id)
primary key (id)`

// CreateTable creates table if not exists
func (a *HitCQL) CreateTable() error {
	return a.conn.Exec(context.Background(), hitCHCreateTable)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// clickhouseSet sets field from value of clickhouse row
func (a *Hit) clickhouseSet(f HitIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case HitIndexID:
		hitCHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case HitIndexUser:
		hitCHAssign(reflect.ValueOf(&a.User).Elem(), reflect.ValueOf(v))
	case HitIndexCountry:
		hitCHAssign(reflect.ValueOf(&a.Country).Elem(), reflect.ValueOf(v))
	case HitIndexIP:
		hitCHAssign(reflect.ValueOf(&a.IP).Elem(), reflect.ValueOf(v))
	case HitIndexPath:
		hitCHAssign(reflect.ValueOf(&a.Path).Elem(), reflect.ValueOf(v))
	case HitIndexOk:
		hitCHAssign(reflect.ValueOf(&a.Ok).Elem(), reflect.ValueOf(v))
	case HitIndexViews:
		hitCHAssign(reflect.ValueOf(&a.Views).Elem(), reflect.ValueOf(v))
	case HitIndexCreated:
		hitCHAssign(reflect.ValueOf(&a.Created).Elem(), reflect.ValueOf(v))
	case HitIndexTags:
		hitCHAssign(reflect.ValueOf(&a.Tags).Elem(), reflect.ValueOf(v))
	case HitIndexCodes:
		hitCHAssign(reflect.ValueOf(&a.Codes).Elem(), reflect.ValueOf(v))
	case HitIndexSizes:
		hitCHAssign(reflect.ValueOf(&a.Sizes).Elem(), reflect.ValueOf(v))
	case HitIndexAttrs:
		hitCHAssign(reflect.ValueOf(&a.Attrs).Elem(), reflect.ValueOf(v))
	case HitIndexCounts:
		hitCHAssign(reflect.ValueOf(&a.Counts).Elem(), reflect.ValueOf(v))
	case HitIndexFrom:
		hitCHAssign(reflect.ValueOf(&a.From).Elem(), reflect.ValueOf(v))
	}
}

// hitCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func hitCHAssign(d, s reflect.Value) {
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
		hitCHAssign(p.Elem(), s)
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
			hitCHAssign(r.Index(i), s.Index(i))
		}
		d.Set(r)

	// Map(String, Int64) > map[string]int
	case d.Kind() == reflect.Map && s.Kind() == reflect.Map:
		r := reflect.MakeMapWithSize(d.Type(), s.Len())
		for it := s.MapRange(); it.Next(); {
			k := reflect.New(d.Type().Key()).Elem()
			v := reflect.New(d.Type().Elem()).Elem()
			hitCHAssign(k, it.Key())
			hitCHAssign(v, it.Value())
			r.SetMapIndex(k, v)
		}
		d.Set(r)

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}
