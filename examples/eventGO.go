package examples

import (
	"context"
	"database/sql"
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

	CLICKHOUSE FUNCTIONS: methods of interface become methods of EventCQL
	type eventCQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (Event, error)

		// select role from events where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from events where id = @id
		Music(id int) ([]int, error)

		// select id from events where role = @role
		IDs(role role.Role) ([]int64, error)

		// alter table events delete where created < @before
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

// 64 bytes (go padding)
type Event struct {
	ID      uint64   `json:"id,omitempty"`      // ch{primary}
	Country string   `json:"country,omitempty"` // ch{low}
	IP      string   `json:"ip,omitempty"`      // ch{type=ip}
	Path    string   `json:"path,omitempty"`    //
	Tags    []string `json:"tags,omitempty"`    //
}

// field type
type EventIndexTyped int

// int index
const (
	EventIndexID = EventIndexTyped(iota)
	EventIndexCountry
	EventIndexIP
	EventIndexPath
	EventIndexTags
)

// string index
const (
	EventFieldID      = "id"      // uint64 ch{primary}
	EventFieldCountry = "country" // string ch{low}
	EventFieldIP      = "ip"      // string ch{type=ip}
	EventFieldPath    = "path"    // string
	EventFieldTags    = "tags"    // []string
)

// index func
func EventIndexes() []EventIndexTyped {
	return []EventIndexTyped{EventIndexID, EventIndexCountry, EventIndexIP, EventIndexPath, EventIndexTags}

}

// key index string
func (a EventIndexTyped) String() string {
	switch a {
	case EventIndexID:
		return "id"
	case EventIndexCountry:
		return "country"
	case EventIndexIP:
		return "ip"
	case EventIndexPath:
		return "path"
	case EventIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index string
func (a EventIndexTyped) SQLName() string {
	switch a {
	case EventIndexID:
		return "id"
	case EventIndexCountry:
		return "country"
	case EventIndexIP:
		return "ip"
	case EventIndexPath:
		return "path"
	case EventIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index clickhouse string
func (a EventIndexTyped) ClickhouseName() string {
	switch a {
	case EventIndexID:
		return "id"
	case EventIndexCountry:
		return "country"
	case EventIndexIP:
		return "ip"
	case EventIndexPath:
		return "path"
	case EventIndexTags:
		return "tags"
	default:
		return ""
	}
}

// key index type
func (a EventIndexTyped) Type() string {
	switch a {
	case EventIndexID:
		return "uint64"
	case EventIndexCountry:
		return "string"
	case EventIndexIP:
		return "string"
	case EventIndexPath:
		return "string"
	case EventIndexTags:
		return "[]string"
	default:
		return ""
	}
}

// custom title
func (a EventIndexTyped) Title() string {
	switch a {
	case EventIndexID:
		return "ID"
	case EventIndexCountry:
		return "Country"
	case EventIndexIP:
		return "IP"
	case EventIndexPath:
		return "Path"
	case EventIndexTags:
		return "Tags"
	default:
		return ""
	}
}

// custom desc
func (a EventIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func EventKeyIndex(key string) EventIndexTyped {
	switch key {
	case "id":
		return EventIndexID
	case "country":
		return EventIndexCountry
	case "ip":
		return EventIndexIP
	case "path":
		return EventIndexPath
	case "tags":
		return EventIndexTags
	default:
		return 0
	}
}

// valid struct key check
func EventValidKey(key string) bool {
	switch key {
	case "id", "country", "ip", "path", "tags":
		return true
	default:
		return false
	}
}

// Tuple create an array from struct
func (a *Event) Tuple() (r []any) {
	return []any{a.ID, a.Country, a.IP, a.Path, a.Tags}
}

// get struct value with function
func (a *Event) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //uint64
	case "country":
		return a.Country //string
	case "ip":
		return a.IP //string
	case "path":
		return a.Path //string
	case "tags":
		return a.Tags //[]string
	}
	return
}

// get any struct value as string
func (a *Event) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //uint64
	case "country":
		return fmt.Sprint(a.Country) //string
	case "ip":
		return fmt.Sprint(a.IP) //string
	case "path":
		return fmt.Sprint(a.Path) //string
	case "tags":
		return fmt.Sprint(a.Tags) //[]string
	}
	return
}

// struct to map
func (a *Event) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"country": a.Country,
		"ip":      a.IP,
		"path":    a.Path,
		"tags":    a.Tags,
	}
}

// struct to map
func (a *Event) Iterate(f func(k EventIndexTyped, v any)) {
	for _, x := range EventIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Event) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseEvent(v []byte) (a Event, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// Tuple create an array from struct for clickhouse
func (a *Event) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Country, net.ParseIP(a.IP), a.Path, a.Tags}
}

// EventCQL clickhouse queries of table events
//
// Rules:
//   - safe for concurrent use
//   - not found row is sql.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "country = $1 and views > $2"
//   - rows are only inserted, selected and deleted
type EventCQL struct {
	conn driver.Conn
	mu   sync.Mutex
	list []Event // buffer of Add
}

// NewEventCQL creates queries on connection and table if not exists.
// Rows of Add are written every 10 minutes and by Push.
// Error of table is not returned: call CreateTable to see it
func NewEventCQL(conn driver.Conn) *EventCQL {
	a := &EventCQL{conn: conn}
	_ = a.CreateTable()
	go a.flush()
	return a
}

// Conn returns connection, for own queries
func (a *EventCQL) Conn() driver.Conn {
	return a.conn
}

// TableName returns table name
func (a *EventCQL) TableName() string {
	return "events"
}

// Fields returns all columns
func (a *EventCQL) Fields() []EventIndexTyped {
	return slices.Clone(eventCHFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all columns
var eventCHFields = []EventIndexTyped{EventIndexID, EventIndexCountry, EventIndexIP, EventIndexPath, EventIndexTags}

// column names
var eventCHColumns = [...]string{
	EventIndexID:      "id",
	EventIndexCountry: "country",
	EventIndexIP:      "ip",
	EventIndexPath:    "path",
	EventIndexTags:    "tags",
}

const eventCHSelectAll = "id, country, ip, path, tags"

// mutations wait until the data is changed
const eventCHSync = " settings mutations_sync = 1"

// eventCHColumn returns column name of field
func eventCHColumn(f EventIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(eventCHColumns) || eventCHColumns[f] == "" {
		return "", fmt.Errorf("events: field %d is not a clickhouse column", int(f))
	}
	return eventCHColumns[f], nil
}

// eventCHSelect returns select list of fields, all columns when empty
func eventCHSelect(fields []EventIndexTyped) (string, []EventIndexTyped, error) {
	if len(fields) == 0 {
		return eventCHSelectAll, eventCHFields, nil
	}
	list := make([]string, 0, len(fields))
	for _, f := range fields {
		col, err := eventCHColumn(f)
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
func (a *EventCQL) Add(v ...Event) {
	a.mu.Lock()
	a.list = append(a.list, v...)
	a.mu.Unlock()
}

// Len returns count of rows in buffer
func (a *EventCQL) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.list)
}

// Push writes rows of buffer. Rows stay in buffer when the write fails
func (a *EventCQL) Push() error {
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
func (a *EventCQL) Insert(v ...Event) error {
	if len(v) == 0 {
		return nil
	}
	return a.send(v)
}

// send writes rows by one batch
func (a *EventCQL) send(list []Event) error {
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into events (id, country, ip, path, tags)")
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
func (a *EventCQL) flush() {
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
func (a *EventCQL) query(fields []EventIndexTyped, tail string, args ...any) (res []Event, err error) {
	cols, fields, err := eventCHSelect(fields)
	if err != nil {
		return
	}
	rows, err := a.conn.Query(context.Background(), "select "+cols+" from events"+tail, args...)
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
		var v Event
		for i, f := range fields {
			v.clickhouseSet(f, scan[i])
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// queryRow selects one row
func (a *EventCQL) queryRow(fields []EventIndexTyped, tail string, args ...any) (v Event, err error) {
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
func (a *EventCQL) Get(id uint64, fields ...EventIndexTyped) (Event, error) {
	return a.queryRow(fields, " where id = $1 limit 1", id)
}

// Exists checks row by id
func (a *EventCQL) Exists(id uint64) (bool, error) {
	n, err := a.CountWhere("id = $1", id)
	return n > 0, err
}

// GetBy returns first row with field = v
func (a *EventCQL) GetBy(field EventIndexTyped, v any, fields ...EventIndexTyped) (res Event, err error) {
	col, err := eventCHColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "country = $1 order by created desc", "DE")
func (a *EventCQL) GetWhere(fields []EventIndexTyped, where string, args ...any) (Event, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *EventCQL) Has(field EventIndexTyped, v any) (bool, error) {
	col, err := eventCHColumn(field)
	if err != nil {
		return false, err
	}
	n, err := a.CountWhere(col+" = $1", v)
	return n > 0, err
}

// All returns all rows
func (a *EventCQL) All(fields ...EventIndexTyped) ([]Event, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *EventCQL) List(limit, offset int, fields ...EventIndexTyped) ([]Event, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *EventCQL) Select(fields []EventIndexTyped, where string, args ...any) ([]Event, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *EventCQL) Count() (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from events").Scan(&n)
	return int(n), err
}

// CountWhere returns count of rows by condition
func (a *EventCQL) CountWhere(where string, args ...any) (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from events where "+where, args...).Scan(&n)
	return int(n), err
}

// Stat returns count of rows by values of field: {"DE": 425, "US": 134}
func (a *EventCQL) Stat(field EventIndexTyped) (map[string]int, error) {
	col, err := eventCHColumn(field)
	if err != nil {
		return nil, err
	}
	rows, err := a.conn.Query(context.Background(), "select toString("+col+"), count() from events group by "+col)
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
func (a *EventCQL) Delete(id uint64) error {
	return a.conn.Exec(context.Background(), "alter table events delete where id = $1"+eventCHSync, id)
}

// DeleteWhere removes rows by condition
func (a *EventCQL) DeleteWhere(where string, args ...any) error {
	return a.conn.Exec(context.Background(), "alter table events delete where "+where+eventCHSync, args...)
}

// ---------------------------------------------------------------------------
// own functions from interface of model file
// ---------------------------------------------------------------------------

// Top: where country = @country order by id desc limit 10
func (a *EventCQL) Top(country string) (res []Event, err error) {
	return a.query(nil, " where country = $1 order by id desc limit 10", country)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const eventCHCreateTable = `create table if not exists events (
	id          UInt64,
	country     LowCardinality(String),
	ip          IPv4,
	path        String,
	tags        Array(String)
)
engine = MergeTree()
order by (id)
primary key (id)`

// CreateTable creates table if not exists
func (a *EventCQL) CreateTable() error {
	return a.conn.Exec(context.Background(), eventCHCreateTable)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// clickhouseSet sets field from value of clickhouse row
func (a *Event) clickhouseSet(f EventIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case EventIndexID:
		eventCHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case EventIndexCountry:
		eventCHAssign(reflect.ValueOf(&a.Country).Elem(), reflect.ValueOf(v))
	case EventIndexIP:
		eventCHAssign(reflect.ValueOf(&a.IP).Elem(), reflect.ValueOf(v))
	case EventIndexPath:
		eventCHAssign(reflect.ValueOf(&a.Path).Elem(), reflect.ValueOf(v))
	case EventIndexTags:
		eventCHAssign(reflect.ValueOf(&a.Tags).Elem(), reflect.ValueOf(v))
	}
}

// eventCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func eventCHAssign(d, s reflect.Value) {
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
		eventCHAssign(p.Elem(), s)
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
			eventCHAssign(r.Index(i), s.Index(i))
		}
		d.Set(r)

	// Map(String, Int64) > map[string]int
	case d.Kind() == reflect.Map && s.Kind() == reflect.Map:
		r := reflect.MakeMapWithSize(d.Type(), s.Len())
		for it := s.MapRange(); it.Next(); {
			k := reflect.New(d.Type().Key()).Elem()
			v := reflect.New(d.Type().Elem()).Elem()
			eventCHAssign(k, it.Key())
			eventCHAssign(v, it.Value())
			r.SetMapIndex(k, v)
		}
		d.Set(r)

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}
