package examples

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
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

// field type
type EventIndexTyped int

// int index
const (
	EventIndexID = EventIndexTyped(iota)
	EventIndexCountry
	EventIndexIP
	EventIndexPath
)

// string index
const (
	EventFieldID      = "id"      // uint64 ch{primary}
	EventFieldCountry = "country" // string ch{low}
	EventFieldIP      = "ip"      // string ch{type=ip}
	EventFieldPath    = "path"    // string
)

// index func
func EventIndexes() []EventIndexTyped {
	return []EventIndexTyped{EventIndexID, EventIndexCountry, EventIndexIP, EventIndexPath}

}

// 56 bytes (go padding)
//
//easyjson:json
type Event struct {
	ID      uint64 `json:"id,omitempty"`      // ch{primary}
	Country string `json:"country,omitempty"` // ch{low}
	IP      string `json:"ip,omitempty"`      // ch{type=ip}
	Path    string `json:"path,omitempty"`    //
}

// Tuple create an array from struct
func (a *Event) Tuple() (r []any) {
	return []any{a.ID, a.Country, a.IP, a.Path}
}

// Tuple create an array from struct for clickhouse
func (a *Event) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Country, net.ParseIP(a.IP), a.Path}
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
	}
	return
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
	default:
		return 0
	}
}

// valid struct key check
func EventValidKey(key string) bool {
	switch key {
	case "id", "country", "ip", "path":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Event) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"country": a.Country,
		"ip":      a.IP,
		"path":    a.Path,
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

// clickhouse EventCQL class
type EventCQL struct {
	conn driver.Conn
	list []Event
	sync.Mutex
}

func NewEventCQL(conn driver.Conn) (a *EventCQL) {
	a = new(EventCQL)
	a.conn = conn
	a.CreateTable()
	go a.deamon()
	return
}

// parse clickhouse query
func (a *EventCQL) TableName() (res string) {
	return "events"
}

func (a *EventCQL) Count(where ...string) (count int, err error) {

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	var q string
	switch len(where) {
	case 0:
		q = "select count() from events"
	default:
		q = "select count() from events where " + strings.Join(where, " ")
	}
	var n uint64
	err = a.conn.QueryRow(context.Background(), q).Scan(&n)
	count = int(n)
	return
}

// Add items to clickhouse queue
func (a *EventCQL) Add(items ...Event) {
	a.Lock()
	defer a.Unlock()
	a.list = append(a.list, items...)
}

// Push queued items to clickhouse
func (a *EventCQL) Push() (err error) {
	a.Lock()
	defer a.Unlock()
	if len(a.list) == 0 {
		return
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into events (id, country, ip, path)")
	if err != nil {
		return
	}
	for _, item := range a.list {
		err = batch.Append(item.clickhouseTuple()...)
		if err != nil {
			_ = batch.Abort()
			return
		}
	}
	err = batch.Send()
	if err != nil {
		_ = batch.Abort()
		return
	}
	a.list = a.list[:0]
	return
}

// deamon pushes queued items every 10 minutes
func (a *EventCQL) deamon() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		a.Lock()
		hasItems := len(a.list) > 0
		a.Unlock()
		if hasItems {
			_ = a.Push()
		}
	}
}

// parse clickhouse query
func (a *EventCQL) Get(id any, fields ...EventIndexTyped) (res *Event, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from events where id = ? limit 1", fieldlist)
	res = new(Event)

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	rows, err := a.conn.Query(context.Background(), q, id)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = errors.New("not found")
		return
	}
	// scan targets by column types: clickhouse needs exact types
	scan := make([]any, len(fields))
	for i, t := range rows.ColumnTypes() {
		scan[i] = reflect.New(t.ScanType()).Interface()
	}
	err = rows.Scan(scan...)
	if err != nil {
		return
	}
	for pos, x := range fields {
		res.clickhouseSet(x, scan[pos])
	}
	err = rows.Err()

	return
}

// parse clickhouse query
func (a *EventCQL) GetWhere(where string, fields ...EventIndexTyped) (res *Event, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from events where %s limit 1", fieldlist, where)
	res = new(Event)

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	rows, err := a.conn.Query(context.Background(), q)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = errors.New("not found")
		return
	}
	// scan targets by column types: clickhouse needs exact types
	scan := make([]any, len(fields))
	for i, t := range rows.ColumnTypes() {
		scan[i] = reflect.New(t.ScanType()).Interface()
	}
	err = rows.Scan(scan...)
	if err != nil {
		return
	}
	for pos, x := range fields {
		res.clickhouseSet(x, scan[pos])
	}
	err = rows.Err()

	return
}

// update clickhouse query
func (a *EventCQL) Update(id any, k string, v any, where ...string) (err error) {
	if !EventValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := EventKeyIndex(k)
	q := fmt.Sprintf("alter table events update %s = ? where id = ?", in.ClickhouseName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, v, id)
}

// update clickhouse query
func (a *EventCQL) UpdateWhere(k string, v any, where string) (err error) {
	if !EventValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := EventKeyIndex(k)
	q := fmt.Sprintf("alter table %s update %s = ? where %s", a.TableName(), in.ClickhouseName(), where)
	return a.conn.Exec(context.Background(), q, v)
}

// update clickhouse query
func (a *EventCQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !EventValidKey(k) {
			return errors.New(k)
		}
		in := EventKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")
	values = append(values, id)

	q := fmt.Sprintf("alter table events update %s where id = ?", list)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, values...)
}

// update clickhouse query
func (a *EventCQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !EventValidKey(k) {
			return errors.New(k)
		}
		in := EventKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("alter table events update %s where %s", list, where)
	return a.conn.Exec(context.Background(), q, values...)
}

// select custom clickhouse query
func (a *EventCQL) Select(where string, fields ...EventIndexTyped) (res []*Event, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from events", fieldlist)
	if where != "" {
		q += " where " + where
	}

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	rows, err := a.conn.Query(context.Background(), q)
	if err != nil {
		return
	}
	defer rows.Close()
	// scan targets by column types: clickhouse needs exact types
	types := rows.ColumnTypes()
	for rows.Next() {
		var item Event
		scan := make([]any, len(types))
		for i, t := range types {
			scan[i] = reflect.New(t.ScanType()).Interface()
		}
		if err = rows.Scan(scan...); err != nil {
			return
		}
		for pos, x := range fields {
			item.clickhouseSet(x, scan[pos])
		}
		res = append(res, &item)
	}
	err = rows.Err()

	return
}

// delete clickhouse item
func (a *EventCQL) Delete(id any) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table events delete where id = ?"
	return a.conn.Exec(context.Background(), q, id)
}

// delete clickhouse items
func (a *EventCQL) DeleteWhere(where string) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table events delete where " + where
	return a.conn.Exec(context.Background(), q)
}

// has value in clickhouse
func (a *EventCQL) Has(field EventIndexTyped, v any) (has bool, err error) {
	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	q := fmt.Sprintf("select count() from events where %s = ? limit 1", field.ClickhouseName())
	var n uint64
	err = a.conn.QueryRow(context.Background(), q, v).Scan(&n)
	has = n > 0
	return
}

// stat by clickhouse field
func (a *EventCQL) Stat(field EventIndexTyped) (res map[string]int, err error) {
	res = make(map[string]int)
	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	name := field.ClickhouseName()
	q := fmt.Sprintf("select toString(%[1]s), count() from %[2]s group by %[1]s", name, a.TableName())
	rows, err := a.conn.Query(context.Background(), q)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var n uint64
		err = rows.Scan(&k, &n)
		if err != nil {
			return
		}
		res[k] = int(n)
	}
	err = rows.Err()
	return
}

// Create clickhouse table
func (a *EventCQL) CreateTable() (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := `create table if not exists events (
	id          UInt64,
	country     LowCardinality(String),
	ip          IPv4,
	path        String
)
engine = MergeTree()
order by (id)
primary key (id)`
	return a.conn.Exec(context.Background(), q)
}

// parse clickhouse query
func (a *EventCQL) Fields() (res []EventIndexTyped) {
	return []EventIndexTyped{EventIndexID, EventIndexCountry, EventIndexIP, EventIndexPath}
}

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

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}
