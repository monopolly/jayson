package news

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
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	jsoniter "github.com/json-iterator/go"
	"github.com/niubaoshu/gotiny"
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
	go=News                       Set golang struct names. Ex: go=News1 > type News1 struct{}
	gotiny                        Create gotiny marshal/unmarshal
	lock                          Lock model for generation. Can't change model.
	msgp                          Create message pack marshal/unmarshal
	noinit                        No New() init function for struct
	noprefix                      Generate simple index IndexID instead IndexNewsID
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
	nofunc                        Do not create any jsons functions for fiels
	req{}                         Add user required fields
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
type Stat2IndexTyped int

// int index
const (
	Stat2IndexID = Stat2IndexTyped(iota)
	Stat2IndexUID
	Stat2IndexType
	Stat2IndexName
	Stat2IndexSource
	Stat2IndexIP
	Stat2IndexImage
)

// string index
const (
	Stat2FieldID     = "id"     // int sql{inc}  #readonly
	Stat2FieldUID    = "uid"    // int ch{primary}
	Stat2FieldType   = "type"   // int ch{low}
	Stat2FieldName   = "name"   // string
	Stat2FieldSource = "source" // string
	Stat2FieldIP     = "ip"     // string
	Stat2FieldImage  = "image"  // bool
)

// index func
func Stat2Indexes() []Stat2IndexTyped {
	return []Stat2IndexTyped{Stat2IndexID, Stat2IndexUID, Stat2IndexType, Stat2IndexName, Stat2IndexSource, Stat2IndexIP, Stat2IndexImage}

}

// 80 bytes (go padding)
//
//easyjson:json
type Stat2 struct {
	ID     int    `json:"id,omitempty"`     // sql{inc}  #readonly
	UID    int    `json:"uid,omitempty"`    // ch{primary}
	Type   int    `json:"type,omitempty"`   // ch{low}
	Name   string `json:"name,omitempty"`   //
	Source string `json:"source,omitempty"` //
	IP     string `json:"ip,omitempty"`     //
	Image  bool   `json:"image,omitempty"`  //
}

// Tuple create an array from struct
func (a *Stat2) Tuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, a.IP, a.Image}
}

// Tuple create an array from struct for clickhouse
func (a *Stat2) clickhouseTuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, net.ParseIP(a.IP), a.Image}
}

// get struct value with function
func (a *Stat2) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "uid":
		return a.UID //int
	case "type":
		return a.Type //int
	case "name":
		return a.Name //string
	case "source":
		return a.Source //string
	case "ip":
		return a.IP //string
	case "image":
		return a.Image //bool
	}
	return
}

// get any struct value as string
func (a *Stat2) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "uid":
		return fmt.Sprint(a.UID) //int
	case "type":
		return fmt.Sprint(a.Type) //int
	case "name":
		return fmt.Sprint(a.Name) //string
	case "source":
		return fmt.Sprint(a.Source) //string
	case "ip":
		return fmt.Sprint(a.IP) //string
	case "image":
		return fmt.Sprint(a.Image) //bool
	}
	return
}

func Stat2ReadonlyList() []Stat2IndexTyped {
	return []Stat2IndexTyped{Stat2IndexID}
}

func (a Stat2IndexTyped) Readonly() bool {
	switch a {
	case Stat2IndexID:
		return true
	default:
		return false
	}
}

// key index string
func (a Stat2IndexTyped) String() string {
	switch a {
	case Stat2IndexID:
		return "id"
	case Stat2IndexUID:
		return "uid"
	case Stat2IndexType:
		return "type"
	case Stat2IndexName:
		return "name"
	case Stat2IndexSource:
		return "source"
	case Stat2IndexIP:
		return "ip"
	case Stat2IndexImage:
		return "image"
	default:
		return ""
	}
}

// key index string
func (a Stat2IndexTyped) SQLName() string {
	switch a {
	case Stat2IndexID:
		return "id"
	case Stat2IndexUID:
		return "uid"
	case Stat2IndexType:
		return "type"
	case Stat2IndexName:
		return "name"
	case Stat2IndexSource:
		return "source"
	case Stat2IndexIP:
		return "ip"
	case Stat2IndexImage:
		return "image"
	default:
		return ""
	}
}

// key index clickhouse string
func (a Stat2IndexTyped) ClickhouseName() string {
	switch a {
	case Stat2IndexID:
		return "id"
	case Stat2IndexUID:
		return "uid"
	case Stat2IndexType:
		return "type"
	case Stat2IndexName:
		return "name"
	case Stat2IndexSource:
		return "source"
	case Stat2IndexIP:
		return "ip"
	case Stat2IndexImage:
		return "image"
	default:
		return ""
	}
}

// key index type
func (a Stat2IndexTyped) Type() string {
	switch a {
	case Stat2IndexID:
		return "int"
	case Stat2IndexUID:
		return "int"
	case Stat2IndexType:
		return "int"
	case Stat2IndexName:
		return "string"
	case Stat2IndexSource:
		return "string"
	case Stat2IndexIP:
		return "string"
	case Stat2IndexImage:
		return "bool"
	default:
		return ""
	}
}

// custom title
func (a Stat2IndexTyped) Title() string {
	switch a {
	case Stat2IndexID:
		return "ID"
	case Stat2IndexUID:
		return "UID"
	case Stat2IndexType:
		return "Type"
	case Stat2IndexName:
		return "Name"
	case Stat2IndexSource:
		return "Source"
	case Stat2IndexIP:
		return "IP"
	case Stat2IndexImage:
		return "Image"
	default:
		return ""
	}
}

// custom desc
func (a Stat2IndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func Stat2KeyIndex(key string) Stat2IndexTyped {
	switch key {
	case "id":
		return Stat2IndexID
	case "uid":
		return Stat2IndexUID
	case "type":
		return Stat2IndexType
	case "name":
		return Stat2IndexName
	case "source":
		return Stat2IndexSource
	case "ip":
		return Stat2IndexIP
	case "image":
		return Stat2IndexImage
	default:
		return 0
	}
}

// valid struct key check
func Stat2ValidKey(key string) bool {
	switch key {
	case "id", "uid", "type", "name", "source", "ip", "image":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Stat2) Map() map[string]any {
	return map[string]any{
		"id":     a.ID,
		"uid":    a.UID,
		"type":   a.Type,
		"name":   a.Name,
		"source": a.Source,
		"ip":     a.IP,
		"image":  a.Image,
	}
}

// struct to map
func (a *Stat2) Iterate(f func(k Stat2IndexTyped, v any)) {
	for _, x := range Stat2Indexes() {
		f(x, a.Get(x.String()))
	}
}

// gotiny marshal
func (a *Stat2) MarshalGotiny() []byte {
	return gotiny.Marshal(&a.ID, &a.UID, &a.Type, &a.Name, &a.Source, &a.IP, &a.Image)
}

// parse gotiny
func UnmarshalStat2Gotiny(v []byte) (a Stat2) {
	gotiny.Unmarshal(v, &a.ID, &a.UID, &a.Type, &a.Name, &a.Source, &a.IP, &a.Image)
	return
}

// fast json marshal
func (a *Stat2) Pack() []byte {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	b, _ := jsoner.Marshal(a)
	return b
}

// fast json unmarshal
func ParseStat2(v []byte) (a Stat2, err error) {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	err = jsoner.Unmarshal(v, &a)
	return
}

// clickhouse Stat2CQL class
type Stat2CQL struct {
	conn driver.Conn
	list []Stat2
	sync.Mutex
}

func NewStat2CQL(conn driver.Conn) (a *Stat2CQL) {
	a = new(Stat2CQL)
	a.conn = conn
	a.CreateTable()
	go a.deamon()
	return
}

// parse clickhouse query
func (a *Stat2CQL) TableName() (res string) {
	return "stat2"
}

func (a *Stat2CQL) Count(where ...string) (count int, err error) {

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	var q string
	switch len(where) {
	case 0:
		q = "select count() from stat2"
	default:
		q = "select count() from stat2 where " + strings.Join(where, " ")
	}
	var n uint64
	err = a.conn.QueryRow(context.Background(), q).Scan(&n)
	count = int(n)
	return
}

// Add items to clickhouse queue
func (a *Stat2CQL) Add(items ...Stat2) {
	a.Lock()
	defer a.Unlock()
	a.list = append(a.list, items...)
}

// Push queued items to clickhouse
func (a *Stat2CQL) Push() (err error) {
	a.Lock()
	defer a.Unlock()
	if len(a.list) == 0 {
		return
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into stat2 (id, uid, type, name, source, ip, image)")
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
func (a *Stat2CQL) deamon() {
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
func (a *Stat2CQL) Get(id any, fields ...Stat2IndexTyped) (res *Stat2, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat2 where uid = ? limit 1", fieldlist)
	res = new(Stat2)

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
func (a *Stat2CQL) GetWhere(where string, fields ...Stat2IndexTyped) (res *Stat2, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat2 where %s limit 1", fieldlist, where)
	res = new(Stat2)

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
func (a *Stat2CQL) Update(id any, k string, v any, where ...string) (err error) {
	if !Stat2ValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := Stat2KeyIndex(k)
	q := fmt.Sprintf("alter table stat2 update %s = ? where uid = ?", in.ClickhouseName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, v, id)
}

// update clickhouse query
func (a *Stat2CQL) UpdateWhere(k string, v any, where string) (err error) {
	if !Stat2ValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := Stat2KeyIndex(k)
	q := fmt.Sprintf("alter table %s update %s = ? where %s", a.TableName(), in.ClickhouseName(), where)
	return a.conn.Exec(context.Background(), q, v)
}

// update clickhouse query
func (a *Stat2CQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !Stat2ValidKey(k) {
			return errors.New(k)
		}
		in := Stat2KeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")
	values = append(values, id)

	q := fmt.Sprintf("alter table stat2 update %s where uid = ?", list)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, values...)
}

// update clickhouse query
func (a *Stat2CQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !Stat2ValidKey(k) {
			return errors.New(k)
		}
		in := Stat2KeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("alter table stat2 update %s where %s", list, where)
	return a.conn.Exec(context.Background(), q, values...)
}

// select custom clickhouse query
func (a *Stat2CQL) Select(where string, fields ...Stat2IndexTyped) (res []*Stat2, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat2", fieldlist)
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
		var item Stat2
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
func (a *Stat2CQL) Delete(id any) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table stat2 delete where uid = ?"
	return a.conn.Exec(context.Background(), q, id)
}

// delete clickhouse items
func (a *Stat2CQL) DeleteWhere(where string) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table stat2 delete where " + where
	return a.conn.Exec(context.Background(), q)
}

// has value in clickhouse
func (a *Stat2CQL) Has(field Stat2IndexTyped, v any) (has bool, err error) {
	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	q := fmt.Sprintf("select count() from stat2 where %s = ? limit 1", field.ClickhouseName())
	var n uint64
	err = a.conn.QueryRow(context.Background(), q, v).Scan(&n)
	has = n > 0
	return
}

// stat by clickhouse field
func (a *Stat2CQL) Stat(field Stat2IndexTyped) (res map[string]int, err error) {
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
func (a *Stat2CQL) CreateTable() (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := `create table if not exists stat2 (
	id         Int64,
	uid        Int64,
	type       LowCardinality(Int64),
	name       String,
	source     String,
	ip         IPv4,
	image      Bool
)
engine = MergeTree()
order by (uid)
primary key (uid)`
	return a.conn.Exec(context.Background(), q)
}

// parse clickhouse query
func (a *Stat2CQL) Fields() (res []Stat2IndexTyped) {
	return []Stat2IndexTyped{Stat2IndexID, Stat2IndexUID, Stat2IndexType, Stat2IndexName, Stat2IndexSource, Stat2IndexIP, Stat2IndexImage}
}

// clickhouseSet sets field from value of clickhouse row
func (a *Stat2) clickhouseSet(f Stat2IndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case Stat2IndexID:
		stat2CHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case Stat2IndexUID:
		stat2CHAssign(reflect.ValueOf(&a.UID).Elem(), reflect.ValueOf(v))
	case Stat2IndexType:
		stat2CHAssign(reflect.ValueOf(&a.Type).Elem(), reflect.ValueOf(v))
	case Stat2IndexName:
		stat2CHAssign(reflect.ValueOf(&a.Name).Elem(), reflect.ValueOf(v))
	case Stat2IndexSource:
		stat2CHAssign(reflect.ValueOf(&a.Source).Elem(), reflect.ValueOf(v))
	case Stat2IndexIP:
		stat2CHAssign(reflect.ValueOf(&a.IP).Elem(), reflect.ValueOf(v))
	case Stat2IndexImage:
		stat2CHAssign(reflect.ValueOf(&a.Image).Elem(), reflect.ValueOf(v))
	}
}

// stat2CHAssign sets d from clickhouse value s: same type, number, string, time, slice
func stat2CHAssign(d, s reflect.Value) {
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
		stat2CHAssign(p.Elem(), s)
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
			stat2CHAssign(r.Index(i), s.Index(i))
		}
		d.Set(r)

	// named types
	case s.Kind() == d.Kind() && s.Type().ConvertibleTo(d.Type()):
		d.Set(s.Convert(d.Type()))
	}
}

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Stat2Proto struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            int64                  `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Uid           int64                  `protobuf:"varint,2,opt,name=uid,proto3" json:"uid,omitempty"`
	Type          int64                  `protobuf:"varint,3,opt,name=type,proto3" json:"type,omitempty"`
	Name          string                 `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	Source        string                 `protobuf:"bytes,5,opt,name=source,proto3" json:"source,omitempty"`
	Ip            string                 `protobuf:"bytes,6,opt,name=ip,proto3" json:"ip,omitempty"`
	Image         bool                   `protobuf:"varint,7,opt,name=image,proto3" json:"image,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Stat2Proto) Reset() {
	*x = Stat2Proto{}
	mi := &file_test_ch2_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Stat2Proto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Stat2Proto) ProtoMessage() {}

func (x *Stat2Proto) ProtoReflect() protoreflect.Message {
	mi := &file_test_ch2_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Stat2Proto.ProtoReflect.Descriptor instead.
func (*Stat2Proto) Descriptor() ([]byte, []int) {
	return file_test_ch2_proto_rawDescGZIP(), []int{0}
}

func (x *Stat2Proto) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *Stat2Proto) GetUid() int64 {
	if x != nil {
		return x.Uid
	}
	return 0
}

func (x *Stat2Proto) GetType() int64 {
	if x != nil {
		return x.Type
	}
	return 0
}

func (x *Stat2Proto) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Stat2Proto) GetSource() string {
	if x != nil {
		return x.Source
	}
	return ""
}

func (x *Stat2Proto) GetIp() string {
	if x != nil {
		return x.Ip
	}
	return ""
}

func (x *Stat2Proto) GetImage() bool {
	if x != nil {
		return x.Image
	}
	return false
}

var File_test_ch2_proto protoreflect.FileDescriptor

const file_test_ch2_proto_rawDesc = "" +
	"\n" +
	"\x0etest_ch2.proto\x12\x04news\"\x94\x01\n" +
	"\n" +
	"Stat2Proto\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x03R\x02id\x12\x10\n" +
	"\x03uid\x18\x02 \x01(\x03R\x03uid\x12\x12\n" +
	"\x04type\x18\x03 \x01(\x03R\x04type\x12\x12\n" +
	"\x04name\x18\x04 \x01(\tR\x04name\x12\x16\n" +
	"\x06source\x18\x05 \x01(\tR\x06source\x12\x0e\n" +
	"\x02ip\x18\x06 \x01(\tR\x02ip\x12\x14\n" +
	"\x05image\x18\a \x01(\bR\x05imageB\tZ\a./;newsb\x06proto3"

var (
	file_test_ch2_proto_rawDescOnce sync.Once
	file_test_ch2_proto_rawDescData []byte
)

func file_test_ch2_proto_rawDescGZIP() []byte {
	file_test_ch2_proto_rawDescOnce.Do(func() {
		file_test_ch2_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_test_ch2_proto_rawDesc), len(file_test_ch2_proto_rawDesc)))
	})
	return file_test_ch2_proto_rawDescData
}

var file_test_ch2_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_test_ch2_proto_goTypes = []any{
	(*Stat2Proto)(nil), // 0: news.Stat2Proto
}
var file_test_ch2_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

var _ = initStat2Proto()

func initStat2Proto() struct{} {
	file_test_ch2_proto_init()
	return struct{}{}
}
func file_test_ch2_proto_init() {
	if File_test_ch2_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_test_ch2_proto_rawDesc), len(file_test_ch2_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_test_ch2_proto_goTypes,
		DependencyIndexes: file_test_ch2_proto_depIdxs,
		MessageInfos:      file_test_ch2_proto_msgTypes,
	}.Build()
	File_test_ch2_proto = out.File
	file_test_ch2_proto_goTypes = nil
	file_test_ch2_proto_depIdxs = nil
}
