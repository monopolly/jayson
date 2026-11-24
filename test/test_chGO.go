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
type StatIndexTyped int

// int index
const (
	StatIndexID = StatIndexTyped(iota)
	StatIndexUID
	StatIndexType
	StatIndexName
	StatIndexSource
	StatIndexIP
	StatIndexImage
	StatIndexStack
	StatIndexUints
	StatIndexUints8
	StatIndexUints16
	StatIndexUints32
	StatIndexUints64
	StatIndexInts8
	StatIndexInts16
	StatIndexInts32
	StatIndexInts64
)

// string index
const (
	StatFieldID      = "id"      // int sql{inc}  #readonly
	StatFieldUID     = "uid"     // int ch{primary}
	StatFieldType    = "type"    // int ch{low}
	StatFieldName    = "name"    // string
	StatFieldSource  = "source"  // string
	StatFieldIP      = "ip"      // string
	StatFieldImage   = "image"   // bool
	StatFieldStack   = "stack"   // []string
	StatFieldUints   = "uints"   // []uint
	StatFieldUints8  = "uints8"  // []uint8
	StatFieldUints16 = "uints16" // []uint16
	StatFieldUints32 = "uints32" // []uint32
	StatFieldUints64 = "uints64" // []uint64
	StatFieldInts8   = "ints8"   // []int8
	StatFieldInts16  = "ints16"  // []int16
	StatFieldInts32  = "ints32"  // []int32
	StatFieldInts64  = "ints64"  // []int64
)

// index func
func StatIndexes() []StatIndexTyped {
	return []StatIndexTyped{StatIndexID, StatIndexUID, StatIndexType, StatIndexName, StatIndexSource, StatIndexIP, StatIndexImage, StatIndexStack, StatIndexUints, StatIndexUints8, StatIndexUints16, StatIndexUints32, StatIndexUints64, StatIndexInts8, StatIndexInts16, StatIndexInts32, StatIndexInts64}

}

// 160 bytes (go padding)
//
//easyjson:json
type Stat struct {
	ID      int      `json:"id,omitempty"`      // sql{inc}  #readonly
	UID     int      `json:"uid,omitempty"`     // ch{primary}
	Type    int      `json:"type,omitempty"`    // ch{low}
	Name    string   `json:"name,omitempty"`    //
	Source  string   `json:"source,omitempty"`  //
	IP      string   `json:"ip,omitempty"`      //
	Image   bool     `json:"image,omitempty"`   //
	Stack   []string `json:"stack,omitempty"`   //
	Uints   []uint   `json:"uints,omitempty"`   //
	Uints8  []uint8  `json:"uints8,omitempty"`  //
	Uints16 []uint16 `json:"uints16,omitempty"` //
	Uints32 []uint32 `json:"uints32,omitempty"` //
	Uints64 []uint64 `json:"uints64,omitempty"` //
	Ints8   []int8   `json:"ints8,omitempty"`   //
	Ints16  []int16  `json:"ints16,omitempty"`  //
	Ints32  []int32  `json:"ints32,omitempty"`  //
	Ints64  []int64  `json:"ints64,omitempty"`  //
}

// Tuple create an array from struct
func (a *Stat) Tuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, a.IP, a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// Tuple create an array from struct for clickhouse
func (a *Stat) clickhouseTuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, net.ParseIP(a.IP), a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// get struct value with function
func (a *Stat) Get(k string) (v any) {
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
	case "stack":
		return a.Stack //[]string
	case "uints":
		return a.Uints //[]uint
	case "uints8":
		return a.Uints8 //[]uint8
	case "uints16":
		return a.Uints16 //[]uint16
	case "uints32":
		return a.Uints32 //[]uint32
	case "uints64":
		return a.Uints64 //[]uint64
	case "ints8":
		return a.Ints8 //[]int8
	case "ints16":
		return a.Ints16 //[]int16
	case "ints32":
		return a.Ints32 //[]int32
	case "ints64":
		return a.Ints64 //[]int64
	}
	return
}

// get any struct value as string
func (a *Stat) String(k string) (v string) {
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
	case "stack":
		return fmt.Sprint(a.Stack) //[]string
	case "uints":
		return fmt.Sprint(a.Uints) //[]uint
	case "uints8":
		return fmt.Sprint(a.Uints8) //[]uint8
	case "uints16":
		return fmt.Sprint(a.Uints16) //[]uint16
	case "uints32":
		return fmt.Sprint(a.Uints32) //[]uint32
	case "uints64":
		return fmt.Sprint(a.Uints64) //[]uint64
	case "ints8":
		return fmt.Sprint(a.Ints8) //[]int8
	case "ints16":
		return fmt.Sprint(a.Ints16) //[]int16
	case "ints32":
		return fmt.Sprint(a.Ints32) //[]int32
	case "ints64":
		return fmt.Sprint(a.Ints64) //[]int64
	}
	return
}

func StatReadonlyList() []StatIndexTyped {
	return []StatIndexTyped{StatIndexID}
}

func (a StatIndexTyped) Readonly() bool {
	switch a {
	case StatIndexID:
		return true
	default:
		return false
	}
}

// key index string
func (a StatIndexTyped) String() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexUID:
		return "uid"
	case StatIndexType:
		return "type"
	case StatIndexName:
		return "name"
	case StatIndexSource:
		return "source"
	case StatIndexIP:
		return "ip"
	case StatIndexImage:
		return "image"
	case StatIndexStack:
		return "stack"
	case StatIndexUints:
		return "uints"
	case StatIndexUints8:
		return "uints8"
	case StatIndexUints16:
		return "uints16"
	case StatIndexUints32:
		return "uints32"
	case StatIndexUints64:
		return "uints64"
	case StatIndexInts8:
		return "ints8"
	case StatIndexInts16:
		return "ints16"
	case StatIndexInts32:
		return "ints32"
	case StatIndexInts64:
		return "ints64"
	default:
		return ""
	}
}

// key index string
func (a StatIndexTyped) SQLName() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexUID:
		return "uid"
	case StatIndexType:
		return "type"
	case StatIndexName:
		return "name"
	case StatIndexSource:
		return "source"
	case StatIndexIP:
		return "ip"
	case StatIndexImage:
		return "image"
	case StatIndexStack:
		return "stack"
	case StatIndexUints:
		return "uints"
	case StatIndexUints8:
		return "uints8"
	case StatIndexUints16:
		return "uints16"
	case StatIndexUints32:
		return "uints32"
	case StatIndexUints64:
		return "uints64"
	case StatIndexInts8:
		return "ints8"
	case StatIndexInts16:
		return "ints16"
	case StatIndexInts32:
		return "ints32"
	case StatIndexInts64:
		return "ints64"
	default:
		return ""
	}
}

// key index clickhouse string
func (a StatIndexTyped) ClickhouseName() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexUID:
		return "uid"
	case StatIndexType:
		return "type"
	case StatIndexName:
		return "name"
	case StatIndexSource:
		return "source"
	case StatIndexIP:
		return "ip"
	case StatIndexImage:
		return "image"
	case StatIndexStack:
		return "stack"
	case StatIndexUints:
		return "uints"
	case StatIndexUints8:
		return "uints8"
	case StatIndexUints16:
		return "uints16"
	case StatIndexUints32:
		return "uints32"
	case StatIndexUints64:
		return "uints64"
	case StatIndexInts8:
		return "ints8"
	case StatIndexInts16:
		return "ints16"
	case StatIndexInts32:
		return "ints32"
	case StatIndexInts64:
		return "ints64"
	default:
		return ""
	}
}

// key index type
func (a StatIndexTyped) Type() string {
	switch a {
	case StatIndexID:
		return "int"
	case StatIndexUID:
		return "int"
	case StatIndexType:
		return "int"
	case StatIndexName:
		return "string"
	case StatIndexSource:
		return "string"
	case StatIndexIP:
		return "string"
	case StatIndexImage:
		return "bool"
	case StatIndexStack:
		return "[]string"
	case StatIndexUints:
		return "[]uint"
	case StatIndexUints8:
		return "[]uint8"
	case StatIndexUints16:
		return "[]uint16"
	case StatIndexUints32:
		return "[]uint32"
	case StatIndexUints64:
		return "[]uint64"
	case StatIndexInts8:
		return "[]int8"
	case StatIndexInts16:
		return "[]int16"
	case StatIndexInts32:
		return "[]int32"
	case StatIndexInts64:
		return "[]int64"
	default:
		return ""
	}
}

// custom title
func (a StatIndexTyped) Title() string {
	switch a {
	case StatIndexID:
		return "ID"
	case StatIndexUID:
		return "UID"
	case StatIndexType:
		return "Type"
	case StatIndexName:
		return "Name"
	case StatIndexSource:
		return "Source"
	case StatIndexIP:
		return "IP"
	case StatIndexImage:
		return "Image"
	case StatIndexStack:
		return "Stack"
	case StatIndexUints:
		return "Uints"
	case StatIndexUints8:
		return "Uints8"
	case StatIndexUints16:
		return "Uints16"
	case StatIndexUints32:
		return "Uints32"
	case StatIndexUints64:
		return "Uints64"
	case StatIndexInts8:
		return "Ints8"
	case StatIndexInts16:
		return "Ints16"
	case StatIndexInts32:
		return "Ints32"
	case StatIndexInts64:
		return "Ints64"
	default:
		return ""
	}
}

// custom desc
func (a StatIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func StatKeyIndex(key string) StatIndexTyped {
	switch key {
	case "id":
		return StatIndexID
	case "uid":
		return StatIndexUID
	case "type":
		return StatIndexType
	case "name":
		return StatIndexName
	case "source":
		return StatIndexSource
	case "ip":
		return StatIndexIP
	case "image":
		return StatIndexImage
	case "stack":
		return StatIndexStack
	case "uints":
		return StatIndexUints
	case "uints8":
		return StatIndexUints8
	case "uints16":
		return StatIndexUints16
	case "uints32":
		return StatIndexUints32
	case "uints64":
		return StatIndexUints64
	case "ints8":
		return StatIndexInts8
	case "ints16":
		return StatIndexInts16
	case "ints32":
		return StatIndexInts32
	case "ints64":
		return StatIndexInts64
	default:
		return 0
	}
}

// valid struct key check
func StatValidKey(key string) bool {
	switch key {
	case "id", "uid", "type", "name", "source", "ip", "image", "stack", "uints", "uints8", "uints16", "uints32", "uints64", "ints8", "ints16", "ints32", "ints64":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Stat) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"uid":     a.UID,
		"type":    a.Type,
		"name":    a.Name,
		"source":  a.Source,
		"ip":      a.IP,
		"image":   a.Image,
		"stack":   a.Stack,
		"uints":   a.Uints,
		"uints8":  a.Uints8,
		"uints16": a.Uints16,
		"uints32": a.Uints32,
		"uints64": a.Uints64,
		"ints8":   a.Ints8,
		"ints16":  a.Ints16,
		"ints32":  a.Ints32,
		"ints64":  a.Ints64,
	}
}

// struct to map
func (a *Stat) Iterate(f func(k StatIndexTyped, v any)) {
	for _, x := range StatIndexes() {
		f(x, a.Get(x.String()))
	}
}

// gotiny marshal
func (a *Stat) MarshalGotiny() []byte {
	return gotiny.Marshal(&a.ID, &a.UID, &a.Type, &a.Name, &a.Source, &a.IP, &a.Image, &a.Stack, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64)
}

// parse gotiny
func UnmarshalStatGotiny(v []byte) (a Stat) {
	gotiny.Unmarshal(v, &a.ID, &a.UID, &a.Type, &a.Name, &a.Source, &a.IP, &a.Image, &a.Stack, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64)
	return
}

// fast json marshal
func (a *Stat) Pack() []byte {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	b, _ := jsoner.Marshal(a)
	return b
}

// fast json unmarshal
func ParseStat(v []byte) (a Stat, err error) {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	err = jsoner.Unmarshal(v, &a)
	return
}

// clickhouse StatCQL class
type StatCQL struct {
	conn driver.Conn
	list []Stat
	sync.Mutex
}

func NewStatCQL(conn driver.Conn) (a *StatCQL) {
	a = new(StatCQL)
	a.conn = conn
	a.CreateTable()
	go a.deamon()
	return
}

// parse clickhouse query
func (a *StatCQL) TableName() (res string) {
	return "stat"
}

func (a *StatCQL) Count(where ...string) (count int, err error) {

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	var q string
	switch len(where) {
	case 0:
		q = "select count() from stat"
	default:
		q = "select count() from stat where " + strings.Join(where, " ")
	}
	var n uint64
	err = a.conn.QueryRow(context.Background(), q).Scan(&n)
	count = int(n)
	return
}

// Add items to clickhouse queue
func (a *StatCQL) Add(items ...Stat) {
	a.Lock()
	defer a.Unlock()
	a.list = append(a.list, items...)
}

// Push queued items to clickhouse
func (a *StatCQL) Push() (err error) {
	a.Lock()
	defer a.Unlock()
	if len(a.list) == 0 {
		return
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into stat (id, uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64)")
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
func (a *StatCQL) deamon() {
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
func (a *StatCQL) Get(id any, fields ...StatIndexTyped) (res *Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat where uid = ? limit 1", fieldlist)
	res = new(Stat)

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
func (a *StatCQL) GetWhere(where string, fields ...StatIndexTyped) (res *Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat where %s limit 1", fieldlist, where)
	res = new(Stat)

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
func (a *StatCQL) Update(id any, k string, v any, where ...string) (err error) {
	if !StatValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := StatKeyIndex(k)
	q := fmt.Sprintf("alter table stat update %s = ? where uid = ?", in.ClickhouseName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, v, id)
}

// update clickhouse query
func (a *StatCQL) UpdateWhere(k string, v any, where string) (err error) {
	if !StatValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := StatKeyIndex(k)
	q := fmt.Sprintf("alter table %s update %s = ? where %s", a.TableName(), in.ClickhouseName(), where)
	return a.conn.Exec(context.Background(), q, v)
}

// update clickhouse query
func (a *StatCQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !StatValidKey(k) {
			return errors.New(k)
		}
		in := StatKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")
	values = append(values, id)

	q := fmt.Sprintf("alter table stat update %s where uid = ?", list)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, values...)
}

// update clickhouse query
func (a *StatCQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !StatValidKey(k) {
			return errors.New(k)
		}
		in := StatKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("alter table stat update %s where %s", list, where)
	return a.conn.Exec(context.Background(), q, values...)
}

// select custom clickhouse query
func (a *StatCQL) Select(where string, fields ...StatIndexTyped) (res []*Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from stat", fieldlist)
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
		var item Stat
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
func (a *StatCQL) Delete(id any) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table stat delete where uid = ?"
	return a.conn.Exec(context.Background(), q, id)
}

// delete clickhouse items
func (a *StatCQL) DeleteWhere(where string) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table stat delete where " + where
	return a.conn.Exec(context.Background(), q)
}

// has value in clickhouse
func (a *StatCQL) Has(field StatIndexTyped, v any) (has bool, err error) {
	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	q := fmt.Sprintf("select count() from stat where %s = ? limit 1", field.ClickhouseName())
	var n uint64
	err = a.conn.QueryRow(context.Background(), q, v).Scan(&n)
	has = n > 0
	return
}

// stat by clickhouse field
func (a *StatCQL) Stat(field StatIndexTyped) (res map[string]int, err error) {
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
func (a *StatCQL) CreateTable() (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := `create table if not exists stat (
	id          Int64,
	uid         Int64,
	type        LowCardinality(Int64),
	name        String,
	source      String,
	ip          IPv4,
	image       Bool,
	stack       Array(String),
	uints       Array(UInt64),
	uints8      Array(UInt8),
	uints16     Array(UInt16),
	uints32     Array(UInt32),
	uints64     Array(UInt64),
	ints8       Array(Int8),
	ints16      Array(Int16),
	ints32      Array(Int32),
	ints64      Array(Int64)
)
engine = MergeTree()
order by (uid)
primary key (uid)`
	return a.conn.Exec(context.Background(), q)
}

// parse clickhouse query
func (a *StatCQL) Fields() (res []StatIndexTyped) {
	return []StatIndexTyped{StatIndexID, StatIndexUID, StatIndexType, StatIndexName, StatIndexSource, StatIndexIP, StatIndexImage, StatIndexStack, StatIndexUints, StatIndexUints8, StatIndexUints16, StatIndexUints32, StatIndexUints64, StatIndexInts8, StatIndexInts16, StatIndexInts32, StatIndexInts64}
}

// clickhouseSet sets field from value of clickhouse row
func (a *Stat) clickhouseSet(f StatIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case StatIndexID:
		statCHAssign(reflect.ValueOf(&a.ID).Elem(), reflect.ValueOf(v))
	case StatIndexUID:
		statCHAssign(reflect.ValueOf(&a.UID).Elem(), reflect.ValueOf(v))
	case StatIndexType:
		statCHAssign(reflect.ValueOf(&a.Type).Elem(), reflect.ValueOf(v))
	case StatIndexName:
		statCHAssign(reflect.ValueOf(&a.Name).Elem(), reflect.ValueOf(v))
	case StatIndexSource:
		statCHAssign(reflect.ValueOf(&a.Source).Elem(), reflect.ValueOf(v))
	case StatIndexIP:
		statCHAssign(reflect.ValueOf(&a.IP).Elem(), reflect.ValueOf(v))
	case StatIndexImage:
		statCHAssign(reflect.ValueOf(&a.Image).Elem(), reflect.ValueOf(v))
	case StatIndexStack:
		statCHAssign(reflect.ValueOf(&a.Stack).Elem(), reflect.ValueOf(v))
	case StatIndexUints:
		statCHAssign(reflect.ValueOf(&a.Uints).Elem(), reflect.ValueOf(v))
	case StatIndexUints8:
		statCHAssign(reflect.ValueOf(&a.Uints8).Elem(), reflect.ValueOf(v))
	case StatIndexUints16:
		statCHAssign(reflect.ValueOf(&a.Uints16).Elem(), reflect.ValueOf(v))
	case StatIndexUints32:
		statCHAssign(reflect.ValueOf(&a.Uints32).Elem(), reflect.ValueOf(v))
	case StatIndexUints64:
		statCHAssign(reflect.ValueOf(&a.Uints64).Elem(), reflect.ValueOf(v))
	case StatIndexInts8:
		statCHAssign(reflect.ValueOf(&a.Ints8).Elem(), reflect.ValueOf(v))
	case StatIndexInts16:
		statCHAssign(reflect.ValueOf(&a.Ints16).Elem(), reflect.ValueOf(v))
	case StatIndexInts32:
		statCHAssign(reflect.ValueOf(&a.Ints32).Elem(), reflect.ValueOf(v))
	case StatIndexInts64:
		statCHAssign(reflect.ValueOf(&a.Ints64).Elem(), reflect.ValueOf(v))
	}
}

// statCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func statCHAssign(d, s reflect.Value) {
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
		statCHAssign(p.Elem(), s)
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
			statCHAssign(r.Index(i), s.Index(i))
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

type StatProto struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            int64                  `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Uid           int64                  `protobuf:"varint,2,opt,name=uid,proto3" json:"uid,omitempty"`
	Type          int64                  `protobuf:"varint,3,opt,name=type,proto3" json:"type,omitempty"`
	Name          string                 `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	Source        string                 `protobuf:"bytes,5,opt,name=source,proto3" json:"source,omitempty"`
	Ip            string                 `protobuf:"bytes,6,opt,name=ip,proto3" json:"ip,omitempty"`
	Image         bool                   `protobuf:"varint,7,opt,name=image,proto3" json:"image,omitempty"`
	Stack         []string               `protobuf:"bytes,8,rep,name=stack,proto3" json:"stack,omitempty"`
	Uints         []uint64               `protobuf:"varint,9,rep,packed,name=uints,proto3" json:"uints,omitempty"`
	Uints8        []byte                 `protobuf:"bytes,10,opt,name=uints8,proto3" json:"uints8,omitempty"`
	Uints16       []uint32               `protobuf:"varint,11,rep,packed,name=uints16,proto3" json:"uints16,omitempty"`
	Uints32       []uint32               `protobuf:"varint,12,rep,packed,name=uints32,proto3" json:"uints32,omitempty"`
	Uints64       []uint64               `protobuf:"varint,13,rep,packed,name=uints64,proto3" json:"uints64,omitempty"`
	Ints8         []int32                `protobuf:"varint,14,rep,packed,name=ints8,proto3" json:"ints8,omitempty"`
	Ints16        []int32                `protobuf:"varint,15,rep,packed,name=ints16,proto3" json:"ints16,omitempty"`
	Ints32        []int32                `protobuf:"varint,16,rep,packed,name=ints32,proto3" json:"ints32,omitempty"`
	Ints64        []int64                `protobuf:"varint,17,rep,packed,name=ints64,proto3" json:"ints64,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StatProto) Reset() {
	*x = StatProto{}
	mi := &file_test_ch_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StatProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatProto) ProtoMessage() {}

func (x *StatProto) ProtoReflect() protoreflect.Message {
	mi := &file_test_ch_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatProto.ProtoReflect.Descriptor instead.
func (*StatProto) Descriptor() ([]byte, []int) {
	return file_test_ch_proto_rawDescGZIP(), []int{0}
}

func (x *StatProto) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *StatProto) GetUid() int64 {
	if x != nil {
		return x.Uid
	}
	return 0
}

func (x *StatProto) GetType() int64 {
	if x != nil {
		return x.Type
	}
	return 0
}

func (x *StatProto) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *StatProto) GetSource() string {
	if x != nil {
		return x.Source
	}
	return ""
}

func (x *StatProto) GetIp() string {
	if x != nil {
		return x.Ip
	}
	return ""
}

func (x *StatProto) GetImage() bool {
	if x != nil {
		return x.Image
	}
	return false
}

func (x *StatProto) GetStack() []string {
	if x != nil {
		return x.Stack
	}
	return nil
}

func (x *StatProto) GetUints() []uint64 {
	if x != nil {
		return x.Uints
	}
	return nil
}

func (x *StatProto) GetUints8() []byte {
	if x != nil {
		return x.Uints8
	}
	return nil
}

func (x *StatProto) GetUints16() []uint32 {
	if x != nil {
		return x.Uints16
	}
	return nil
}

func (x *StatProto) GetUints32() []uint32 {
	if x != nil {
		return x.Uints32
	}
	return nil
}

func (x *StatProto) GetUints64() []uint64 {
	if x != nil {
		return x.Uints64
	}
	return nil
}

func (x *StatProto) GetInts8() []int32 {
	if x != nil {
		return x.Ints8
	}
	return nil
}

func (x *StatProto) GetInts16() []int32 {
	if x != nil {
		return x.Ints16
	}
	return nil
}

func (x *StatProto) GetInts32() []int32 {
	if x != nil {
		return x.Ints32
	}
	return nil
}

func (x *StatProto) GetInts64() []int64 {
	if x != nil {
		return x.Ints64
	}
	return nil
}

var File_test_ch_proto protoreflect.FileDescriptor

const file_test_ch_proto_rawDesc = "" +
	"\n" +
	"\rtest_ch.proto\x12\x04news\"\x83\x03\n" +
	"\tStatProto\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x03R\x02id\x12\x10\n" +
	"\x03uid\x18\x02 \x01(\x03R\x03uid\x12\x12\n" +
	"\x04type\x18\x03 \x01(\x03R\x04type\x12\x12\n" +
	"\x04name\x18\x04 \x01(\tR\x04name\x12\x16\n" +
	"\x06source\x18\x05 \x01(\tR\x06source\x12\x0e\n" +
	"\x02ip\x18\x06 \x01(\tR\x02ip\x12\x14\n" +
	"\x05image\x18\a \x01(\bR\x05image\x12\x14\n" +
	"\x05stack\x18\b \x03(\tR\x05stack\x12\x14\n" +
	"\x05uints\x18\t \x03(\x04R\x05uints\x12\x16\n" +
	"\x06uints8\x18\n" +
	" \x01(\fR\x06uints8\x12\x18\n" +
	"\auints16\x18\v \x03(\rR\auints16\x12\x18\n" +
	"\auints32\x18\f \x03(\rR\auints32\x12\x18\n" +
	"\auints64\x18\r \x03(\x04R\auints64\x12\x14\n" +
	"\x05ints8\x18\x0e \x03(\x05R\x05ints8\x12\x16\n" +
	"\x06ints16\x18\x0f \x03(\x05R\x06ints16\x12\x16\n" +
	"\x06ints32\x18\x10 \x03(\x05R\x06ints32\x12\x16\n" +
	"\x06ints64\x18\x11 \x03(\x03R\x06ints64B\tZ\a./;newsb\x06proto3"

var (
	file_test_ch_proto_rawDescOnce sync.Once
	file_test_ch_proto_rawDescData []byte
)

func file_test_ch_proto_rawDescGZIP() []byte {
	file_test_ch_proto_rawDescOnce.Do(func() {
		file_test_ch_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_test_ch_proto_rawDesc), len(file_test_ch_proto_rawDesc)))
	})
	return file_test_ch_proto_rawDescData
}

var file_test_ch_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_test_ch_proto_goTypes = []any{
	(*StatProto)(nil), // 0: news.StatProto
}
var file_test_ch_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

var _ = initStatProto()

func initStatProto() struct{} {
	file_test_ch_proto_init()
	return struct{}{}
}
func file_test_ch_proto_init() {
	if File_test_ch_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_test_ch_proto_rawDesc), len(file_test_ch_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_test_ch_proto_goTypes,
		DependencyIndexes: file_test_ch_proto_depIdxs,
		MessageInfos:      file_test_ch_proto_msgTypes,
	}.Build()
	File_test_ch_proto = out.File
	file_test_ch_proto_goTypes = nil
	file_test_ch_proto_depIdxs = nil
}
