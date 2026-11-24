package news

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	jsoniter "github.com/json-iterator/go"
	"github.com/monopolly/cast"
	"github.com/monopolly/jsons"
	"github.com/niubaoshu/gotiny"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
)

/*
	Auto-generate. Do not change!
	Help to auto-generated correct models for golang structures.
	Sergey Keplin (c) 2026
	a@senthy.com, lava.mobi@gmail.com, https://t.me/martinprestone
	github.com/monopolly/jsons
	Ex: //! [] go=News js=NewsJson sql=news noinit up...

	STRUCT:
	msgp                          Create message pack marshal/unmarshal
	!omit                         No omit tag for json
	go=News                       Set golang struct names. Ex: go=News1 > type News1 struct{}
	proto=NewsProto               Generate proto file and compile to current package
	ts=news                       Set typescript struct names. Ex: ts=NewsJson
	swift                         Generate swift model
	sql=news                      Set sql table name. Ex: sql=accounts
	ch=views                      Set clickhouse sql table name. Ex: ch=views
	lock                          Lock model for generation. Can't change model.
	noinit                        No New() init function for struct
	enum                          Generate swift enum
	optimize                      Try to create golang padding optimized struct
	chengine=MergeTree            Optional, mergeTree by default
	debug                         Debug mode
	demo                          Generate demo json file with default values
	test                          Generate go tests for generated model
	gotiny                        Create gotiny marshal/unmarshal
	js=NewsJson                   Change json struct names. Ex: js=NewsJson
	noprefix                      Generate simple index IndexID instead IndexNewsID

	Field:
	title{}                       Add custom title for index. title{Nice & Sweet}
	desc{}                        Add custom desc for index. desc{This field for success}

	GO:
	nofunc                        Do not create any jsons functions for fiels
	up                            Make uppercase for functions
	base=""                       Underlying type for custom type (detected automatically). Ex: role role.Role // go{base="int"}
	name=""                       Replace golang struct name. Ex: name="NewsList"
	desc=""                       Add custom desc for index
	title{}                       Add field user title title{Nice}
	desc{}                        Add field user title desc{Use it nice}
	#                             Add lists for fields. Ex: id int //#readonly #must...
	must                          Create one validation function for all must fields
	type=""                       Replace golang type. Ex: type="[]*News"
	title=""                      Add custom title for index
	req{}                         Add user required fields
	@                             Add custom structs for fields. Ex: id int //@public @team...

	SQL:
	index                         Create simple default index or gin for jsonb and arrays
	type="date"                   Explicit postgres type: date, timestamp, time, interval, varchar(64), numeric(18,2), json, vector(3)...
	defaults                      Add default value based on field type
	noinsert                      Do use field for insert function
	search                        Add tsvector index by group. Ex: search="tsv": tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || brand)). For search: SELECT brand, title FROM assets WHERE search @@ to_tsquery('english', 'f8');
	type="jsonb"                  Rewrite sql type for field. Ex: type="jsonb"
	add="primary key"             Append sql for field. Ex: replace="primary key"
	renames="oldname"             Create an alter table record in SQL file. Rename table column.
	primary                       Primary key. One field: asset bigint primary key. Several fields: primary key (asset, user)
	altertable                    Add field line Alter table to SQL file with current time comment
	idx                           Add index fields by group name. Ex: idx="nameIndex" idx="credsIndex"
	skip                          Skip field for sql queries
	unique="groupname"            Add unique fields constrains by group (you need set group name, then generator join fields). Ex: unique="group1" unique="group2"
	ver="v4"                      Create an alter table record in SQL file. Add new column. With new version in comment. Ex: ver="2"
	notnull                       Add not null to column
	unix                          Add default value: extract(epoch from now())
	replace="bigint primary key"  Rewrite sql for field. Ex: replace="bigint primary key"

	Clickhouse:
	low                           ch{low} Wrap field type in LowCardinality(...)
	type=ip                       ch{type=ip} Use ClickHouse IPv4 type for IP address fields
	primary"                      ch{primary} Make primary key

	PROTO:
	type="string"                 Rewrite proto type for field. Ex: type="google.protobuf.Value"
	name="field"                  Rewrite proto field name
	skip                          Skip field for proto

	SWIFT:
	must                          Swift field with required values
	skip                          Ignore field for Swift
	type=""                       Replace type for Swift model. Ex: type="string"
	file=""                       Create another swift file for this field Swift model. file="f1", file="f2"

	JSON:
	time                          Create convert jsons function for unixtime fields
	raw                           Set Raw json function inside field
	name=""                       Replace json field name. Ex: name="sid"
	skip                          Skip json field
	inc                           Add inc jsons function for numbers fields
	bool                          Add set jsons function for bool fields
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

// Parse []any to ID struct
func ParseStatToStruct(r []any) (a Stat) {

	for pos, x := range r {
		switch StatIndexTyped(pos) {
		case StatIndexID:
			cast.Convert(&a.ID, x) //int
		case StatIndexUID:
			cast.Convert(&a.UID, x) //int
		case StatIndexType:
			cast.Convert(&a.Type, x) //int
		case StatIndexName:
			cast.Convert(&a.Name, x) //string
		case StatIndexSource:
			cast.Convert(&a.Source, x) //string
		case StatIndexIP:
			cast.Convert(&a.IP, x) //string
		case StatIndexImage:
			cast.Convert(&a.Image, x) //bool
		case StatIndexStack:
			switch v0 := x.(type) {
			case nil:
			case []string:
				a.Stack = v0
			case []any:
				r0 := make([]string, 0, len(v0))
				for _, e0 := range v0 {
					var el0 string
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Stack = r0
			default:
				cast.Convert(&a.Stack, v0)
			} //[]string
		case StatIndexUints:
			switch v0 := x.(type) {
			case nil:
			case []uint:
				a.Uints = v0
			case []any:
				r0 := make([]uint, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint
					if n, ok := e0.(interface{ MarshalJSON() ([]byte, error) }); ok {
						b, _ := n.MarshalJSON()
						cast.Convert(&el0, string(b))
					} else {
						cast.Convert(&el0, e0)
					}
					r0 = append(r0, el0)
				}
				a.Uints = r0
			default:
				cast.Convert(&a.Uints, v0)
			} //[]uint
		case StatIndexUints8:
			cast.Convert(&a.Uints8, x) //[]uint8
		case StatIndexUints16:
			switch v0 := x.(type) {
			case nil:
			case []uint16:
				a.Uints16 = v0
			case []any:
				r0 := make([]uint16, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint16
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Uints16 = r0
			default:
				cast.Convert(&a.Uints16, v0)
			} //[]uint16
		case StatIndexUints32:
			switch v0 := x.(type) {
			case nil:
			case []uint32:
				a.Uints32 = v0
			case []any:
				r0 := make([]uint32, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint32
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Uints32 = r0
			default:
				cast.Convert(&a.Uints32, v0)
			} //[]uint32
		case StatIndexUints64:
			switch v0 := x.(type) {
			case nil:
			case []uint64:
				a.Uints64 = v0
			case []any:
				r0 := make([]uint64, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint64
					if n, ok := e0.(interface{ MarshalJSON() ([]byte, error) }); ok {
						b, _ := n.MarshalJSON()
						cast.Convert(&el0, string(b))
					} else {
						cast.Convert(&el0, e0)
					}
					r0 = append(r0, el0)
				}
				a.Uints64 = r0
			default:
				cast.Convert(&a.Uints64, v0)
			} //[]uint64
		case StatIndexInts8:
			switch v0 := x.(type) {
			case nil:
			case []int8:
				a.Ints8 = v0
			case []any:
				r0 := make([]int8, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int8
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Ints8 = r0
			default:
				cast.Convert(&a.Ints8, v0)
			} //[]int8
		case StatIndexInts16:
			switch v0 := x.(type) {
			case nil:
			case []int16:
				a.Ints16 = v0
			case []any:
				r0 := make([]int16, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int16
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Ints16 = r0
			default:
				cast.Convert(&a.Ints16, v0)
			} //[]int16
		case StatIndexInts32:
			switch v0 := x.(type) {
			case nil:
			case []int32:
				a.Ints32 = v0
			case []any:
				r0 := make([]int32, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int32
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Ints32 = r0
			default:
				cast.Convert(&a.Ints32, v0)
			} //[]int32
		case StatIndexInts64:
			switch v0 := x.(type) {
			case nil:
			case []int64:
				a.Ints64 = v0
			case []any:
				r0 := make([]int64, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int64
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Ints64 = r0
			default:
				cast.Convert(&a.Ints64, v0)
			} //[]int64
		}
	}
	return
}

// Tuple create an array from struct
func (a *Stat) Tuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, a.IP, a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// Tuple create an array from struct
func (a *Stat) sqlTuple() (r []any) {
	return []any{a.UID, a.Type, a.Name, a.Source, a.IP, a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// Tuple create an array from struct
func (a *Stat) sqlAllTuples() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, a.IP, a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// Tuple create an array from struct for clickhouse
func (a *Stat) clickhouseTuple() (r []any) {
	return []any{a.ID, a.UID, a.Type, a.Name, a.Source, net.ParseIP(a.IP), a.Image, a.Stack, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Ints8, a.Ints16, a.Ints32, a.Ints64}
}

// update struct with function
func (a *Stat) Update(k string, x any) {
	switch k {
	case "id":
		cast.Convert(&a.ID, x) //int
	case "uid":
		cast.Convert(&a.UID, x) //int
	case "type":
		cast.Convert(&a.Type, x) //int
	case "name":
		cast.Convert(&a.Name, x) //string
	case "source":
		cast.Convert(&a.Source, x) //string
	case "ip":
		cast.Convert(&a.IP, x) //string
	case "image":
		cast.Convert(&a.Image, x) //bool
	case "stack":
		switch v0 := x.(type) {
		case nil:
		case []string:
			a.Stack = v0
		case []any:
			r0 := make([]string, 0, len(v0))
			for _, e0 := range v0 {
				var el0 string
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Stack = r0
		default:
			cast.Convert(&a.Stack, v0)
		} //[]string
	case "uints":
		switch v0 := x.(type) {
		case nil:
		case []uint:
			a.Uints = v0
		case []any:
			r0 := make([]uint, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint
				if n, ok := e0.(interface{ MarshalJSON() ([]byte, error) }); ok {
					b, _ := n.MarshalJSON()
					cast.Convert(&el0, string(b))
				} else {
					cast.Convert(&el0, e0)
				}
				r0 = append(r0, el0)
			}
			a.Uints = r0
		default:
			cast.Convert(&a.Uints, v0)
		} //[]uint
	case "uints8":
		cast.Convert(&a.Uints8, x) //[]uint8
	case "uints16":
		switch v0 := x.(type) {
		case nil:
		case []uint16:
			a.Uints16 = v0
		case []any:
			r0 := make([]uint16, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint16
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Uints16 = r0
		default:
			cast.Convert(&a.Uints16, v0)
		} //[]uint16
	case "uints32":
		switch v0 := x.(type) {
		case nil:
		case []uint32:
			a.Uints32 = v0
		case []any:
			r0 := make([]uint32, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint32
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Uints32 = r0
		default:
			cast.Convert(&a.Uints32, v0)
		} //[]uint32
	case "uints64":
		switch v0 := x.(type) {
		case nil:
		case []uint64:
			a.Uints64 = v0
		case []any:
			r0 := make([]uint64, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint64
				if n, ok := e0.(interface{ MarshalJSON() ([]byte, error) }); ok {
					b, _ := n.MarshalJSON()
					cast.Convert(&el0, string(b))
				} else {
					cast.Convert(&el0, e0)
				}
				r0 = append(r0, el0)
			}
			a.Uints64 = r0
		default:
			cast.Convert(&a.Uints64, v0)
		} //[]uint64
	case "ints8":
		switch v0 := x.(type) {
		case nil:
		case []int8:
			a.Ints8 = v0
		case []any:
			r0 := make([]int8, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int8
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Ints8 = r0
		default:
			cast.Convert(&a.Ints8, v0)
		} //[]int8
	case "ints16":
		switch v0 := x.(type) {
		case nil:
		case []int16:
			a.Ints16 = v0
		case []any:
			r0 := make([]int16, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int16
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Ints16 = r0
		default:
			cast.Convert(&a.Ints16, v0)
		} //[]int16
	case "ints32":
		switch v0 := x.(type) {
		case nil:
		case []int32:
			a.Ints32 = v0
		case []any:
			r0 := make([]int32, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int32
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Ints32 = r0
		default:
			cast.Convert(&a.Ints32, v0)
		} //[]int32
	case "ints64":
		switch v0 := x.(type) {
		case nil:
		case []int64:
			a.Ints64 = v0
		case []any:
			r0 := make([]int64, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int64
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Ints64 = r0
		default:
			cast.Convert(&a.Ints64, v0)
		} //[]int64
	}
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

// Struct to json
func (a *Stat) ToJson() (r []byte) {
	js := jsons.Create().
		Add(StatFieldID, a.ID).
		Add(StatFieldUID, a.UID).
		Add(StatFieldType, a.Type).
		Add(StatFieldName, a.Name).
		Add(StatFieldSource, a.Source).
		Add(StatFieldIP, a.IP).
		Add(StatFieldImage, a.Image).
		Add(StatFieldStack, a.Stack).
		Add(StatFieldUints, a.Uints).
		Add(StatFieldUints8, a.Uints8).
		Add(StatFieldUints16, a.Uints16).
		Add(StatFieldUints32, a.Uints32).
		Add(StatFieldUints64, a.Uints64).
		Add(StatFieldInts8, a.Ints8).
		Add(StatFieldInts16, a.Ints16).
		Add(StatFieldInts32, a.Ints32).
		Add(StatFieldInts64, a.Ints64)
	return js.Bytes()
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

// Parse []any to json
func ParseTupleToStatJson(r []any) (a StatJson) {
	for pos, x := range r {
		switch StatIndexTyped(pos) {
		case StatIndexID:
			a.Set(StatFieldID, x)
		case StatIndexUID:
			a.Set(StatFieldUID, x)
		case StatIndexType:
			a.Set(StatFieldType, x)
		case StatIndexName:
			a.Set(StatFieldName, x)
		case StatIndexSource:
			a.Set(StatFieldSource, x)
		case StatIndexIP:
			a.Set(StatFieldIP, x)
		case StatIndexImage:
			a.Set(StatFieldImage, x)
		case StatIndexStack:
			a.Set(StatFieldStack, x)
		case StatIndexUints:
			a.Set(StatFieldUints, x)
		case StatIndexUints8:
			a.Set(StatFieldUints8, x)
		case StatIndexUints16:
			a.Set(StatFieldUints16, x)
		case StatIndexUints32:
			a.Set(StatFieldUints32, x)
		case StatIndexUints64:
			a.Set(StatFieldUints64, x)
		case StatIndexInts8:
			a.Set(StatFieldInts8, x)
		case StatIndexInts16:
			a.Set(StatFieldInts16, x)
		case StatIndexInts32:
			a.Set(StatFieldInts32, x)
		case StatIndexInts64:
			a.Set(StatFieldInts64, x)
		}
	}
	return
}

// NewStatJson create struct
func NewStatJson() StatJson {
	return []byte("{}")
}

// StatJson is a struct
type StatJson []byte

// Set value
func (a *StatJson) Set(k string, v any) *StatJson {
	(*a) = jsons.Set((*a), k, v)
	return a
}

// Get value
func (a *StatJson) Get(k string) jsons.Result {
	return jsons.Get((*a), k)
}

// Get value
func (a *StatJson) DeleteFields(fields ...string) {
	(*a) = jsons.Delete((*a), fields...)
}

// ID set or get value
func (a *StatJson) ID(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), StatFieldID)
	}
	a.Set(StatFieldID, v[0])
	return
}

// UID set or get value
func (a *StatJson) UID(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), StatFieldUID)
	}
	a.Set(StatFieldUID, v[0])
	return
}

// Type set or get value
func (a *StatJson) Type(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), StatFieldType)
	}
	a.Set(StatFieldType, v[0])
	return
}

// Name set or get value
func (a *StatJson) Name(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), StatFieldName)
	}
	a.Set(StatFieldName, v[0])
	return
}

// Source set or get value
func (a *StatJson) Source(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), StatFieldSource)
	}
	a.Set(StatFieldSource, v[0])
	return
}

// IP set or get value
func (a *StatJson) IP(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), StatFieldIP)
	}
	a.Set(StatFieldIP, v[0])
	return
}

// Image set or get value
func (a *StatJson) Image(v ...bool) (res bool) {
	if v == nil {
		return jsons.Bool((*a), StatFieldImage)
	}
	a.Set(StatFieldImage, v[0])
	return
}

// Stack set or get value
func (a *StatJson) Stack(v ...string) (res []string) {
	if v == nil {
		return jsons.ArrayString((*a), StatFieldStack)
	}
	a.Set(StatFieldStack, v)
	return
}

// StackAdd add values
func (a *StatJson) StackAdd(v ...string) {
	a.Set(StatFieldStack, append(a.Stack(), v...))
}

// StackPrepend add values
func (a *StatJson) StackPrepend(v ...string) {
	a.Set(StatFieldStack, append(v, a.Stack()...))
}

// StackAddUnique add unique values only
func (a *StatJson) StackAddUnique(v ...string) {
	var list []string
	un := map[string]bool{}
	for _, x := range a.Stack() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Stack(list...)
}

// StackDelete add unique values only
func (a *StatJson) StackDelete(v string) {
	var list []string
	for _, x := range a.Stack() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Stack(list...)
}

// Uints set or get value
func (a *StatJson) Uints(v ...uint) (res []uint) {
	if v == nil {
		return jsons.ArrayUint((*a), StatFieldUints)
	}
	a.Set(StatFieldUints, v)
	return
}

// UintsAdd add values
func (a *StatJson) UintsAdd(v ...uint) {
	a.Set(StatFieldUints, append(a.Uints(), v...))
}

// UintsPrepend add values
func (a *StatJson) UintsPrepend(v ...uint) {
	a.Set(StatFieldUints, append(v, a.Uints()...))
}

// UintsAddUnique add unique values only
func (a *StatJson) UintsAddUnique(v ...uint) {
	var list []uint
	un := map[uint]bool{}
	for _, x := range a.Uints() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Uints(list...)
}

// UintsDelete add unique values only
func (a *StatJson) UintsDelete(v uint) {
	var list []uint
	for _, x := range a.Uints() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Uints(list...)
}

// Uints8 set or get value
func (a *StatJson) Uints8(v ...uint8) (res []uint8) {
	if v == nil {
		return jsons.ArrayUint8((*a), StatFieldUints8)
	}
	a.Set(StatFieldUints8, v)
	return
}

// Uints8Add add values
func (a *StatJson) Uints8Add(v ...uint8) {
	a.Set(StatFieldUints8, append(a.Uints8(), v...))
}

// Uints8Prepend add values
func (a *StatJson) Uints8Prepend(v ...uint8) {
	a.Set(StatFieldUints8, append(v, a.Uints8()...))
}

// Uints8AddUnique add unique values only
func (a *StatJson) Uints8AddUnique(v ...uint8) {
	var list []uint8
	un := map[uint8]bool{}
	for _, x := range a.Uints8() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Uints8(list...)
}

// Uints8Delete add unique values only
func (a *StatJson) Uints8Delete(v uint8) {
	var list []uint8
	for _, x := range a.Uints8() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Uints8(list...)
}

// Uints16 set or get value
func (a *StatJson) Uints16(v ...uint16) (res []uint16) {
	if v == nil {
		return jsons.ArrayUint16((*a), StatFieldUints16)
	}
	a.Set(StatFieldUints16, v)
	return
}

// Uints16Add add values
func (a *StatJson) Uints16Add(v ...uint16) {
	a.Set(StatFieldUints16, append(a.Uints16(), v...))
}

// Uints16Prepend add values
func (a *StatJson) Uints16Prepend(v ...uint16) {
	a.Set(StatFieldUints16, append(v, a.Uints16()...))
}

// Uints16AddUnique add unique values only
func (a *StatJson) Uints16AddUnique(v ...uint16) {
	var list []uint16
	un := map[uint16]bool{}
	for _, x := range a.Uints16() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Uints16(list...)
}

// Uints16Delete add unique values only
func (a *StatJson) Uints16Delete(v uint16) {
	var list []uint16
	for _, x := range a.Uints16() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Uints16(list...)
}

// Uints32 set or get value
func (a *StatJson) Uints32(v ...uint32) (res []uint32) {
	if v == nil {
		return jsons.ArrayUint32((*a), StatFieldUints32)
	}
	a.Set(StatFieldUints32, v)
	return
}

// Uints32Add add values
func (a *StatJson) Uints32Add(v ...uint32) {
	a.Set(StatFieldUints32, append(a.Uints32(), v...))
}

// Uints32Prepend add values
func (a *StatJson) Uints32Prepend(v ...uint32) {
	a.Set(StatFieldUints32, append(v, a.Uints32()...))
}

// Uints32AddUnique add unique values only
func (a *StatJson) Uints32AddUnique(v ...uint32) {
	var list []uint32
	un := map[uint32]bool{}
	for _, x := range a.Uints32() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Uints32(list...)
}

// Uints32Delete add unique values only
func (a *StatJson) Uints32Delete(v uint32) {
	var list []uint32
	for _, x := range a.Uints32() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Uints32(list...)
}

// Uints64 set or get value
func (a *StatJson) Uints64(v ...uint64) (res []uint64) {
	if v == nil {
		return jsons.ArrayUint64((*a), StatFieldUints64)
	}
	a.Set(StatFieldUints64, v)
	return
}

// Uints64Add add values
func (a *StatJson) Uints64Add(v ...uint64) {
	a.Set(StatFieldUints64, append(a.Uints64(), v...))
}

// Uints64Prepend add values
func (a *StatJson) Uints64Prepend(v ...uint64) {
	a.Set(StatFieldUints64, append(v, a.Uints64()...))
}

// Uints64AddUnique add unique values only
func (a *StatJson) Uints64AddUnique(v ...uint64) {
	var list []uint64
	un := map[uint64]bool{}
	for _, x := range a.Uints64() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Uints64(list...)
}

// Uints64Delete add unique values only
func (a *StatJson) Uints64Delete(v uint64) {
	var list []uint64
	for _, x := range a.Uints64() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Uints64(list...)
}

// Ints8 set or get value
func (a *StatJson) Ints8(v ...int8) (res []int8) {
	if v == nil {
		return jsons.ArrayInt8((*a), StatFieldInts8)
	}
	a.Set(StatFieldInts8, v)
	return
}

// Ints8Add add values
func (a *StatJson) Ints8Add(v ...int8) {
	a.Set(StatFieldInts8, append(a.Ints8(), v...))
}

// Ints8Prepend add values
func (a *StatJson) Ints8Prepend(v ...int8) {
	a.Set(StatFieldInts8, append(v, a.Ints8()...))
}

// Ints8AddUnique add unique values only
func (a *StatJson) Ints8AddUnique(v ...int8) {
	var list []int8
	un := map[int8]bool{}
	for _, x := range a.Ints8() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Ints8(list...)
}

// Ints8Delete add unique values only
func (a *StatJson) Ints8Delete(v int8) {
	var list []int8
	for _, x := range a.Ints8() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Ints8(list...)
}

// Ints16 set or get value
func (a *StatJson) Ints16(v ...int16) (res []int16) {
	if v == nil {
		return jsons.ArrayInt16((*a), StatFieldInts16)
	}
	a.Set(StatFieldInts16, v)
	return
}

// Ints16Add add values
func (a *StatJson) Ints16Add(v ...int16) {
	a.Set(StatFieldInts16, append(a.Ints16(), v...))
}

// Ints16Prepend add values
func (a *StatJson) Ints16Prepend(v ...int16) {
	a.Set(StatFieldInts16, append(v, a.Ints16()...))
}

// Ints16AddUnique add unique values only
func (a *StatJson) Ints16AddUnique(v ...int16) {
	var list []int16
	un := map[int16]bool{}
	for _, x := range a.Ints16() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Ints16(list...)
}

// Ints16Delete add unique values only
func (a *StatJson) Ints16Delete(v int16) {
	var list []int16
	for _, x := range a.Ints16() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Ints16(list...)
}

// Ints32 set or get value
func (a *StatJson) Ints32(v ...int32) (res []int32) {
	if v == nil {
		return jsons.ArrayInt32((*a), StatFieldInts32)
	}
	a.Set(StatFieldInts32, v)
	return
}

// Ints32Add add values
func (a *StatJson) Ints32Add(v ...int32) {
	a.Set(StatFieldInts32, append(a.Ints32(), v...))
}

// Ints32Prepend add values
func (a *StatJson) Ints32Prepend(v ...int32) {
	a.Set(StatFieldInts32, append(v, a.Ints32()...))
}

// Ints32AddUnique add unique values only
func (a *StatJson) Ints32AddUnique(v ...int32) {
	var list []int32
	un := map[int32]bool{}
	for _, x := range a.Ints32() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Ints32(list...)
}

// Ints32Delete add unique values only
func (a *StatJson) Ints32Delete(v int32) {
	var list []int32
	for _, x := range a.Ints32() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Ints32(list...)
}

// Ints64 set or get value
func (a *StatJson) Ints64(v ...int64) (res []int64) {
	if v == nil {
		return jsons.ArrayInt64((*a), StatFieldInts64)
	}
	a.Set(StatFieldInts64, v)
	return
}

// Ints64Add add values
func (a *StatJson) Ints64Add(v ...int64) {
	a.Set(StatFieldInts64, append(a.Ints64(), v...))
}

// Ints64Prepend add values
func (a *StatJson) Ints64Prepend(v ...int64) {
	a.Set(StatFieldInts64, append(v, a.Ints64()...))
}

// Ints64AddUnique add unique values only
func (a *StatJson) Ints64AddUnique(v ...int64) {
	var list []int64
	un := map[int64]bool{}
	for _, x := range a.Ints64() {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}
	for _, x := range v {
		if un[x] {
			continue
		}
		list = append(list, x)
		un[x] = true
	}

	a.Ints64(list...)
}

// Ints64Delete add unique values only
func (a *StatJson) Ints64Delete(v int64) {
	var list []int64
	for _, x := range a.Ints64() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Ints64(list...)
}

// sql StatSQL class
type StatSQL struct{ pool *pgxpool.Pool }

func NewStatSQL(pool *pgxpool.Pool) (a *StatSQL) {
	a = new(StatSQL)
	a.pool = pool
	a.CreateTable()
	return
}

func ConnStatSQL(pool *pgxpool.Pool) (a *StatSQL) {
	return &StatSQL{pool}
}

//new conn

func (a *StatSQL) Conn(f func(conn *pgxpool.Conn) (err error)) (err error) {

	conn, err := a.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer conn.Release()
	return f(conn)
}

// parse sql query
func (a *StatSQL) TableName() (res string) {
	return "stat"
}

func (a *StatSQL) Count(where ...string) (count int, err error) {

	var q string

	switch len(where) {
	case 0:
		q = "select count(*) from stat"
	default:
		q = "select count(*) from stat where " + strings.Join(where, " ")
	}

	err = a.Conn(func(conn *pgxpool.Conn) (err error) {
		return conn.QueryRow(context.Background(), q).Scan(&count)
	})
	return
}

// Insert struct and return int id
func (a *StatSQL) Insert(v Stat) (id int, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into stat (uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) returning id"
		err = conn.QueryRow(c, q, v.sqlTuple()...).Scan(&id)
		return
	})
	return
}

// Insert struct and return int id
func (a *StatSQL) InsertNoConflict(v *Stat) (id int, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into stat (uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) on conflict do nothing returning id"
		err = conn.QueryRow(c, q, v.sqlTuple()...).Scan(&id)
		return
	})
	return
}

// Insert full struct ID must be (ignore all skips)
func (a *StatSQL) InsertFull(v Stat) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into stat (id, uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
		_, err = conn.Exec(c, q, v.sqlAllTuples()...)
		return
	})
	return
}

// update serial counter (count all items, add 1 and plus custom int)
// useful if you insert with ID
func (a *StatSQL) ReindexIDSerialCounter(add ...int) (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		plus := 1
		if len(add) > 0 {
			plus = add[0]
		}
		_, err = conn.Exec(context.Background(), "SELECT setval(pg_get_serial_sequence('stat', 'id'), COALESCE((SELECT MAX(id) FROM stat), 0) + $1, false)", plus)
		return
	})
}

// set serial counter
func (a *StatSQL) SetIDSerialCounter(value int) (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "SELECT setval(pg_get_serial_sequence('stat', 'id'), $1, false)", value)
		return
	})
}

// parse sql query
func (a *StatSQL) Get(id any, fields ...StatIndexTyped) (res Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		q := fmt.Sprintf("select %s ", fieldlist)
		q = q + "from stat where id = $1 limit 1"
		rows, err := conn.Query(c, q, id)
		if err != nil {
			return
		}
		defer rows.Close()
		if !rows.Next() {
			err = errors.New("not found")
			return
		}
		v, err := rows.Values()
		if err != nil {
			return
		}
		if len(v) != len(fields) {
			err = errors.New("len")
			return
		}
		for pos, x := range fields {
			res.Update(x.String(), v[pos])
		}
		err = rows.Err()
		return
	})
	return
}

// parse sql query
func (a *StatSQL) GetWhere(where string, fields ...StatIndexTyped) (res Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		q := fmt.Sprintf("select %s", fieldlist)
		q = q + " from stat"
		q = q + fmt.Sprintf(" where %s ", where)
		q = q + " limit 1"
		rows, err := conn.Query(c, q)
		if err != nil {
			return
		}
		defer rows.Close()
		if !rows.Next() {
			err = errors.New("not found")
			return
		}
		v, err := rows.Values()
		if err != nil {
			return
		}
		if len(v) != len(fields) {
			err = errors.New("len")
			return
		}
		for pos, x := range fields {
			res.Update(x.String(), v[pos])
		}
		err = rows.Err()
		return
	})
	return
}

// parse sql query
func (a *StatSQL) Row(eq map[string]any, fields ...StatIndexTyped) (res Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		var andlist []string
		var values []any
		var count int
		for k, v := range eq {
			if !StatValidKey(k) {
				err = errors.New(k)
				return
			}
			in := StatKeyIndex(k)
			count++
			andlist = append(andlist, fmt.Sprintf("%s = $%d", in.SQLName(), count))
			values = append(values, v)
		}
		if len(andlist) == 0 {
			err = errors.New("emptykeys")
			return
		}
		keys := strings.Join(andlist, " and ")

		q := fmt.Sprintf("select %s from %s where %s limit 1", fieldlist, a.TableName(), keys)
		rows, err := conn.Query(c, q, values...)
		if err != nil {
			return
		}
		defer rows.Close()
		if !rows.Next() {
			err = errors.New("not found")
			return
		}
		v, err := rows.Values()
		if err != nil {
			return
		}
		if len(v) != len(fields) {
			err = errors.New("len")
			return
		}
		for pos, x := range fields {
			res.Update(x.String(), v[pos])
		}
		err = rows.Err()
		return
	})
	return
}

// get simple list from db
func (a *StatSQL) All(fields ...StatIndexTyped) (res []*Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		q := fmt.Sprintf("select %s", fieldlist)
		q += " from stat"
		rows, err := conn.Query(c, q)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Stat
			v, err := rows.Values()
			if err != nil {
				continue
			}
			if len(v) != len(fields) {
				continue
			}
			for pos, x := range fields {
				item.Update(x.String(), v[pos])
			}
			res = append(res, &item)
		}
		err = rows.Err()

		return
	})
	return
}

// get simple list from db
func (a *StatSQL) List(limit, offset int, fields ...StatIndexTyped) (res []Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		q := fmt.Sprintf("select %s", fieldlist)
		q += " from stat limit $1 offset $2"
		rows, err := conn.Query(c, q, limit, offset)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Stat
			v, err := rows.Values()
			if err != nil {
				continue
			}
			if len(v) != len(fields) {
				continue
			}
			for pos, x := range fields {
				item.Update(x.String(), v[pos])
			}
			res = append(res, item)
		}
		err = rows.Err()

		return
	})
	return
}

// update sql query
func (a *StatSQL) Update(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if !StatValidKey(k) {
			return errors.New("invalid key")
		}
		in := StatKeyIndex(k)
		q := fmt.Sprintf("update stat set %s = $1 where id = $2", in.SQLName())
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// update sql query
func (a *StatSQL) UpdateWhere(k string, v any, where string) (err error) {
	if !StatValidKey(k) {
		return errors.New("invalid key")
	}
	in := StatKeyIndex(k)
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		q := fmt.Sprintf("update %s set %s = $1 where %s", a.TableName(), in.SQLName(), where)
		_, err = conn.Exec(context.Background(), q, v)
		return
	})
}

// update sql query
func (a *StatSQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if keys == nil {
			return errors.New("emptykeys")
		}

		var fields []string
		var values []any
		var count int
		for k, v := range keys {
			if !StatValidKey(k) {
				return errors.New(k)
			}
			in := StatKeyIndex(k)
			count++
			fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
			values = append(values, v)
		}

		list := strings.Join(fields, ", ")
		count++
		values = append(values, id)

		q := fmt.Sprintf("update stat set %s where id = $%d", list, count)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, values...)
		return
	})
	return
}

// update sql query
func (a *StatSQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if keys == nil {
			return errors.New("emptykeys")
		}

		var fields []string
		var values []any
		var count int
		for k, v := range keys {
			if !StatValidKey(k) {
				return errors.New(k)
			}
			in := StatKeyIndex(k)
			count++
			fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
			values = append(values, v)
		}

		list := strings.Join(fields, ", ")
		count++

		where = fmt.Sprintf(where, args...)
		q := fmt.Sprintf("update stat set %s where %s", list, where)
		_, err = conn.Exec(c, q, values...)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddStack(id any, v ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set stack = stack || $1::text[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteStack(id any, v string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set stack = array_remove(stack, $1::text) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddStackWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set stack = array_append(stack, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteStackWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set stack = array_remove(stack, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddUints(id any, v ...uint) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints = uints || $1::numeric(20,0)[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints(id any, v uint, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints = array_remove(uints, $1::numeric(20,0)) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddUintsWhere(v uint, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints = array_append(uints, $1::numeric(20,0)) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUintsWhere(v uint, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints = array_remove(uints, $1::numeric(20,0)) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddUints16(id any, v ...uint16) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints16 = uints16 || $1::integer[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints16(id any, v uint16, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints16 = array_remove(uints16, $1::integer) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddUints16Where(v uint16, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints16 = array_append(uints16, $1::integer) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints16Where(v uint16, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints16 = array_remove(uints16, $1::integer) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddUints32(id any, v ...uint32) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints32 = uints32 || $1::bigint[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints32(id any, v uint32, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints32 = array_remove(uints32, $1::bigint) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddUints32Where(v uint32, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints32 = array_append(uints32, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints32Where(v uint32, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints32 = array_remove(uints32, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddUints64(id any, v ...uint64) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints64 = uints64 || $1::numeric(20,0)[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints64(id any, v uint64, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints64 = array_remove(uints64, $1::numeric(20,0)) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddUints64Where(v uint64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints64 = array_append(uints64, $1::numeric(20,0)) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteUints64Where(v uint64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set uints64 = array_remove(uints64, $1::numeric(20,0)) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddInts8(id any, v ...int8) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints8 = ints8 || $1::smallint[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts8(id any, v int8, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints8 = array_remove(ints8, $1::smallint) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddInts8Where(v int8, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints8 = array_append(ints8, $1::smallint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts8Where(v int8, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints8 = array_remove(ints8, $1::smallint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddInts16(id any, v ...int16) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints16 = ints16 || $1::smallint[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts16(id any, v int16, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints16 = array_remove(ints16, $1::smallint) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddInts16Where(v int16, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints16 = array_append(ints16, $1::smallint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts16Where(v int16, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints16 = array_remove(ints16, $1::smallint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddInts32(id any, v ...int32) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints32 = ints32 || $1::integer[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts32(id any, v int32, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints32 = array_remove(ints32, $1::integer) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddInts32Where(v int32, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints32 = array_append(ints32, $1::integer) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts32Where(v int32, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints32 = array_remove(ints32, $1::integer) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *StatSQL) AddInts64(id any, v ...int64) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints64 = ints64 || $1::bigint[] where id = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts64(id any, v int64, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints64 = array_remove(ints64, $1::bigint) where id = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *StatSQL) AddInts64Where(v int64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints64 = array_append(ints64, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *StatSQL) DeleteInts64Where(v int64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update stat set ints64 = array_remove(ints64, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

type StatQuery struct {
	Limit  int                 `json:"limit,omitempty"`
	Offset int                 `json:"offset,omitempty"`
	Sort   string              `json:"sort,omitempty"`
	Desc   bool                `json:"desc,omitempty"`
	EQ     map[string]any      `json:"eq,omitempty"`   //equal
	GT     map[string]any      `json:"gt,omitempty"`   //greater then...
	LT     map[string]any      `json:"lt,omitempty"`   //less then
	NOT    map[string][]any    `json:"not,omitempty"`  //less then
	Like   map[string]string   `json:"like,omitempty"` //full text search
	Custom string              `json:"-"`              //append unsafe where condition
	Fields []string            `json:"fields,omitempty"`
	IN     map[string][]int    `json:"in,omitempty"`
	INS    map[string][]string `json:"ins,omitempty"`
	INSQL  map[string]string   `json:"insql,omitempty"` //unsafe in (condition)
}

func (a *StatQuery) Render() (sql string, fields []StatIndexTyped, values []any) {

	switch len(a.Fields) == 0 {
	case true:
		fields = []StatIndexTyped{StatIndexID, StatIndexUID, StatIndexType, StatIndexName, StatIndexSource, StatIndexIP, StatIndexImage, StatIndexStack, StatIndexUints, StatIndexUints8, StatIndexUints16, StatIndexUints32, StatIndexUints64, StatIndexInts8, StatIndexInts16, StatIndexInts32, StatIndexInts64}
	default:
		for _, x := range a.Fields {
			if !StatValidKey(x) {
				continue
			}
			fields = append(fields, StatKeyIndex(x))
		}
	}

	var fieldsStrings []string
	for _, x := range fields {
		fieldsStrings = append(fieldsStrings, x.SQLName())
	}

	for k, v := range a.EQ {
		if fmt.Sprint(v) == "" {
			delete(a.EQ, k)
		}
	}
	for k, v := range a.GT {
		if fmt.Sprint(v) == "" {
			delete(a.GT, k)
		}
	}
	for k, v := range a.LT {
		if fmt.Sprint(v) == "" {
			delete(a.LT, k)
		}
	}
	for k, v := range a.NOT {
		if v == nil {
			delete(a.NOT, k)
		}
	}
	for k, v := range a.Like {
		if v == "" {
			delete(a.Like, k)
		}
	}

	// sql
	var list []string

	var count int

	// select
	list = append(list, "select")
	list = append(list, strings.Join(fieldsStrings, ", "))

	// from
	list = append(list, "from stat")

	var andlist []string

	// EQ where
	for k, v := range a.EQ {
		if !StatValidKey(k) {
			continue
		}
		p := StatKeyIndex(k)
		sqlName := p.SQLName()
		switch p.Type() {
		case "bool":
			switch cast.Bool(v) {
			case true:
				andlist = append(andlist, sqlName)
			case false:
				andlist = append(andlist, fmt.Sprintf("(not %[1]s or %[1]s is null)", sqlName))
			}

		default:
			count++
			andlist = append(andlist, fmt.Sprintf("%s = $%d", sqlName, count))
			values = append(values, v)
		}
	}

	// GT where
	for k, v := range a.GT {

		if !StatValidKey(k) {
			continue
		}
		sqlName := StatKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s > $%d", sqlName, count))
		values = append(values, v)
	}

	// LT where
	for k, v := range a.LT {
		if !StatValidKey(k) {
			continue
		}
		sqlName := StatKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s < $%d", sqlName, count))
		values = append(values, v)
	}

	// NOT where
	for k, v := range a.NOT {
		if !StatValidKey(k) {
			continue
		}
		sqlName := StatKeyIndex(k).SQLName()

		for _, x := range v {
			count++
			andlist = append(andlist, fmt.Sprintf("%s != $%d", sqlName, count))
			values = append(values, x)
		}

	}

	// LIKE where
	for k, v := range a.Like {
		if !StatValidKey(k) {
			continue
		}
		sqlName := StatKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s ilike $%d", sqlName, count))
		values = append(values, "%"+v+"%")
	}

	// IN where
	if a.IN != nil {
		for k, v := range a.IN {
			if !StatValidKey(k) {
				continue
			}
			sqlName := StatKeyIndex(k).SQLName()
			var inlist []string
			for _, num := range v {
				count++
				inlist = append(inlist, fmt.Sprintf("$%d", count))
				values = append(values, num)
			}
			if len(inlist) > 0 {
				andlist = append(andlist, fmt.Sprintf("%s in (%s)", sqlName, strings.Join(inlist, ",")))
			}
		}
	}

	// INS where
	if a.INS != nil {
		for k, v := range a.INS {
			if !StatValidKey(k) {
				continue
			}
			sqlName := StatKeyIndex(k).SQLName()
			var inlist []string
			for _, str := range v {
				count++
				inlist = append(inlist, fmt.Sprintf("$%d", count))
				values = append(values, str)
			}
			if len(inlist) > 0 {
				andlist = append(andlist, fmt.Sprintf("%s in (%s)", sqlName, strings.Join(inlist, ",")))
			}
		}
	}

	// INSQ where
	if a.INSQL != nil {
		for k, v := range a.INSQL {
			if !StatValidKey(k) {
				continue
			}
			sqlName := StatKeyIndex(k).SQLName()
			count++
			andlist = append(andlist, fmt.Sprintf("%s in (%s)", sqlName, v))
		}
	}

	if a.Custom != "" {
		andlist = append(andlist, a.Custom)
	}

	// render where
	if andlist != nil {
		list = append(list, "where")
		list = append(list, strings.Join(andlist, " and "))
	}

	// sort by
	if a.Sort != "" && StatValidKey(a.Sort) {
		list = append(list, fmt.Sprintf("order by %s", StatKeyIndex(a.Sort).SQLName()))
		if a.Desc {
			list = append(list, "desc")
		}
	}

	// limit, offset
	if a.Limit > 0 {
		list = append(list, fmt.Sprintf("limit %d", a.Limit))
	}
	if a.Offset > 0 {
		list = append(list, fmt.Sprintf("offset %d", a.Offset))
	}

	// render sql
	sql = strings.Join(list, " ")
	return
}

func NewStatQuery() *StatQuery {
	a := new(StatQuery)
	a.EQ = make(map[string]any)
	a.GT = make(map[string]any)
	a.LT = make(map[string]any)
	a.NOT = make(map[string][]any)
	a.Like = make(map[string]string)
	a.IN = make(map[string][]int)
	a.INS = make(map[string][]string)
	a.INSQL = make(map[string]string)
	return a
}

func (a *StatSQL) Search(q *StatQuery) (res []Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		sql, fields, values := q.Render()

		rows, err := conn.Query(c, sql, values...)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Stat
			v, err := rows.Values()
			if err != nil {
				continue
			}
			if len(v) != len(fields) {
				continue
			}

			for pos, x := range fields {
				item.Update(x.String(), v[pos])
			}
			res = append(res, item)
		}
		err = rows.Err()
		return
	})

	return
}

// select custom sql query
func (a *StatSQL) Select(where string, fields ...StatIndexTyped) (res []Stat, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if fields == nil {
			fields = a.Fields()
		}

		var list []string
		for _, x := range fields {
			list = append(list, x.SQLName())
		}
		fieldlist := strings.Join(list, ", ")

		q := fmt.Sprintf("select %s ", fieldlist)
		q = q + "from stat"
		if where != "" {
			q += " where " + where
		}
		rows, err := conn.Query(c, q)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item Stat
			v, err := rows.Values()
			if err != nil {
				continue
			}
			if len(v) != len(fields) {
				continue
			}

			for pos, x := range fields {
				item.Update(x.String(), v[pos])
			}
			res = append(res, item)
		}
		err = rows.Err()
		return
	})
	return
}

// delete item
func (a *StatSQL) Delete(id any) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		_, err = conn.Exec(c, "delete from stat where id = $1", id)
		return

	})
	return
}

// delete item where: i = 1 and w = 'nice'
func (a *StatSQL) DeleteWhere(where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		_, err = conn.Exec(c, fmt.Sprintf("delete from stat where %s", where))
		return

	})
	return
}

// has value in db
func (a *StatSQL) Has(field StatIndexTyped, v any) (has bool, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := fmt.Sprintf("select exists (select 1 from stat where %s = $1 limit 1)", field.SQLName())
		err = conn.QueryRow(c, q, v).Scan(&has)
		return
	})
	return
}

// Create table
func (a *StatSQL) CreateTable() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		q := `create table if not exists stat (
	id          bigserial primary key,
	uid         bigint,
	type        bigint,
	name        text,
	source      text,
	ip          text,
	image       boolean,
	stack       text[] default '{}',
	uints       numeric(20,0)[] default '{}',
	uints8      bytea,
	uints16     integer[] default '{}',
	uints32     bigint[] default '{}',
	uints64     numeric(20,0)[] default '{}',
	ints8       smallint[] default '{}',
	ints16      smallint[] default '{}',
	ints32      integer[] default '{}',
	ints64      bigint[] default '{}'
)
`
		_, err = conn.Exec(context.Background(), q)
		return
	})
}

// parse sql query
func (a *StatSQL) Fields() (res []StatIndexTyped) {
	return []StatIndexTyped{StatIndexID, StatIndexUID, StatIndexType, StatIndexName, StatIndexSource, StatIndexIP, StatIndexImage, StatIndexStack, StatIndexUints, StatIndexUints8, StatIndexUints16, StatIndexUints32, StatIndexUints64, StatIndexInts8, StatIndexInts16, StatIndexInts32, StatIndexInts64}
}

// begin transaction for TX methods:
//
//	tx, ctx, err := a.Begin()
//	defer tx.Rollback(ctx)
//	a.InsertTX(tx, ctx, item)
//	err = tx.Commit(ctx)
func (a *StatSQL) Begin() (tx pgx.Tx, ctx context.Context, err error) {
	ctx = context.Background()
	tx, err = a.pool.Begin(ctx)
	return
}

// CountTX transaction version of Count
func (a *StatSQL) CountTX(tx pgx.Tx, ctx context.Context, where ...string) (count int, err error) {

	var q string

	switch len(where) {
	case 0:
		q = "select count(*) from stat"
	default:
		q = "select count(*) from stat where " + strings.Join(where, " ")
	}
	err = tx.QueryRow(ctx, q).Scan(&count)
	return

}

// InsertTX transaction version of Insert
func (a *StatSQL) InsertTX(tx pgx.Tx, ctx context.Context, v Stat) (id int, err error) {

	q := "insert into stat (uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) returning id"
	err = tx.QueryRow(ctx, q, v.sqlTuple()...).Scan(&id)
	return

}

// InsertNoConflictTX transaction version of InsertNoConflict
func (a *StatSQL) InsertNoConflictTX(tx pgx.Tx, ctx context.Context, v *Stat) (id int, err error) {

	q := "insert into stat (uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) on conflict do nothing returning id"
	err = tx.QueryRow(ctx, q, v.sqlTuple()...).Scan(&id)
	return

}

// InsertFullTX transaction version of InsertFull
func (a *StatSQL) InsertFullTX(tx pgx.Tx, ctx context.Context, v Stat) (err error) {

	q := "insert into stat (id, uid, type, name, source, ip, image, stack, uints, uints8, uints16, uints32, uints64, ints8, ints16, ints32, ints64) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
	_, err = tx.Exec(ctx, q, v.sqlAllTuples()...)
	return

}

// ReindexIDSerialCounterTX transaction version of ReindexIDSerialCounter
func (a *StatSQL) ReindexIDSerialCounterTX(tx pgx.Tx, ctx context.Context, add ...int) (err error) {

	plus := 1
	if len(add) > 0 {
		plus = add[0]
	}
	_, err = tx.Exec(ctx, "SELECT setval(pg_get_serial_sequence('stat', 'id'), COALESCE((SELECT MAX(id) FROM stat), 0) + $1, false)", plus)
	return
}

// SetIDSerialCounterTX transaction version of SetIDSerialCounter
func (a *StatSQL) SetIDSerialCounterTX(tx pgx.Tx, ctx context.Context, value int) (err error) {

	_, err = tx.Exec(ctx, "SELECT setval(pg_get_serial_sequence('stat', 'id'), $1, false)", value)
	return
}

// GetTX transaction version of Get
func (a *StatSQL) GetTX(tx pgx.Tx, ctx context.Context, id any, fields ...StatIndexTyped) (res Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s ", fieldlist)
	q = q + "from stat where id = $1 limit 1"
	rows, err := tx.Query(ctx, q, id)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = errors.New("not found")
		return
	}
	v, err := rows.Values()
	if err != nil {
		return
	}
	if len(v) != len(fields) {
		err = errors.New("len")
		return
	}
	for pos, x := range fields {
		res.Update(x.String(), v[pos])
	}
	err = rows.Err()
	return

}

// GetWhereTX transaction version of GetWhere
func (a *StatSQL) GetWhereTX(tx pgx.Tx, ctx context.Context, where string, fields ...StatIndexTyped) (res Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q = q + " from stat"
	q = q + fmt.Sprintf(" where %s ", where)
	q = q + " limit 1"
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = errors.New("not found")
		return
	}
	v, err := rows.Values()
	if err != nil {
		return
	}
	if len(v) != len(fields) {
		err = errors.New("len")
		return
	}
	for pos, x := range fields {
		res.Update(x.String(), v[pos])
	}
	err = rows.Err()
	return

}

// RowTX transaction version of Row
func (a *StatSQL) RowTX(tx pgx.Tx, ctx context.Context, eq map[string]any, fields ...StatIndexTyped) (res Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	var andlist []string
	var values []any
	var count int
	for k, v := range eq {
		if !StatValidKey(k) {
			err = errors.New(k)
			return
		}
		in := StatKeyIndex(k)
		count++
		andlist = append(andlist, fmt.Sprintf("%s = $%d", in.SQLName(), count))
		values = append(values, v)
	}
	if len(andlist) == 0 {
		err = errors.New("emptykeys")
		return
	}
	keys := strings.Join(andlist, " and ")

	q := fmt.Sprintf("select %s from %s where %s limit 1", fieldlist, a.TableName(), keys)
	rows, err := tx.Query(ctx, q, values...)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		err = errors.New("not found")
		return
	}
	v, err := rows.Values()
	if err != nil {
		return
	}
	if len(v) != len(fields) {
		err = errors.New("len")
		return
	}
	for pos, x := range fields {
		res.Update(x.String(), v[pos])
	}
	err = rows.Err()
	return

}

// AllTX transaction version of All
func (a *StatSQL) AllTX(tx pgx.Tx, ctx context.Context, fields ...StatIndexTyped) (res []*Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q += " from stat"
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item Stat
		v, err := rows.Values()
		if err != nil {
			continue
		}
		if len(v) != len(fields) {
			continue
		}
		for pos, x := range fields {
			item.Update(x.String(), v[pos])
		}
		res = append(res, &item)
	}
	err = rows.Err()

	return

}

// ListTX transaction version of List
func (a *StatSQL) ListTX(tx pgx.Tx, ctx context.Context, limit, offset int, fields ...StatIndexTyped) (res []Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q += " from stat limit $1 offset $2"
	rows, err := tx.Query(ctx, q, limit, offset)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item Stat
		v, err := rows.Values()
		if err != nil {
			continue
		}
		if len(v) != len(fields) {
			continue
		}
		for pos, x := range fields {
			item.Update(x.String(), v[pos])
		}
		res = append(res, item)
	}
	err = rows.Err()

	return

}

// UpdateTX transaction version of Update
func (a *StatSQL) UpdateTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	if !StatValidKey(k) {
		return errors.New("invalid key")
	}
	in := StatKeyIndex(k)
	q := fmt.Sprintf("update stat set %s = $1 where id = $2", in.SQLName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// UpdateWhereTX transaction version of UpdateWhere
func (a *StatSQL) UpdateWhereTX(tx pgx.Tx, ctx context.Context, k string, v any, where string) (err error) {
	if !StatValidKey(k) {
		return errors.New("invalid key")
	}
	in := StatKeyIndex(k)

	q := fmt.Sprintf("update %s set %s = $1 where %s", a.TableName(), in.SQLName(), where)
	_, err = tx.Exec(ctx, q, v)
	return

}

// UpdatesTX transaction version of Updates
func (a *StatSQL) UpdatesTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}

	var fields []string
	var values []any
	var count int
	for k, v := range keys {
		if !StatValidKey(k) {
			return errors.New(k)
		}
		in := StatKeyIndex(k)
		count++
		fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
		values = append(values, v)
	}

	list := strings.Join(fields, ", ")
	count++
	values = append(values, id)

	q := fmt.Sprintf("update stat set %s where id = $%d", list, count)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, values...)
	return

}

// UpdatesWhereTX transaction version of UpdatesWhere
func (a *StatSQL) UpdatesWhereTX(tx pgx.Tx, ctx context.Context, keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}

	var fields []string
	var values []any
	var count int
	for k, v := range keys {
		if !StatValidKey(k) {
			return errors.New(k)
		}
		in := StatKeyIndex(k)
		count++
		fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
		values = append(values, v)
	}

	list := strings.Join(fields, ", ")
	count++

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("update stat set %s where %s", list, where)
	_, err = tx.Exec(ctx, q, values...)
	return

}

// AddStackTX transaction version of AddStack
func (a *StatSQL) AddStackTX(tx pgx.Tx, ctx context.Context, id any, v ...string) (err error) {

	q := "update stat set stack = stack || $1::text[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteStackTX transaction version of DeleteStack
func (a *StatSQL) DeleteStackTX(tx pgx.Tx, ctx context.Context, id any, v string, where ...string) (err error) {

	q := "update stat set stack = array_remove(stack, $1::text) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddStackWhereTX transaction version of AddStackWhere
func (a *StatSQL) AddStackWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update stat set stack = array_append(stack, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteStackWhereTX transaction version of DeleteStackWhere
func (a *StatSQL) DeleteStackWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update stat set stack = array_remove(stack, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddUintsTX transaction version of AddUints
func (a *StatSQL) AddUintsTX(tx pgx.Tx, ctx context.Context, id any, v ...uint) (err error) {

	q := "update stat set uints = uints || $1::numeric(20,0)[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteUintsTX transaction version of DeleteUints
func (a *StatSQL) DeleteUintsTX(tx pgx.Tx, ctx context.Context, id any, v uint, where ...string) (err error) {

	q := "update stat set uints = array_remove(uints, $1::numeric(20,0)) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddUintsWhereTX transaction version of AddUintsWhere
func (a *StatSQL) AddUintsWhereTX(tx pgx.Tx, ctx context.Context, v uint, where string) (err error) {

	q := "update stat set uints = array_append(uints, $1::numeric(20,0)) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteUintsWhereTX transaction version of DeleteUintsWhere
func (a *StatSQL) DeleteUintsWhereTX(tx pgx.Tx, ctx context.Context, v uint, where string) (err error) {

	q := "update stat set uints = array_remove(uints, $1::numeric(20,0)) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddUints16TX transaction version of AddUints16
func (a *StatSQL) AddUints16TX(tx pgx.Tx, ctx context.Context, id any, v ...uint16) (err error) {

	q := "update stat set uints16 = uints16 || $1::integer[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteUints16TX transaction version of DeleteUints16
func (a *StatSQL) DeleteUints16TX(tx pgx.Tx, ctx context.Context, id any, v uint16, where ...string) (err error) {

	q := "update stat set uints16 = array_remove(uints16, $1::integer) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddUints16WhereTX transaction version of AddUints16Where
func (a *StatSQL) AddUints16WhereTX(tx pgx.Tx, ctx context.Context, v uint16, where string) (err error) {

	q := "update stat set uints16 = array_append(uints16, $1::integer) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteUints16WhereTX transaction version of DeleteUints16Where
func (a *StatSQL) DeleteUints16WhereTX(tx pgx.Tx, ctx context.Context, v uint16, where string) (err error) {

	q := "update stat set uints16 = array_remove(uints16, $1::integer) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddUints32TX transaction version of AddUints32
func (a *StatSQL) AddUints32TX(tx pgx.Tx, ctx context.Context, id any, v ...uint32) (err error) {

	q := "update stat set uints32 = uints32 || $1::bigint[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteUints32TX transaction version of DeleteUints32
func (a *StatSQL) DeleteUints32TX(tx pgx.Tx, ctx context.Context, id any, v uint32, where ...string) (err error) {

	q := "update stat set uints32 = array_remove(uints32, $1::bigint) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddUints32WhereTX transaction version of AddUints32Where
func (a *StatSQL) AddUints32WhereTX(tx pgx.Tx, ctx context.Context, v uint32, where string) (err error) {

	q := "update stat set uints32 = array_append(uints32, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteUints32WhereTX transaction version of DeleteUints32Where
func (a *StatSQL) DeleteUints32WhereTX(tx pgx.Tx, ctx context.Context, v uint32, where string) (err error) {

	q := "update stat set uints32 = array_remove(uints32, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddUints64TX transaction version of AddUints64
func (a *StatSQL) AddUints64TX(tx pgx.Tx, ctx context.Context, id any, v ...uint64) (err error) {

	q := "update stat set uints64 = uints64 || $1::numeric(20,0)[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteUints64TX transaction version of DeleteUints64
func (a *StatSQL) DeleteUints64TX(tx pgx.Tx, ctx context.Context, id any, v uint64, where ...string) (err error) {

	q := "update stat set uints64 = array_remove(uints64, $1::numeric(20,0)) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddUints64WhereTX transaction version of AddUints64Where
func (a *StatSQL) AddUints64WhereTX(tx pgx.Tx, ctx context.Context, v uint64, where string) (err error) {

	q := "update stat set uints64 = array_append(uints64, $1::numeric(20,0)) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteUints64WhereTX transaction version of DeleteUints64Where
func (a *StatSQL) DeleteUints64WhereTX(tx pgx.Tx, ctx context.Context, v uint64, where string) (err error) {

	q := "update stat set uints64 = array_remove(uints64, $1::numeric(20,0)) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddInts8TX transaction version of AddInts8
func (a *StatSQL) AddInts8TX(tx pgx.Tx, ctx context.Context, id any, v ...int8) (err error) {

	q := "update stat set ints8 = ints8 || $1::smallint[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteInts8TX transaction version of DeleteInts8
func (a *StatSQL) DeleteInts8TX(tx pgx.Tx, ctx context.Context, id any, v int8, where ...string) (err error) {

	q := "update stat set ints8 = array_remove(ints8, $1::smallint) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddInts8WhereTX transaction version of AddInts8Where
func (a *StatSQL) AddInts8WhereTX(tx pgx.Tx, ctx context.Context, v int8, where string) (err error) {

	q := "update stat set ints8 = array_append(ints8, $1::smallint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteInts8WhereTX transaction version of DeleteInts8Where
func (a *StatSQL) DeleteInts8WhereTX(tx pgx.Tx, ctx context.Context, v int8, where string) (err error) {

	q := "update stat set ints8 = array_remove(ints8, $1::smallint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddInts16TX transaction version of AddInts16
func (a *StatSQL) AddInts16TX(tx pgx.Tx, ctx context.Context, id any, v ...int16) (err error) {

	q := "update stat set ints16 = ints16 || $1::smallint[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteInts16TX transaction version of DeleteInts16
func (a *StatSQL) DeleteInts16TX(tx pgx.Tx, ctx context.Context, id any, v int16, where ...string) (err error) {

	q := "update stat set ints16 = array_remove(ints16, $1::smallint) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddInts16WhereTX transaction version of AddInts16Where
func (a *StatSQL) AddInts16WhereTX(tx pgx.Tx, ctx context.Context, v int16, where string) (err error) {

	q := "update stat set ints16 = array_append(ints16, $1::smallint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteInts16WhereTX transaction version of DeleteInts16Where
func (a *StatSQL) DeleteInts16WhereTX(tx pgx.Tx, ctx context.Context, v int16, where string) (err error) {

	q := "update stat set ints16 = array_remove(ints16, $1::smallint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddInts32TX transaction version of AddInts32
func (a *StatSQL) AddInts32TX(tx pgx.Tx, ctx context.Context, id any, v ...int32) (err error) {

	q := "update stat set ints32 = ints32 || $1::integer[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteInts32TX transaction version of DeleteInts32
func (a *StatSQL) DeleteInts32TX(tx pgx.Tx, ctx context.Context, id any, v int32, where ...string) (err error) {

	q := "update stat set ints32 = array_remove(ints32, $1::integer) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddInts32WhereTX transaction version of AddInts32Where
func (a *StatSQL) AddInts32WhereTX(tx pgx.Tx, ctx context.Context, v int32, where string) (err error) {

	q := "update stat set ints32 = array_append(ints32, $1::integer) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteInts32WhereTX transaction version of DeleteInts32Where
func (a *StatSQL) DeleteInts32WhereTX(tx pgx.Tx, ctx context.Context, v int32, where string) (err error) {

	q := "update stat set ints32 = array_remove(ints32, $1::integer) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddInts64TX transaction version of AddInts64
func (a *StatSQL) AddInts64TX(tx pgx.Tx, ctx context.Context, id any, v ...int64) (err error) {

	q := "update stat set ints64 = ints64 || $1::bigint[] where id = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteInts64TX transaction version of DeleteInts64
func (a *StatSQL) DeleteInts64TX(tx pgx.Tx, ctx context.Context, id any, v int64, where ...string) (err error) {

	q := "update stat set ints64 = array_remove(ints64, $1::bigint) where id = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddInts64WhereTX transaction version of AddInts64Where
func (a *StatSQL) AddInts64WhereTX(tx pgx.Tx, ctx context.Context, v int64, where string) (err error) {

	q := "update stat set ints64 = array_append(ints64, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteInts64WhereTX transaction version of DeleteInts64Where
func (a *StatSQL) DeleteInts64WhereTX(tx pgx.Tx, ctx context.Context, v int64, where string) (err error) {

	q := "update stat set ints64 = array_remove(ints64, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// SearchTX transaction version of Search
func (a *StatSQL) SearchTX(tx pgx.Tx, ctx context.Context, q *StatQuery) (res []Stat, err error) {

	sql, fields, values := q.Render()

	rows, err := tx.Query(ctx, sql, values...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item Stat
		v, err := rows.Values()
		if err != nil {
			continue
		}
		if len(v) != len(fields) {
			continue
		}

		for pos, x := range fields {
			item.Update(x.String(), v[pos])
		}
		res = append(res, item)
	}
	err = rows.Err()
	return

}

// SelectTX transaction version of Select
func (a *StatSQL) SelectTX(tx pgx.Tx, ctx context.Context, where string, fields ...StatIndexTyped) (res []Stat, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s ", fieldlist)
	q = q + "from stat"
	if where != "" {
		q += " where " + where
	}
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item Stat
		v, err := rows.Values()
		if err != nil {
			continue
		}
		if len(v) != len(fields) {
			continue
		}

		for pos, x := range fields {
			item.Update(x.String(), v[pos])
		}
		res = append(res, item)
	}
	err = rows.Err()
	return

}

// DeleteTX transaction version of Delete
func (a *StatSQL) DeleteTX(tx pgx.Tx, ctx context.Context, id any) (err error) {

	_, err = tx.Exec(ctx, "delete from stat where id = $1", id)
	return

}

// DeleteWhereTX transaction version of DeleteWhere
func (a *StatSQL) DeleteWhereTX(tx pgx.Tx, ctx context.Context, where string) (err error) {

	_, err = tx.Exec(ctx, fmt.Sprintf("delete from stat where %s", where))
	return

}

// HasTX transaction version of Has
func (a *StatSQL) HasTX(tx pgx.Tx, ctx context.Context, field StatIndexTyped, v any) (has bool, err error) {

	q := fmt.Sprintf("select exists (select 1 from stat where %s = $1 limit 1)", field.SQLName())
	err = tx.QueryRow(ctx, q, v).Scan(&has)
	return

}

// CreateTableTX transaction version of CreateTable
func (a *StatSQL) CreateTableTX(tx pgx.Tx, ctx context.Context) (err error) {

	q := `create table if not exists stat (
	id          bigserial primary key,
	uid         bigint,
	type        bigint,
	name        text,
	source      text,
	ip          text,
	image       boolean,
	stack       text[] default '{}',
	uints       numeric(20,0)[] default '{}',
	uints8      bytea,
	uints16     integer[] default '{}',
	uints32     bigint[] default '{}',
	uints64     numeric(20,0)[] default '{}',
	ints8       smallint[] default '{}',
	ints16      smallint[] default '{}',
	ints32      integer[] default '{}',
	ints64      bigint[] default '{}'
)
`
	_, err = tx.Exec(ctx, q)
	return

} //clickhouse StatCQL class
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
	rowValues := make([]any, len(fields))
	scan := make([]any, len(fields))
	for i := range rowValues {
		scan[i] = &rowValues[i]
	}
	err = rows.Scan(scan...)
	if err != nil {
		return
	}
	for pos, x := range fields {
		res.Update(x.String(), rowValues[pos])
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
	rowValues := make([]any, len(fields))
	scan := make([]any, len(fields))
	for i := range rowValues {
		scan[i] = &rowValues[i]
	}
	err = rows.Scan(scan...)
	if err != nil {
		return
	}
	for pos, x := range fields {
		res.Update(x.String(), rowValues[pos])
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
	for rows.Next() {
		var item Stat
		values := make([]any, len(fields))
		scan := make([]any, len(fields))
		for i := range values {
			scan[i] = &values[i]
		}
		err = rows.Scan(scan...)
		if err != nil {
			continue
		}
		for pos, x := range fields {
			item.Update(x.String(), values[pos])
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
