package news

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	jsoniter "github.com/json-iterator/go"
	"github.com/monopolly/cast"
	"github.com/monopolly/jsons"
	"github.com/niubaoshu/gotiny"
	"github.com/vmihailenco/msgpack/v5"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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
	!omit                         No omit tag for json
	ch=views                      Set clickhouse sql table name. Ex: ch=views
	chengine=MergeTree            Optional, mergeTree by default
	debug                         Debug mode
	demo                          Generate demo json file with default values
	enum                          Generate swift enum
	go=News                       Set golang struct names. Ex: go=News1 > type News1 struct{}
	gotiny                        Create gotiny marshal/unmarshal
	js=NewsJson                   Change json struct names. Ex: js=NewsJson
	lock                          Lock model for generation. Can't change model.
	msgp                          Create message pack marshal/unmarshal
	noinit                        No New() init function for struct
	noprefix                      Generate simple index IndexID instead IndexNewsID
	optimize                      Try to create golang padding optimized struct
	proto=NewsProto               Generate proto file and compile to current package
	sql=news                      Set sql table name. Ex: sql=accounts
	swift                         Generate swift model
	test                          Generate go tests for generated model
	ts=news                       Set typescript struct names. Ex: ts=NewsJson

	Field:
	desc{}                        Add custom desc for index. desc{This field for success}
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

	SWIFT:
	file=""                       Create another swift file for this field Swift model. file="f1", file="f2"
	must                          Swift field with required values
	skip                          Ignore field for Swift
	type=""                       Replace type for Swift model. Ex: type="string"

	JSON:
	bool                          Add set jsons function for bool fields
	inc                           Add inc jsons function for numbers fields
	name=""                       Replace json field name. Ex: name="sid"
	raw                           Set Raw json function inside field
	skip                          Skip json field
	time                          Create convert jsons function for unixtime fields
*/

// field type
type NewsIndexTyped int

// int index
const (
	NewsIndexInc = NewsIndexTyped(iota)
	NewsIndexRole
	NewsIndexKey
	NewsIndexT
	NewsIndexD
	NewsIndexInts
	NewsIndexInts8
	NewsIndexInts16
	NewsIndexInts32
	NewsIndexInts64
	NewsIndexUints
	NewsIndexUints8
	NewsIndexUints16
	NewsIndexUints32
	NewsIndexUints64
	NewsIndexFloats32
	NewsIndexFloats64
	NewsIndexBools
	NewsIndexByte1
	NewsIndexBytes
	NewsIndexList_ints
	NewsIndexList_string
	NewsIndexList_float
	NewsIndexMap_string_string
	NewsIndexMap_string_bytes
	NewsIndexMap_string_bool
	NewsIndexMap_string_int
	NewsIndexMap_string_float64
	NewsIndexMap_string_any
	NewsIndexMap_int_string
	NewsIndexMap_int_int
	NewsIndexMap_int_bool
	NewsIndexRenameSQL
	NewsIndexRenameGO_OK
	NewsIndexRenameJS
	NewsIndexMAST_UPPER_GO
	NewsIndexIntToSmallInt
	NewsIndexSkip
	NewsIndexSql_unique_u1_1
	NewsIndexSql_unique_u1_2
	NewsIndexSql_index1_1
	NewsIndexSql_index1_2
	NewsIndexSql_index1_3
	NewsIndexSql_keys_1
	NewsIndexSql_keys_2
	NewsIndexSql_keys_3
	NewsIndexSql_search
	NewsIndexSql_get
	NewsIndexSql_unique_x1
	NewsIndexSql_unique_x2
	NewsIndexSql_unique_x1_x2
	NewsIndexSql_primary
	NewsIndexSql_jsonb_index
	NewsIndexTime_duration
	NewsIndexGo_type_int_to_strings
	NewsIndexPublic_field1
	NewsIndexPublic_field2
	NewsIndexPublic_field3
	NewsIndexPublic_field_me1
	NewsIndexPublic_field_me2
	NewsIndexPublic_field_me3
)

// string index
const (
	NewsFieldInc                    = "inc"                    // int sql{inc}  #readonly
	NewsFieldRole                   = "role"                   // Role
	NewsFieldKey                    = "key"                    // uuid.UUID
	NewsFieldT                      = "t"                      // time.Time
	NewsFieldD                      = "d"                      // time.Duration
	NewsFieldInts                   = "ints"                   // int
	NewsFieldInts8                  = "ints8"                  // int8
	NewsFieldInts16                 = "ints16"                 // int16
	NewsFieldInts32                 = "ints32"                 // int32
	NewsFieldInts64                 = "ints64"                 // int64
	NewsFieldUints                  = "uints"                  // uint
	NewsFieldUints8                 = "uints8"                 // uint8
	NewsFieldUints16                = "uints16"                // uint16
	NewsFieldUints32                = "uints32"                // uint32
	NewsFieldUints64                = "uints64"                // uint64
	NewsFieldFloats32               = "floats32"               // float32
	NewsFieldFloats64               = "floats64"               // float64
	NewsFieldBools                  = "bools"                  // bool
	NewsFieldByte1                  = "byte1"                  // byte
	NewsFieldBytes                  = "bytes"                  // []byte
	NewsFieldList_ints              = "list_ints"              // []int
	NewsFieldList_string            = "list_string"            // []string
	NewsFieldList_float             = "list_float"             // []float64
	NewsFieldMap_string_string      = "map_string_string"      // map[string]string
	NewsFieldMap_string_bytes       = "map_string_bytes"       // map[string][]byte
	NewsFieldMap_string_bool        = "map_string_bool"        // map[string]bool
	NewsFieldMap_string_int         = "map_string_int"         // map[string]int
	NewsFieldMap_string_float64     = "map_string_float64"     // map[string]float64
	NewsFieldMap_string_any         = "map_string_any"         // map[string]any
	NewsFieldMap_int_string         = "map_int_string"         // map[int]string
	NewsFieldMap_int_int            = "map_int_int"            // map[int]int
	NewsFieldMap_int_bool           = "map_int_bool"           // map[int]bool
	NewsFieldRenameSQL              = "renameSQL"              // string sql{name="renameSQL_OK"}
	NewsFieldRenameGO_OK            = "renameGO"               // string go{name="RenameGO_OK"}
	NewsFieldRenameJS               = "renameJS_OK"            // string js{name="renameJS_OK"}
	NewsFieldMAST_UPPER_GO          = "mast_upper_go"          // string go{up}
	NewsFieldIntToSmallInt          = "intToSmallInt"          // int sql{type="smallint"} rename
	NewsFieldSkip                   = "skip"                   // string sql{skip} swift{skip} js{skip}
	NewsFieldSql_unique_u1_1        = "sql_unique_u1_1"        // int sql{unique="u1"}
	NewsFieldSql_unique_u1_2        = "sql_unique_u1_2"        // int sql{unique="u1"}
	NewsFieldSql_index1_1           = "sql_index1_1"           // int sql{idx="index1"}
	NewsFieldSql_index1_2           = "sql_index1_2"           // int sql{idx="index1"}
	NewsFieldSql_index1_3           = "sql_index1_3"           // int sql{idx="index1"}
	NewsFieldSql_keys_1             = "sql_keys_1"             // int sql{keys="keys1"}
	NewsFieldSql_keys_2             = "sql_keys_2"             // int sql{keys="keys1"}
	NewsFieldSql_keys_3             = "sql_keys_3"             // int sql{keys="keys1"}
	NewsFieldSql_search             = "sql_search"             // string sql{search="search" get}
	NewsFieldSql_get                = "sql_get"                // string sql{get}
	NewsFieldSql_unique_x1          = "sql_unique_x1"          // int sql{unique="x1"}
	NewsFieldSql_unique_x2          = "sql_unique_x2"          // int sql{unique="x2"}
	NewsFieldSql_unique_x1_x2       = "sql_unique_x1_x2"       // int sql{unique="x1", unique="x2"}
	NewsFieldSql_primary            = "sql_primary"            // float64 sql{primarykey}
	NewsFieldSql_jsonb_index        = "sql_jsonb_index"        // map[string]any sql{index}
	NewsFieldTime_duration          = "time_duration"          // time.Duration
	NewsFieldGo_type_int_to_strings = "go_type_int_to_strings" // int go{type="[]string"}
	NewsFieldPublic_field1          = "public_field1"          // int @public
	NewsFieldPublic_field2          = "public_field2"          // int @public
	NewsFieldPublic_field3          = "public_field3"          // int @public
	NewsFieldPublic_field_me1       = "public_field_me1"       // int @me
	NewsFieldPublic_field_me2       = "public_field_me2"       // int @me
	NewsFieldPublic_field_me3       = "public_field_me3"       // int @me
)

// index func
func NewsIndexes() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexList_ints, NewsIndexList_string, NewsIndexList_float, NewsIndexMap_string_string, NewsIndexMap_string_bytes, NewsIndexMap_string_bool, NewsIndexMap_string_int, NewsIndexMap_string_float64, NewsIndexMap_string_any, NewsIndexMap_int_string, NewsIndexMap_int_int, NewsIndexMap_int_bool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMAST_UPPER_GO, NewsIndexIntToSmallInt, NewsIndexSkip, NewsIndexSql_unique_u1_1, NewsIndexSql_unique_u1_2, NewsIndexSql_index1_1, NewsIndexSql_index1_2, NewsIndexSql_index1_3, NewsIndexSql_keys_1, NewsIndexSql_keys_2, NewsIndexSql_keys_3, NewsIndexSql_search, NewsIndexSql_get, NewsIndexSql_unique_x1, NewsIndexSql_unique_x2, NewsIndexSql_unique_x1_x2, NewsIndexSql_primary, NewsIndexSql_jsonb_index, NewsIndexTime_duration, NewsIndexGo_type_int_to_strings, NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3, NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3}

}

// 504 bytes (go padding)
//
//easyjson:json
type News struct {
	Inc                    int                `json:"inc,omitempty" msg:"inc,omitempty"`                                       // sql{inc}  #readonly
	Role                   Role               `json:"role,omitempty" msg:"role,omitempty"`                                     //
	Key                    uuid.UUID          `json:"key,omitempty" msg:"key,omitempty"`                                       //
	T                      time.Time          `json:"t,omitempty" msg:"t,omitempty"`                                           //
	D                      time.Duration      `json:"d,omitempty" msg:"d,omitempty"`                                           //
	Ints                   int                `json:"ints,omitempty" msg:"ints,omitempty"`                                     //
	Ints8                  int8               `json:"ints8,omitempty" msg:"ints8,omitempty"`                                   //
	Ints16                 int16              `json:"ints16,omitempty" msg:"ints16,omitempty"`                                 //
	Ints32                 int32              `json:"ints32,omitempty" msg:"ints32,omitempty"`                                 //
	Ints64                 int64              `json:"ints64,omitempty" msg:"ints64,omitempty"`                                 //
	Uints                  uint               `json:"uints,omitempty" msg:"uints,omitempty"`                                   //
	Uints8                 uint8              `json:"uints8,omitempty" msg:"uints8,omitempty"`                                 //
	Uints16                uint16             `json:"uints16,omitempty" msg:"uints16,omitempty"`                               //
	Uints32                uint32             `json:"uints32,omitempty" msg:"uints32,omitempty"`                               //
	Uints64                uint64             `json:"uints64,omitempty" msg:"uints64,omitempty"`                               //
	Floats32               float32            `json:"floats32,omitempty" msg:"floats32,omitempty"`                             //
	Floats64               float64            `json:"floats64,omitempty" msg:"floats64,omitempty"`                             //
	Bools                  bool               `json:"bools,omitempty" msg:"bools,omitempty"`                                   //
	Byte1                  byte               `json:"byte1,omitempty" msg:"byte1,omitempty"`                                   //
	Bytes                  []byte             `json:"bytes,omitempty" msg:"bytes,omitempty"`                                   //
	List_ints              []int              `json:"list_ints,omitempty" msg:"list_ints,omitempty"`                           //
	List_string            []string           `json:"list_string,omitempty" msg:"list_string,omitempty"`                       //
	List_float             []float64          `json:"list_float,omitempty" msg:"list_float,omitempty"`                         //
	Map_string_string      map[string]string  `json:"map_string_string,omitempty" msg:"map_string_string,omitempty"`           //
	Map_string_bytes       map[string][]byte  `json:"map_string_bytes,omitempty" msg:"map_string_bytes,omitempty"`             //
	Map_string_bool        map[string]bool    `json:"map_string_bool,omitempty" msg:"map_string_bool,omitempty"`               //
	Map_string_int         map[string]int     `json:"map_string_int,omitempty" msg:"map_string_int,omitempty"`                 //
	Map_string_float64     map[string]float64 `json:"map_string_float64,omitempty" msg:"map_string_float64,omitempty"`         //
	Map_string_any         map[string]any     `json:"map_string_any,omitempty" msg:"map_string_any,omitempty"`                 //
	Map_int_string         map[int]string     `json:"map_int_string,omitempty" msg:"map_int_string,omitempty"`                 //
	Map_int_int            map[int]int        `json:"map_int_int,omitempty" msg:"map_int_int,omitempty"`                       //
	Map_int_bool           map[int]bool       `json:"map_int_bool,omitempty" msg:"map_int_bool,omitempty"`                     //
	RenameSQL              string             `json:"renameSQL,omitempty" msg:"renameSQL,omitempty"`                           // sql{name="renameSQL_OK"}
	RenameGO_OK            string             `json:"renameGO,omitempty" msg:"renameGO,omitempty"`                             // go{name="RenameGO_OK"}
	RenameJS               string             `json:"renameJS_OK,omitempty" msg:"renameJS_OK,omitempty"`                       // js{name="renameJS_OK"}
	MAST_UPPER_GO          string             `json:"mast_upper_go,omitempty" msg:"mast_upper_go,omitempty"`                   // go{up}
	IntToSmallInt          int                `json:"intToSmallInt,omitempty" msg:"intToSmallInt,omitempty"`                   // sql{type="smallint"} rename
	Skip                   string             `json:"skip,omitempty" msg:"skip,omitempty"`                                     // sql{skip} swift{skip} js{skip}
	Sql_unique_u1_1        int                `json:"sql_unique_u1_1,omitempty" msg:"sql_unique_u1_1,omitempty"`               // sql{unique="u1"}
	Sql_unique_u1_2        int                `json:"sql_unique_u1_2,omitempty" msg:"sql_unique_u1_2,omitempty"`               // sql{unique="u1"}
	Sql_index1_1           int                `json:"sql_index1_1,omitempty" msg:"sql_index1_1,omitempty"`                     // sql{idx="index1"}
	Sql_index1_2           int                `json:"sql_index1_2,omitempty" msg:"sql_index1_2,omitempty"`                     // sql{idx="index1"}
	Sql_index1_3           int                `json:"sql_index1_3,omitempty" msg:"sql_index1_3,omitempty"`                     // sql{idx="index1"}
	Sql_keys_1             int                `json:"sql_keys_1,omitempty" msg:"sql_keys_1,omitempty"`                         // sql{keys="keys1"}
	Sql_keys_2             int                `json:"sql_keys_2,omitempty" msg:"sql_keys_2,omitempty"`                         // sql{keys="keys1"}
	Sql_keys_3             int                `json:"sql_keys_3,omitempty" msg:"sql_keys_3,omitempty"`                         // sql{keys="keys1"}
	Sql_search             string             `json:"sql_search,omitempty" msg:"sql_search,omitempty"`                         // sql{search="search" get}
	Sql_get                string             `json:"sql_get,omitempty" msg:"sql_get,omitempty"`                               // sql{get}
	Sql_unique_x1          int                `json:"sql_unique_x1,omitempty" msg:"sql_unique_x1,omitempty"`                   // sql{unique="x1"}
	Sql_unique_x2          int                `json:"sql_unique_x2,omitempty" msg:"sql_unique_x2,omitempty"`                   // sql{unique="x2"}
	Sql_unique_x1_x2       int                `json:"sql_unique_x1_x2,omitempty" msg:"sql_unique_x1_x2,omitempty"`             // sql{unique="x1", unique="x2"}
	Sql_primary            float64            `json:"sql_primary,omitempty" msg:"sql_primary,omitempty"`                       // sql{primarykey}
	Sql_jsonb_index        map[string]any     `json:"sql_jsonb_index,omitempty" msg:"sql_jsonb_index,omitempty"`               // sql{index}
	Time_duration          time.Duration      `json:"time_duration,omitempty" msg:"time_duration,omitempty"`                   //
	Go_type_int_to_strings []string           `json:"go_type_int_to_strings,omitempty" msg:"go_type_int_to_strings,omitempty"` // go{type="[]string"}
	Public_field1          int                `json:"public_field1,omitempty" msg:"public_field1,omitempty"`                   // @public
	Public_field2          int                `json:"public_field2,omitempty" msg:"public_field2,omitempty"`                   // @public
	Public_field3          int                `json:"public_field3,omitempty" msg:"public_field3,omitempty"`                   // @public
	Public_field_me1       int                `json:"public_field_me1,omitempty" msg:"public_field_me1,omitempty"`             // @me
	Public_field_me2       int                `json:"public_field_me2,omitempty" msg:"public_field_me2,omitempty"`             // @me
	Public_field_me3       int                `json:"public_field_me3,omitempty" msg:"public_field_me3,omitempty"`             // @me
}

type NewsMe struct {
	Public_field_me1 int `json:"public_field_me1,omitempty" msg:"public_field_me1,omitempty"` // @me
	Public_field_me2 int `json:"public_field_me2,omitempty" msg:"public_field_me2,omitempty"` // @me
	Public_field_me3 int `json:"public_field_me3,omitempty" msg:"public_field_me3,omitempty"` // @me
}

func (a *News) NewsMe() (res *NewsMe) {
	return &NewsMe{
		Public_field_me1: a.Public_field_me1,
		Public_field_me2: a.Public_field_me2,
		Public_field_me3: a.Public_field_me3,
	}
}

func (a *NewsMe) News() (res *News) {
	return &News{
		Public_field_me1: a.Public_field_me1,
		Public_field_me2: a.Public_field_me2,
		Public_field_me3: a.Public_field_me3,
	}
}

func (a *NewsMe) Pack() (res []byte) {
	res, _ = jsoniter.Marshal(a)
	return
}

type NewsPublic struct {
	Public_field1 int `json:"public_field1,omitempty" msg:"public_field1,omitempty"` // @public
	Public_field2 int `json:"public_field2,omitempty" msg:"public_field2,omitempty"` // @public
	Public_field3 int `json:"public_field3,omitempty" msg:"public_field3,omitempty"` // @public
}

func (a *News) NewsPublic() (res *NewsPublic) {
	return &NewsPublic{
		Public_field1: a.Public_field1,
		Public_field2: a.Public_field2,
		Public_field3: a.Public_field3,
	}
}

func (a *NewsPublic) News() (res *News) {
	return &News{
		Public_field1: a.Public_field1,
		Public_field2: a.Public_field2,
		Public_field3: a.Public_field3,
	}
}

func (a *NewsPublic) Pack() (res []byte) {
	res, _ = jsoniter.Marshal(a)
	return
}

// init struct
func NewNews() (a *News) {
	a = new(News)
	a.Map_string_string = make(map[string]string)
	a.Map_string_bytes = make(map[string][]byte)
	a.Map_string_bool = make(map[string]bool)
	a.Map_string_int = make(map[string]int)
	a.Map_string_float64 = make(map[string]float64)
	a.Map_string_any = make(map[string]any)
	a.Map_int_string = make(map[int]string)
	a.Map_int_int = make(map[int]int)
	a.Map_int_bool = make(map[int]bool)
	a.Sql_jsonb_index = make(map[string]any)
	return
}

// Parse []any to ID struct
func ParseNewsToStruct(r []any) (a News) {

	for pos, x := range r {
		switch NewsIndexTyped(pos) {
		case NewsIndexInc:
			cast.Convert(&a.Inc, x) //int
		case NewsIndexRole:
			var v int
			cast.Convert(&v, x)
			a.Role = Role(v) //Role
		case NewsIndexKey:
			switch v := x.(type) {
			case uuid.UUID:
				a.Key = v
			case [16]byte:
				a.Key = v
			case string:
				a.Key, _ = uuid.Parse(v)
			case []byte:
				if len(v) == 16 {
					a.Key, _ = uuid.FromBytes(v)
				} else {
					a.Key, _ = uuid.ParseBytes(v)
				}
			} //uuid.UUID
		case NewsIndexT:
			cast.Convert(&a.T, x) //time.Time
		case NewsIndexD:
			cast.Convert(&a.D, x) //time.Duration
		case NewsIndexInts:
			cast.Convert(&a.Ints, x) //int
		case NewsIndexInts8:
			cast.Convert(&a.Ints8, x) //int8
		case NewsIndexInts16:
			cast.Convert(&a.Ints16, x) //int16
		case NewsIndexInts32:
			cast.Convert(&a.Ints32, x) //int32
		case NewsIndexInts64:
			cast.Convert(&a.Ints64, x) //int64
		case NewsIndexUints:
			if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
				b, _ := n.MarshalJSON()
				cast.Convert(&a.Uints, string(b))
			} else {
				cast.Convert(&a.Uints, x)
			} //uint
		case NewsIndexUints8:
			cast.Convert(&a.Uints8, x) //uint8
		case NewsIndexUints16:
			cast.Convert(&a.Uints16, x) //uint16
		case NewsIndexUints32:
			cast.Convert(&a.Uints32, x) //uint32
		case NewsIndexUints64:
			if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
				b, _ := n.MarshalJSON()
				cast.Convert(&a.Uints64, string(b))
			} else {
				cast.Convert(&a.Uints64, x)
			} //uint64
		case NewsIndexFloats32:
			cast.Convert(&a.Floats32, x) //float32
		case NewsIndexFloats64:
			cast.Convert(&a.Floats64, x) //float64
		case NewsIndexBools:
			cast.Convert(&a.Bools, x) //bool
		case NewsIndexByte1:
			cast.Convert(&a.Byte1, x) //byte
		case NewsIndexBytes:
			cast.Convert(&a.Bytes, x) //[]byte
		case NewsIndexList_ints:
			switch v0 := x.(type) {
			case nil:
			case []int:
				a.List_ints = v0
			case []any:
				r0 := make([]int, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.List_ints = r0
			default:
				cast.Convert(&a.List_ints, v0)
			} //[]int
		case NewsIndexList_string:
			switch v0 := x.(type) {
			case nil:
			case []string:
				a.List_string = v0
			case []any:
				r0 := make([]string, 0, len(v0))
				for _, e0 := range v0 {
					var el0 string
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.List_string = r0
			default:
				cast.Convert(&a.List_string, v0)
			} //[]string
		case NewsIndexList_float:
			switch v0 := x.(type) {
			case nil:
			case []float64:
				a.List_float = v0
			case []any:
				r0 := make([]float64, 0, len(v0))
				for _, e0 := range v0 {
					var el0 float64
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.List_float = r0
			default:
				cast.Convert(&a.List_float, v0)
			} //[]float64
		case NewsIndexMap_string_string:
			cast.Convert(&a.Map_string_string, x) //map[string]string
		case NewsIndexMap_string_bytes:
			cast.Convert(&a.Map_string_bytes, x) //map[string][]byte
		case NewsIndexMap_string_bool:
			cast.Convert(&a.Map_string_bool, x) //map[string]bool
		case NewsIndexMap_string_int:
			cast.Convert(&a.Map_string_int, x) //map[string]int
		case NewsIndexMap_string_float64:
			cast.Convert(&a.Map_string_float64, x) //map[string]float64
		case NewsIndexMap_string_any:
			cast.Convert(&a.Map_string_any, x) //map[string]any
		case NewsIndexMap_int_string:
			cast.Convert(&a.Map_int_string, x) //map[int]string
		case NewsIndexMap_int_int:
			cast.Convert(&a.Map_int_int, x) //map[int]int
		case NewsIndexMap_int_bool:
			cast.Convert(&a.Map_int_bool, x) //map[int]bool
		case NewsIndexRenameSQL:
			cast.Convert(&a.RenameSQL, x) //string
		case NewsIndexRenameGO_OK:
			cast.Convert(&a.RenameGO_OK, x) //string
		case NewsIndexRenameJS:
			cast.Convert(&a.RenameJS, x) //string
		case NewsIndexMAST_UPPER_GO:
			cast.Convert(&a.MAST_UPPER_GO, x) //string
		case NewsIndexIntToSmallInt:
			cast.Convert(&a.IntToSmallInt, x) //int
		case NewsIndexSkip:
			cast.Convert(&a.Skip, x) //string
		case NewsIndexSql_unique_u1_1:
			cast.Convert(&a.Sql_unique_u1_1, x) //int
		case NewsIndexSql_unique_u1_2:
			cast.Convert(&a.Sql_unique_u1_2, x) //int
		case NewsIndexSql_index1_1:
			cast.Convert(&a.Sql_index1_1, x) //int
		case NewsIndexSql_index1_2:
			cast.Convert(&a.Sql_index1_2, x) //int
		case NewsIndexSql_index1_3:
			cast.Convert(&a.Sql_index1_3, x) //int
		case NewsIndexSql_keys_1:
			cast.Convert(&a.Sql_keys_1, x) //int
		case NewsIndexSql_keys_2:
			cast.Convert(&a.Sql_keys_2, x) //int
		case NewsIndexSql_keys_3:
			cast.Convert(&a.Sql_keys_3, x) //int
		case NewsIndexSql_search:
			cast.Convert(&a.Sql_search, x) //string
		case NewsIndexSql_get:
			cast.Convert(&a.Sql_get, x) //string
		case NewsIndexSql_unique_x1:
			cast.Convert(&a.Sql_unique_x1, x) //int
		case NewsIndexSql_unique_x2:
			cast.Convert(&a.Sql_unique_x2, x) //int
		case NewsIndexSql_unique_x1_x2:
			cast.Convert(&a.Sql_unique_x1_x2, x) //int
		case NewsIndexSql_primary:
			cast.Convert(&a.Sql_primary, x) //float64
		case NewsIndexSql_jsonb_index:
			cast.Convert(&a.Sql_jsonb_index, x) //map[string]any
		case NewsIndexTime_duration:
			cast.Convert(&a.Time_duration, x) //time.Duration
		case NewsIndexPublic_field1:
			cast.Convert(&a.Public_field1, x) //int
		case NewsIndexPublic_field2:
			cast.Convert(&a.Public_field2, x) //int
		case NewsIndexPublic_field3:
			cast.Convert(&a.Public_field3, x) //int
		case NewsIndexPublic_field_me1:
			cast.Convert(&a.Public_field_me1, x) //int
		case NewsIndexPublic_field_me2:
			cast.Convert(&a.Public_field_me2, x) //int
		case NewsIndexPublic_field_me3:
			cast.Convert(&a.Public_field_me3, x) //int
		}
	}
	return
}

// Tuple create an array from struct
func (a *News) Tuple() (r []any) {
	return []any{a.Inc, a.Role, a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Skip, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
}

// Tuple create an array from struct
func (a *News) sqlTuple() (r []any) {
	return []any{int(a.Role), a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
}

// Tuple create an array from struct
func (a *News) sqlAllTuples() (r []any) {
	return []any{a.Inc, int(a.Role), a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
}

// Tuple create an array from struct for clickhouse
func (a *News) clickhouseTuple() (r []any) {
	return []any{a.Inc, int(a.Role), a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Skip, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
}

// update struct with function
func (a *News) Update(k string, x any) {
	switch k {
	case "inc":
		cast.Convert(&a.Inc, x) //int
	case "role":
		var v int
		cast.Convert(&v, x)
		a.Role = Role(v) //Role
	case "key":
		switch v := x.(type) {
		case uuid.UUID:
			a.Key = v
		case [16]byte:
			a.Key = v
		case string:
			a.Key, _ = uuid.Parse(v)
		case []byte:
			if len(v) == 16 {
				a.Key, _ = uuid.FromBytes(v)
			} else {
				a.Key, _ = uuid.ParseBytes(v)
			}
		} //uuid.UUID
	case "t":
		cast.Convert(&a.T, x) //time.Time
	case "d":
		cast.Convert(&a.D, x) //time.Duration
	case "ints":
		cast.Convert(&a.Ints, x) //int
	case "ints8":
		cast.Convert(&a.Ints8, x) //int8
	case "ints16":
		cast.Convert(&a.Ints16, x) //int16
	case "ints32":
		cast.Convert(&a.Ints32, x) //int32
	case "ints64":
		cast.Convert(&a.Ints64, x) //int64
	case "uints":
		if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
			b, _ := n.MarshalJSON()
			cast.Convert(&a.Uints, string(b))
		} else {
			cast.Convert(&a.Uints, x)
		} //uint
	case "uints8":
		cast.Convert(&a.Uints8, x) //uint8
	case "uints16":
		cast.Convert(&a.Uints16, x) //uint16
	case "uints32":
		cast.Convert(&a.Uints32, x) //uint32
	case "uints64":
		if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
			b, _ := n.MarshalJSON()
			cast.Convert(&a.Uints64, string(b))
		} else {
			cast.Convert(&a.Uints64, x)
		} //uint64
	case "floats32":
		cast.Convert(&a.Floats32, x) //float32
	case "floats64":
		cast.Convert(&a.Floats64, x) //float64
	case "bools":
		cast.Convert(&a.Bools, x) //bool
	case "byte1":
		cast.Convert(&a.Byte1, x) //byte
	case "bytes":
		cast.Convert(&a.Bytes, x) //[]byte
	case "list_ints":
		switch v0 := x.(type) {
		case nil:
		case []int:
			a.List_ints = v0
		case []any:
			r0 := make([]int, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.List_ints = r0
		default:
			cast.Convert(&a.List_ints, v0)
		} //[]int
	case "list_string":
		switch v0 := x.(type) {
		case nil:
		case []string:
			a.List_string = v0
		case []any:
			r0 := make([]string, 0, len(v0))
			for _, e0 := range v0 {
				var el0 string
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.List_string = r0
		default:
			cast.Convert(&a.List_string, v0)
		} //[]string
	case "list_float":
		switch v0 := x.(type) {
		case nil:
		case []float64:
			a.List_float = v0
		case []any:
			r0 := make([]float64, 0, len(v0))
			for _, e0 := range v0 {
				var el0 float64
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.List_float = r0
		default:
			cast.Convert(&a.List_float, v0)
		} //[]float64
	case "map_string_string":
		cast.Convert(&a.Map_string_string, x) //map[string]string
	case "map_string_bytes":
		cast.Convert(&a.Map_string_bytes, x) //map[string][]byte
	case "map_string_bool":
		cast.Convert(&a.Map_string_bool, x) //map[string]bool
	case "map_string_int":
		cast.Convert(&a.Map_string_int, x) //map[string]int
	case "map_string_float64":
		cast.Convert(&a.Map_string_float64, x) //map[string]float64
	case "map_string_any":
		cast.Convert(&a.Map_string_any, x) //map[string]any
	case "map_int_string":
		cast.Convert(&a.Map_int_string, x) //map[int]string
	case "map_int_int":
		cast.Convert(&a.Map_int_int, x) //map[int]int
	case "map_int_bool":
		cast.Convert(&a.Map_int_bool, x) //map[int]bool
	case "renameSQL":
		cast.Convert(&a.RenameSQL, x) //string
	case "renameGO":
		cast.Convert(&a.RenameGO_OK, x) //string
	case "renameJS":
		cast.Convert(&a.RenameJS, x) //string
	case "mast_upper_go":
		cast.Convert(&a.MAST_UPPER_GO, x) //string
	case "intToSmallInt":
		cast.Convert(&a.IntToSmallInt, x) //int
	case "skip":
		cast.Convert(&a.Skip, x) //string
	case "sql_unique_u1_1":
		cast.Convert(&a.Sql_unique_u1_1, x) //int
	case "sql_unique_u1_2":
		cast.Convert(&a.Sql_unique_u1_2, x) //int
	case "sql_index1_1":
		cast.Convert(&a.Sql_index1_1, x) //int
	case "sql_index1_2":
		cast.Convert(&a.Sql_index1_2, x) //int
	case "sql_index1_3":
		cast.Convert(&a.Sql_index1_3, x) //int
	case "sql_keys_1":
		cast.Convert(&a.Sql_keys_1, x) //int
	case "sql_keys_2":
		cast.Convert(&a.Sql_keys_2, x) //int
	case "sql_keys_3":
		cast.Convert(&a.Sql_keys_3, x) //int
	case "sql_search":
		cast.Convert(&a.Sql_search, x) //string
	case "sql_get":
		cast.Convert(&a.Sql_get, x) //string
	case "sql_unique_x1":
		cast.Convert(&a.Sql_unique_x1, x) //int
	case "sql_unique_x2":
		cast.Convert(&a.Sql_unique_x2, x) //int
	case "sql_unique_x1_x2":
		cast.Convert(&a.Sql_unique_x1_x2, x) //int
	case "sql_primary":
		cast.Convert(&a.Sql_primary, x) //float64
	case "sql_jsonb_index":
		cast.Convert(&a.Sql_jsonb_index, x) //map[string]any
	case "time_duration":
		cast.Convert(&a.Time_duration, x) //time.Duration
	case "public_field1":
		cast.Convert(&a.Public_field1, x) //int
	case "public_field2":
		cast.Convert(&a.Public_field2, x) //int
	case "public_field3":
		cast.Convert(&a.Public_field3, x) //int
	case "public_field_me1":
		cast.Convert(&a.Public_field_me1, x) //int
	case "public_field_me2":
		cast.Convert(&a.Public_field_me2, x) //int
	case "public_field_me3":
		cast.Convert(&a.Public_field_me3, x) //int
	}
}

// get struct value with function
func (a *News) Get(k string) (v any) {
	switch k {
	case "inc":
		return a.Inc //int
	case "role":
		return a.Role //Role
	case "key":
		return a.Key //uuid.UUID
	case "t":
		return a.T //time.Time
	case "d":
		return a.D //time.Duration
	case "ints":
		return a.Ints //int
	case "ints8":
		return a.Ints8 //int8
	case "ints16":
		return a.Ints16 //int16
	case "ints32":
		return a.Ints32 //int32
	case "ints64":
		return a.Ints64 //int64
	case "uints":
		return a.Uints //uint
	case "uints8":
		return a.Uints8 //uint8
	case "uints16":
		return a.Uints16 //uint16
	case "uints32":
		return a.Uints32 //uint32
	case "uints64":
		return a.Uints64 //uint64
	case "floats32":
		return a.Floats32 //float32
	case "floats64":
		return a.Floats64 //float64
	case "bools":
		return a.Bools //bool
	case "byte1":
		return a.Byte1 //byte
	case "bytes":
		return a.Bytes //[]byte
	case "list_ints":
		return a.List_ints //[]int
	case "list_string":
		return a.List_string //[]string
	case "list_float":
		return a.List_float //[]float64
	case "map_string_string":
		return a.Map_string_string //map[string]string
	case "map_string_bytes":
		return a.Map_string_bytes //map[string][]byte
	case "map_string_bool":
		return a.Map_string_bool //map[string]bool
	case "map_string_int":
		return a.Map_string_int //map[string]int
	case "map_string_float64":
		return a.Map_string_float64 //map[string]float64
	case "map_string_any":
		return a.Map_string_any //map[string]any
	case "map_int_string":
		return a.Map_int_string //map[int]string
	case "map_int_int":
		return a.Map_int_int //map[int]int
	case "map_int_bool":
		return a.Map_int_bool //map[int]bool
	case "renameSQL":
		return a.RenameSQL //string
	case "renameGO":
		return a.RenameGO_OK //string
	case "renameJS":
		return a.RenameJS //string
	case "mast_upper_go":
		return a.MAST_UPPER_GO //string
	case "intToSmallInt":
		return a.IntToSmallInt //int
	case "skip":
		return a.Skip //string
	case "sql_unique_u1_1":
		return a.Sql_unique_u1_1 //int
	case "sql_unique_u1_2":
		return a.Sql_unique_u1_2 //int
	case "sql_index1_1":
		return a.Sql_index1_1 //int
	case "sql_index1_2":
		return a.Sql_index1_2 //int
	case "sql_index1_3":
		return a.Sql_index1_3 //int
	case "sql_keys_1":
		return a.Sql_keys_1 //int
	case "sql_keys_2":
		return a.Sql_keys_2 //int
	case "sql_keys_3":
		return a.Sql_keys_3 //int
	case "sql_search":
		return a.Sql_search //string
	case "sql_get":
		return a.Sql_get //string
	case "sql_unique_x1":
		return a.Sql_unique_x1 //int
	case "sql_unique_x2":
		return a.Sql_unique_x2 //int
	case "sql_unique_x1_x2":
		return a.Sql_unique_x1_x2 //int
	case "sql_primary":
		return a.Sql_primary //float64
	case "sql_jsonb_index":
		return a.Sql_jsonb_index //map[string]any
	case "time_duration":
		return a.Time_duration //time.Duration
	case "go_type_int_to_strings":
		return a.Go_type_int_to_strings //int
	case "public_field1":
		return a.Public_field1 //int
	case "public_field2":
		return a.Public_field2 //int
	case "public_field3":
		return a.Public_field3 //int
	case "public_field_me1":
		return a.Public_field_me1 //int
	case "public_field_me2":
		return a.Public_field_me2 //int
	case "public_field_me3":
		return a.Public_field_me3 //int
	}
	return
}

// get any struct value as string
func (a *News) String(k string) (v string) {
	switch k {
	case "inc":
		return fmt.Sprint(a.Inc) //int
	case "role":
		return fmt.Sprint(a.Role) //Role
	case "key":
		return fmt.Sprint(a.Key) //uuid.UUID
	case "t":
		return fmt.Sprint(a.T) //time.Time
	case "d":
		return fmt.Sprint(a.D) //time.Duration
	case "ints":
		return fmt.Sprint(a.Ints) //int
	case "ints8":
		return fmt.Sprint(a.Ints8) //int8
	case "ints16":
		return fmt.Sprint(a.Ints16) //int16
	case "ints32":
		return fmt.Sprint(a.Ints32) //int32
	case "ints64":
		return fmt.Sprint(a.Ints64) //int64
	case "uints":
		return fmt.Sprint(a.Uints) //uint
	case "uints8":
		return fmt.Sprint(a.Uints8) //uint8
	case "uints16":
		return fmt.Sprint(a.Uints16) //uint16
	case "uints32":
		return fmt.Sprint(a.Uints32) //uint32
	case "uints64":
		return fmt.Sprint(a.Uints64) //uint64
	case "floats32":
		return fmt.Sprint(a.Floats32) //float32
	case "floats64":
		return fmt.Sprint(a.Floats64) //float64
	case "bools":
		return fmt.Sprint(a.Bools) //bool
	case "byte1":
		return fmt.Sprint(a.Byte1) //byte
	case "bytes":
		return fmt.Sprint(a.Bytes) //[]byte
	case "list_ints":
		return fmt.Sprint(a.List_ints) //[]int
	case "list_string":
		return fmt.Sprint(a.List_string) //[]string
	case "list_float":
		return fmt.Sprint(a.List_float) //[]float64
	case "map_string_string":
		return fmt.Sprint(a.Map_string_string) //map[string]string
	case "map_string_bytes":
		return fmt.Sprint(a.Map_string_bytes) //map[string][]byte
	case "map_string_bool":
		return fmt.Sprint(a.Map_string_bool) //map[string]bool
	case "map_string_int":
		return fmt.Sprint(a.Map_string_int) //map[string]int
	case "map_string_float64":
		return fmt.Sprint(a.Map_string_float64) //map[string]float64
	case "map_string_any":
		return fmt.Sprint(a.Map_string_any) //map[string]any
	case "map_int_string":
		return fmt.Sprint(a.Map_int_string) //map[int]string
	case "map_int_int":
		return fmt.Sprint(a.Map_int_int) //map[int]int
	case "map_int_bool":
		return fmt.Sprint(a.Map_int_bool) //map[int]bool
	case "renameSQL":
		return fmt.Sprint(a.RenameSQL) //string
	case "renameGO":
		return fmt.Sprint(a.RenameGO_OK) //string
	case "renameJS":
		return fmt.Sprint(a.RenameJS) //string
	case "mast_upper_go":
		return fmt.Sprint(a.MAST_UPPER_GO) //string
	case "intToSmallInt":
		return fmt.Sprint(a.IntToSmallInt) //int
	case "skip":
		return fmt.Sprint(a.Skip) //string
	case "sql_unique_u1_1":
		return fmt.Sprint(a.Sql_unique_u1_1) //int
	case "sql_unique_u1_2":
		return fmt.Sprint(a.Sql_unique_u1_2) //int
	case "sql_index1_1":
		return fmt.Sprint(a.Sql_index1_1) //int
	case "sql_index1_2":
		return fmt.Sprint(a.Sql_index1_2) //int
	case "sql_index1_3":
		return fmt.Sprint(a.Sql_index1_3) //int
	case "sql_keys_1":
		return fmt.Sprint(a.Sql_keys_1) //int
	case "sql_keys_2":
		return fmt.Sprint(a.Sql_keys_2) //int
	case "sql_keys_3":
		return fmt.Sprint(a.Sql_keys_3) //int
	case "sql_search":
		return fmt.Sprint(a.Sql_search) //string
	case "sql_get":
		return fmt.Sprint(a.Sql_get) //string
	case "sql_unique_x1":
		return fmt.Sprint(a.Sql_unique_x1) //int
	case "sql_unique_x2":
		return fmt.Sprint(a.Sql_unique_x2) //int
	case "sql_unique_x1_x2":
		return fmt.Sprint(a.Sql_unique_x1_x2) //int
	case "sql_primary":
		return fmt.Sprint(a.Sql_primary) //float64
	case "sql_jsonb_index":
		return fmt.Sprint(a.Sql_jsonb_index) //map[string]any
	case "time_duration":
		return fmt.Sprint(a.Time_duration) //time.Duration
	case "go_type_int_to_strings":
		return fmt.Sprint(a.Go_type_int_to_strings) //int
	case "public_field1":
		return fmt.Sprint(a.Public_field1) //int
	case "public_field2":
		return fmt.Sprint(a.Public_field2) //int
	case "public_field3":
		return fmt.Sprint(a.Public_field3) //int
	case "public_field_me1":
		return fmt.Sprint(a.Public_field_me1) //int
	case "public_field_me2":
		return fmt.Sprint(a.Public_field_me2) //int
	case "public_field_me3":
		return fmt.Sprint(a.Public_field_me3) //int
	}
	return
}

// Struct to json
func (a *News) ToJson() (r []byte) {
	js := jsons.Create().
		Add(NewsFieldInc, a.Inc).
		Add(NewsFieldRole, a.Role).
		Add(NewsFieldKey, a.Key).
		Add(NewsFieldT, a.T).
		Add(NewsFieldD, a.D).
		Add(NewsFieldInts, a.Ints).
		Add(NewsFieldInts8, a.Ints8).
		Add(NewsFieldInts16, a.Ints16).
		Add(NewsFieldInts32, a.Ints32).
		Add(NewsFieldInts64, a.Ints64).
		Add(NewsFieldUints, a.Uints).
		Add(NewsFieldUints8, a.Uints8).
		Add(NewsFieldUints16, a.Uints16).
		Add(NewsFieldUints32, a.Uints32).
		Add(NewsFieldUints64, a.Uints64).
		Add(NewsFieldFloats32, a.Floats32).
		Add(NewsFieldFloats64, a.Floats64).
		Add(NewsFieldBools, a.Bools).
		Add(NewsFieldByte1, a.Byte1).
		Add(NewsFieldBytes, a.Bytes).
		Add(NewsFieldList_ints, a.List_ints).
		Add(NewsFieldList_string, a.List_string).
		Add(NewsFieldList_float, a.List_float).
		Add(NewsFieldMap_string_string, a.Map_string_string).
		Add(NewsFieldMap_string_bytes, a.Map_string_bytes).
		Add(NewsFieldMap_string_bool, a.Map_string_bool).
		Add(NewsFieldMap_string_int, a.Map_string_int).
		Add(NewsFieldMap_string_float64, a.Map_string_float64).
		Add(NewsFieldMap_string_any, a.Map_string_any).
		Add(NewsFieldMap_int_string, a.Map_int_string).
		Add(NewsFieldMap_int_int, a.Map_int_int).
		Add(NewsFieldMap_int_bool, a.Map_int_bool).
		Add(NewsFieldRenameSQL, a.RenameSQL).
		Add(NewsFieldRenameGO_OK, a.RenameGO_OK).
		Add(NewsFieldRenameJS, a.RenameJS).
		Add(NewsFieldMAST_UPPER_GO, a.MAST_UPPER_GO).
		Add(NewsFieldIntToSmallInt, a.IntToSmallInt).
		Add(NewsFieldSkip, a.Skip).
		Add(NewsFieldSql_unique_u1_1, a.Sql_unique_u1_1).
		Add(NewsFieldSql_unique_u1_2, a.Sql_unique_u1_2).
		Add(NewsFieldSql_index1_1, a.Sql_index1_1).
		Add(NewsFieldSql_index1_2, a.Sql_index1_2).
		Add(NewsFieldSql_index1_3, a.Sql_index1_3).
		Add(NewsFieldSql_keys_1, a.Sql_keys_1).
		Add(NewsFieldSql_keys_2, a.Sql_keys_2).
		Add(NewsFieldSql_keys_3, a.Sql_keys_3).
		Add(NewsFieldSql_search, a.Sql_search).
		Add(NewsFieldSql_get, a.Sql_get).
		Add(NewsFieldSql_unique_x1, a.Sql_unique_x1).
		Add(NewsFieldSql_unique_x2, a.Sql_unique_x2).
		Add(NewsFieldSql_unique_x1_x2, a.Sql_unique_x1_x2).
		Add(NewsFieldSql_primary, a.Sql_primary).
		Add(NewsFieldSql_jsonb_index, a.Sql_jsonb_index).
		Add(NewsFieldTime_duration, a.Time_duration).
		Add(NewsFieldGo_type_int_to_strings, a.Go_type_int_to_strings).
		Add(NewsFieldPublic_field1, a.Public_field1).
		Add(NewsFieldPublic_field2, a.Public_field2).
		Add(NewsFieldPublic_field3, a.Public_field3).
		Add(NewsFieldPublic_field_me1, a.Public_field_me1).
		Add(NewsFieldPublic_field_me2, a.Public_field_me2).
		Add(NewsFieldPublic_field_me3, a.Public_field_me3)
	return js.Bytes()
}

func NewsMeList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3}
}

func NewsPublicList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3}
}

func NewsReadonlyList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexInc}
}

func (a NewsIndexTyped) Me() bool {
	switch a {
	case NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3:
		return true
	default:
		return false
	}
}
func (a NewsIndexTyped) Public() bool {
	switch a {
	case NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3:
		return true
	default:
		return false
	}
}
func (a NewsIndexTyped) Readonly() bool {
	switch a {
	case NewsIndexInc:
		return true
	default:
		return false
	}
}

// key index string
func (a NewsIndexTyped) String() string {
	switch a {
	case NewsIndexInc:
		return "inc"
	case NewsIndexRole:
		return "role"
	case NewsIndexKey:
		return "key"
	case NewsIndexT:
		return "t"
	case NewsIndexD:
		return "d"
	case NewsIndexInts:
		return "ints"
	case NewsIndexInts8:
		return "ints8"
	case NewsIndexInts16:
		return "ints16"
	case NewsIndexInts32:
		return "ints32"
	case NewsIndexInts64:
		return "ints64"
	case NewsIndexUints:
		return "uints"
	case NewsIndexUints8:
		return "uints8"
	case NewsIndexUints16:
		return "uints16"
	case NewsIndexUints32:
		return "uints32"
	case NewsIndexUints64:
		return "uints64"
	case NewsIndexFloats32:
		return "floats32"
	case NewsIndexFloats64:
		return "floats64"
	case NewsIndexBools:
		return "bools"
	case NewsIndexByte1:
		return "byte1"
	case NewsIndexBytes:
		return "bytes"
	case NewsIndexList_ints:
		return "list_ints"
	case NewsIndexList_string:
		return "list_string"
	case NewsIndexList_float:
		return "list_float"
	case NewsIndexMap_string_string:
		return "map_string_string"
	case NewsIndexMap_string_bytes:
		return "map_string_bytes"
	case NewsIndexMap_string_bool:
		return "map_string_bool"
	case NewsIndexMap_string_int:
		return "map_string_int"
	case NewsIndexMap_string_float64:
		return "map_string_float64"
	case NewsIndexMap_string_any:
		return "map_string_any"
	case NewsIndexMap_int_string:
		return "map_int_string"
	case NewsIndexMap_int_int:
		return "map_int_int"
	case NewsIndexMap_int_bool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMAST_UPPER_GO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSql_unique_u1_1:
		return "sql_unique_u1_1"
	case NewsIndexSql_unique_u1_2:
		return "sql_unique_u1_2"
	case NewsIndexSql_index1_1:
		return "sql_index1_1"
	case NewsIndexSql_index1_2:
		return "sql_index1_2"
	case NewsIndexSql_index1_3:
		return "sql_index1_3"
	case NewsIndexSql_keys_1:
		return "sql_keys_1"
	case NewsIndexSql_keys_2:
		return "sql_keys_2"
	case NewsIndexSql_keys_3:
		return "sql_keys_3"
	case NewsIndexSql_search:
		return "sql_search"
	case NewsIndexSql_get:
		return "sql_get"
	case NewsIndexSql_unique_x1:
		return "sql_unique_x1"
	case NewsIndexSql_unique_x2:
		return "sql_unique_x2"
	case NewsIndexSql_unique_x1_x2:
		return "sql_unique_x1_x2"
	case NewsIndexSql_primary:
		return "sql_primary"
	case NewsIndexSql_jsonb_index:
		return "sql_jsonb_index"
	case NewsIndexTime_duration:
		return "time_duration"
	case NewsIndexGo_type_int_to_strings:
		return "go_type_int_to_strings"
	case NewsIndexPublic_field1:
		return "public_field1"
	case NewsIndexPublic_field2:
		return "public_field2"
	case NewsIndexPublic_field3:
		return "public_field3"
	case NewsIndexPublic_field_me1:
		return "public_field_me1"
	case NewsIndexPublic_field_me2:
		return "public_field_me2"
	case NewsIndexPublic_field_me3:
		return "public_field_me3"
	default:
		return ""
	}
}

// key index string
func (a NewsIndexTyped) SQLName() string {
	switch a {
	case NewsIndexInc:
		return "inc"
	case NewsIndexRole:
		return "role"
	case NewsIndexKey:
		return "key"
	case NewsIndexT:
		return "t"
	case NewsIndexD:
		return "d"
	case NewsIndexInts:
		return "ints"
	case NewsIndexInts8:
		return "ints8"
	case NewsIndexInts16:
		return "ints16"
	case NewsIndexInts32:
		return "ints32"
	case NewsIndexInts64:
		return "ints64"
	case NewsIndexUints:
		return "uints"
	case NewsIndexUints8:
		return "uints8"
	case NewsIndexUints16:
		return "uints16"
	case NewsIndexUints32:
		return "uints32"
	case NewsIndexUints64:
		return "uints64"
	case NewsIndexFloats32:
		return "floats32"
	case NewsIndexFloats64:
		return "floats64"
	case NewsIndexBools:
		return "bools"
	case NewsIndexByte1:
		return "byte1"
	case NewsIndexBytes:
		return "bytes"
	case NewsIndexList_ints:
		return "list_ints"
	case NewsIndexList_string:
		return "list_string"
	case NewsIndexList_float:
		return "list_float"
	case NewsIndexMap_string_string:
		return "map_string_string"
	case NewsIndexMap_string_bytes:
		return "map_string_bytes"
	case NewsIndexMap_string_bool:
		return "map_string_bool"
	case NewsIndexMap_string_int:
		return "map_string_int"
	case NewsIndexMap_string_float64:
		return "map_string_float64"
	case NewsIndexMap_string_any:
		return "map_string_any"
	case NewsIndexMap_int_string:
		return "map_int_string"
	case NewsIndexMap_int_int:
		return "map_int_int"
	case NewsIndexMap_int_bool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL_OK"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMAST_UPPER_GO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSql_unique_u1_1:
		return "sql_unique_u1_1"
	case NewsIndexSql_unique_u1_2:
		return "sql_unique_u1_2"
	case NewsIndexSql_index1_1:
		return "sql_index1_1"
	case NewsIndexSql_index1_2:
		return "sql_index1_2"
	case NewsIndexSql_index1_3:
		return "sql_index1_3"
	case NewsIndexSql_keys_1:
		return "sql_keys_1"
	case NewsIndexSql_keys_2:
		return "sql_keys_2"
	case NewsIndexSql_keys_3:
		return "sql_keys_3"
	case NewsIndexSql_search:
		return "sql_search"
	case NewsIndexSql_get:
		return "sql_get"
	case NewsIndexSql_unique_x1:
		return "sql_unique_x1"
	case NewsIndexSql_unique_x2:
		return "sql_unique_x2"
	case NewsIndexSql_unique_x1_x2:
		return "sql_unique_x1_x2"
	case NewsIndexSql_primary:
		return "sql_primary"
	case NewsIndexSql_jsonb_index:
		return "sql_jsonb_index"
	case NewsIndexTime_duration:
		return "time_duration"
	case NewsIndexGo_type_int_to_strings:
		return "go_type_int_to_strings"
	case NewsIndexPublic_field1:
		return "public_field1"
	case NewsIndexPublic_field2:
		return "public_field2"
	case NewsIndexPublic_field3:
		return "public_field3"
	case NewsIndexPublic_field_me1:
		return "public_field_me1"
	case NewsIndexPublic_field_me2:
		return "public_field_me2"
	case NewsIndexPublic_field_me3:
		return "public_field_me3"
	default:
		return ""
	}
}

// key index clickhouse string
func (a NewsIndexTyped) ClickhouseName() string {
	switch a {
	case NewsIndexInc:
		return "inc"
	case NewsIndexRole:
		return "role"
	case NewsIndexKey:
		return "key"
	case NewsIndexT:
		return "t"
	case NewsIndexD:
		return "d"
	case NewsIndexInts:
		return "ints"
	case NewsIndexInts8:
		return "ints8"
	case NewsIndexInts16:
		return "ints16"
	case NewsIndexInts32:
		return "ints32"
	case NewsIndexInts64:
		return "ints64"
	case NewsIndexUints:
		return "uints"
	case NewsIndexUints8:
		return "uints8"
	case NewsIndexUints16:
		return "uints16"
	case NewsIndexUints32:
		return "uints32"
	case NewsIndexUints64:
		return "uints64"
	case NewsIndexFloats32:
		return "floats32"
	case NewsIndexFloats64:
		return "floats64"
	case NewsIndexBools:
		return "bools"
	case NewsIndexByte1:
		return "byte1"
	case NewsIndexBytes:
		return "bytes"
	case NewsIndexList_ints:
		return "list_ints"
	case NewsIndexList_string:
		return "list_string"
	case NewsIndexList_float:
		return "list_float"
	case NewsIndexMap_string_string:
		return "map_string_string"
	case NewsIndexMap_string_bytes:
		return "map_string_bytes"
	case NewsIndexMap_string_bool:
		return "map_string_bool"
	case NewsIndexMap_string_int:
		return "map_string_int"
	case NewsIndexMap_string_float64:
		return "map_string_float64"
	case NewsIndexMap_string_any:
		return "map_string_any"
	case NewsIndexMap_int_string:
		return "map_int_string"
	case NewsIndexMap_int_int:
		return "map_int_int"
	case NewsIndexMap_int_bool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMAST_UPPER_GO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSql_unique_u1_1:
		return "sql_unique_u1_1"
	case NewsIndexSql_unique_u1_2:
		return "sql_unique_u1_2"
	case NewsIndexSql_index1_1:
		return "sql_index1_1"
	case NewsIndexSql_index1_2:
		return "sql_index1_2"
	case NewsIndexSql_index1_3:
		return "sql_index1_3"
	case NewsIndexSql_keys_1:
		return "sql_keys_1"
	case NewsIndexSql_keys_2:
		return "sql_keys_2"
	case NewsIndexSql_keys_3:
		return "sql_keys_3"
	case NewsIndexSql_search:
		return "sql_search"
	case NewsIndexSql_get:
		return "sql_get"
	case NewsIndexSql_unique_x1:
		return "sql_unique_x1"
	case NewsIndexSql_unique_x2:
		return "sql_unique_x2"
	case NewsIndexSql_unique_x1_x2:
		return "sql_unique_x1_x2"
	case NewsIndexSql_primary:
		return "sql_primary"
	case NewsIndexSql_jsonb_index:
		return "sql_jsonb_index"
	case NewsIndexTime_duration:
		return "time_duration"
	case NewsIndexGo_type_int_to_strings:
		return "go_type_int_to_strings"
	case NewsIndexPublic_field1:
		return "public_field1"
	case NewsIndexPublic_field2:
		return "public_field2"
	case NewsIndexPublic_field3:
		return "public_field3"
	case NewsIndexPublic_field_me1:
		return "public_field_me1"
	case NewsIndexPublic_field_me2:
		return "public_field_me2"
	case NewsIndexPublic_field_me3:
		return "public_field_me3"
	default:
		return ""
	}
}

// key index type
func (a NewsIndexTyped) Type() string {
	switch a {
	case NewsIndexInc:
		return "int"
	case NewsIndexRole:
		return "Role"
	case NewsIndexKey:
		return "uuid.UUID"
	case NewsIndexT:
		return "time.Time"
	case NewsIndexD:
		return "time.Duration"
	case NewsIndexInts:
		return "int"
	case NewsIndexInts8:
		return "int8"
	case NewsIndexInts16:
		return "int16"
	case NewsIndexInts32:
		return "int32"
	case NewsIndexInts64:
		return "int64"
	case NewsIndexUints:
		return "uint"
	case NewsIndexUints8:
		return "uint8"
	case NewsIndexUints16:
		return "uint16"
	case NewsIndexUints32:
		return "uint32"
	case NewsIndexUints64:
		return "uint64"
	case NewsIndexFloats32:
		return "float32"
	case NewsIndexFloats64:
		return "float64"
	case NewsIndexBools:
		return "bool"
	case NewsIndexByte1:
		return "byte"
	case NewsIndexBytes:
		return "[]byte"
	case NewsIndexList_ints:
		return "[]int"
	case NewsIndexList_string:
		return "[]string"
	case NewsIndexList_float:
		return "[]float64"
	case NewsIndexMap_string_string:
		return "map[string]string"
	case NewsIndexMap_string_bytes:
		return "map[string][]byte"
	case NewsIndexMap_string_bool:
		return "map[string]bool"
	case NewsIndexMap_string_int:
		return "map[string]int"
	case NewsIndexMap_string_float64:
		return "map[string]float64"
	case NewsIndexMap_string_any:
		return "map[string]any"
	case NewsIndexMap_int_string:
		return "map[int]string"
	case NewsIndexMap_int_int:
		return "map[int]int"
	case NewsIndexMap_int_bool:
		return "map[int]bool"
	case NewsIndexRenameSQL:
		return "string"
	case NewsIndexRenameGO_OK:
		return "string"
	case NewsIndexRenameJS:
		return "string"
	case NewsIndexMAST_UPPER_GO:
		return "string"
	case NewsIndexIntToSmallInt:
		return "int"
	case NewsIndexSkip:
		return "string"
	case NewsIndexSql_unique_u1_1:
		return "int"
	case NewsIndexSql_unique_u1_2:
		return "int"
	case NewsIndexSql_index1_1:
		return "int"
	case NewsIndexSql_index1_2:
		return "int"
	case NewsIndexSql_index1_3:
		return "int"
	case NewsIndexSql_keys_1:
		return "int"
	case NewsIndexSql_keys_2:
		return "int"
	case NewsIndexSql_keys_3:
		return "int"
	case NewsIndexSql_search:
		return "string"
	case NewsIndexSql_get:
		return "string"
	case NewsIndexSql_unique_x1:
		return "int"
	case NewsIndexSql_unique_x2:
		return "int"
	case NewsIndexSql_unique_x1_x2:
		return "int"
	case NewsIndexSql_primary:
		return "float64"
	case NewsIndexSql_jsonb_index:
		return "map[string]any"
	case NewsIndexTime_duration:
		return "time.Duration"
	case NewsIndexGo_type_int_to_strings:
		return "int"
	case NewsIndexPublic_field1:
		return "int"
	case NewsIndexPublic_field2:
		return "int"
	case NewsIndexPublic_field3:
		return "int"
	case NewsIndexPublic_field_me1:
		return "int"
	case NewsIndexPublic_field_me2:
		return "int"
	case NewsIndexPublic_field_me3:
		return "int"
	default:
		return ""
	}
}

// custom title
func (a NewsIndexTyped) Title() string {
	switch a {
	case NewsIndexInc:
		return "Inc"
	case NewsIndexRole:
		return "Role"
	case NewsIndexKey:
		return "Key"
	case NewsIndexT:
		return "T"
	case NewsIndexD:
		return "D"
	case NewsIndexInts:
		return "Ints"
	case NewsIndexInts8:
		return "Ints8"
	case NewsIndexInts16:
		return "Ints16"
	case NewsIndexInts32:
		return "Ints32"
	case NewsIndexInts64:
		return "Ints64"
	case NewsIndexUints:
		return "Uints"
	case NewsIndexUints8:
		return "Uints8"
	case NewsIndexUints16:
		return "Uints16"
	case NewsIndexUints32:
		return "Uints32"
	case NewsIndexUints64:
		return "Uints64"
	case NewsIndexFloats32:
		return "Floats32"
	case NewsIndexFloats64:
		return "Floats64"
	case NewsIndexBools:
		return "Bools"
	case NewsIndexByte1:
		return "Byte1"
	case NewsIndexBytes:
		return "Bytes"
	case NewsIndexList_ints:
		return "List_ints"
	case NewsIndexList_string:
		return "List_string"
	case NewsIndexList_float:
		return "List_float"
	case NewsIndexMap_string_string:
		return "Map_string_string"
	case NewsIndexMap_string_bytes:
		return "Map_string_bytes"
	case NewsIndexMap_string_bool:
		return "Map_string_bool"
	case NewsIndexMap_string_int:
		return "Map_string_int"
	case NewsIndexMap_string_float64:
		return "Map_string_float64"
	case NewsIndexMap_string_any:
		return "Map_string_any"
	case NewsIndexMap_int_string:
		return "Map_int_string"
	case NewsIndexMap_int_int:
		return "Map_int_int"
	case NewsIndexMap_int_bool:
		return "Map_int_bool"
	case NewsIndexRenameSQL:
		return "RenameSQL"
	case NewsIndexRenameGO_OK:
		return "RenameGO_OK"
	case NewsIndexRenameJS:
		return "RenameJS"
	case NewsIndexMAST_UPPER_GO:
		return "MAST_UPPER_GO"
	case NewsIndexIntToSmallInt:
		return "IntToSmallInt"
	case NewsIndexSkip:
		return "Skip"
	case NewsIndexSql_unique_u1_1:
		return "Sql_unique_u1_1"
	case NewsIndexSql_unique_u1_2:
		return "Sql_unique_u1_2"
	case NewsIndexSql_index1_1:
		return "Sql_index1_1"
	case NewsIndexSql_index1_2:
		return "Sql_index1_2"
	case NewsIndexSql_index1_3:
		return "Sql_index1_3"
	case NewsIndexSql_keys_1:
		return "Sql_keys_1"
	case NewsIndexSql_keys_2:
		return "Sql_keys_2"
	case NewsIndexSql_keys_3:
		return "Sql_keys_3"
	case NewsIndexSql_search:
		return "Sql_search"
	case NewsIndexSql_get:
		return "Sql_get"
	case NewsIndexSql_unique_x1:
		return "Sql_unique_x1"
	case NewsIndexSql_unique_x2:
		return "Sql_unique_x2"
	case NewsIndexSql_unique_x1_x2:
		return "Sql_unique_x1_x2"
	case NewsIndexSql_primary:
		return "Sql_primary"
	case NewsIndexSql_jsonb_index:
		return "Sql_jsonb_index"
	case NewsIndexTime_duration:
		return "Time_duration"
	case NewsIndexGo_type_int_to_strings:
		return "Go_type_int_to_strings"
	case NewsIndexPublic_field1:
		return "Public_field1"
	case NewsIndexPublic_field2:
		return "Public_field2"
	case NewsIndexPublic_field3:
		return "Public_field3"
	case NewsIndexPublic_field_me1:
		return "Public_field_me1"
	case NewsIndexPublic_field_me2:
		return "Public_field_me2"
	case NewsIndexPublic_field_me3:
		return "Public_field_me3"
	default:
		return ""
	}
}

// custom desc
func (a NewsIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func NewsKeyIndex(key string) NewsIndexTyped {
	switch key {
	case "inc":
		return NewsIndexInc
	case "role":
		return NewsIndexRole
	case "key":
		return NewsIndexKey
	case "t":
		return NewsIndexT
	case "d":
		return NewsIndexD
	case "ints":
		return NewsIndexInts
	case "ints8":
		return NewsIndexInts8
	case "ints16":
		return NewsIndexInts16
	case "ints32":
		return NewsIndexInts32
	case "ints64":
		return NewsIndexInts64
	case "uints":
		return NewsIndexUints
	case "uints8":
		return NewsIndexUints8
	case "uints16":
		return NewsIndexUints16
	case "uints32":
		return NewsIndexUints32
	case "uints64":
		return NewsIndexUints64
	case "floats32":
		return NewsIndexFloats32
	case "floats64":
		return NewsIndexFloats64
	case "bools":
		return NewsIndexBools
	case "byte1":
		return NewsIndexByte1
	case "bytes":
		return NewsIndexBytes
	case "list_ints":
		return NewsIndexList_ints
	case "list_string":
		return NewsIndexList_string
	case "list_float":
		return NewsIndexList_float
	case "map_string_string":
		return NewsIndexMap_string_string
	case "map_string_bytes":
		return NewsIndexMap_string_bytes
	case "map_string_bool":
		return NewsIndexMap_string_bool
	case "map_string_int":
		return NewsIndexMap_string_int
	case "map_string_float64":
		return NewsIndexMap_string_float64
	case "map_string_any":
		return NewsIndexMap_string_any
	case "map_int_string":
		return NewsIndexMap_int_string
	case "map_int_int":
		return NewsIndexMap_int_int
	case "map_int_bool":
		return NewsIndexMap_int_bool
	case "renameSQL":
		return NewsIndexRenameSQL
	case "renameGO":
		return NewsIndexRenameGO_OK
	case "renameJS":
		return NewsIndexRenameJS
	case "mast_upper_go":
		return NewsIndexMAST_UPPER_GO
	case "intToSmallInt":
		return NewsIndexIntToSmallInt
	case "skip":
		return NewsIndexSkip
	case "sql_unique_u1_1":
		return NewsIndexSql_unique_u1_1
	case "sql_unique_u1_2":
		return NewsIndexSql_unique_u1_2
	case "sql_index1_1":
		return NewsIndexSql_index1_1
	case "sql_index1_2":
		return NewsIndexSql_index1_2
	case "sql_index1_3":
		return NewsIndexSql_index1_3
	case "sql_keys_1":
		return NewsIndexSql_keys_1
	case "sql_keys_2":
		return NewsIndexSql_keys_2
	case "sql_keys_3":
		return NewsIndexSql_keys_3
	case "sql_search":
		return NewsIndexSql_search
	case "sql_get":
		return NewsIndexSql_get
	case "sql_unique_x1":
		return NewsIndexSql_unique_x1
	case "sql_unique_x2":
		return NewsIndexSql_unique_x2
	case "sql_unique_x1_x2":
		return NewsIndexSql_unique_x1_x2
	case "sql_primary":
		return NewsIndexSql_primary
	case "sql_jsonb_index":
		return NewsIndexSql_jsonb_index
	case "time_duration":
		return NewsIndexTime_duration
	case "go_type_int_to_strings":
		return NewsIndexGo_type_int_to_strings
	case "public_field1":
		return NewsIndexPublic_field1
	case "public_field2":
		return NewsIndexPublic_field2
	case "public_field3":
		return NewsIndexPublic_field3
	case "public_field_me1":
		return NewsIndexPublic_field_me1
	case "public_field_me2":
		return NewsIndexPublic_field_me2
	case "public_field_me3":
		return NewsIndexPublic_field_me3
	default:
		return 0
	}
}

// valid struct key check
func NewsValidKey(key string) bool {
	switch key {
	case "inc", "role", "key", "t", "d", "ints", "ints8", "ints16", "ints32", "ints64", "uints", "uints8", "uints16", "uints32", "uints64", "floats32", "floats64", "bools", "byte1", "bytes", "list_ints", "list_string", "list_float", "map_string_string", "map_string_bytes", "map_string_bool", "map_string_int", "map_string_float64", "map_string_any", "map_int_string", "map_int_int", "map_int_bool", "renameSQL", "renameGO", "renameJS", "mast_upper_go", "intToSmallInt", "skip", "sql_unique_u1_1", "sql_unique_u1_2", "sql_index1_1", "sql_index1_2", "sql_index1_3", "sql_keys_1", "sql_keys_2", "sql_keys_3", "sql_search", "sql_get", "sql_unique_x1", "sql_unique_x2", "sql_unique_x1_x2", "sql_primary", "sql_jsonb_index", "time_duration", "go_type_int_to_strings", "public_field1", "public_field2", "public_field3", "public_field_me1", "public_field_me2", "public_field_me3":
		return true
	default:
		return false
	}
}

// struct to map
func (a *News) Map() map[string]any {
	return map[string]any{
		"inc":                    a.Inc,
		"role":                   a.Role,
		"key":                    a.Key,
		"t":                      a.T,
		"d":                      a.D,
		"ints":                   a.Ints,
		"ints8":                  a.Ints8,
		"ints16":                 a.Ints16,
		"ints32":                 a.Ints32,
		"ints64":                 a.Ints64,
		"uints":                  a.Uints,
		"uints8":                 a.Uints8,
		"uints16":                a.Uints16,
		"uints32":                a.Uints32,
		"uints64":                a.Uints64,
		"floats32":               a.Floats32,
		"floats64":               a.Floats64,
		"bools":                  a.Bools,
		"byte1":                  a.Byte1,
		"bytes":                  a.Bytes,
		"list_ints":              a.List_ints,
		"list_string":            a.List_string,
		"list_float":             a.List_float,
		"map_string_string":      a.Map_string_string,
		"map_string_bytes":       a.Map_string_bytes,
		"map_string_bool":        a.Map_string_bool,
		"map_string_int":         a.Map_string_int,
		"map_string_float64":     a.Map_string_float64,
		"map_string_any":         a.Map_string_any,
		"map_int_string":         a.Map_int_string,
		"map_int_int":            a.Map_int_int,
		"map_int_bool":           a.Map_int_bool,
		"renameSQL":              a.RenameSQL,
		"renameGO":               a.RenameGO_OK,
		"renameJS_OK":            a.RenameJS,
		"mast_upper_go":          a.MAST_UPPER_GO,
		"intToSmallInt":          a.IntToSmallInt,
		"skip":                   a.Skip,
		"sql_unique_u1_1":        a.Sql_unique_u1_1,
		"sql_unique_u1_2":        a.Sql_unique_u1_2,
		"sql_index1_1":           a.Sql_index1_1,
		"sql_index1_2":           a.Sql_index1_2,
		"sql_index1_3":           a.Sql_index1_3,
		"sql_keys_1":             a.Sql_keys_1,
		"sql_keys_2":             a.Sql_keys_2,
		"sql_keys_3":             a.Sql_keys_3,
		"sql_search":             a.Sql_search,
		"sql_get":                a.Sql_get,
		"sql_unique_x1":          a.Sql_unique_x1,
		"sql_unique_x2":          a.Sql_unique_x2,
		"sql_unique_x1_x2":       a.Sql_unique_x1_x2,
		"sql_primary":            a.Sql_primary,
		"sql_jsonb_index":        a.Sql_jsonb_index,
		"time_duration":          a.Time_duration,
		"go_type_int_to_strings": a.Go_type_int_to_strings,
		"public_field1":          a.Public_field1,
		"public_field2":          a.Public_field2,
		"public_field3":          a.Public_field3,
		"public_field_me1":       a.Public_field_me1,
		"public_field_me2":       a.Public_field_me2,
		"public_field_me3":       a.Public_field_me3,
	}
}

// struct to map
func (a *News) Iterate(f func(k NewsIndexTyped, v any)) {
	for _, x := range NewsIndexes() {
		f(x, a.Get(x.String()))
	}
}

// gotiny marshal
func (a *News) MarshalGotiny() []byte {
	return gotiny.Marshal(&a.Inc, &a.Role, &a.Key, &a.T, &a.D, &a.Ints, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Floats32, &a.Floats64, &a.Bools, &a.Byte1, &a.Bytes, &a.List_ints, &a.List_string, &a.List_float, &a.Map_string_string, &a.Map_string_bytes, &a.Map_string_bool, &a.Map_string_int, &a.Map_string_float64, &a.Map_string_any, &a.Map_int_string, &a.Map_int_int, &a.Map_int_bool, &a.RenameSQL, &a.RenameGO_OK, &a.RenameJS, &a.MAST_UPPER_GO, &a.IntToSmallInt, &a.Skip, &a.Sql_unique_u1_1, &a.Sql_unique_u1_2, &a.Sql_index1_1, &a.Sql_index1_2, &a.Sql_index1_3, &a.Sql_keys_1, &a.Sql_keys_2, &a.Sql_keys_3, &a.Sql_search, &a.Sql_get, &a.Sql_unique_x1, &a.Sql_unique_x2, &a.Sql_unique_x1_x2, &a.Sql_primary, &a.Sql_jsonb_index, &a.Time_duration, &a.Go_type_int_to_strings, &a.Public_field1, &a.Public_field2, &a.Public_field3, &a.Public_field_me1, &a.Public_field_me2, &a.Public_field_me3)
}

// parse gotiny
func UnmarshalNewsGotiny(v []byte) (a News) {
	gotiny.Unmarshal(v, &a.Inc, &a.Role, &a.Key, &a.T, &a.D, &a.Ints, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Floats32, &a.Floats64, &a.Bools, &a.Byte1, &a.Bytes, &a.List_ints, &a.List_string, &a.List_float, &a.Map_string_string, &a.Map_string_bytes, &a.Map_string_bool, &a.Map_string_int, &a.Map_string_float64, &a.Map_string_any, &a.Map_int_string, &a.Map_int_int, &a.Map_int_bool, &a.RenameSQL, &a.RenameGO_OK, &a.RenameJS, &a.MAST_UPPER_GO, &a.IntToSmallInt, &a.Skip, &a.Sql_unique_u1_1, &a.Sql_unique_u1_2, &a.Sql_index1_1, &a.Sql_index1_2, &a.Sql_index1_3, &a.Sql_keys_1, &a.Sql_keys_2, &a.Sql_keys_3, &a.Sql_search, &a.Sql_get, &a.Sql_unique_x1, &a.Sql_unique_x2, &a.Sql_unique_x1_x2, &a.Sql_primary, &a.Sql_jsonb_index, &a.Time_duration, &a.Go_type_int_to_strings, &a.Public_field1, &a.Public_field2, &a.Public_field3, &a.Public_field_me1, &a.Public_field_me2, &a.Public_field_me3)
	return
}

// msgp marshal
func (a *News) MessagePack() []byte {
	b, _ := msgpack.Marshal(a)
	return b
}

// msgp unmarshal
func ParseNewsMessagePack(v []byte) (a News, err error) {
	err = msgpack.Unmarshal(v, &a)
	return
}

// fast json marshal
func (a *News) Pack() []byte {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	b, _ := jsoner.Marshal(a)
	return b
}

// fast json unmarshal
func ParseNews(v []byte) (a News, err error) {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	err = jsoner.Unmarshal(v, &a)
	return
}

// Parse []any to json
func ParseTupleToNewsJson(r []any) (a NewsJson) {
	for pos, x := range r {
		switch NewsIndexTyped(pos) {
		case NewsIndexInc:
			a.Set(NewsFieldInc, x)
		case NewsIndexRole:
			a.Set(NewsFieldRole, x)
		case NewsIndexKey:
			a.Set(NewsFieldKey, x)
		case NewsIndexT:
			a.Set(NewsFieldT, x)
		case NewsIndexD:
			a.Set(NewsFieldD, x)
		case NewsIndexInts:
			a.Set(NewsFieldInts, x)
		case NewsIndexInts8:
			a.Set(NewsFieldInts8, x)
		case NewsIndexInts16:
			a.Set(NewsFieldInts16, x)
		case NewsIndexInts32:
			a.Set(NewsFieldInts32, x)
		case NewsIndexInts64:
			a.Set(NewsFieldInts64, x)
		case NewsIndexUints:
			a.Set(NewsFieldUints, x)
		case NewsIndexUints8:
			a.Set(NewsFieldUints8, x)
		case NewsIndexUints16:
			a.Set(NewsFieldUints16, x)
		case NewsIndexUints32:
			a.Set(NewsFieldUints32, x)
		case NewsIndexUints64:
			a.Set(NewsFieldUints64, x)
		case NewsIndexFloats32:
			a.Set(NewsFieldFloats32, x)
		case NewsIndexFloats64:
			a.Set(NewsFieldFloats64, x)
		case NewsIndexBools:
			a.Set(NewsFieldBools, x)
		case NewsIndexByte1:
			a.Set(NewsFieldByte1, x)
		case NewsIndexBytes:
			a.Set(NewsFieldBytes, x)
		case NewsIndexList_ints:
			a.Set(NewsFieldList_ints, x)
		case NewsIndexList_string:
			a.Set(NewsFieldList_string, x)
		case NewsIndexList_float:
			a.Set(NewsFieldList_float, x)
		case NewsIndexMap_string_string:
			a.Set(NewsFieldMap_string_string, x)
		case NewsIndexMap_string_bytes:
			a.Set(NewsFieldMap_string_bytes, x)
		case NewsIndexMap_string_bool:
			a.Set(NewsFieldMap_string_bool, x)
		case NewsIndexMap_string_int:
			a.Set(NewsFieldMap_string_int, x)
		case NewsIndexMap_string_float64:
			a.Set(NewsFieldMap_string_float64, x)
		case NewsIndexMap_string_any:
			a.Set(NewsFieldMap_string_any, x)
		case NewsIndexMap_int_string:
			a.Set(NewsFieldMap_int_string, x)
		case NewsIndexMap_int_int:
			a.Set(NewsFieldMap_int_int, x)
		case NewsIndexMap_int_bool:
			a.Set(NewsFieldMap_int_bool, x)
		case NewsIndexRenameSQL:
			a.Set(NewsFieldRenameSQL, x)
		case NewsIndexRenameGO_OK:
			a.Set(NewsFieldRenameGO_OK, x)
		case NewsIndexRenameJS:
			a.Set(NewsFieldRenameJS, x)
		case NewsIndexMAST_UPPER_GO:
			a.Set(NewsFieldMAST_UPPER_GO, x)
		case NewsIndexIntToSmallInt:
			a.Set(NewsFieldIntToSmallInt, x)
		case NewsIndexSkip:
			a.Set(NewsFieldSkip, x)
		case NewsIndexSql_unique_u1_1:
			a.Set(NewsFieldSql_unique_u1_1, x)
		case NewsIndexSql_unique_u1_2:
			a.Set(NewsFieldSql_unique_u1_2, x)
		case NewsIndexSql_index1_1:
			a.Set(NewsFieldSql_index1_1, x)
		case NewsIndexSql_index1_2:
			a.Set(NewsFieldSql_index1_2, x)
		case NewsIndexSql_index1_3:
			a.Set(NewsFieldSql_index1_3, x)
		case NewsIndexSql_keys_1:
			a.Set(NewsFieldSql_keys_1, x)
		case NewsIndexSql_keys_2:
			a.Set(NewsFieldSql_keys_2, x)
		case NewsIndexSql_keys_3:
			a.Set(NewsFieldSql_keys_3, x)
		case NewsIndexSql_search:
			a.Set(NewsFieldSql_search, x)
		case NewsIndexSql_get:
			a.Set(NewsFieldSql_get, x)
		case NewsIndexSql_unique_x1:
			a.Set(NewsFieldSql_unique_x1, x)
		case NewsIndexSql_unique_x2:
			a.Set(NewsFieldSql_unique_x2, x)
		case NewsIndexSql_unique_x1_x2:
			a.Set(NewsFieldSql_unique_x1_x2, x)
		case NewsIndexSql_primary:
			a.Set(NewsFieldSql_primary, x)
		case NewsIndexSql_jsonb_index:
			a.Set(NewsFieldSql_jsonb_index, x)
		case NewsIndexTime_duration:
			a.Set(NewsFieldTime_duration, x)
		case NewsIndexGo_type_int_to_strings:
			a.Set(NewsFieldGo_type_int_to_strings, x)
		case NewsIndexPublic_field1:
			a.Set(NewsFieldPublic_field1, x)
		case NewsIndexPublic_field2:
			a.Set(NewsFieldPublic_field2, x)
		case NewsIndexPublic_field3:
			a.Set(NewsFieldPublic_field3, x)
		case NewsIndexPublic_field_me1:
			a.Set(NewsFieldPublic_field_me1, x)
		case NewsIndexPublic_field_me2:
			a.Set(NewsFieldPublic_field_me2, x)
		case NewsIndexPublic_field_me3:
			a.Set(NewsFieldPublic_field_me3, x)
		}
	}
	return
}

// NewNewsJson create struct
func NewNewsJson() NewsJson {
	return []byte("{}")
}

// NewsJson is a struct
type NewsJson []byte

// Set value
func (a *NewsJson) Set(k string, v any) *NewsJson {
	(*a) = jsons.Set((*a), k, v)
	return a
}

// Get value
func (a *NewsJson) Get(k string) jsons.Result {
	return jsons.Get((*a), k)
}

// Get value
func (a *NewsJson) DeleteFields(fields ...string) {
	(*a) = jsons.Delete((*a), fields...)
}

// Inc set or get value
func (a *NewsJson) Inc(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldInc)
	}
	a.Set(NewsFieldInc, v[0])
	return
}

// Role set or get value
func (a *NewsJson) Role(v ...Role) (res Role) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldRole).Raw), &res)
		return
	}
	a.Set(NewsFieldRole, v[0])
	return
}

// Key set or get value
func (a *NewsJson) Key(v ...uuid.UUID) (res uuid.UUID) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldKey).Raw), &res)
		return
	}
	a.Set(NewsFieldKey, v[0])
	return
}

// T set or get value
func (a *NewsJson) T(v ...time.Time) (res time.Time) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldT).Raw), &res)
		return
	}
	a.Set(NewsFieldT, v[0])
	return
}

// D set or get value
func (a *NewsJson) D(v ...time.Duration) (res time.Duration) {
	if v == nil {
		return jsons.TimeDuration((*a), NewsFieldD)
	}
	a.Set(NewsFieldD, v[0])
	return
}

// DString get value as time
func (a *NewsJson) DString() (res string) {
	return a.D().String()
}

// Ints set or get value
func (a *NewsJson) Ints(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldInts)
	}
	a.Set(NewsFieldInts, v[0])
	return
}

// Ints8 set or get value
func (a *NewsJson) Ints8(v ...int8) (res int8) {
	if v == nil {
		return int8(jsons.Int((*a), NewsFieldInts8))
	}
	a.Set(NewsFieldInts8, v[0])
	return
}

// Ints16 set or get value
func (a *NewsJson) Ints16(v ...int16) (res int16) {
	if v == nil {
		return int16(jsons.Int((*a), NewsFieldInts16))
	}
	a.Set(NewsFieldInts16, v[0])
	return
}

// Ints32 set or get value
func (a *NewsJson) Ints32(v ...int32) (res int32) {
	if v == nil {
		return int32(jsons.Int((*a), NewsFieldInts32))
	}
	a.Set(NewsFieldInts32, v[0])
	return
}

// Ints64 set or get value
func (a *NewsJson) Ints64(v ...int64) (res int64) {
	if v == nil {
		return jsons.Int64((*a), NewsFieldInts64)
	}
	a.Set(NewsFieldInts64, v[0])
	return
}

// Uints set or get value
func (a *NewsJson) Uints(v ...uint) (res uint) {
	if v == nil {
		return jsons.Uint((*a), NewsFieldUints)
	}
	a.Set(NewsFieldUints, v[0])
	return
}

// Uints8 set or get value
func (a *NewsJson) Uints8(v ...uint8) (res uint8) {
	if v == nil {
		return jsons.Uint8((*a), NewsFieldUints8)
	}
	a.Set(NewsFieldUints8, v[0])
	return
}

// Uints16 set or get value
func (a *NewsJson) Uints16(v ...uint16) (res uint16) {
	if v == nil {
		return uint16(jsons.Uint((*a), NewsFieldUints16))
	}
	a.Set(NewsFieldUints16, v[0])
	return
}

// Uints32 set or get value
func (a *NewsJson) Uints32(v ...uint32) (res uint32) {
	if v == nil {
		return jsons.Uint32((*a), NewsFieldUints32)
	}
	a.Set(NewsFieldUints32, v[0])
	return
}

// Uints64 set or get value
func (a *NewsJson) Uints64(v ...uint64) (res uint64) {
	if v == nil {
		return jsons.Uint64((*a), NewsFieldUints64)
	}
	a.Set(NewsFieldUints64, v[0])
	return
}

// Floats32 set or get value
func (a *NewsJson) Floats32(v ...float32) (res float32) {
	if v == nil {
		return jsons.Float32((*a), NewsFieldFloats32)
	}
	a.Set(NewsFieldFloats32, v[0])
	return
}

// Floats64 set or get value
func (a *NewsJson) Floats64(v ...float64) (res float64) {
	if v == nil {
		return jsons.Float64((*a), NewsFieldFloats64)
	}
	a.Set(NewsFieldFloats64, v[0])
	return
}

// Bools set or get value
func (a *NewsJson) Bools(v ...bool) (res bool) {
	if v == nil {
		return jsons.Bool((*a), NewsFieldBools)
	}
	a.Set(NewsFieldBools, v[0])
	return
}

// Byte1 set or get value
func (a *NewsJson) Byte1(v ...byte) (res byte) {
	if v == nil {
		return jsons.Byte((*a), NewsFieldByte1)
	}
	a.Set(NewsFieldByte1, v[0])
	return
}

// Bytes set or get value
func (a *NewsJson) Bytes(v ...[]byte) (res []byte) {
	if v == nil {
		return jsons.Bytes((*a), NewsFieldBytes)
	}
	a.Set(NewsFieldBytes, v[0])
	return
}

// List_ints set or get value
func (a *NewsJson) List_ints(v ...int) (res []int) {
	if v == nil {
		return jsons.ArrayInt((*a), NewsFieldList_ints)
	}
	a.Set(NewsFieldList_ints, v)
	return
}

// List_intsAdd add values
func (a *NewsJson) List_intsAdd(v ...int) {
	a.Set(NewsFieldList_ints, append(a.List_ints(), v...))
}

// List_intsPrepend add values
func (a *NewsJson) List_intsPrepend(v ...int) {
	a.Set(NewsFieldList_ints, append(v, a.List_ints()...))
}

// List_intsAddUnique add unique values only
func (a *NewsJson) List_intsAddUnique(v ...int) {
	var list []int
	un := map[int]bool{}
	for _, x := range a.List_ints() {
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

	a.List_ints(list...)
}

// List_intsDelete add unique values only
func (a *NewsJson) List_intsDelete(v int) {
	var list []int
	for _, x := range a.List_ints() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.List_ints(list...)
}

// List_string set or get value
func (a *NewsJson) List_string(v ...string) (res []string) {
	if v == nil {
		return jsons.ArrayString((*a), NewsFieldList_string)
	}
	a.Set(NewsFieldList_string, v)
	return
}

// List_stringAdd add values
func (a *NewsJson) List_stringAdd(v ...string) {
	a.Set(NewsFieldList_string, append(a.List_string(), v...))
}

// List_stringPrepend add values
func (a *NewsJson) List_stringPrepend(v ...string) {
	a.Set(NewsFieldList_string, append(v, a.List_string()...))
}

// List_stringAddUnique add unique values only
func (a *NewsJson) List_stringAddUnique(v ...string) {
	var list []string
	un := map[string]bool{}
	for _, x := range a.List_string() {
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

	a.List_string(list...)
}

// List_stringDelete add unique values only
func (a *NewsJson) List_stringDelete(v string) {
	var list []string
	for _, x := range a.List_string() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.List_string(list...)
}

// List_float set or get value
func (a *NewsJson) List_float(v ...float64) (res []float64) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldList_float).Raw), &res)
		return
	}
	a.Set(NewsFieldList_float, v)
	return
}

// List_floatAdd add values
func (a *NewsJson) List_floatAdd(v ...float64) {
	a.Set(NewsFieldList_float, append(a.List_float(), v...))
}

// List_floatPrepend add values
func (a *NewsJson) List_floatPrepend(v ...float64) {
	a.Set(NewsFieldList_float, append(v, a.List_float()...))
}

// List_floatAddUnique add unique values only
func (a *NewsJson) List_floatAddUnique(v ...float64) {
	var list []float64
	un := map[float64]bool{}
	for _, x := range a.List_float() {
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

	a.List_float(list...)
}

// List_floatDelete add unique values only
func (a *NewsJson) List_floatDelete(v float64) {
	var list []float64
	for _, x := range a.List_float() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.List_float(list...)
}

// Map_string_string set or get value
func (a *NewsJson) Map_string_string(v ...map[string]string) (res map[string]string) {
	if v == nil {
		return jsons.MapString((*a), NewsFieldMap_string_string)
	}
	a.Set(NewsFieldMap_string_string, v[0])
	return
}

// Map_string_stringAdd add values
func (a *NewsJson) Map_string_stringAdd(k string, v string) {
	maps := a.Map_string_string()
	maps[k] = v
	a.Map_string_string(maps)
}

// Map_string_stringDelete add unique values only
func (a *NewsJson) Map_string_stringDelete(k string) {
	maps := a.Map_string_string()
	delete(maps, k)
	a.Map_string_string(maps)
}

// Map_string_stringHas check value
func (a *NewsJson) Map_string_stringHas(k string) bool {
	maps := a.Map_string_string()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_string_bytes set or get value
func (a *NewsJson) Map_string_bytes(v ...map[string][]byte) (res map[string][]byte) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldMap_string_bytes).Raw), &res)
		return
	}
	a.Set(NewsFieldMap_string_bytes, v[0])
	return
}

// Map_string_bool set or get value
func (a *NewsJson) Map_string_bool(v ...map[string]bool) (res map[string]bool) {
	if v == nil {
		return jsons.MapBool((*a), NewsFieldMap_string_bool)
	}
	a.Set(NewsFieldMap_string_bool, v[0])
	return
}

// Map_string_boolAdd add values
func (a *NewsJson) Map_string_boolAdd(k string, v bool) {
	maps := a.Map_string_bool()
	maps[k] = v
	a.Map_string_bool(maps)
}

// Map_string_boolDelete add unique values only
func (a *NewsJson) Map_string_boolDelete(k string) {
	maps := a.Map_string_bool()
	delete(maps, k)
	a.Map_string_bool(maps)
}

// Map_string_boolHas check value
func (a *NewsJson) Map_string_boolHas(k string) bool {
	maps := a.Map_string_bool()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_string_int set or get value
func (a *NewsJson) Map_string_int(v ...map[string]int) (res map[string]int) {
	if v == nil {
		return jsons.MapInt((*a), NewsFieldMap_string_int)
	}
	a.Set(NewsFieldMap_string_int, v[0])
	return
}

// Map_string_intAdd add values
func (a *NewsJson) Map_string_intAdd(k string, v int) {
	maps := a.Map_string_int()
	maps[k] = v
	a.Map_string_int(maps)
}

// Map_string_intDelete add unique values only
func (a *NewsJson) Map_string_intDelete(k string) {
	maps := a.Map_string_int()
	delete(maps, k)
	a.Map_string_int(maps)
}

// Map_string_intHas check value
func (a *NewsJson) Map_string_intHas(k string) bool {
	maps := a.Map_string_int()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_string_float64 set or get value
func (a *NewsJson) Map_string_float64(v ...map[string]float64) (res map[string]float64) {
	if v == nil {
		return jsons.MapFloats((*a), NewsFieldMap_string_float64)
	}
	a.Set(NewsFieldMap_string_float64, v[0])
	return
}

// Map_string_float64Add add values
func (a *NewsJson) Map_string_float64Add(k string, v float64) {
	maps := a.Map_string_float64()
	maps[k] = v
	a.Map_string_float64(maps)
}

// Map_string_float64Delete add unique values only
func (a *NewsJson) Map_string_float64Delete(k string) {
	maps := a.Map_string_float64()
	delete(maps, k)
	a.Map_string_float64(maps)
}

// Map_string_float64Has check value
func (a *NewsJson) Map_string_float64Has(k string) bool {
	maps := a.Map_string_float64()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_string_any set or get value
func (a *NewsJson) Map_string_any(v ...map[string]any) (res map[string]any) {
	if v == nil {
		return jsons.MapAny((*a), NewsFieldMap_string_any)
	}
	a.Set(NewsFieldMap_string_any, v[0])
	return
}

// Map_string_anyAdd add values
func (a *NewsJson) Map_string_anyAdd(k string, v any) {
	maps := a.Map_string_any()
	maps[k] = v
	a.Map_string_any(maps)
}

// Map_string_anyDelete add unique values only
func (a *NewsJson) Map_string_anyDelete(k string) {
	maps := a.Map_string_any()
	delete(maps, k)
	a.Map_string_any(maps)
}

// Map_string_anyHas check value
func (a *NewsJson) Map_string_anyHas(k string) bool {
	maps := a.Map_string_any()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_int_string set or get value
func (a *NewsJson) Map_int_string(v ...map[int]string) (res map[int]string) {
	if v == nil {
		return jsons.MapIntString((*a), NewsFieldMap_int_string)
	}
	a.Set(NewsFieldMap_int_string, v[0])
	return
}

// Map_int_stringAdd add values
func (a *NewsJson) Map_int_stringAdd(k int, v string) {
	maps := a.Map_int_string()
	maps[k] = v
	a.Map_int_string(maps)
}

// Map_int_stringDelete add unique values only
func (a *NewsJson) Map_int_stringDelete(k int) {
	maps := a.Map_int_string()
	delete(maps, k)
	a.Map_int_string(maps)
}

// Map_int_stringHas check value
func (a *NewsJson) Map_int_stringHas(k int) bool {
	maps := a.Map_int_string()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_int_int set or get value
func (a *NewsJson) Map_int_int(v ...map[int]int) (res map[int]int) {
	if v == nil {
		return jsons.MapIntInt((*a), NewsFieldMap_int_int)
	}
	a.Set(NewsFieldMap_int_int, v[0])
	return
}

// Map_int_intAdd add values
func (a *NewsJson) Map_int_intAdd(k int, v int) {
	maps := a.Map_int_int()
	maps[k] = v
	a.Map_int_int(maps)
}

// Map_int_intDelete add unique values only
func (a *NewsJson) Map_int_intDelete(k int) {
	maps := a.Map_int_int()
	delete(maps, k)
	a.Map_int_int(maps)
}

// Map_int_intHas check value
func (a *NewsJson) Map_int_intHas(k int) bool {
	maps := a.Map_int_int()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Map_int_bool set or get value
func (a *NewsJson) Map_int_bool(v ...map[int]bool) (res map[int]bool) {
	if v == nil {
		_ = jsoniter.Unmarshal([]byte(jsons.Get((*a), NewsFieldMap_int_bool).Raw), &res)
		return
	}
	a.Set(NewsFieldMap_int_bool, v[0])
	return
}

// RenameSQL set or get value
func (a *NewsJson) RenameSQL(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldRenameSQL)
	}
	a.Set(NewsFieldRenameSQL, v[0])
	return
}

// RenameGO_OK set or get value
func (a *NewsJson) RenameGO_OK(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldRenameGO_OK)
	}
	a.Set(NewsFieldRenameGO_OK, v[0])
	return
}

// RenameJS set or get value
func (a *NewsJson) RenameJS(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldRenameJS)
	}
	a.Set(NewsFieldRenameJS, v[0])
	return
}

// MAST_UPPER_GO set or get value
func (a *NewsJson) MAST_UPPER_GO(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldMAST_UPPER_GO)
	}
	a.Set(NewsFieldMAST_UPPER_GO, v[0])
	return
}

// IntToSmallInt set or get value
func (a *NewsJson) IntToSmallInt(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldIntToSmallInt)
	}
	a.Set(NewsFieldIntToSmallInt, v[0])
	return
}

// Skip set or get value
func (a *NewsJson) Skip(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldSkip)
	}
	a.Set(NewsFieldSkip, v[0])
	return
}

// Sql_unique_u1_1 set or get value
func (a *NewsJson) Sql_unique_u1_1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_unique_u1_1)
	}
	a.Set(NewsFieldSql_unique_u1_1, v[0])
	return
}

// Sql_unique_u1_2 set or get value
func (a *NewsJson) Sql_unique_u1_2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_unique_u1_2)
	}
	a.Set(NewsFieldSql_unique_u1_2, v[0])
	return
}

// Sql_index1_1 set or get value
func (a *NewsJson) Sql_index1_1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_index1_1)
	}
	a.Set(NewsFieldSql_index1_1, v[0])
	return
}

// Sql_index1_2 set or get value
func (a *NewsJson) Sql_index1_2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_index1_2)
	}
	a.Set(NewsFieldSql_index1_2, v[0])
	return
}

// Sql_index1_3 set or get value
func (a *NewsJson) Sql_index1_3(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_index1_3)
	}
	a.Set(NewsFieldSql_index1_3, v[0])
	return
}

// Sql_keys_1 set or get value
func (a *NewsJson) Sql_keys_1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_keys_1)
	}
	a.Set(NewsFieldSql_keys_1, v[0])
	return
}

// Sql_keys_2 set or get value
func (a *NewsJson) Sql_keys_2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_keys_2)
	}
	a.Set(NewsFieldSql_keys_2, v[0])
	return
}

// Sql_keys_3 set or get value
func (a *NewsJson) Sql_keys_3(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_keys_3)
	}
	a.Set(NewsFieldSql_keys_3, v[0])
	return
}

// Sql_search set or get value
func (a *NewsJson) Sql_search(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldSql_search)
	}
	a.Set(NewsFieldSql_search, v[0])
	return
}

// Sql_get set or get value
func (a *NewsJson) Sql_get(v ...string) (res string) {
	if v == nil {
		return jsons.String((*a), NewsFieldSql_get)
	}
	a.Set(NewsFieldSql_get, v[0])
	return
}

// Sql_unique_x1 set or get value
func (a *NewsJson) Sql_unique_x1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_unique_x1)
	}
	a.Set(NewsFieldSql_unique_x1, v[0])
	return
}

// Sql_unique_x2 set or get value
func (a *NewsJson) Sql_unique_x2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_unique_x2)
	}
	a.Set(NewsFieldSql_unique_x2, v[0])
	return
}

// Sql_unique_x1_x2 set or get value
func (a *NewsJson) Sql_unique_x1_x2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldSql_unique_x1_x2)
	}
	a.Set(NewsFieldSql_unique_x1_x2, v[0])
	return
}

// Sql_primary set or get value
func (a *NewsJson) Sql_primary(v ...float64) (res float64) {
	if v == nil {
		return jsons.Float64((*a), NewsFieldSql_primary)
	}
	a.Set(NewsFieldSql_primary, v[0])
	return
}

// Sql_jsonb_index set or get value
func (a *NewsJson) Sql_jsonb_index(v ...map[string]any) (res map[string]any) {
	if v == nil {
		return jsons.MapAny((*a), NewsFieldSql_jsonb_index)
	}
	a.Set(NewsFieldSql_jsonb_index, v[0])
	return
}

// Sql_jsonb_indexAdd add values
func (a *NewsJson) Sql_jsonb_indexAdd(k string, v any) {
	maps := a.Sql_jsonb_index()
	maps[k] = v
	a.Sql_jsonb_index(maps)
}

// Sql_jsonb_indexDelete add unique values only
func (a *NewsJson) Sql_jsonb_indexDelete(k string) {
	maps := a.Sql_jsonb_index()
	delete(maps, k)
	a.Sql_jsonb_index(maps)
}

// Sql_jsonb_indexHas check value
func (a *NewsJson) Sql_jsonb_indexHas(k string) bool {
	maps := a.Sql_jsonb_index()
	return !reflect.ValueOf(maps[k]).IsZero()
}

// Time_duration set or get value
func (a *NewsJson) Time_duration(v ...time.Duration) (res time.Duration) {
	if v == nil {
		return jsons.TimeDuration((*a), NewsFieldTime_duration)
	}
	a.Set(NewsFieldTime_duration, v[0])
	return
}

// Time_durationString get value as time
func (a *NewsJson) Time_durationString() (res string) {
	return a.Time_duration().String()
}

// Go_type_int_to_strings set or get value
func (a *NewsJson) Go_type_int_to_strings(v ...string) (res []string) {
	if v == nil {
		return jsons.ArrayString((*a), NewsFieldGo_type_int_to_strings)
	}
	a.Set(NewsFieldGo_type_int_to_strings, v)
	return
}

// Go_type_int_to_stringsAdd add values
func (a *NewsJson) Go_type_int_to_stringsAdd(v ...string) {
	a.Set(NewsFieldGo_type_int_to_strings, append(a.Go_type_int_to_strings(), v...))
}

// Go_type_int_to_stringsPrepend add values
func (a *NewsJson) Go_type_int_to_stringsPrepend(v ...string) {
	a.Set(NewsFieldGo_type_int_to_strings, append(v, a.Go_type_int_to_strings()...))
}

// Go_type_int_to_stringsAddUnique add unique values only
func (a *NewsJson) Go_type_int_to_stringsAddUnique(v ...string) {
	var list []string
	un := map[string]bool{}
	for _, x := range a.Go_type_int_to_strings() {
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

	a.Go_type_int_to_strings(list...)
}

// Go_type_int_to_stringsDelete add unique values only
func (a *NewsJson) Go_type_int_to_stringsDelete(v string) {
	var list []string
	for _, x := range a.Go_type_int_to_strings() {
		if x == v {
			continue
		}
		list = append(list, x)
	}
	a.Go_type_int_to_strings(list...)
}

// Public_field1 set or get value
func (a *NewsJson) Public_field1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field1)
	}
	a.Set(NewsFieldPublic_field1, v[0])
	return
}

// Public_field2 set or get value
func (a *NewsJson) Public_field2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field2)
	}
	a.Set(NewsFieldPublic_field2, v[0])
	return
}

// Public_field3 set or get value
func (a *NewsJson) Public_field3(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field3)
	}
	a.Set(NewsFieldPublic_field3, v[0])
	return
}

// Public_field_me1 set or get value
func (a *NewsJson) Public_field_me1(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field_me1)
	}
	a.Set(NewsFieldPublic_field_me1, v[0])
	return
}

// Public_field_me2 set or get value
func (a *NewsJson) Public_field_me2(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field_me2)
	}
	a.Set(NewsFieldPublic_field_me2, v[0])
	return
}

// Public_field_me3 set or get value
func (a *NewsJson) Public_field_me3(v ...int) (res int) {
	if v == nil {
		return jsons.Int((*a), NewsFieldPublic_field_me3)
	}
	a.Set(NewsFieldPublic_field_me3, v[0])
	return
}

// sql NewsSQL class
type NewsSQL struct{ pool *pgxpool.Pool }

func NewNewsSQL(pool *pgxpool.Pool) (a *NewsSQL) {
	a = new(NewsSQL)
	a.pool = pool
	a.CreateTable()
	return
}

func ConnNewsSQL(pool *pgxpool.Pool) (a *NewsSQL) {
	return &NewsSQL{pool}
}

//new conn

func (a *NewsSQL) Conn(f func(conn *pgxpool.Conn) (err error)) (err error) {

	conn, err := a.pool.Acquire(context.Background())
	if err != nil {
		return
	}
	defer conn.Release()
	return f(conn)
}

// parse sql query
func (a *NewsSQL) TableName() (res string) {
	return "news"
}

func (a *NewsSQL) Count(where ...string) (count int, err error) {

	var q string

	switch len(where) {
	case 0:
		q = "select count(*) from news"
	default:
		q = "select count(*) from news where " + strings.Join(where, " ")
	}

	err = a.Conn(func(conn *pgxpool.Conn) (err error) {
		return conn.QueryRow(context.Background(), q).Scan(&count)
	})
	return
}

// Insert struct and return int id
func (a *NewsSQL) Insert(v News) (id int, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into news (role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59) returning inc"
		err = conn.QueryRow(c, q, v.sqlTuple()...).Scan(&id)
		return
	})
	return
}

// Insert struct and return int id
func (a *NewsSQL) InsertNoConflict(v *News) (id int, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into news (role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59) on conflict do nothing returning inc"
		err = conn.QueryRow(c, q, v.sqlTuple()...).Scan(&id)
		return
	})
	return
}

// Insert full struct ID must be (ignore all skips)
func (a *NewsSQL) InsertFull(v News) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60)"
		_, err = conn.Exec(c, q, v.sqlAllTuples()...)
		return
	})
	return
}

// update serial counter (count all items, add 1 and plus custom int)
// useful if you insert with ID
func (a *NewsSQL) ReindexIncSerialCounter(add ...int) (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		plus := 1
		if len(add) > 0 {
			plus = add[0]
		}
		_, err = conn.Exec(context.Background(), "SELECT setval(pg_get_serial_sequence('news', 'inc'), COALESCE((SELECT MAX(inc) FROM news), 0) + $1, false)", plus)
		return
	})
}

// set serial counter
func (a *NewsSQL) SetIncSerialCounter(value int) (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "SELECT setval(pg_get_serial_sequence('news', 'inc'), $1, false)", value)
		return
	})
}

// parse sql query
func (a *NewsSQL) Get(id any, fields ...NewsIndexTyped) (res News, err error) {

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
		q = q + "from news where inc = $1 limit 1"
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

// get custom struct sql query
func (a *NewsSQL) GetPublic(id any) (res NewsPublic, err error) {
	p, err := a.Get(id, NewsPublicList()...)
	if err != nil {
		return
	}
	res = *p.NewsPublic()
	return
}

// get custom struct sql query
func (a *NewsSQL) GetMe(id any) (res NewsMe, err error) {
	p, err := a.Get(id, NewsMeList()...)
	if err != nil {
		return
	}
	res = *p.NewsMe()
	return
}

// parse sql query
func (a *NewsSQL) GetWhere(where string, fields ...NewsIndexTyped) (res News, err error) {

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
		q = q + " from news"
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
func (a *NewsSQL) Row(eq map[string]any, fields ...NewsIndexTyped) (res News, err error) {

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
			if !NewsValidKey(k) {
				err = errors.New(k)
				return
			}
			in := NewsKeyIndex(k)
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
func (a *NewsSQL) All(fields ...NewsIndexTyped) (res []*News, err error) {

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
		q += " from news"
		rows, err := conn.Query(c, q)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item News
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
func (a *NewsSQL) List(limit, offset int, fields ...NewsIndexTyped) (res []News, err error) {

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
		q += " from news limit $1 offset $2"
		rows, err := conn.Query(c, q, limit, offset)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item News
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
func (a *NewsSQL) Update(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if !NewsValidKey(k) {
			return errors.New("invalid key")
		}
		in := NewsKeyIndex(k)
		q := fmt.Sprintf("update news set %s = $1 where inc = $2", in.SQLName())
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateWhere(k string, v any, where string) (err error) {
	if !NewsValidKey(k) {
		return errors.New("invalid key")
	}
	in := NewsKeyIndex(k)
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		q := fmt.Sprintf("update %s set %s = $1 where %s", a.TableName(), in.SQLName(), where)
		_, err = conn.Exec(context.Background(), q, v)
		return
	})
}

// update sql query
func (a *NewsSQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if keys == nil {
			return errors.New("emptykeys")
		}

		var fields []string
		var values []any
		var count int
		for k, v := range keys {
			if !NewsValidKey(k) {
				return errors.New(k)
			}
			in := NewsKeyIndex(k)
			count++
			fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
			values = append(values, v)
		}

		list := strings.Join(fields, ", ")
		count++
		values = append(values, id)

		q := fmt.Sprintf("update news set %s where inc = $%d", list, count)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, values...)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		if keys == nil {
			return errors.New("emptykeys")
		}

		var fields []string
		var values []any
		var count int
		for k, v := range keys {
			if !NewsValidKey(k) {
				return errors.New(k)
			}
			in := NewsKeyIndex(k)
			count++
			fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
			values = append(values, v)
		}

		list := strings.Join(fields, ", ")
		count++

		where = fmt.Sprintf(where, args...)
		q := fmt.Sprintf("update news set %s where %s", list, where)
		_, err = conn.Exec(c, q, values...)
		return
	})
	return
}

// add values to array
func (a *NewsSQL) AddList_ints(id any, v ...int) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_ints = list_ints || $1::bigint[] where inc = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_ints(id any, v int, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_ints = array_remove(list_ints, $1::bigint) where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *NewsSQL) AddList_intsWhere(v int, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_ints = array_append(list_ints, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_intsWhere(v int, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_ints = array_remove(list_ints, $1::bigint) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *NewsSQL) AddList_string(id any, v ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_string = list_string || $1::text[] where inc = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_string(id any, v string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_string = array_remove(list_string, $1::text) where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *NewsSQL) AddList_stringWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_string = array_append(list_string, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_stringWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_string = array_remove(list_string, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// add values to array
func (a *NewsSQL) AddList_float(id any, v ...float64) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_float = list_float || $1::double precision[] where inc = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_float(id any, v float64, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_float = array_remove(list_float, $1::double precision) where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *NewsSQL) AddList_floatWhere(v float64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_float = array_append(list_float, $1::double precision) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteList_floatWhere(v float64, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set list_float = array_remove(list_float, $1::double precision) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_string(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_string = map_string_string || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_string(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_string = map_string_string || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_string(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_string = map_string_string - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_string(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_string = map_string_string - $1 || jsonb_build_object($2, map_string_string->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_bytes(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_bytes = map_string_bytes || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_bytes(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_bytes = map_string_bytes || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_bytes(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_bytes = map_string_bytes - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_bytes(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_bytes = map_string_bytes - $1 || jsonb_build_object($2, map_string_bytes->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_bool(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_bool = map_string_bool || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_bool(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_bool = map_string_bool || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_bool(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_bool = map_string_bool - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_bool(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_bool = map_string_bool - $1 || jsonb_build_object($2, map_string_bool->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_int(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_int = map_string_int || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_int(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_int = map_string_int || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_int(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_int = map_string_int - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_int(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_int = map_string_int - $1 || jsonb_build_object($2, map_string_int->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_float64(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_float64 = map_string_float64 || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_float64(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_float64 = map_string_float64 || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_float64(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_float64 = map_string_float64 - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_float64(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_float64 = map_string_float64 - $1 || jsonb_build_object($2, map_string_float64->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_string_any(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_any = map_string_any || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_string_any(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_string_any = map_string_any || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_string_any(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_any = map_string_any - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_string_any(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_string_any = map_string_any - $1 || jsonb_build_object($2, map_string_any->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_int_string(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_string = map_int_string || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_int_string(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_string = map_int_string || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_int_string(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_string = map_int_string - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_int_string(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_string = map_int_string - $1 || jsonb_build_object($2, map_int_string->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_int_int(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_int = map_int_int || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_int_int(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_int = map_int_int || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_int_int(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_int = map_int_int - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_int_int(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_int = map_int_int - $1 || jsonb_build_object($2, map_int_int->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateMap_int_bool(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_bool = map_int_bool || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesMap_int_bool(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set map_int_bool = map_int_bool || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeyMap_int_bool(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_bool = map_int_bool - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeyMap_int_bool(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set map_int_bool = map_int_bool - $1 || jsonb_build_object($2, map_int_bool->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdateSql_jsonb_index(id any, k string, v any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		// json escape
		res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
		q := fmt.Sprintf("update news set sql_jsonb_index = sql_jsonb_index || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// update sql query
func (a *NewsSQL) UpdatesSql_jsonb_index(id any, keys map[string]any, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		b, _ := jsoniter.Marshal(keys)
		// json escape
		res := strings.ReplaceAll(string(b), "$$", "$ $")
		q := fmt.Sprintf("update news set sql_jsonb_index = sql_jsonb_index || $$%s$$::jsonb where inc = $1", res)
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, id)
		return
	})
	return
}

// delete key from jsonb
func (a *NewsSQL) DeleteKeySql_jsonb_index(id any, k string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set sql_jsonb_index = sql_jsonb_index - $1 where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, k, id)
		return
	})
	return
}

// rename map key jsonb
func (a *NewsSQL) RenameKeySql_jsonb_index(id any, k, newkey string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set sql_jsonb_index = sql_jsonb_index - $1 || jsonb_build_object($2, sql_jsonb_index->$1) where inc = $3"
		_, err = conn.Exec(c, q, k, newkey, id)
		return
	})
	return
}

// add values to array
func (a *NewsSQL) AddGo_type_int_to_strings(id any, v ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set go_type_int_to_strings = go_type_int_to_strings || $1::text[] where inc = $2"
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteGo_type_int_to_strings(id any, v string, where ...string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set go_type_int_to_strings = array_remove(go_type_int_to_strings, $1::text) where inc = $2"
		if len(where) > 0 {
			q += " and " + where[0]
		}
		_, err = conn.Exec(c, q, v, id)
		return
	})
	return
}

// add value to array
func (a *NewsSQL) AddGo_type_int_to_stringsWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set go_type_int_to_strings = array_append(go_type_int_to_strings, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

// delete value from array
func (a *NewsSQL) DeleteGo_type_int_to_stringsWhere(v string, where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := "update news set go_type_int_to_strings = array_remove(go_type_int_to_strings, $1::text) where " + where
		_, err = conn.Exec(c, q, v)
		return
	})
	return
}

type NewsQuery struct {
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

func (a *NewsQuery) Render() (sql string, fields []NewsIndexTyped, values []any) {

	switch len(a.Fields) == 0 {
	case true:
		fields = []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexList_ints, NewsIndexList_string, NewsIndexList_float, NewsIndexMap_string_string, NewsIndexMap_string_bytes, NewsIndexMap_string_bool, NewsIndexMap_string_int, NewsIndexMap_string_float64, NewsIndexMap_string_any, NewsIndexMap_int_string, NewsIndexMap_int_int, NewsIndexMap_int_bool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMAST_UPPER_GO, NewsIndexIntToSmallInt, NewsIndexSql_unique_u1_1, NewsIndexSql_unique_u1_2, NewsIndexSql_index1_1, NewsIndexSql_index1_2, NewsIndexSql_index1_3, NewsIndexSql_keys_1, NewsIndexSql_keys_2, NewsIndexSql_keys_3, NewsIndexSql_search, NewsIndexSql_get, NewsIndexSql_unique_x1, NewsIndexSql_unique_x2, NewsIndexSql_unique_x1_x2, NewsIndexSql_primary, NewsIndexSql_jsonb_index, NewsIndexTime_duration, NewsIndexGo_type_int_to_strings, NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3, NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3}
	default:
		for _, x := range a.Fields {
			if !NewsValidKey(x) {
				continue
			}
			fields = append(fields, NewsKeyIndex(x))
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
	list = append(list, "from news")

	var andlist []string

	// EQ where
	for k, v := range a.EQ {
		if !NewsValidKey(k) {
			continue
		}
		p := NewsKeyIndex(k)
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

		if !NewsValidKey(k) {
			continue
		}
		sqlName := NewsKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s > $%d", sqlName, count))
		values = append(values, v)
	}

	// LT where
	for k, v := range a.LT {
		if !NewsValidKey(k) {
			continue
		}
		sqlName := NewsKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s < $%d", sqlName, count))
		values = append(values, v)
	}

	// NOT where
	for k, v := range a.NOT {
		if !NewsValidKey(k) {
			continue
		}
		sqlName := NewsKeyIndex(k).SQLName()

		for _, x := range v {
			count++
			andlist = append(andlist, fmt.Sprintf("%s != $%d", sqlName, count))
			values = append(values, x)
		}

	}

	// LIKE where
	for k, v := range a.Like {
		if !NewsValidKey(k) {
			continue
		}
		sqlName := NewsKeyIndex(k).SQLName()
		count++
		andlist = append(andlist, fmt.Sprintf("%s ilike $%d", sqlName, count))
		values = append(values, "%"+v+"%")
	}

	// IN where
	if a.IN != nil {
		for k, v := range a.IN {
			if !NewsValidKey(k) {
				continue
			}
			sqlName := NewsKeyIndex(k).SQLName()
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
			if !NewsValidKey(k) {
				continue
			}
			sqlName := NewsKeyIndex(k).SQLName()
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
			if !NewsValidKey(k) {
				continue
			}
			sqlName := NewsKeyIndex(k).SQLName()
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
	if a.Sort != "" && NewsValidKey(a.Sort) {
		list = append(list, fmt.Sprintf("order by %s", NewsKeyIndex(a.Sort).SQLName()))
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

func NewNewsQuery() *NewsQuery {
	a := new(NewsQuery)
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

func (a *NewsSQL) Search(q *NewsQuery) (res []News, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		sql, fields, values := q.Render()

		rows, err := conn.Query(c, sql, values...)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item News
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
func (a *NewsSQL) Select(where string, fields ...NewsIndexTyped) (res []News, err error) {

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
		q = q + "from news"
		if where != "" {
			q += " where " + where
		}
		rows, err := conn.Query(c, q)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item News
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
func (a *NewsSQL) Delete(id any) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		_, err = conn.Exec(c, "delete from news where inc = $1", id)
		return

	})
	return
}

// delete item where: i = 1 and w = 'nice'
func (a *NewsSQL) DeleteWhere(where string) (err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		_, err = conn.Exec(c, fmt.Sprintf("delete from news where %s", where))
		return

	})
	return
}

// has value in db
func (a *NewsSQL) Has(field NewsIndexTyped, v any) (has bool, err error) {

	c := context.Background()
	err = a.Conn(func(conn *pgxpool.Conn) (err error) {

		q := fmt.Sprintf("select exists (select 1 from news where %s = $1 limit 1)", field.SQLName())
		err = conn.QueryRow(c, q, v).Scan(&has)
		return
	})
	return
}

// Create table
func (a *NewsSQL) CreateTable() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		q := `create table if not exists news (
	inc                        bigserial,
	role                       bigint,
	key                        uuid,
	t                          timestamptz,
	d                          bigint,
	ints                       bigint,
	ints8                      smallint,
	ints16                     smallint,
	ints32                     integer,
	ints64                     bigint,
	uints                      numeric(20,0),
	uints8                     smallint,
	uints16                    integer,
	uints32                    bigint,
	uints64                    numeric(20,0),
	floats32                   real,
	floats64                   double precision,
	bools                      boolean,
	byte1                      smallint,
	bytes                      bytea,
	list_ints                  bigint[] default '{}',
	list_string                text[] default '{}',
	list_float                 double precision[] default '{}',
	map_string_string          jsonb default '{}'::jsonb,
	map_string_bytes           jsonb default '{}'::jsonb,
	map_string_bool            jsonb default '{}'::jsonb,
	map_string_int             jsonb default '{}'::jsonb,
	map_string_float64         jsonb default '{}'::jsonb,
	map_string_any             jsonb default '{}'::jsonb,
	map_int_string             jsonb default '{}'::jsonb,
	map_int_int                jsonb default '{}'::jsonb,
	map_int_bool               jsonb default '{}'::jsonb,
	renameSQL_OK               text,
	renameGO                   text,
	renameJS                   text,
	mast_upper_go              text,
	intToSmallInt              smallint,
	sql_unique_u1_1            bigint,
	sql_unique_u1_2            bigint,
	sql_index1_1               bigint,
	sql_index1_2               bigint,
	sql_index1_3               bigint,
	sql_keys_1                 bigint,
	sql_keys_2                 bigint,
	sql_keys_3                 bigint,
	sql_search                 text,
	sql_get                    text,
	sql_unique_x1              bigint,
	sql_unique_x2              bigint,
	sql_unique_x1_x2           bigint,
	sql_primary                double precision primary key,
	sql_jsonb_index            jsonb default '{}'::jsonb,
	time_duration              bigint,
	go_type_int_to_strings     text[] default '{}',
	public_field1              bigint,
	public_field2              bigint,
	public_field3              bigint,
	public_field_me1           bigint,
	public_field_me2           bigint,
	public_field_me3           bigint,
	search                     tsvector generated always as (to_tsvector('simple', sql_search)) stored,
	unique(sql_unique_u1_1,sql_unique_u1_2),
	unique(sql_unique_x1_x2,sql_unique_x1),
	unique(sql_unique_x2,sql_unique_x1_x2)
)
`
		_, err = conn.Exec(context.Background(), q)
		return
	})
}

// create index idx_news_sql_index1_1
func (a *NewsSQL) CreateIndexSql_index1_1() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists idx_news_sql_index1_1 on news (sql_index1_1)")
		return
	})
}

// create index idx_news_sql_index1_2
func (a *NewsSQL) CreateIndexSql_index1_2() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists idx_news_sql_index1_2 on news (sql_index1_2)")
		return
	})
}

// create index idx_news_sql_index1_3
func (a *NewsSQL) CreateIndexSql_index1_3() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists idx_news_sql_index1_3 on news (sql_index1_3)")
		return
	})
}

// create index gin_news_sql_jsonb_index
func (a *NewsSQL) CreateIndexSql_jsonb_index() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists gin_news_sql_jsonb_index on news using gin(sql_jsonb_index)")
		return
	})
}

// create index idx_news_index1
func (a *NewsSQL) CreateIndexIndex1() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists idx_news_index1 on news (sql_index1_1, sql_index1_2, sql_index1_3)")
		return
	})
}

// create index gin_news_search
func (a *NewsSQL) CreateIndexSearch() (err error) {
	return a.Conn(func(conn *pgxpool.Conn) (err error) {
		_, err = conn.Exec(context.Background(), "create index if not exists gin_news_search on news using gin(search)")
		return
	})
}

// create all indexes
func (a *NewsSQL) CreateIndexes() (err error) {
	if err = a.CreateIndexSql_index1_1(); err != nil {
		return
	}
	if err = a.CreateIndexSql_index1_2(); err != nil {
		return
	}
	if err = a.CreateIndexSql_index1_3(); err != nil {
		return
	}
	if err = a.CreateIndexSql_jsonb_index(); err != nil {
		return
	}
	if err = a.CreateIndexIndex1(); err != nil {
		return
	}
	if err = a.CreateIndexSearch(); err != nil {
		return
	}
	return
}

// parse sql query
func (a *NewsSQL) Fields() (res []NewsIndexTyped) {
	return []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexList_ints, NewsIndexList_string, NewsIndexList_float, NewsIndexMap_string_string, NewsIndexMap_string_bytes, NewsIndexMap_string_bool, NewsIndexMap_string_int, NewsIndexMap_string_float64, NewsIndexMap_string_any, NewsIndexMap_int_string, NewsIndexMap_int_int, NewsIndexMap_int_bool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMAST_UPPER_GO, NewsIndexIntToSmallInt, NewsIndexSql_unique_u1_1, NewsIndexSql_unique_u1_2, NewsIndexSql_index1_1, NewsIndexSql_index1_2, NewsIndexSql_index1_3, NewsIndexSql_keys_1, NewsIndexSql_keys_2, NewsIndexSql_keys_3, NewsIndexSql_search, NewsIndexSql_get, NewsIndexSql_unique_x1, NewsIndexSql_unique_x2, NewsIndexSql_unique_x1_x2, NewsIndexSql_primary, NewsIndexSql_jsonb_index, NewsIndexTime_duration, NewsIndexGo_type_int_to_strings, NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3, NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3}
}

// begin transaction for TX methods:
//
//	tx, ctx, err := a.Begin()
//	defer tx.Rollback(ctx)
//	a.InsertTX(tx, ctx, item)
//	err = tx.Commit(ctx)
func (a *NewsSQL) Begin() (tx pgx.Tx, ctx context.Context, err error) {
	ctx = context.Background()
	tx, err = a.pool.Begin(ctx)
	return
}

// CountTX transaction version of Count
func (a *NewsSQL) CountTX(tx pgx.Tx, ctx context.Context, where ...string) (count int, err error) {

	var q string

	switch len(where) {
	case 0:
		q = "select count(*) from news"
	default:
		q = "select count(*) from news where " + strings.Join(where, " ")
	}
	err = tx.QueryRow(ctx, q).Scan(&count)
	return

}

// InsertTX transaction version of Insert
func (a *NewsSQL) InsertTX(tx pgx.Tx, ctx context.Context, v News) (id int, err error) {

	q := "insert into news (role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59) returning inc"
	err = tx.QueryRow(ctx, q, v.sqlTuple()...).Scan(&id)
	return

}

// InsertNoConflictTX transaction version of InsertNoConflict
func (a *NewsSQL) InsertNoConflictTX(tx pgx.Tx, ctx context.Context, v *News) (id int, err error) {

	q := "insert into news (role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59) on conflict do nothing returning inc"
	err = tx.QueryRow(ctx, q, v.sqlTuple()...).Scan(&id)
	return

}

// InsertFullTX transaction version of InsertFull
func (a *NewsSQL) InsertFullTX(tx pgx.Tx, ctx context.Context, v News) (err error) {

	q := "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60)"
	_, err = tx.Exec(ctx, q, v.sqlAllTuples()...)
	return

}

// ReindexIncSerialCounterTX transaction version of ReindexIncSerialCounter
func (a *NewsSQL) ReindexIncSerialCounterTX(tx pgx.Tx, ctx context.Context, add ...int) (err error) {

	plus := 1
	if len(add) > 0 {
		plus = add[0]
	}
	_, err = tx.Exec(ctx, "SELECT setval(pg_get_serial_sequence('news', 'inc'), COALESCE((SELECT MAX(inc) FROM news), 0) + $1, false)", plus)
	return
}

// SetIncSerialCounterTX transaction version of SetIncSerialCounter
func (a *NewsSQL) SetIncSerialCounterTX(tx pgx.Tx, ctx context.Context, value int) (err error) {

	_, err = tx.Exec(ctx, "SELECT setval(pg_get_serial_sequence('news', 'inc'), $1, false)", value)
	return
}

// GetTX transaction version of Get
func (a *NewsSQL) GetTX(tx pgx.Tx, ctx context.Context, id any, fields ...NewsIndexTyped) (res News, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s ", fieldlist)
	q = q + "from news where inc = $1 limit 1"
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

// GetPublicTX transaction version of GetPublic
func (a *NewsSQL) GetPublicTX(tx pgx.Tx, ctx context.Context, id any) (res NewsPublic, err error) {
	p, err := a.GetTX(tx, ctx, id, NewsPublicList()...)
	if err != nil {
		return
	}
	res = *p.NewsPublic()
	return
}

// GetMeTX transaction version of GetMe
func (a *NewsSQL) GetMeTX(tx pgx.Tx, ctx context.Context, id any) (res NewsMe, err error) {
	p, err := a.GetTX(tx, ctx, id, NewsMeList()...)
	if err != nil {
		return
	}
	res = *p.NewsMe()
	return
}

// GetWhereTX transaction version of GetWhere
func (a *NewsSQL) GetWhereTX(tx pgx.Tx, ctx context.Context, where string, fields ...NewsIndexTyped) (res News, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q = q + " from news"
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
func (a *NewsSQL) RowTX(tx pgx.Tx, ctx context.Context, eq map[string]any, fields ...NewsIndexTyped) (res News, err error) {

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
		if !NewsValidKey(k) {
			err = errors.New(k)
			return
		}
		in := NewsKeyIndex(k)
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
func (a *NewsSQL) AllTX(tx pgx.Tx, ctx context.Context, fields ...NewsIndexTyped) (res []*News, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q += " from news"
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item News
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
func (a *NewsSQL) ListTX(tx pgx.Tx, ctx context.Context, limit, offset int, fields ...NewsIndexTyped) (res []News, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s", fieldlist)
	q += " from news limit $1 offset $2"
	rows, err := tx.Query(ctx, q, limit, offset)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item News
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
func (a *NewsSQL) UpdateTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	if !NewsValidKey(k) {
		return errors.New("invalid key")
	}
	in := NewsKeyIndex(k)
	q := fmt.Sprintf("update news set %s = $1 where inc = $2", in.SQLName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// UpdateWhereTX transaction version of UpdateWhere
func (a *NewsSQL) UpdateWhereTX(tx pgx.Tx, ctx context.Context, k string, v any, where string) (err error) {
	if !NewsValidKey(k) {
		return errors.New("invalid key")
	}
	in := NewsKeyIndex(k)

	q := fmt.Sprintf("update %s set %s = $1 where %s", a.TableName(), in.SQLName(), where)
	_, err = tx.Exec(ctx, q, v)
	return

}

// UpdatesTX transaction version of Updates
func (a *NewsSQL) UpdatesTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}

	var fields []string
	var values []any
	var count int
	for k, v := range keys {
		if !NewsValidKey(k) {
			return errors.New(k)
		}
		in := NewsKeyIndex(k)
		count++
		fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
		values = append(values, v)
	}

	list := strings.Join(fields, ", ")
	count++
	values = append(values, id)

	q := fmt.Sprintf("update news set %s where inc = $%d", list, count)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, values...)
	return

}

// UpdatesWhereTX transaction version of UpdatesWhere
func (a *NewsSQL) UpdatesWhereTX(tx pgx.Tx, ctx context.Context, keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}

	var fields []string
	var values []any
	var count int
	for k, v := range keys {
		if !NewsValidKey(k) {
			return errors.New(k)
		}
		in := NewsKeyIndex(k)
		count++
		fields = append(fields, fmt.Sprintf("%s = $%d", in.SQLName(), count))
		values = append(values, v)
	}

	list := strings.Join(fields, ", ")
	count++

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("update news set %s where %s", list, where)
	_, err = tx.Exec(ctx, q, values...)
	return

}

// AddList_intsTX transaction version of AddList_ints
func (a *NewsSQL) AddList_intsTX(tx pgx.Tx, ctx context.Context, id any, v ...int) (err error) {

	q := "update news set list_ints = list_ints || $1::bigint[] where inc = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteList_intsTX transaction version of DeleteList_ints
func (a *NewsSQL) DeleteList_intsTX(tx pgx.Tx, ctx context.Context, id any, v int, where ...string) (err error) {

	q := "update news set list_ints = array_remove(list_ints, $1::bigint) where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddList_intsWhereTX transaction version of AddList_intsWhere
func (a *NewsSQL) AddList_intsWhereTX(tx pgx.Tx, ctx context.Context, v int, where string) (err error) {

	q := "update news set list_ints = array_append(list_ints, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteList_intsWhereTX transaction version of DeleteList_intsWhere
func (a *NewsSQL) DeleteList_intsWhereTX(tx pgx.Tx, ctx context.Context, v int, where string) (err error) {

	q := "update news set list_ints = array_remove(list_ints, $1::bigint) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddList_stringTX transaction version of AddList_string
func (a *NewsSQL) AddList_stringTX(tx pgx.Tx, ctx context.Context, id any, v ...string) (err error) {

	q := "update news set list_string = list_string || $1::text[] where inc = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteList_stringTX transaction version of DeleteList_string
func (a *NewsSQL) DeleteList_stringTX(tx pgx.Tx, ctx context.Context, id any, v string, where ...string) (err error) {

	q := "update news set list_string = array_remove(list_string, $1::text) where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddList_stringWhereTX transaction version of AddList_stringWhere
func (a *NewsSQL) AddList_stringWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update news set list_string = array_append(list_string, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteList_stringWhereTX transaction version of DeleteList_stringWhere
func (a *NewsSQL) DeleteList_stringWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update news set list_string = array_remove(list_string, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// AddList_floatTX transaction version of AddList_float
func (a *NewsSQL) AddList_floatTX(tx pgx.Tx, ctx context.Context, id any, v ...float64) (err error) {

	q := "update news set list_float = list_float || $1::double precision[] where inc = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteList_floatTX transaction version of DeleteList_float
func (a *NewsSQL) DeleteList_floatTX(tx pgx.Tx, ctx context.Context, id any, v float64, where ...string) (err error) {

	q := "update news set list_float = array_remove(list_float, $1::double precision) where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddList_floatWhereTX transaction version of AddList_floatWhere
func (a *NewsSQL) AddList_floatWhereTX(tx pgx.Tx, ctx context.Context, v float64, where string) (err error) {

	q := "update news set list_float = array_append(list_float, $1::double precision) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteList_floatWhereTX transaction version of DeleteList_floatWhere
func (a *NewsSQL) DeleteList_floatWhereTX(tx pgx.Tx, ctx context.Context, v float64, where string) (err error) {

	q := "update news set list_float = array_remove(list_float, $1::double precision) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// UpdateMap_string_stringTX transaction version of UpdateMap_string_string
func (a *NewsSQL) UpdateMap_string_stringTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_string = map_string_string || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_stringTX transaction version of UpdatesMap_string_string
func (a *NewsSQL) UpdatesMap_string_stringTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_string = map_string_string || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_stringTX transaction version of DeleteKeyMap_string_string
func (a *NewsSQL) DeleteKeyMap_string_stringTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_string = map_string_string - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_stringTX transaction version of RenameKeyMap_string_string
func (a *NewsSQL) RenameKeyMap_string_stringTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_string = map_string_string - $1 || jsonb_build_object($2, map_string_string->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_string_bytesTX transaction version of UpdateMap_string_bytes
func (a *NewsSQL) UpdateMap_string_bytesTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_bytes = map_string_bytes || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_bytesTX transaction version of UpdatesMap_string_bytes
func (a *NewsSQL) UpdatesMap_string_bytesTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_bytes = map_string_bytes || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_bytesTX transaction version of DeleteKeyMap_string_bytes
func (a *NewsSQL) DeleteKeyMap_string_bytesTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_bytes = map_string_bytes - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_bytesTX transaction version of RenameKeyMap_string_bytes
func (a *NewsSQL) RenameKeyMap_string_bytesTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_bytes = map_string_bytes - $1 || jsonb_build_object($2, map_string_bytes->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_string_boolTX transaction version of UpdateMap_string_bool
func (a *NewsSQL) UpdateMap_string_boolTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_bool = map_string_bool || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_boolTX transaction version of UpdatesMap_string_bool
func (a *NewsSQL) UpdatesMap_string_boolTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_bool = map_string_bool || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_boolTX transaction version of DeleteKeyMap_string_bool
func (a *NewsSQL) DeleteKeyMap_string_boolTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_bool = map_string_bool - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_boolTX transaction version of RenameKeyMap_string_bool
func (a *NewsSQL) RenameKeyMap_string_boolTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_bool = map_string_bool - $1 || jsonb_build_object($2, map_string_bool->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_string_intTX transaction version of UpdateMap_string_int
func (a *NewsSQL) UpdateMap_string_intTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_int = map_string_int || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_intTX transaction version of UpdatesMap_string_int
func (a *NewsSQL) UpdatesMap_string_intTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_int = map_string_int || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_intTX transaction version of DeleteKeyMap_string_int
func (a *NewsSQL) DeleteKeyMap_string_intTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_int = map_string_int - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_intTX transaction version of RenameKeyMap_string_int
func (a *NewsSQL) RenameKeyMap_string_intTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_int = map_string_int - $1 || jsonb_build_object($2, map_string_int->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_string_float64TX transaction version of UpdateMap_string_float64
func (a *NewsSQL) UpdateMap_string_float64TX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_float64 = map_string_float64 || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_float64TX transaction version of UpdatesMap_string_float64
func (a *NewsSQL) UpdatesMap_string_float64TX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_float64 = map_string_float64 || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_float64TX transaction version of DeleteKeyMap_string_float64
func (a *NewsSQL) DeleteKeyMap_string_float64TX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_float64 = map_string_float64 - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_float64TX transaction version of RenameKeyMap_string_float64
func (a *NewsSQL) RenameKeyMap_string_float64TX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_float64 = map_string_float64 - $1 || jsonb_build_object($2, map_string_float64->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_string_anyTX transaction version of UpdateMap_string_any
func (a *NewsSQL) UpdateMap_string_anyTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_any = map_string_any || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_string_anyTX transaction version of UpdatesMap_string_any
func (a *NewsSQL) UpdatesMap_string_anyTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_string_any = map_string_any || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_string_anyTX transaction version of DeleteKeyMap_string_any
func (a *NewsSQL) DeleteKeyMap_string_anyTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_string_any = map_string_any - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_string_anyTX transaction version of RenameKeyMap_string_any
func (a *NewsSQL) RenameKeyMap_string_anyTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_string_any = map_string_any - $1 || jsonb_build_object($2, map_string_any->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_int_stringTX transaction version of UpdateMap_int_string
func (a *NewsSQL) UpdateMap_int_stringTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_string = map_int_string || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_int_stringTX transaction version of UpdatesMap_int_string
func (a *NewsSQL) UpdatesMap_int_stringTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_string = map_int_string || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_int_stringTX transaction version of DeleteKeyMap_int_string
func (a *NewsSQL) DeleteKeyMap_int_stringTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_int_string = map_int_string - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_int_stringTX transaction version of RenameKeyMap_int_string
func (a *NewsSQL) RenameKeyMap_int_stringTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_int_string = map_int_string - $1 || jsonb_build_object($2, map_int_string->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_int_intTX transaction version of UpdateMap_int_int
func (a *NewsSQL) UpdateMap_int_intTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_int = map_int_int || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_int_intTX transaction version of UpdatesMap_int_int
func (a *NewsSQL) UpdatesMap_int_intTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_int = map_int_int || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_int_intTX transaction version of DeleteKeyMap_int_int
func (a *NewsSQL) DeleteKeyMap_int_intTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_int_int = map_int_int - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_int_intTX transaction version of RenameKeyMap_int_int
func (a *NewsSQL) RenameKeyMap_int_intTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_int_int = map_int_int - $1 || jsonb_build_object($2, map_int_int->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateMap_int_boolTX transaction version of UpdateMap_int_bool
func (a *NewsSQL) UpdateMap_int_boolTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_bool = map_int_bool || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesMap_int_boolTX transaction version of UpdatesMap_int_bool
func (a *NewsSQL) UpdatesMap_int_boolTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set map_int_bool = map_int_bool || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeyMap_int_boolTX transaction version of DeleteKeyMap_int_bool
func (a *NewsSQL) DeleteKeyMap_int_boolTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set map_int_bool = map_int_bool - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeyMap_int_boolTX transaction version of RenameKeyMap_int_bool
func (a *NewsSQL) RenameKeyMap_int_boolTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set map_int_bool = map_int_bool - $1 || jsonb_build_object($2, map_int_bool->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// UpdateSql_jsonb_indexTX transaction version of UpdateSql_jsonb_index
func (a *NewsSQL) UpdateSql_jsonb_indexTX(tx pgx.Tx, ctx context.Context, id any, k string, v any, where ...string) (err error) {

	res := strings.ReplaceAll(jsons.Creates(k, v).String(), "$$", "$ $")
	q := fmt.Sprintf("update news set sql_jsonb_index = sql_jsonb_index || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// UpdatesSql_jsonb_indexTX transaction version of UpdatesSql_jsonb_index
func (a *NewsSQL) UpdatesSql_jsonb_indexTX(tx pgx.Tx, ctx context.Context, id any, keys map[string]any, where ...string) (err error) {

	b, _ := jsoniter.Marshal(keys)

	res := strings.ReplaceAll(string(b), "$$", "$ $")
	q := fmt.Sprintf("update news set sql_jsonb_index = sql_jsonb_index || $$%s$$::jsonb where inc = $1", res)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, id)
	return

}

// DeleteKeySql_jsonb_indexTX transaction version of DeleteKeySql_jsonb_index
func (a *NewsSQL) DeleteKeySql_jsonb_indexTX(tx pgx.Tx, ctx context.Context, id any, k string, where ...string) (err error) {

	q := "update news set sql_jsonb_index = sql_jsonb_index - $1 where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, k, id)
	return

}

// RenameKeySql_jsonb_indexTX transaction version of RenameKeySql_jsonb_index
func (a *NewsSQL) RenameKeySql_jsonb_indexTX(tx pgx.Tx, ctx context.Context, id any, k, newkey string) (err error) {

	q := "update news set sql_jsonb_index = sql_jsonb_index - $1 || jsonb_build_object($2, sql_jsonb_index->$1) where inc = $3"
	_, err = tx.Exec(ctx, q, k, newkey, id)
	return

}

// AddGo_type_int_to_stringsTX transaction version of AddGo_type_int_to_strings
func (a *NewsSQL) AddGo_type_int_to_stringsTX(tx pgx.Tx, ctx context.Context, id any, v ...string) (err error) {

	q := "update news set go_type_int_to_strings = go_type_int_to_strings || $1::text[] where inc = $2"
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// DeleteGo_type_int_to_stringsTX transaction version of DeleteGo_type_int_to_strings
func (a *NewsSQL) DeleteGo_type_int_to_stringsTX(tx pgx.Tx, ctx context.Context, id any, v string, where ...string) (err error) {

	q := "update news set go_type_int_to_strings = array_remove(go_type_int_to_strings, $1::text) where inc = $2"
	if len(where) > 0 {
		q += " and " + where[0]
	}
	_, err = tx.Exec(ctx, q, v, id)
	return

}

// AddGo_type_int_to_stringsWhereTX transaction version of AddGo_type_int_to_stringsWhere
func (a *NewsSQL) AddGo_type_int_to_stringsWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update news set go_type_int_to_strings = array_append(go_type_int_to_strings, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// DeleteGo_type_int_to_stringsWhereTX transaction version of DeleteGo_type_int_to_stringsWhere
func (a *NewsSQL) DeleteGo_type_int_to_stringsWhereTX(tx pgx.Tx, ctx context.Context, v string, where string) (err error) {

	q := "update news set go_type_int_to_strings = array_remove(go_type_int_to_strings, $1::text) where " + where
	_, err = tx.Exec(ctx, q, v)
	return

}

// SearchTX transaction version of Search
func (a *NewsSQL) SearchTX(tx pgx.Tx, ctx context.Context, q *NewsQuery) (res []News, err error) {

	sql, fields, values := q.Render()

	rows, err := tx.Query(ctx, sql, values...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item News
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
func (a *NewsSQL) SelectTX(tx pgx.Tx, ctx context.Context, where string, fields ...NewsIndexTyped) (res []News, err error) {

	if fields == nil {
		fields = a.Fields()
	}

	var list []string
	for _, x := range fields {
		list = append(list, x.SQLName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s ", fieldlist)
	q = q + "from news"
	if where != "" {
		q += " where " + where
	}
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var item News
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
func (a *NewsSQL) DeleteTX(tx pgx.Tx, ctx context.Context, id any) (err error) {

	_, err = tx.Exec(ctx, "delete from news where inc = $1", id)
	return

}

// DeleteWhereTX transaction version of DeleteWhere
func (a *NewsSQL) DeleteWhereTX(tx pgx.Tx, ctx context.Context, where string) (err error) {

	_, err = tx.Exec(ctx, fmt.Sprintf("delete from news where %s", where))
	return

}

// HasTX transaction version of Has
func (a *NewsSQL) HasTX(tx pgx.Tx, ctx context.Context, field NewsIndexTyped, v any) (has bool, err error) {

	q := fmt.Sprintf("select exists (select 1 from news where %s = $1 limit 1)", field.SQLName())
	err = tx.QueryRow(ctx, q, v).Scan(&has)
	return

}

// CreateTableTX transaction version of CreateTable
func (a *NewsSQL) CreateTableTX(tx pgx.Tx, ctx context.Context) (err error) {

	q := `create table if not exists news (
	inc                        bigserial,
	role                       bigint,
	key                        uuid,
	t                          timestamptz,
	d                          bigint,
	ints                       bigint,
	ints8                      smallint,
	ints16                     smallint,
	ints32                     integer,
	ints64                     bigint,
	uints                      numeric(20,0),
	uints8                     smallint,
	uints16                    integer,
	uints32                    bigint,
	uints64                    numeric(20,0),
	floats32                   real,
	floats64                   double precision,
	bools                      boolean,
	byte1                      smallint,
	bytes                      bytea,
	list_ints                  bigint[] default '{}',
	list_string                text[] default '{}',
	list_float                 double precision[] default '{}',
	map_string_string          jsonb default '{}'::jsonb,
	map_string_bytes           jsonb default '{}'::jsonb,
	map_string_bool            jsonb default '{}'::jsonb,
	map_string_int             jsonb default '{}'::jsonb,
	map_string_float64         jsonb default '{}'::jsonb,
	map_string_any             jsonb default '{}'::jsonb,
	map_int_string             jsonb default '{}'::jsonb,
	map_int_int                jsonb default '{}'::jsonb,
	map_int_bool               jsonb default '{}'::jsonb,
	renameSQL_OK               text,
	renameGO                   text,
	renameJS                   text,
	mast_upper_go              text,
	intToSmallInt              smallint,
	sql_unique_u1_1            bigint,
	sql_unique_u1_2            bigint,
	sql_index1_1               bigint,
	sql_index1_2               bigint,
	sql_index1_3               bigint,
	sql_keys_1                 bigint,
	sql_keys_2                 bigint,
	sql_keys_3                 bigint,
	sql_search                 text,
	sql_get                    text,
	sql_unique_x1              bigint,
	sql_unique_x2              bigint,
	sql_unique_x1_x2           bigint,
	sql_primary                double precision primary key,
	sql_jsonb_index            jsonb default '{}'::jsonb,
	time_duration              bigint,
	go_type_int_to_strings     text[] default '{}',
	public_field1              bigint,
	public_field2              bigint,
	public_field3              bigint,
	public_field_me1           bigint,
	public_field_me2           bigint,
	public_field_me3           bigint,
	search                     tsvector generated always as (to_tsvector('simple', sql_search)) stored,
	unique(sql_unique_u1_1,sql_unique_u1_2),
	unique(sql_unique_x1_x2,sql_unique_x1),
	unique(sql_unique_x2,sql_unique_x1_x2)
)
`
	_, err = tx.Exec(ctx, q)
	return

}

// CreateIndexSql_index1_1TX transaction version of CreateIndexSql_index1_1
func (a *NewsSQL) CreateIndexSql_index1_1TX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists idx_news_sql_index1_1 on news (sql_index1_1)")
	return

}

// CreateIndexSql_index1_2TX transaction version of CreateIndexSql_index1_2
func (a *NewsSQL) CreateIndexSql_index1_2TX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists idx_news_sql_index1_2 on news (sql_index1_2)")
	return

}

// CreateIndexSql_index1_3TX transaction version of CreateIndexSql_index1_3
func (a *NewsSQL) CreateIndexSql_index1_3TX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists idx_news_sql_index1_3 on news (sql_index1_3)")
	return

}

// CreateIndexSql_jsonb_indexTX transaction version of CreateIndexSql_jsonb_index
func (a *NewsSQL) CreateIndexSql_jsonb_indexTX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists gin_news_sql_jsonb_index on news using gin(sql_jsonb_index)")
	return

}

// CreateIndexIndex1TX transaction version of CreateIndexIndex1
func (a *NewsSQL) CreateIndexIndex1TX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists idx_news_index1 on news (sql_index1_1, sql_index1_2, sql_index1_3)")
	return

}

// CreateIndexSearchTX transaction version of CreateIndexSearch
func (a *NewsSQL) CreateIndexSearchTX(tx pgx.Tx, ctx context.Context) (err error) {

	_, err = tx.Exec(ctx, "create index if not exists gin_news_search on news using gin(search)")
	return

}

// CreateIndexesTX transaction version of CreateIndexes
func (a *NewsSQL) CreateIndexesTX(tx pgx.Tx, ctx context.Context) (err error) {
	if err = a.CreateIndexSql_index1_1TX(tx, ctx); err != nil {
		return
	}
	if err = a.CreateIndexSql_index1_2TX(tx, ctx); err != nil {
		return
	}
	if err = a.CreateIndexSql_index1_3TX(tx, ctx); err != nil {
		return
	}
	if err = a.CreateIndexSql_jsonb_indexTX(tx, ctx); err != nil {
		return
	}
	if err = a.CreateIndexIndex1TX(tx, ctx); err != nil {
		return
	}
	if err = a.CreateIndexSearchTX(tx, ctx); err != nil {
		return
	}
	return
} //clickhouse NewsCQL class
type NewsCQL struct {
	conn driver.Conn
	list []News
	sync.Mutex
}

func NewNewsCQL(conn driver.Conn) (a *NewsCQL) {
	a = new(NewsCQL)
	a.conn = conn
	a.CreateTable()
	go a.deamon()
	return
}

// parse clickhouse query
func (a *NewsCQL) TableName() (res string) {
	return "news"
}

func (a *NewsCQL) Count(where ...string) (count int, err error) {

	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	var q string
	switch len(where) {
	case 0:
		q = "select count() from news"
	default:
		q = "select count() from news where " + strings.Join(where, " ")
	}
	var n uint64
	err = a.conn.QueryRow(context.Background(), q).Scan(&n)
	count = int(n)
	return
}

// Add items to clickhouse queue
func (a *NewsCQL) Add(items ...News) {
	a.Lock()
	defer a.Unlock()
	a.list = append(a.list, items...)
}

// Push queued items to clickhouse
func (a *NewsCQL) Push() (err error) {
	a.Lock()
	defer a.Unlock()
	if len(a.list) == 0 {
		return
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL, renameGO, renameJS, mast_upper_go, intToSmallInt, skip, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3)")
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
func (a *NewsCQL) deamon() {
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
func (a *NewsCQL) Get(id any, fields ...NewsIndexTyped) (res *News, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from news where inc = ? limit 1", fieldlist)
	res = new(News)

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
func (a *NewsCQL) GetWhere(where string, fields ...NewsIndexTyped) (res *News, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from news where %s limit 1", fieldlist, where)
	res = new(News)

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
func (a *NewsCQL) Update(id any, k string, v any, where ...string) (err error) {
	if !NewsValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := NewsKeyIndex(k)
	q := fmt.Sprintf("alter table news update %s = ? where inc = ?", in.ClickhouseName())
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, v, id)
}

// update clickhouse query
func (a *NewsCQL) UpdateWhere(k string, v any, where string) (err error) {
	if !NewsValidKey(k) {
		return errors.New("invalid key")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	in := NewsKeyIndex(k)
	q := fmt.Sprintf("alter table %s update %s = ? where %s", a.TableName(), in.ClickhouseName(), where)
	return a.conn.Exec(context.Background(), q, v)
}

// update clickhouse query
func (a *NewsCQL) Updates(id any, keys map[string]any, where ...string) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !NewsValidKey(k) {
			return errors.New(k)
		}
		in := NewsKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")
	values = append(values, id)

	q := fmt.Sprintf("alter table news update %s where inc = ?", list)
	if len(where) > 0 {
		q += " and " + where[0]
	}
	return a.conn.Exec(context.Background(), q, values...)
}

// update clickhouse query
func (a *NewsCQL) UpdatesWhere(keys map[string]any, where string, args ...any) (err error) {

	if keys == nil {
		return errors.New("emptykeys")
	}
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	var fields []string
	var values []any
	for k, v := range keys {
		if !NewsValidKey(k) {
			return errors.New(k)
		}
		in := NewsKeyIndex(k)
		fields = append(fields, fmt.Sprintf("%s = ?", in.ClickhouseName()))
		values = append(values, v)
	}
	list := strings.Join(fields, ", ")

	where = fmt.Sprintf(where, args...)
	q := fmt.Sprintf("alter table news update %s where %s", list, where)
	return a.conn.Exec(context.Background(), q, values...)
}

// select custom clickhouse query
func (a *NewsCQL) Select(where string, fields ...NewsIndexTyped) (res []*News, err error) {

	if fields == nil {
		fields = a.Fields()
	}
	var list []string
	for _, x := range fields {
		list = append(list, x.ClickhouseName())
	}
	fieldlist := strings.Join(list, ", ")

	q := fmt.Sprintf("select %s from news", fieldlist)
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
		var item News
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
func (a *NewsCQL) Delete(id any) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table news delete where inc = ?"
	return a.conn.Exec(context.Background(), q, id)
}

// delete clickhouse items
func (a *NewsCQL) DeleteWhere(where string) (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := "alter table news delete where " + where
	return a.conn.Exec(context.Background(), q)
}

// has value in clickhouse
func (a *NewsCQL) Has(field NewsIndexTyped, v any) (has bool, err error) {
	if a.conn == nil {
		err = errors.New("clickhouse connection is nil")
		return
	}
	q := fmt.Sprintf("select count() from news where %s = ? limit 1", field.ClickhouseName())
	var n uint64
	err = a.conn.QueryRow(context.Background(), q, v).Scan(&n)
	has = n > 0
	return
}

// stat by clickhouse field
func (a *NewsCQL) Stat(field NewsIndexTyped) (res map[string]int, err error) {
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
func (a *NewsCQL) CreateTable() (err error) {
	if a.conn == nil {
		return errors.New("clickhouse connection is nil")
	}
	q := `create table if not exists news (
	inc                        Int64,
	role                       Int64,
	key                        UUID,
	t                          DateTime64(3),
	d                          Int64,
	ints                       Int64,
	ints8                      Int8,
	ints16                     Int16,
	ints32                     Int32,
	ints64                     Int64,
	uints                      UInt64,
	uints8                     UInt8,
	uints16                    UInt16,
	uints32                    UInt32,
	uints64                    UInt64,
	floats32                   Float32,
	floats64                   Float64,
	bools                      Bool,
	byte1                      String,
	bytes                      String,
	list_ints                  Array(Int64),
	list_string                Array(String),
	list_float                 Array(Float64),
	map_string_string          Map(String, String),
	map_string_bytes           JSON,
	map_string_bool            Map(String, Bool),
	map_string_int             Map(String, Int64),
	map_string_float64         Map(String, Float64),
	map_string_any             JSON,
	map_int_string             Map(Int64, String),
	map_int_int                Map(Int64, Int64),
	map_int_bool               Map(Int64, Bool),
	renameSQL                  String,
	renameGO                   String,
	renameJS                   String,
	mast_upper_go              String,
	intToSmallInt              Int64,
	skip                       String,
	sql_unique_u1_1            Int64,
	sql_unique_u1_2            Int64,
	sql_index1_1               Int64,
	sql_index1_2               Int64,
	sql_index1_3               Int64,
	sql_keys_1                 Int64,
	sql_keys_2                 Int64,
	sql_keys_3                 Int64,
	sql_search                 String,
	sql_get                    String,
	sql_unique_x1              Int64,
	sql_unique_x2              Int64,
	sql_unique_x1_x2           Int64,
	sql_primary                Float64,
	sql_jsonb_index            JSON,
	time_duration              Int64,
	go_type_int_to_strings     Array(String),
	public_field1              Int64,
	public_field2              Int64,
	public_field3              Int64,
	public_field_me1           Int64,
	public_field_me2           Int64,
	public_field_me3           Int64
)
engine = MergeTree()
order by tuple()`
	return a.conn.Exec(context.Background(), q)
}

// parse clickhouse query
func (a *NewsCQL) Fields() (res []NewsIndexTyped) {
	return []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexList_ints, NewsIndexList_string, NewsIndexList_float, NewsIndexMap_string_string, NewsIndexMap_string_bytes, NewsIndexMap_string_bool, NewsIndexMap_string_int, NewsIndexMap_string_float64, NewsIndexMap_string_any, NewsIndexMap_int_string, NewsIndexMap_int_int, NewsIndexMap_int_bool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMAST_UPPER_GO, NewsIndexIntToSmallInt, NewsIndexSkip, NewsIndexSql_unique_u1_1, NewsIndexSql_unique_u1_2, NewsIndexSql_index1_1, NewsIndexSql_index1_2, NewsIndexSql_index1_3, NewsIndexSql_keys_1, NewsIndexSql_keys_2, NewsIndexSql_keys_3, NewsIndexSql_search, NewsIndexSql_get, NewsIndexSql_unique_x1, NewsIndexSql_unique_x2, NewsIndexSql_unique_x1_x2, NewsIndexSql_primary, NewsIndexSql_jsonb_index, NewsIndexTime_duration, NewsIndexGo_type_int_to_strings, NewsIndexPublic_field1, NewsIndexPublic_field2, NewsIndexPublic_field3, NewsIndexPublic_field_me1, NewsIndexPublic_field_me2, NewsIndexPublic_field_me3}
}

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type NewsProto struct {
	state              protoimpl.MessageState     `protogen:"open.v1"`
	Inc                int64                      `protobuf:"varint,1,opt,name=inc,proto3" json:"inc,omitempty"`
	Role               int64                      `protobuf:"varint,2,opt,name=role,proto3" json:"role,omitempty"`
	Key                string                     `protobuf:"bytes,3,opt,name=key,proto3" json:"key,omitempty"`
	T                  *timestamppb.Timestamp     `protobuf:"bytes,4,opt,name=t,proto3" json:"t,omitempty"`
	D                  int64                      `protobuf:"varint,5,opt,name=d,proto3" json:"d,omitempty"`
	Ints               int64                      `protobuf:"varint,6,opt,name=ints,proto3" json:"ints,omitempty"`
	Ints8              int32                      `protobuf:"varint,7,opt,name=ints8,proto3" json:"ints8,omitempty"`
	Ints16             int32                      `protobuf:"varint,8,opt,name=ints16,proto3" json:"ints16,omitempty"`
	Ints32             int32                      `protobuf:"varint,9,opt,name=ints32,proto3" json:"ints32,omitempty"`
	Ints64             int64                      `protobuf:"varint,10,opt,name=ints64,proto3" json:"ints64,omitempty"`
	Uints              uint64                     `protobuf:"varint,11,opt,name=uints,proto3" json:"uints,omitempty"`
	Uints8             uint32                     `protobuf:"varint,12,opt,name=uints8,proto3" json:"uints8,omitempty"`
	Uints16            uint32                     `protobuf:"varint,13,opt,name=uints16,proto3" json:"uints16,omitempty"`
	Uints32            uint32                     `protobuf:"varint,14,opt,name=uints32,proto3" json:"uints32,omitempty"`
	Uints64            uint64                     `protobuf:"varint,15,opt,name=uints64,proto3" json:"uints64,omitempty"`
	Floats32           float32                    `protobuf:"fixed32,16,opt,name=floats32,proto3" json:"floats32,omitempty"`
	Floats64           float64                    `protobuf:"fixed64,17,opt,name=floats64,proto3" json:"floats64,omitempty"`
	Bools              bool                       `protobuf:"varint,18,opt,name=bools,proto3" json:"bools,omitempty"`
	Byte1              uint32                     `protobuf:"varint,19,opt,name=byte1,proto3" json:"byte1,omitempty"`
	Bytes              []byte                     `protobuf:"bytes,20,opt,name=bytes,proto3" json:"bytes,omitempty"`
	ListInts           []int64                    `protobuf:"varint,21,rep,packed,name=list_ints,json=listInts,proto3" json:"list_ints,omitempty"`
	ListString         []string                   `protobuf:"bytes,22,rep,name=list_string,json=listString,proto3" json:"list_string,omitempty"`
	ListFloat          []float64                  `protobuf:"fixed64,23,rep,packed,name=list_float,json=listFloat,proto3" json:"list_float,omitempty"`
	MapStringString    map[string]string          `protobuf:"bytes,24,rep,name=map_string_string,json=mapStringString,proto3" json:"map_string_string,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	MapStringBytes     map[string][]byte          `protobuf:"bytes,25,rep,name=map_string_bytes,json=mapStringBytes,proto3" json:"map_string_bytes,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	MapStringBool      map[string]bool            `protobuf:"bytes,26,rep,name=map_string_bool,json=mapStringBool,proto3" json:"map_string_bool,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	MapStringInt       map[string]int64           `protobuf:"bytes,27,rep,name=map_string_int,json=mapStringInt,proto3" json:"map_string_int,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	MapStringFloat64   map[string]float64         `protobuf:"bytes,28,rep,name=map_string_float64,json=mapStringFloat64,proto3" json:"map_string_float64,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"fixed64,2,opt,name=value"`
	MapStringAny       map[string]*structpb.Value `protobuf:"bytes,29,rep,name=map_string_any,json=mapStringAny,proto3" json:"map_string_any,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	MapIntString       map[int64]string           `protobuf:"bytes,30,rep,name=map_int_string,json=mapIntString,proto3" json:"map_int_string,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	MapIntInt          map[int64]int64            `protobuf:"bytes,31,rep,name=map_int_int,json=mapIntInt,proto3" json:"map_int_int,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	MapIntBool         map[int64]bool             `protobuf:"bytes,32,rep,name=map_int_bool,json=mapIntBool,proto3" json:"map_int_bool,omitempty" protobuf_key:"varint,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	RenameSql          string                     `protobuf:"bytes,33,opt,name=rename_sql,json=renameSql,proto3" json:"rename_sql,omitempty"`
	RenameGo           string                     `protobuf:"bytes,34,opt,name=rename_go,json=renameGo,proto3" json:"rename_go,omitempty"`
	RenameJs           string                     `protobuf:"bytes,35,opt,name=rename_js,json=renameJs,proto3" json:"rename_js,omitempty"`
	MastUpperGo        string                     `protobuf:"bytes,36,opt,name=mast_upper_go,json=mastUpperGo,proto3" json:"mast_upper_go,omitempty"`
	IntToSmallInt      int64                      `protobuf:"varint,37,opt,name=int_to_small_int,json=intToSmallInt,proto3" json:"int_to_small_int,omitempty"`
	Skip               string                     `protobuf:"bytes,38,opt,name=skip,proto3" json:"skip,omitempty"`
	SqlUniqueU1_1      int64                      `protobuf:"varint,39,opt,name=sql_unique_u1_1,json=sqlUniqueU11,proto3" json:"sql_unique_u1_1,omitempty"`
	SqlUniqueU1_2      int64                      `protobuf:"varint,40,opt,name=sql_unique_u1_2,json=sqlUniqueU12,proto3" json:"sql_unique_u1_2,omitempty"`
	SqlIndex1_1        int64                      `protobuf:"varint,41,opt,name=sql_index1_1,json=sqlIndex11,proto3" json:"sql_index1_1,omitempty"`
	SqlIndex1_2        int64                      `protobuf:"varint,42,opt,name=sql_index1_2,json=sqlIndex12,proto3" json:"sql_index1_2,omitempty"`
	SqlIndex1_3        int64                      `protobuf:"varint,43,opt,name=sql_index1_3,json=sqlIndex13,proto3" json:"sql_index1_3,omitempty"`
	SqlKeys_1          int64                      `protobuf:"varint,44,opt,name=sql_keys_1,json=sqlKeys1,proto3" json:"sql_keys_1,omitempty"`
	SqlKeys_2          int64                      `protobuf:"varint,45,opt,name=sql_keys_2,json=sqlKeys2,proto3" json:"sql_keys_2,omitempty"`
	SqlKeys_3          int64                      `protobuf:"varint,46,opt,name=sql_keys_3,json=sqlKeys3,proto3" json:"sql_keys_3,omitempty"`
	SqlSearch          string                     `protobuf:"bytes,47,opt,name=sql_search,json=sqlSearch,proto3" json:"sql_search,omitempty"`
	SqlGet             string                     `protobuf:"bytes,48,opt,name=sql_get,json=sqlGet,proto3" json:"sql_get,omitempty"`
	SqlUniqueX1        int64                      `protobuf:"varint,49,opt,name=sql_unique_x1,json=sqlUniqueX1,proto3" json:"sql_unique_x1,omitempty"`
	SqlUniqueX2        int64                      `protobuf:"varint,50,opt,name=sql_unique_x2,json=sqlUniqueX2,proto3" json:"sql_unique_x2,omitempty"`
	SqlUniqueX1X2      int64                      `protobuf:"varint,51,opt,name=sql_unique_x1_x2,json=sqlUniqueX1X2,proto3" json:"sql_unique_x1_x2,omitempty"`
	SqlPrimary         float64                    `protobuf:"fixed64,52,opt,name=sql_primary,json=sqlPrimary,proto3" json:"sql_primary,omitempty"`
	SqlJsonbIndex      map[string]*structpb.Value `protobuf:"bytes,53,rep,name=sql_jsonb_index,json=sqlJsonbIndex,proto3" json:"sql_jsonb_index,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	TimeDuration       int64                      `protobuf:"varint,54,opt,name=time_duration,json=timeDuration,proto3" json:"time_duration,omitempty"`
	GoTypeIntToStrings []string                   `protobuf:"bytes,55,rep,name=go_type_int_to_strings,json=goTypeIntToStrings,proto3" json:"go_type_int_to_strings,omitempty"`
	PublicField1       int64                      `protobuf:"varint,56,opt,name=public_field1,json=publicField1,proto3" json:"public_field1,omitempty"`
	PublicField2       int64                      `protobuf:"varint,57,opt,name=public_field2,json=publicField2,proto3" json:"public_field2,omitempty"`
	PublicField3       int64                      `protobuf:"varint,58,opt,name=public_field3,json=publicField3,proto3" json:"public_field3,omitempty"`
	PublicFieldMe1     int64                      `protobuf:"varint,59,opt,name=public_field_me1,json=publicFieldMe1,proto3" json:"public_field_me1,omitempty"`
	PublicFieldMe2     int64                      `protobuf:"varint,60,opt,name=public_field_me2,json=publicFieldMe2,proto3" json:"public_field_me2,omitempty"`
	PublicFieldMe3     int64                      `protobuf:"varint,61,opt,name=public_field_me3,json=publicFieldMe3,proto3" json:"public_field_me3,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *NewsProto) Reset() {
	*x = NewsProto{}
	mi := &file_test_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NewsProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NewsProto) ProtoMessage() {}

func (x *NewsProto) ProtoReflect() protoreflect.Message {
	mi := &file_test_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NewsProto.ProtoReflect.Descriptor instead.
func (*NewsProto) Descriptor() ([]byte, []int) {
	return file_test_proto_rawDescGZIP(), []int{0}
}

func (x *NewsProto) GetInc() int64 {
	if x != nil {
		return x.Inc
	}
	return 0
}

func (x *NewsProto) GetRole() int64 {
	if x != nil {
		return x.Role
	}
	return 0
}

func (x *NewsProto) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *NewsProto) GetT() *timestamppb.Timestamp {
	if x != nil {
		return x.T
	}
	return nil
}

func (x *NewsProto) GetD() int64 {
	if x != nil {
		return x.D
	}
	return 0
}

func (x *NewsProto) GetInts() int64 {
	if x != nil {
		return x.Ints
	}
	return 0
}

func (x *NewsProto) GetInts8() int32 {
	if x != nil {
		return x.Ints8
	}
	return 0
}

func (x *NewsProto) GetInts16() int32 {
	if x != nil {
		return x.Ints16
	}
	return 0
}

func (x *NewsProto) GetInts32() int32 {
	if x != nil {
		return x.Ints32
	}
	return 0
}

func (x *NewsProto) GetInts64() int64 {
	if x != nil {
		return x.Ints64
	}
	return 0
}

func (x *NewsProto) GetUints() uint64 {
	if x != nil {
		return x.Uints
	}
	return 0
}

func (x *NewsProto) GetUints8() uint32 {
	if x != nil {
		return x.Uints8
	}
	return 0
}

func (x *NewsProto) GetUints16() uint32 {
	if x != nil {
		return x.Uints16
	}
	return 0
}

func (x *NewsProto) GetUints32() uint32 {
	if x != nil {
		return x.Uints32
	}
	return 0
}

func (x *NewsProto) GetUints64() uint64 {
	if x != nil {
		return x.Uints64
	}
	return 0
}

func (x *NewsProto) GetFloats32() float32 {
	if x != nil {
		return x.Floats32
	}
	return 0
}

func (x *NewsProto) GetFloats64() float64 {
	if x != nil {
		return x.Floats64
	}
	return 0
}

func (x *NewsProto) GetBools() bool {
	if x != nil {
		return x.Bools
	}
	return false
}

func (x *NewsProto) GetByte1() uint32 {
	if x != nil {
		return x.Byte1
	}
	return 0
}

func (x *NewsProto) GetBytes() []byte {
	if x != nil {
		return x.Bytes
	}
	return nil
}

func (x *NewsProto) GetListInts() []int64 {
	if x != nil {
		return x.ListInts
	}
	return nil
}

func (x *NewsProto) GetListString() []string {
	if x != nil {
		return x.ListString
	}
	return nil
}

func (x *NewsProto) GetListFloat() []float64 {
	if x != nil {
		return x.ListFloat
	}
	return nil
}

func (x *NewsProto) GetMapStringString() map[string]string {
	if x != nil {
		return x.MapStringString
	}
	return nil
}

func (x *NewsProto) GetMapStringBytes() map[string][]byte {
	if x != nil {
		return x.MapStringBytes
	}
	return nil
}

func (x *NewsProto) GetMapStringBool() map[string]bool {
	if x != nil {
		return x.MapStringBool
	}
	return nil
}

func (x *NewsProto) GetMapStringInt() map[string]int64 {
	if x != nil {
		return x.MapStringInt
	}
	return nil
}

func (x *NewsProto) GetMapStringFloat64() map[string]float64 {
	if x != nil {
		return x.MapStringFloat64
	}
	return nil
}

func (x *NewsProto) GetMapStringAny() map[string]*structpb.Value {
	if x != nil {
		return x.MapStringAny
	}
	return nil
}

func (x *NewsProto) GetMapIntString() map[int64]string {
	if x != nil {
		return x.MapIntString
	}
	return nil
}

func (x *NewsProto) GetMapIntInt() map[int64]int64 {
	if x != nil {
		return x.MapIntInt
	}
	return nil
}

func (x *NewsProto) GetMapIntBool() map[int64]bool {
	if x != nil {
		return x.MapIntBool
	}
	return nil
}

func (x *NewsProto) GetRenameSql() string {
	if x != nil {
		return x.RenameSql
	}
	return ""
}

func (x *NewsProto) GetRenameGo() string {
	if x != nil {
		return x.RenameGo
	}
	return ""
}

func (x *NewsProto) GetRenameJs() string {
	if x != nil {
		return x.RenameJs
	}
	return ""
}

func (x *NewsProto) GetMastUpperGo() string {
	if x != nil {
		return x.MastUpperGo
	}
	return ""
}

func (x *NewsProto) GetIntToSmallInt() int64 {
	if x != nil {
		return x.IntToSmallInt
	}
	return 0
}

func (x *NewsProto) GetSkip() string {
	if x != nil {
		return x.Skip
	}
	return ""
}

func (x *NewsProto) GetSqlUniqueU1_1() int64 {
	if x != nil {
		return x.SqlUniqueU1_1
	}
	return 0
}

func (x *NewsProto) GetSqlUniqueU1_2() int64 {
	if x != nil {
		return x.SqlUniqueU1_2
	}
	return 0
}

func (x *NewsProto) GetSqlIndex1_1() int64 {
	if x != nil {
		return x.SqlIndex1_1
	}
	return 0
}

func (x *NewsProto) GetSqlIndex1_2() int64 {
	if x != nil {
		return x.SqlIndex1_2
	}
	return 0
}

func (x *NewsProto) GetSqlIndex1_3() int64 {
	if x != nil {
		return x.SqlIndex1_3
	}
	return 0
}

func (x *NewsProto) GetSqlKeys_1() int64 {
	if x != nil {
		return x.SqlKeys_1
	}
	return 0
}

func (x *NewsProto) GetSqlKeys_2() int64 {
	if x != nil {
		return x.SqlKeys_2
	}
	return 0
}

func (x *NewsProto) GetSqlKeys_3() int64 {
	if x != nil {
		return x.SqlKeys_3
	}
	return 0
}

func (x *NewsProto) GetSqlSearch() string {
	if x != nil {
		return x.SqlSearch
	}
	return ""
}

func (x *NewsProto) GetSqlGet() string {
	if x != nil {
		return x.SqlGet
	}
	return ""
}

func (x *NewsProto) GetSqlUniqueX1() int64 {
	if x != nil {
		return x.SqlUniqueX1
	}
	return 0
}

func (x *NewsProto) GetSqlUniqueX2() int64 {
	if x != nil {
		return x.SqlUniqueX2
	}
	return 0
}

func (x *NewsProto) GetSqlUniqueX1X2() int64 {
	if x != nil {
		return x.SqlUniqueX1X2
	}
	return 0
}

func (x *NewsProto) GetSqlPrimary() float64 {
	if x != nil {
		return x.SqlPrimary
	}
	return 0
}

func (x *NewsProto) GetSqlJsonbIndex() map[string]*structpb.Value {
	if x != nil {
		return x.SqlJsonbIndex
	}
	return nil
}

func (x *NewsProto) GetTimeDuration() int64 {
	if x != nil {
		return x.TimeDuration
	}
	return 0
}

func (x *NewsProto) GetGoTypeIntToStrings() []string {
	if x != nil {
		return x.GoTypeIntToStrings
	}
	return nil
}

func (x *NewsProto) GetPublicField1() int64 {
	if x != nil {
		return x.PublicField1
	}
	return 0
}

func (x *NewsProto) GetPublicField2() int64 {
	if x != nil {
		return x.PublicField2
	}
	return 0
}

func (x *NewsProto) GetPublicField3() int64 {
	if x != nil {
		return x.PublicField3
	}
	return 0
}

func (x *NewsProto) GetPublicFieldMe1() int64 {
	if x != nil {
		return x.PublicFieldMe1
	}
	return 0
}

func (x *NewsProto) GetPublicFieldMe2() int64 {
	if x != nil {
		return x.PublicFieldMe2
	}
	return 0
}

func (x *NewsProto) GetPublicFieldMe3() int64 {
	if x != nil {
		return x.PublicFieldMe3
	}
	return 0
}

var File_test_proto protoreflect.FileDescriptor

const file_test_proto_rawDesc = "" +
	"\n" +
	"\n" +
	"test.proto\x12\x04news\x1a\x1cgoogle/protobuf/struct.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\xc4\x17\n" +
	"\tNewsProto\x12\x10\n" +
	"\x03inc\x18\x01 \x01(\x03R\x03inc\x12\x12\n" +
	"\x04role\x18\x02 \x01(\x03R\x04role\x12\x10\n" +
	"\x03key\x18\x03 \x01(\tR\x03key\x12(\n" +
	"\x01t\x18\x04 \x01(\v2\x1a.google.protobuf.TimestampR\x01t\x12\f\n" +
	"\x01d\x18\x05 \x01(\x03R\x01d\x12\x12\n" +
	"\x04ints\x18\x06 \x01(\x03R\x04ints\x12\x14\n" +
	"\x05ints8\x18\a \x01(\x05R\x05ints8\x12\x16\n" +
	"\x06ints16\x18\b \x01(\x05R\x06ints16\x12\x16\n" +
	"\x06ints32\x18\t \x01(\x05R\x06ints32\x12\x16\n" +
	"\x06ints64\x18\n" +
	" \x01(\x03R\x06ints64\x12\x14\n" +
	"\x05uints\x18\v \x01(\x04R\x05uints\x12\x16\n" +
	"\x06uints8\x18\f \x01(\rR\x06uints8\x12\x18\n" +
	"\auints16\x18\r \x01(\rR\auints16\x12\x18\n" +
	"\auints32\x18\x0e \x01(\rR\auints32\x12\x18\n" +
	"\auints64\x18\x0f \x01(\x04R\auints64\x12\x1a\n" +
	"\bfloats32\x18\x10 \x01(\x02R\bfloats32\x12\x1a\n" +
	"\bfloats64\x18\x11 \x01(\x01R\bfloats64\x12\x14\n" +
	"\x05bools\x18\x12 \x01(\bR\x05bools\x12\x14\n" +
	"\x05byte1\x18\x13 \x01(\rR\x05byte1\x12\x14\n" +
	"\x05bytes\x18\x14 \x01(\fR\x05bytes\x12\x1b\n" +
	"\tlist_ints\x18\x15 \x03(\x03R\blistInts\x12\x1f\n" +
	"\vlist_string\x18\x16 \x03(\tR\n" +
	"listString\x12\x1d\n" +
	"\n" +
	"list_float\x18\x17 \x03(\x01R\tlistFloat\x12P\n" +
	"\x11map_string_string\x18\x18 \x03(\v2$.news.NewsProto.MapStringStringEntryR\x0fmapStringString\x12M\n" +
	"\x10map_string_bytes\x18\x19 \x03(\v2#.news.NewsProto.MapStringBytesEntryR\x0emapStringBytes\x12J\n" +
	"\x0fmap_string_bool\x18\x1a \x03(\v2\".news.NewsProto.MapStringBoolEntryR\rmapStringBool\x12G\n" +
	"\x0emap_string_int\x18\x1b \x03(\v2!.news.NewsProto.MapStringIntEntryR\fmapStringInt\x12S\n" +
	"\x12map_string_float64\x18\x1c \x03(\v2%.news.NewsProto.MapStringFloat64EntryR\x10mapStringFloat64\x12G\n" +
	"\x0emap_string_any\x18\x1d \x03(\v2!.news.NewsProto.MapStringAnyEntryR\fmapStringAny\x12G\n" +
	"\x0emap_int_string\x18\x1e \x03(\v2!.news.NewsProto.MapIntStringEntryR\fmapIntString\x12>\n" +
	"\vmap_int_int\x18\x1f \x03(\v2\x1e.news.NewsProto.MapIntIntEntryR\tmapIntInt\x12A\n" +
	"\fmap_int_bool\x18  \x03(\v2\x1f.news.NewsProto.MapIntBoolEntryR\n" +
	"mapIntBool\x12\x1d\n" +
	"\n" +
	"rename_sql\x18! \x01(\tR\trenameSql\x12\x1b\n" +
	"\trename_go\x18\" \x01(\tR\brenameGo\x12\x1b\n" +
	"\trename_js\x18# \x01(\tR\brenameJs\x12\"\n" +
	"\rmast_upper_go\x18$ \x01(\tR\vmastUpperGo\x12'\n" +
	"\x10int_to_small_int\x18% \x01(\x03R\rintToSmallInt\x12\x12\n" +
	"\x04skip\x18& \x01(\tR\x04skip\x12%\n" +
	"\x0fsql_unique_u1_1\x18' \x01(\x03R\fsqlUniqueU11\x12%\n" +
	"\x0fsql_unique_u1_2\x18( \x01(\x03R\fsqlUniqueU12\x12 \n" +
	"\fsql_index1_1\x18) \x01(\x03R\n" +
	"sqlIndex11\x12 \n" +
	"\fsql_index1_2\x18* \x01(\x03R\n" +
	"sqlIndex12\x12 \n" +
	"\fsql_index1_3\x18+ \x01(\x03R\n" +
	"sqlIndex13\x12\x1c\n" +
	"\n" +
	"sql_keys_1\x18, \x01(\x03R\bsqlKeys1\x12\x1c\n" +
	"\n" +
	"sql_keys_2\x18- \x01(\x03R\bsqlKeys2\x12\x1c\n" +
	"\n" +
	"sql_keys_3\x18. \x01(\x03R\bsqlKeys3\x12\x1d\n" +
	"\n" +
	"sql_search\x18/ \x01(\tR\tsqlSearch\x12\x17\n" +
	"\asql_get\x180 \x01(\tR\x06sqlGet\x12\"\n" +
	"\rsql_unique_x1\x181 \x01(\x03R\vsqlUniqueX1\x12\"\n" +
	"\rsql_unique_x2\x182 \x01(\x03R\vsqlUniqueX2\x12'\n" +
	"\x10sql_unique_x1_x2\x183 \x01(\x03R\rsqlUniqueX1X2\x12\x1f\n" +
	"\vsql_primary\x184 \x01(\x01R\n" +
	"sqlPrimary\x12J\n" +
	"\x0fsql_jsonb_index\x185 \x03(\v2\".news.NewsProto.SqlJsonbIndexEntryR\rsqlJsonbIndex\x12#\n" +
	"\rtime_duration\x186 \x01(\x03R\ftimeDuration\x122\n" +
	"\x16go_type_int_to_strings\x187 \x03(\tR\x12goTypeIntToStrings\x12#\n" +
	"\rpublic_field1\x188 \x01(\x03R\fpublicField1\x12#\n" +
	"\rpublic_field2\x189 \x01(\x03R\fpublicField2\x12#\n" +
	"\rpublic_field3\x18: \x01(\x03R\fpublicField3\x12(\n" +
	"\x10public_field_me1\x18; \x01(\x03R\x0epublicFieldMe1\x12(\n" +
	"\x10public_field_me2\x18< \x01(\x03R\x0epublicFieldMe2\x12(\n" +
	"\x10public_field_me3\x18= \x01(\x03R\x0epublicFieldMe3\x1aB\n" +
	"\x14MapStringStringEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\x1aA\n" +
	"\x13MapStringBytesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\fR\x05value:\x028\x01\x1a@\n" +
	"\x12MapStringBoolEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\bR\x05value:\x028\x01\x1a?\n" +
	"\x11MapStringIntEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01\x1aC\n" +
	"\x15MapStringFloat64Entry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x01R\x05value:\x028\x01\x1aW\n" +
	"\x11MapStringAnyEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12,\n" +
	"\x05value\x18\x02 \x01(\v2\x16.google.protobuf.ValueR\x05value:\x028\x01\x1a?\n" +
	"\x11MapIntStringEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01\x1a<\n" +
	"\x0eMapIntIntEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01\x1a=\n" +
	"\x0fMapIntBoolEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\x03R\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\bR\x05value:\x028\x01\x1aX\n" +
	"\x12SqlJsonbIndexEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12,\n" +
	"\x05value\x18\x02 \x01(\v2\x16.google.protobuf.ValueR\x05value:\x028\x01B\tZ\a./;newsb\x06proto3"

var (
	file_test_proto_rawDescOnce sync.Once
	file_test_proto_rawDescData []byte
)

func file_test_proto_rawDescGZIP() []byte {
	file_test_proto_rawDescOnce.Do(func() {
		file_test_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_test_proto_rawDesc), len(file_test_proto_rawDesc)))
	})
	return file_test_proto_rawDescData
}

var file_test_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_test_proto_goTypes = []any{
	(*NewsProto)(nil),             // 0: news.NewsProto
	nil,                           // 1: news.NewsProto.MapStringStringEntry
	nil,                           // 2: news.NewsProto.MapStringBytesEntry
	nil,                           // 3: news.NewsProto.MapStringBoolEntry
	nil,                           // 4: news.NewsProto.MapStringIntEntry
	nil,                           // 5: news.NewsProto.MapStringFloat64Entry
	nil,                           // 6: news.NewsProto.MapStringAnyEntry
	nil,                           // 7: news.NewsProto.MapIntStringEntry
	nil,                           // 8: news.NewsProto.MapIntIntEntry
	nil,                           // 9: news.NewsProto.MapIntBoolEntry
	nil,                           // 10: news.NewsProto.SqlJsonbIndexEntry
	(*timestamppb.Timestamp)(nil), // 11: google.protobuf.Timestamp
	(*structpb.Value)(nil),        // 12: google.protobuf.Value
}
var file_test_proto_depIdxs = []int32{
	11, // 0: news.NewsProto.t:type_name -> google.protobuf.Timestamp
	1,  // 1: news.NewsProto.map_string_string:type_name -> news.NewsProto.MapStringStringEntry
	2,  // 2: news.NewsProto.map_string_bytes:type_name -> news.NewsProto.MapStringBytesEntry
	3,  // 3: news.NewsProto.map_string_bool:type_name -> news.NewsProto.MapStringBoolEntry
	4,  // 4: news.NewsProto.map_string_int:type_name -> news.NewsProto.MapStringIntEntry
	5,  // 5: news.NewsProto.map_string_float64:type_name -> news.NewsProto.MapStringFloat64Entry
	6,  // 6: news.NewsProto.map_string_any:type_name -> news.NewsProto.MapStringAnyEntry
	7,  // 7: news.NewsProto.map_int_string:type_name -> news.NewsProto.MapIntStringEntry
	8,  // 8: news.NewsProto.map_int_int:type_name -> news.NewsProto.MapIntIntEntry
	9,  // 9: news.NewsProto.map_int_bool:type_name -> news.NewsProto.MapIntBoolEntry
	10, // 10: news.NewsProto.sql_jsonb_index:type_name -> news.NewsProto.SqlJsonbIndexEntry
	12, // 11: news.NewsProto.MapStringAnyEntry.value:type_name -> google.protobuf.Value
	12, // 12: news.NewsProto.SqlJsonbIndexEntry.value:type_name -> google.protobuf.Value
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

var _ = initNewsProto()

func initNewsProto() struct{} {
	file_test_proto_init()
	return struct{}{}
}
func file_test_proto_init() {
	if File_test_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_test_proto_rawDesc), len(file_test_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_test_proto_goTypes,
		DependencyIndexes: file_test_proto_depIdxs,
		MessageInfos:      file_test_proto_msgTypes,
	}.Build()
	File_test_proto = out.File
	file_test_proto_goTypes = nil
	file_test_proto_depIdxs = nil
}
