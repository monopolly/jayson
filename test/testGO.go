package news

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/niubaoshu/gotiny"
	"github.com/vmihailenco/msgpack/v5"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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

// Tuple create an array from struct
func (a *News) Tuple() (r []any) {
	return []any{a.Inc, a.Role, a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Skip, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
}

// Tuple create an array from struct for clickhouse
func (a *News) clickhouseTuple() (r []any) {
	return []any{a.Inc, int(a.Role), a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.List_ints, a.List_string, a.List_float, a.Map_string_string, a.Map_string_bytes, a.Map_string_bool, a.Map_string_int, a.Map_string_float64, a.Map_string_any, a.Map_int_string, a.Map_int_int, a.Map_int_bool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MAST_UPPER_GO, a.IntToSmallInt, a.Skip, a.Sql_unique_u1_1, a.Sql_unique_u1_2, a.Sql_index1_1, a.Sql_index1_2, a.Sql_index1_3, a.Sql_keys_1, a.Sql_keys_2, a.Sql_keys_3, a.Sql_search, a.Sql_get, a.Sql_unique_x1, a.Sql_unique_x2, a.Sql_unique_x1_x2, a.Sql_primary, a.Sql_jsonb_index, a.Time_duration, a.Go_type_int_to_strings, a.Public_field1, a.Public_field2, a.Public_field3, a.Public_field_me1, a.Public_field_me2, a.Public_field_me3}
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

// clickhouse NewsCQL class
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
	// scan targets by column types: clickhouse needs exact types
	types := rows.ColumnTypes()
	for rows.Next() {
		var item News
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

// clickhouseSet sets field from value of clickhouse row
func (a *News) clickhouseSet(f NewsIndexTyped, v any) {
	if v == nil {
		return
	}
	switch f {
	case NewsIndexInc:
		newsCHAssign(reflect.ValueOf(&a.Inc).Elem(), reflect.ValueOf(v))
	case NewsIndexRole:
		newsCHAssign(reflect.ValueOf(&a.Role).Elem(), reflect.ValueOf(v))
	case NewsIndexKey:
		newsCHAssign(reflect.ValueOf(&a.Key).Elem(), reflect.ValueOf(v))
	case NewsIndexT:
		newsCHAssign(reflect.ValueOf(&a.T).Elem(), reflect.ValueOf(v))
	case NewsIndexD:
		newsCHAssign(reflect.ValueOf(&a.D).Elem(), reflect.ValueOf(v))
	case NewsIndexInts:
		newsCHAssign(reflect.ValueOf(&a.Ints).Elem(), reflect.ValueOf(v))
	case NewsIndexInts8:
		newsCHAssign(reflect.ValueOf(&a.Ints8).Elem(), reflect.ValueOf(v))
	case NewsIndexInts16:
		newsCHAssign(reflect.ValueOf(&a.Ints16).Elem(), reflect.ValueOf(v))
	case NewsIndexInts32:
		newsCHAssign(reflect.ValueOf(&a.Ints32).Elem(), reflect.ValueOf(v))
	case NewsIndexInts64:
		newsCHAssign(reflect.ValueOf(&a.Ints64).Elem(), reflect.ValueOf(v))
	case NewsIndexUints:
		newsCHAssign(reflect.ValueOf(&a.Uints).Elem(), reflect.ValueOf(v))
	case NewsIndexUints8:
		newsCHAssign(reflect.ValueOf(&a.Uints8).Elem(), reflect.ValueOf(v))
	case NewsIndexUints16:
		newsCHAssign(reflect.ValueOf(&a.Uints16).Elem(), reflect.ValueOf(v))
	case NewsIndexUints32:
		newsCHAssign(reflect.ValueOf(&a.Uints32).Elem(), reflect.ValueOf(v))
	case NewsIndexUints64:
		newsCHAssign(reflect.ValueOf(&a.Uints64).Elem(), reflect.ValueOf(v))
	case NewsIndexFloats32:
		newsCHAssign(reflect.ValueOf(&a.Floats32).Elem(), reflect.ValueOf(v))
	case NewsIndexFloats64:
		newsCHAssign(reflect.ValueOf(&a.Floats64).Elem(), reflect.ValueOf(v))
	case NewsIndexBools:
		newsCHAssign(reflect.ValueOf(&a.Bools).Elem(), reflect.ValueOf(v))
	case NewsIndexByte1:
		newsCHAssign(reflect.ValueOf(&a.Byte1).Elem(), reflect.ValueOf(v))
	case NewsIndexBytes:
		newsCHAssign(reflect.ValueOf(&a.Bytes).Elem(), reflect.ValueOf(v))
	case NewsIndexList_ints:
		newsCHAssign(reflect.ValueOf(&a.List_ints).Elem(), reflect.ValueOf(v))
	case NewsIndexList_string:
		newsCHAssign(reflect.ValueOf(&a.List_string).Elem(), reflect.ValueOf(v))
	case NewsIndexList_float:
		newsCHAssign(reflect.ValueOf(&a.List_float).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_string:
		newsCHAssign(reflect.ValueOf(&a.Map_string_string).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_bytes:
		newsCHAssign(reflect.ValueOf(&a.Map_string_bytes).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_bool:
		newsCHAssign(reflect.ValueOf(&a.Map_string_bool).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_int:
		newsCHAssign(reflect.ValueOf(&a.Map_string_int).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_float64:
		newsCHAssign(reflect.ValueOf(&a.Map_string_float64).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_string_any:
		newsCHAssign(reflect.ValueOf(&a.Map_string_any).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_int_string:
		newsCHAssign(reflect.ValueOf(&a.Map_int_string).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_int_int:
		newsCHAssign(reflect.ValueOf(&a.Map_int_int).Elem(), reflect.ValueOf(v))
	case NewsIndexMap_int_bool:
		newsCHAssign(reflect.ValueOf(&a.Map_int_bool).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameSQL:
		newsCHAssign(reflect.ValueOf(&a.RenameSQL).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameGO_OK:
		newsCHAssign(reflect.ValueOf(&a.RenameGO_OK).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameJS:
		newsCHAssign(reflect.ValueOf(&a.RenameJS).Elem(), reflect.ValueOf(v))
	case NewsIndexMAST_UPPER_GO:
		newsCHAssign(reflect.ValueOf(&a.MAST_UPPER_GO).Elem(), reflect.ValueOf(v))
	case NewsIndexIntToSmallInt:
		newsCHAssign(reflect.ValueOf(&a.IntToSmallInt).Elem(), reflect.ValueOf(v))
	case NewsIndexSkip:
		newsCHAssign(reflect.ValueOf(&a.Skip).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_unique_u1_1:
		newsCHAssign(reflect.ValueOf(&a.Sql_unique_u1_1).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_unique_u1_2:
		newsCHAssign(reflect.ValueOf(&a.Sql_unique_u1_2).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_index1_1:
		newsCHAssign(reflect.ValueOf(&a.Sql_index1_1).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_index1_2:
		newsCHAssign(reflect.ValueOf(&a.Sql_index1_2).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_index1_3:
		newsCHAssign(reflect.ValueOf(&a.Sql_index1_3).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_keys_1:
		newsCHAssign(reflect.ValueOf(&a.Sql_keys_1).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_keys_2:
		newsCHAssign(reflect.ValueOf(&a.Sql_keys_2).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_keys_3:
		newsCHAssign(reflect.ValueOf(&a.Sql_keys_3).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_search:
		newsCHAssign(reflect.ValueOf(&a.Sql_search).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_get:
		newsCHAssign(reflect.ValueOf(&a.Sql_get).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_unique_x1:
		newsCHAssign(reflect.ValueOf(&a.Sql_unique_x1).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_unique_x2:
		newsCHAssign(reflect.ValueOf(&a.Sql_unique_x2).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_unique_x1_x2:
		newsCHAssign(reflect.ValueOf(&a.Sql_unique_x1_x2).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_primary:
		newsCHAssign(reflect.ValueOf(&a.Sql_primary).Elem(), reflect.ValueOf(v))
	case NewsIndexSql_jsonb_index:
		newsCHAssign(reflect.ValueOf(&a.Sql_jsonb_index).Elem(), reflect.ValueOf(v))
	case NewsIndexTime_duration:
		newsCHAssign(reflect.ValueOf(&a.Time_duration).Elem(), reflect.ValueOf(v))
	case NewsIndexGo_type_int_to_strings:
		newsCHAssign(reflect.ValueOf(&a.Go_type_int_to_strings).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field1:
		newsCHAssign(reflect.ValueOf(&a.Public_field1).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field2:
		newsCHAssign(reflect.ValueOf(&a.Public_field2).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field3:
		newsCHAssign(reflect.ValueOf(&a.Public_field3).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field_me1:
		newsCHAssign(reflect.ValueOf(&a.Public_field_me1).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field_me2:
		newsCHAssign(reflect.ValueOf(&a.Public_field_me2).Elem(), reflect.ValueOf(v))
	case NewsIndexPublic_field_me3:
		newsCHAssign(reflect.ValueOf(&a.Public_field_me3).Elem(), reflect.ValueOf(v))
	}
}

// newsCHAssign sets d from clickhouse value s: same type, number, string, time, slice
func newsCHAssign(d, s reflect.Value) {
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
		newsCHAssign(p.Elem(), s)
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
			newsCHAssign(r.Index(i), s.Index(i))
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
