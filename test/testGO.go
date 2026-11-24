package news

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/jayson/test/role"
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

	SQL FUNCTIONS: methods of interface become methods of NewsSQL
	type newsSQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (News, error)

		// select role from news where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from news where id = @id
		Music(id int) ([]int, error)

		// select id from news where role = @role
		IDs(role role.Role) ([]int, error)

		// update news set created = now() where id = @id
		Touch(id int) error

		// delete from news where created < @before
		Clean(before time.Time) (int64, error) // count of rows
	}

	CLICKHOUSE FUNCTIONS: methods of interface become methods of NewsCQL
	type newsCQL interface {
		// where role = @role and key = @key limit 1
		Creds(role role.Role, key uuid.UUID) (News, error)

		// select role from news where id = @id
		Role(id int) (role.Role, error) // !404: empty value instead of not found

		// select music from news where id = @id
		Music(id int) ([]int, error)

		// select id from news where role = @role
		IDs(role role.Role) ([]int64, error)

		// alter table news delete where created < @before
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

// init struct
func NewNews() (a *News) {
	a = new(News)
	a.MapStringString = make(map[string]string)
	a.MapStringBytes = make(map[string][]byte)
	a.MapStringBool = make(map[string]bool)
	a.MapStringInt = make(map[string]int)
	a.MapStringFloat64 = make(map[string]float64)
	a.MapStringAny = make(map[string]any)
	a.MapIntString = make(map[int]string)
	a.MapIntInt = make(map[int]int)
	a.MapIntBool = make(map[int]bool)
	a.SqlJsonbIndex = make(map[string]any)
	return
}

// 504 bytes (go padding)
type News struct {
	Inc                int                `json:"inc,omitempty" msg:"inc,omitempty"`                                       // sql{inc}  #readonly
	Role               role.Role          `json:"role,omitempty" msg:"role,omitempty"`                                     //
	Key                uuid.UUID          `json:"key,omitempty" msg:"key,omitempty"`                                       //
	T                  time.Time          `json:"t,omitempty" msg:"t,omitempty"`                                           //
	D                  time.Duration      `json:"d,omitempty" msg:"d,omitempty"`                                           //
	Ints               int                `json:"ints,omitempty" msg:"ints,omitempty"`                                     //
	Ints8              int8               `json:"ints8,omitempty" msg:"ints8,omitempty"`                                   //
	Ints16             int16              `json:"ints16,omitempty" msg:"ints16,omitempty"`                                 //
	Ints32             int32              `json:"ints32,omitempty" msg:"ints32,omitempty"`                                 //
	Ints64             int64              `json:"ints64,omitempty" msg:"ints64,omitempty"`                                 //
	Uints              uint               `json:"uints,omitempty" msg:"uints,omitempty"`                                   //
	Uints8             uint8              `json:"uints8,omitempty" msg:"uints8,omitempty"`                                 //
	Uints16            uint16             `json:"uints16,omitempty" msg:"uints16,omitempty"`                               //
	Uints32            uint32             `json:"uints32,omitempty" msg:"uints32,omitempty"`                               //
	Uints64            uint64             `json:"uints64,omitempty" msg:"uints64,omitempty"`                               //
	Floats32           float32            `json:"floats32,omitempty" msg:"floats32,omitempty"`                             //
	Floats64           float64            `json:"floats64,omitempty" msg:"floats64,omitempty"`                             //
	Bools              bool               `json:"bools,omitempty" msg:"bools,omitempty"`                                   //
	Byte1              byte               `json:"byte1,omitempty" msg:"byte1,omitempty"`                                   //
	Bytes              []byte             `json:"bytes,omitempty" msg:"bytes,omitempty"`                                   //
	ListInts           []int              `json:"list_ints,omitempty" msg:"list_ints,omitempty"`                           //
	ListString         []string           `json:"list_string,omitempty" msg:"list_string,omitempty"`                       //
	ListFloat          []float64          `json:"list_float,omitempty" msg:"list_float,omitempty"`                         //
	MapStringString    map[string]string  `json:"map_string_string,omitempty" msg:"map_string_string,omitempty"`           //
	MapStringBytes     map[string][]byte  `json:"map_string_bytes,omitempty" msg:"map_string_bytes,omitempty"`             //
	MapStringBool      map[string]bool    `json:"map_string_bool,omitempty" msg:"map_string_bool,omitempty"`               //
	MapStringInt       map[string]int     `json:"map_string_int,omitempty" msg:"map_string_int,omitempty"`                 //
	MapStringFloat64   map[string]float64 `json:"map_string_float64,omitempty" msg:"map_string_float64,omitempty"`         //
	MapStringAny       map[string]any     `json:"map_string_any,omitempty" msg:"map_string_any,omitempty"`                 //
	MapIntString       map[int]string     `json:"map_int_string,omitempty" msg:"map_int_string,omitempty"`                 //
	MapIntInt          map[int]int        `json:"map_int_int,omitempty" msg:"map_int_int,omitempty"`                       //
	MapIntBool         map[int]bool       `json:"map_int_bool,omitempty" msg:"map_int_bool,omitempty"`                     //
	RenameSQL          string             `json:"renameSQL,omitempty" msg:"renameSQL,omitempty"`                           // sql{name="renameSQL_OK"}
	RenameGO_OK        string             `json:"renameGO,omitempty" msg:"renameGO,omitempty"`                             // go{name="RenameGO_OK"}
	RenameJS           string             `json:"renameJS_OK,omitempty" msg:"renameJS_OK,omitempty"`                       // js{name="renameJS_OK"}
	MASTUPPERGO        string             `json:"mast_upper_go,omitempty" msg:"mast_upper_go,omitempty"`                   // go{up}
	IntToSmallInt      int                `json:"intToSmallInt,omitempty" msg:"intToSmallInt,omitempty"`                   // sql{type="smallint"} rename
	Skip               string             `json:"skip,omitempty" msg:"skip,omitempty"`                                     // sql{skip} swift{skip} js{skip}
	SqlUniqueU11       int                `json:"sql_unique_u1_1,omitempty" msg:"sql_unique_u1_1,omitempty"`               // sql{unique="u1"}
	SqlUniqueU12       int                `json:"sql_unique_u1_2,omitempty" msg:"sql_unique_u1_2,omitempty"`               // sql{unique="u1"}
	SqlIndex11         int                `json:"sql_index1_1,omitempty" msg:"sql_index1_1,omitempty"`                     // sql{idx="index1"}
	SqlIndex12         int                `json:"sql_index1_2,omitempty" msg:"sql_index1_2,omitempty"`                     // sql{idx="index1"}
	SqlIndex13         int                `json:"sql_index1_3,omitempty" msg:"sql_index1_3,omitempty"`                     // sql{idx="index1"}
	SqlKeys1           int                `json:"sql_keys_1,omitempty" msg:"sql_keys_1,omitempty"`                         // sql{keys="keys1"}
	SqlKeys2           int                `json:"sql_keys_2,omitempty" msg:"sql_keys_2,omitempty"`                         // sql{keys="keys1"}
	SqlKeys3           int                `json:"sql_keys_3,omitempty" msg:"sql_keys_3,omitempty"`                         // sql{keys="keys1"}
	SqlSearch          string             `json:"sql_search,omitempty" msg:"sql_search,omitempty"`                         // sql{search="search" get}
	SqlGet             string             `json:"sql_get,omitempty" msg:"sql_get,omitempty"`                               // sql{get}
	SqlUniqueX1        int                `json:"sql_unique_x1,omitempty" msg:"sql_unique_x1,omitempty"`                   // sql{unique="x1"}
	SqlUniqueX2        int                `json:"sql_unique_x2,omitempty" msg:"sql_unique_x2,omitempty"`                   // sql{unique="x2"}
	SqlUniqueX1X2      int                `json:"sql_unique_x1_x2,omitempty" msg:"sql_unique_x1_x2,omitempty"`             // sql{unique="x1", unique="x2"}
	SqlPrimary         float64            `json:"sql_primary,omitempty" msg:"sql_primary,omitempty"`                       // sql{primarykey}
	SqlJsonbIndex      map[string]any     `json:"sql_jsonb_index,omitempty" msg:"sql_jsonb_index,omitempty"`               // sql{index}
	TimeDuration       time.Duration      `json:"time_duration,omitempty" msg:"time_duration,omitempty"`                   //
	GoTypeIntToStrings []string           `json:"go_type_int_to_strings,omitempty" msg:"go_type_int_to_strings,omitempty"` // go{type="[]string"}
	PublicField1       int                `json:"public_field1,omitempty" msg:"public_field1,omitempty"`                   // @public
	PublicField2       int                `json:"public_field2,omitempty" msg:"public_field2,omitempty"`                   // @public
	PublicField3       int                `json:"public_field3,omitempty" msg:"public_field3,omitempty"`                   // @public
	PublicFieldMe1     int                `json:"public_field_me1,omitempty" msg:"public_field_me1,omitempty"`             // @me
	PublicFieldMe2     int                `json:"public_field_me2,omitempty" msg:"public_field_me2,omitempty"`             // @me
	PublicFieldMe3     int                `json:"public_field_me3,omitempty" msg:"public_field_me3,omitempty"`             // @me
}

// newsSQLDB runs queries: pool or transaction
type newsSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// NewsSQL sql queries of table news
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type NewsSQL struct {
	pool *pgxpool.Pool
	db   newsSQLDB // pool, or transaction after TX
}

// NewNewsSQL creates queries on pool, table and indexes if not exist.
// Errors of table are not returned: call CreateTable and CreateIndexes to see them
func NewNewsSQL(pool *pgxpool.Pool) *NewsSQL {
	a := &NewsSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err == nil {
		_ = a.CreateIndexes()
	}
	return a
}

// Pool returns pool
func (a *NewsSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of NewsSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum news")
//		return err
//	})
func (a *NewsSQL) Conn(f func(conn *pgxpool.Conn) error) error {
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
func (a *NewsSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *NewsSQL) TX(tx pgx.Tx) *NewsSQL {
	return &NewsSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *NewsSQL) TableName() string {
	return "news"
}

// Fields returns all sql columns
func (a *NewsSQL) Fields() []NewsIndexTyped {
	return slices.Clone(newsSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var newsSQLFields = []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexListInts, NewsIndexListString, NewsIndexListFloat, NewsIndexMapStringString, NewsIndexMapStringBytes, NewsIndexMapStringBool, NewsIndexMapStringInt, NewsIndexMapStringFloat64, NewsIndexMapStringAny, NewsIndexMapIntString, NewsIndexMapIntInt, NewsIndexMapIntBool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMASTUPPERGO, NewsIndexIntToSmallInt, NewsIndexSqlUniqueU11, NewsIndexSqlUniqueU12, NewsIndexSqlIndex11, NewsIndexSqlIndex12, NewsIndexSqlIndex13, NewsIndexSqlKeys1, NewsIndexSqlKeys2, NewsIndexSqlKeys3, NewsIndexSqlSearch, NewsIndexSqlGet, NewsIndexSqlUniqueX1, NewsIndexSqlUniqueX2, NewsIndexSqlUniqueX1X2, NewsIndexSqlPrimary, NewsIndexSqlJsonbIndex, NewsIndexTimeDuration, NewsIndexGoTypeIntToStrings, NewsIndexPublicField1, NewsIndexPublicField2, NewsIndexPublicField3, NewsIndexPublicFieldMe1, NewsIndexPublicFieldMe2, NewsIndexPublicFieldMe3}

// columns of insert
var newsSQLInsertColumns = []string{"role", "key", "t", "d", "ints", "ints8", "ints16", "ints32", "ints64", "uints", "uints8", "uints16", "uints32", "uints64", "floats32", "floats64", "bools", "byte1", "bytes", "list_ints", "list_string", "list_float", "map_string_string", "map_string_bytes", "map_string_bool", "map_string_int", "map_string_float64", "map_string_any", "map_int_string", "map_int_int", "map_int_bool", "renameSQL_OK", "renameGO", "renameJS", "mast_upper_go", "intToSmallInt", "sql_unique_u1_1", "sql_unique_u1_2", "sql_index1_1", "sql_index1_2", "sql_index1_3", "sql_keys_1", "sql_keys_2", "sql_keys_3", "sql_search", "sql_get", "sql_unique_x1", "sql_unique_x2", "sql_unique_x1_x2", "sql_primary", "sql_jsonb_index", "time_duration", "go_type_int_to_strings", "public_field1", "public_field2", "public_field3", "public_field_me1", "public_field_me2", "public_field_me3"}

// sql column names
var newsSQLColumns = [...]string{
	NewsIndexInc:                "inc",
	NewsIndexRole:               "role",
	NewsIndexKey:                "key",
	NewsIndexT:                  "t",
	NewsIndexD:                  "d",
	NewsIndexInts:               "ints",
	NewsIndexInts8:              "ints8",
	NewsIndexInts16:             "ints16",
	NewsIndexInts32:             "ints32",
	NewsIndexInts64:             "ints64",
	NewsIndexUints:              "uints",
	NewsIndexUints8:             "uints8",
	NewsIndexUints16:            "uints16",
	NewsIndexUints32:            "uints32",
	NewsIndexUints64:            "uints64",
	NewsIndexFloats32:           "floats32",
	NewsIndexFloats64:           "floats64",
	NewsIndexBools:              "bools",
	NewsIndexByte1:              "byte1",
	NewsIndexBytes:              "bytes",
	NewsIndexListInts:           "list_ints",
	NewsIndexListString:         "list_string",
	NewsIndexListFloat:          "list_float",
	NewsIndexMapStringString:    "map_string_string",
	NewsIndexMapStringBytes:     "map_string_bytes",
	NewsIndexMapStringBool:      "map_string_bool",
	NewsIndexMapStringInt:       "map_string_int",
	NewsIndexMapStringFloat64:   "map_string_float64",
	NewsIndexMapStringAny:       "map_string_any",
	NewsIndexMapIntString:       "map_int_string",
	NewsIndexMapIntInt:          "map_int_int",
	NewsIndexMapIntBool:         "map_int_bool",
	NewsIndexRenameSQL:          "renameSQL_OK",
	NewsIndexRenameGO_OK:        "renameGO",
	NewsIndexRenameJS:           "renameJS",
	NewsIndexMASTUPPERGO:        "mast_upper_go",
	NewsIndexIntToSmallInt:      "intToSmallInt",
	NewsIndexSqlUniqueU11:       "sql_unique_u1_1",
	NewsIndexSqlUniqueU12:       "sql_unique_u1_2",
	NewsIndexSqlIndex11:         "sql_index1_1",
	NewsIndexSqlIndex12:         "sql_index1_2",
	NewsIndexSqlIndex13:         "sql_index1_3",
	NewsIndexSqlKeys1:           "sql_keys_1",
	NewsIndexSqlKeys2:           "sql_keys_2",
	NewsIndexSqlKeys3:           "sql_keys_3",
	NewsIndexSqlSearch:          "sql_search",
	NewsIndexSqlGet:             "sql_get",
	NewsIndexSqlUniqueX1:        "sql_unique_x1",
	NewsIndexSqlUniqueX2:        "sql_unique_x2",
	NewsIndexSqlUniqueX1X2:      "sql_unique_x1_x2",
	NewsIndexSqlPrimary:         "sql_primary",
	NewsIndexSqlJsonbIndex:      "sql_jsonb_index",
	NewsIndexTimeDuration:       "time_duration",
	NewsIndexGoTypeIntToStrings: "go_type_int_to_strings",
	NewsIndexPublicField1:       "public_field1",
	NewsIndexPublicField2:       "public_field2",
	NewsIndexPublicField3:       "public_field3",
	NewsIndexPublicFieldMe1:     "public_field_me1",
	NewsIndexPublicFieldMe2:     "public_field_me2",
	NewsIndexPublicFieldMe3:     "public_field_me3",
}

// select expressions: null of scalar column is zero value
var newsSQLSelects = [...]string{
	NewsIndexInc:                "coalesce(inc, 0)",
	NewsIndexRole:               "coalesce(role, 0)",
	NewsIndexKey:                "key",
	NewsIndexT:                  "coalesce(t, '0001-01-01 00:00:00+00')",
	NewsIndexD:                  "coalesce(d, 0)",
	NewsIndexInts:               "coalesce(ints, 0)",
	NewsIndexInts8:              "coalesce(ints8, 0)",
	NewsIndexInts16:             "coalesce(ints16, 0)",
	NewsIndexInts32:             "coalesce(ints32, 0)",
	NewsIndexInts64:             "coalesce(ints64, 0)",
	NewsIndexUints:              "coalesce(uints, 0)",
	NewsIndexUints8:             "coalesce(uints8, 0)",
	NewsIndexUints16:            "coalesce(uints16, 0)",
	NewsIndexUints32:            "coalesce(uints32, 0)",
	NewsIndexUints64:            "coalesce(uints64, 0)",
	NewsIndexFloats32:           "coalesce(floats32, 0)",
	NewsIndexFloats64:           "coalesce(floats64, 0)",
	NewsIndexBools:              "coalesce(bools, false)",
	NewsIndexByte1:              "coalesce(byte1, 0)",
	NewsIndexBytes:              "bytes",
	NewsIndexListInts:           "list_ints",
	NewsIndexListString:         "list_string",
	NewsIndexListFloat:          "list_float",
	NewsIndexMapStringString:    "map_string_string",
	NewsIndexMapStringBytes:     "map_string_bytes",
	NewsIndexMapStringBool:      "map_string_bool",
	NewsIndexMapStringInt:       "map_string_int",
	NewsIndexMapStringFloat64:   "map_string_float64",
	NewsIndexMapStringAny:       "map_string_any",
	NewsIndexMapIntString:       "map_int_string",
	NewsIndexMapIntInt:          "map_int_int",
	NewsIndexMapIntBool:         "map_int_bool",
	NewsIndexRenameSQL:          "coalesce(renameSQL_OK, '')",
	NewsIndexRenameGO_OK:        "coalesce(renameGO, '')",
	NewsIndexRenameJS:           "coalesce(renameJS, '')",
	NewsIndexMASTUPPERGO:        "coalesce(mast_upper_go, '')",
	NewsIndexIntToSmallInt:      "coalesce(intToSmallInt, 0)",
	NewsIndexSqlUniqueU11:       "coalesce(sql_unique_u1_1, 0)",
	NewsIndexSqlUniqueU12:       "coalesce(sql_unique_u1_2, 0)",
	NewsIndexSqlIndex11:         "coalesce(sql_index1_1, 0)",
	NewsIndexSqlIndex12:         "coalesce(sql_index1_2, 0)",
	NewsIndexSqlIndex13:         "coalesce(sql_index1_3, 0)",
	NewsIndexSqlKeys1:           "coalesce(sql_keys_1, 0)",
	NewsIndexSqlKeys2:           "coalesce(sql_keys_2, 0)",
	NewsIndexSqlKeys3:           "coalesce(sql_keys_3, 0)",
	NewsIndexSqlSearch:          "coalesce(sql_search, '')",
	NewsIndexSqlGet:             "coalesce(sql_get, '')",
	NewsIndexSqlUniqueX1:        "coalesce(sql_unique_x1, 0)",
	NewsIndexSqlUniqueX2:        "coalesce(sql_unique_x2, 0)",
	NewsIndexSqlUniqueX1X2:      "coalesce(sql_unique_x1_x2, 0)",
	NewsIndexSqlPrimary:         "coalesce(sql_primary, 0)",
	NewsIndexSqlJsonbIndex:      "sql_jsonb_index",
	NewsIndexTimeDuration:       "coalesce(time_duration, 0)",
	NewsIndexGoTypeIntToStrings: "go_type_int_to_strings",
	NewsIndexPublicField1:       "coalesce(public_field1, 0)",
	NewsIndexPublicField2:       "coalesce(public_field2, 0)",
	NewsIndexPublicField3:       "coalesce(public_field3, 0)",
	NewsIndexPublicFieldMe1:     "coalesce(public_field_me1, 0)",
	NewsIndexPublicFieldMe2:     "coalesce(public_field_me2, 0)",
	NewsIndexPublicFieldMe3:     "coalesce(public_field_me3, 0)",
}

// sql column types
var newsSQLTypes = [...]string{
	NewsIndexInc:                "bigint",
	NewsIndexRole:               "bigint",
	NewsIndexKey:                "uuid",
	NewsIndexT:                  "timestamptz",
	NewsIndexD:                  "bigint",
	NewsIndexInts:               "bigint",
	NewsIndexInts8:              "smallint",
	NewsIndexInts16:             "smallint",
	NewsIndexInts32:             "integer",
	NewsIndexInts64:             "bigint",
	NewsIndexUints:              "numeric(20,0)",
	NewsIndexUints8:             "smallint",
	NewsIndexUints16:            "integer",
	NewsIndexUints32:            "bigint",
	NewsIndexUints64:            "numeric(20,0)",
	NewsIndexFloats32:           "real",
	NewsIndexFloats64:           "double precision",
	NewsIndexBools:              "boolean",
	NewsIndexByte1:              "smallint",
	NewsIndexBytes:              "bytea",
	NewsIndexListInts:           "bigint[]",
	NewsIndexListString:         "text[]",
	NewsIndexListFloat:          "double precision[]",
	NewsIndexMapStringString:    "jsonb",
	NewsIndexMapStringBytes:     "jsonb",
	NewsIndexMapStringBool:      "jsonb",
	NewsIndexMapStringInt:       "jsonb",
	NewsIndexMapStringFloat64:   "jsonb",
	NewsIndexMapStringAny:       "jsonb",
	NewsIndexMapIntString:       "jsonb",
	NewsIndexMapIntInt:          "jsonb",
	NewsIndexMapIntBool:         "jsonb",
	NewsIndexRenameSQL:          "text",
	NewsIndexRenameGO_OK:        "text",
	NewsIndexRenameJS:           "text",
	NewsIndexMASTUPPERGO:        "text",
	NewsIndexIntToSmallInt:      "smallint",
	NewsIndexSqlUniqueU11:       "bigint",
	NewsIndexSqlUniqueU12:       "bigint",
	NewsIndexSqlIndex11:         "bigint",
	NewsIndexSqlIndex12:         "bigint",
	NewsIndexSqlIndex13:         "bigint",
	NewsIndexSqlKeys1:           "bigint",
	NewsIndexSqlKeys2:           "bigint",
	NewsIndexSqlKeys3:           "bigint",
	NewsIndexSqlSearch:          "text",
	NewsIndexSqlGet:             "text",
	NewsIndexSqlUniqueX1:        "bigint",
	NewsIndexSqlUniqueX2:        "bigint",
	NewsIndexSqlUniqueX1X2:      "bigint",
	NewsIndexSqlPrimary:         "double precision",
	NewsIndexSqlJsonbIndex:      "jsonb",
	NewsIndexTimeDuration:       "bigint",
	NewsIndexGoTypeIntToStrings: "text[]",
	NewsIndexPublicField1:       "bigint",
	NewsIndexPublicField2:       "bigint",
	NewsIndexPublicField3:       "bigint",
	NewsIndexPublicFieldMe1:     "bigint",
	NewsIndexPublicFieldMe2:     "bigint",
	NewsIndexPublicFieldMe3:     "bigint",
}

const newsSQLSelectAll = "coalesce(inc, 0), coalesce(role, 0), key, coalesce(t, '0001-01-01 00:00:00+00'), coalesce(d, 0), coalesce(ints, 0), coalesce(ints8, 0), coalesce(ints16, 0), coalesce(ints32, 0), coalesce(ints64, 0), coalesce(uints, 0), coalesce(uints8, 0), coalesce(uints16, 0), coalesce(uints32, 0), coalesce(uints64, 0), coalesce(floats32, 0), coalesce(floats64, 0), coalesce(bools, false), coalesce(byte1, 0), bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, coalesce(renameSQL_OK, ''), coalesce(renameGO, ''), coalesce(renameJS, ''), coalesce(mast_upper_go, ''), coalesce(intToSmallInt, 0), coalesce(sql_unique_u1_1, 0), coalesce(sql_unique_u1_2, 0), coalesce(sql_index1_1, 0), coalesce(sql_index1_2, 0), coalesce(sql_index1_3, 0), coalesce(sql_keys_1, 0), coalesce(sql_keys_2, 0), coalesce(sql_keys_3, 0), coalesce(sql_search, ''), coalesce(sql_get, ''), coalesce(sql_unique_x1, 0), coalesce(sql_unique_x2, 0), coalesce(sql_unique_x1_x2, 0), coalesce(sql_primary, 0), sql_jsonb_index, coalesce(time_duration, 0), go_type_int_to_strings, coalesce(public_field1, 0), coalesce(public_field2, 0), coalesce(public_field3, 0), coalesce(public_field_me1, 0), coalesce(public_field_me2, 0), coalesce(public_field_me3, 0)"

// newsSQLColumn returns sql column name of field
func newsSQLColumn(f NewsIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(newsSQLColumns) || newsSQLColumns[f] == "" {
		return "", fmt.Errorf("news: field %d is not a sql column", int(f))
	}
	return newsSQLColumns[f], nil
}

// newsSQLSelect returns select list of fields, all columns when empty
func newsSQLSelect(fields []NewsIndexTyped) (string, []NewsIndexTyped, error) {
	if len(fields) == 0 {
		return newsSQLSelectAll, newsSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := newsSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(newsSQLSelects[f])
	}
	return b.String(), fields, nil
}

// newsSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func newsSQLSet(v map[NewsIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("news: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := newsSQLColumn(f)
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

// newsSQLAffected returns pgx.ErrNoRows when nothing was changed
func newsSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// newsSQLScanBytes scans bytea to byte array
type newsSQLScanBytes func(b []byte)

func (f newsSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *News) sqlDest(fields []NewsIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case NewsIndexInc:
			dest[i] = &a.Inc
		case NewsIndexRole:
			dest[i] = &a.Role
		case NewsIndexKey:
			dest[i] = &a.Key
		case NewsIndexT:
			dest[i] = &a.T
		case NewsIndexD:
			dest[i] = &a.D
		case NewsIndexInts:
			dest[i] = &a.Ints
		case NewsIndexInts8:
			dest[i] = &a.Ints8
		case NewsIndexInts16:
			dest[i] = &a.Ints16
		case NewsIndexInts32:
			dest[i] = &a.Ints32
		case NewsIndexInts64:
			dest[i] = &a.Ints64
		case NewsIndexUints:
			dest[i] = &a.Uints
		case NewsIndexUints8:
			dest[i] = &a.Uints8
		case NewsIndexUints16:
			dest[i] = &a.Uints16
		case NewsIndexUints32:
			dest[i] = &a.Uints32
		case NewsIndexUints64:
			dest[i] = &a.Uints64
		case NewsIndexFloats32:
			dest[i] = &a.Floats32
		case NewsIndexFloats64:
			dest[i] = &a.Floats64
		case NewsIndexBools:
			dest[i] = &a.Bools
		case NewsIndexByte1:
			dest[i] = &a.Byte1
		case NewsIndexBytes:
			dest[i] = &a.Bytes
		case NewsIndexListInts:
			dest[i] = &a.ListInts
		case NewsIndexListString:
			dest[i] = &a.ListString
		case NewsIndexListFloat:
			dest[i] = &a.ListFloat
		case NewsIndexMapStringString:
			dest[i] = &a.MapStringString
		case NewsIndexMapStringBytes:
			dest[i] = &a.MapStringBytes
		case NewsIndexMapStringBool:
			dest[i] = &a.MapStringBool
		case NewsIndexMapStringInt:
			dest[i] = &a.MapStringInt
		case NewsIndexMapStringFloat64:
			dest[i] = &a.MapStringFloat64
		case NewsIndexMapStringAny:
			dest[i] = &a.MapStringAny
		case NewsIndexMapIntString:
			dest[i] = &a.MapIntString
		case NewsIndexMapIntInt:
			dest[i] = &a.MapIntInt
		case NewsIndexMapIntBool:
			dest[i] = &a.MapIntBool
		case NewsIndexRenameSQL:
			dest[i] = &a.RenameSQL
		case NewsIndexRenameGO_OK:
			dest[i] = &a.RenameGO_OK
		case NewsIndexRenameJS:
			dest[i] = &a.RenameJS
		case NewsIndexMASTUPPERGO:
			dest[i] = &a.MASTUPPERGO
		case NewsIndexIntToSmallInt:
			dest[i] = &a.IntToSmallInt
		case NewsIndexSqlUniqueU11:
			dest[i] = &a.SqlUniqueU11
		case NewsIndexSqlUniqueU12:
			dest[i] = &a.SqlUniqueU12
		case NewsIndexSqlIndex11:
			dest[i] = &a.SqlIndex11
		case NewsIndexSqlIndex12:
			dest[i] = &a.SqlIndex12
		case NewsIndexSqlIndex13:
			dest[i] = &a.SqlIndex13
		case NewsIndexSqlKeys1:
			dest[i] = &a.SqlKeys1
		case NewsIndexSqlKeys2:
			dest[i] = &a.SqlKeys2
		case NewsIndexSqlKeys3:
			dest[i] = &a.SqlKeys3
		case NewsIndexSqlSearch:
			dest[i] = &a.SqlSearch
		case NewsIndexSqlGet:
			dest[i] = &a.SqlGet
		case NewsIndexSqlUniqueX1:
			dest[i] = &a.SqlUniqueX1
		case NewsIndexSqlUniqueX2:
			dest[i] = &a.SqlUniqueX2
		case NewsIndexSqlUniqueX1X2:
			dest[i] = &a.SqlUniqueX1X2
		case NewsIndexSqlPrimary:
			dest[i] = &a.SqlPrimary
		case NewsIndexSqlJsonbIndex:
			dest[i] = &a.SqlJsonbIndex
		case NewsIndexTimeDuration:
			dest[i] = &a.TimeDuration
		case NewsIndexGoTypeIntToStrings:
			dest[i] = &a.GoTypeIntToStrings
		case NewsIndexPublicField1:
			dest[i] = &a.PublicField1
		case NewsIndexPublicField2:
			dest[i] = &a.PublicField2
		case NewsIndexPublicField3:
			dest[i] = &a.PublicField3
		case NewsIndexPublicFieldMe1:
			dest[i] = &a.PublicFieldMe1
		case NewsIndexPublicFieldMe2:
			dest[i] = &a.PublicFieldMe2
		case NewsIndexPublicFieldMe3:
			dest[i] = &a.PublicFieldMe3
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *News) sqlValue(f NewsIndexTyped) any {
	switch f {
	case NewsIndexInc:
		return a.Inc
	case NewsIndexRole:
		return int(a.Role)
	case NewsIndexKey:
		return a.Key
	case NewsIndexT:
		return a.T
	case NewsIndexD:
		return a.D
	case NewsIndexInts:
		return a.Ints
	case NewsIndexInts8:
		return a.Ints8
	case NewsIndexInts16:
		return a.Ints16
	case NewsIndexInts32:
		return a.Ints32
	case NewsIndexInts64:
		return a.Ints64
	case NewsIndexUints:
		return a.Uints
	case NewsIndexUints8:
		return a.Uints8
	case NewsIndexUints16:
		return a.Uints16
	case NewsIndexUints32:
		return a.Uints32
	case NewsIndexUints64:
		return a.Uints64
	case NewsIndexFloats32:
		return a.Floats32
	case NewsIndexFloats64:
		return a.Floats64
	case NewsIndexBools:
		return a.Bools
	case NewsIndexByte1:
		return a.Byte1
	case NewsIndexBytes:
		return a.Bytes
	case NewsIndexListInts:
		return a.ListInts
	case NewsIndexListString:
		return a.ListString
	case NewsIndexListFloat:
		return a.ListFloat
	case NewsIndexMapStringString:
		return a.MapStringString
	case NewsIndexMapStringBytes:
		return a.MapStringBytes
	case NewsIndexMapStringBool:
		return a.MapStringBool
	case NewsIndexMapStringInt:
		return a.MapStringInt
	case NewsIndexMapStringFloat64:
		return a.MapStringFloat64
	case NewsIndexMapStringAny:
		return a.MapStringAny
	case NewsIndexMapIntString:
		return a.MapIntString
	case NewsIndexMapIntInt:
		return a.MapIntInt
	case NewsIndexMapIntBool:
		return a.MapIntBool
	case NewsIndexRenameSQL:
		return a.RenameSQL
	case NewsIndexRenameGO_OK:
		return a.RenameGO_OK
	case NewsIndexRenameJS:
		return a.RenameJS
	case NewsIndexMASTUPPERGO:
		return a.MASTUPPERGO
	case NewsIndexIntToSmallInt:
		return a.IntToSmallInt
	case NewsIndexSqlUniqueU11:
		return a.SqlUniqueU11
	case NewsIndexSqlUniqueU12:
		return a.SqlUniqueU12
	case NewsIndexSqlIndex11:
		return a.SqlIndex11
	case NewsIndexSqlIndex12:
		return a.SqlIndex12
	case NewsIndexSqlIndex13:
		return a.SqlIndex13
	case NewsIndexSqlKeys1:
		return a.SqlKeys1
	case NewsIndexSqlKeys2:
		return a.SqlKeys2
	case NewsIndexSqlKeys3:
		return a.SqlKeys3
	case NewsIndexSqlSearch:
		return a.SqlSearch
	case NewsIndexSqlGet:
		return a.SqlGet
	case NewsIndexSqlUniqueX1:
		return a.SqlUniqueX1
	case NewsIndexSqlUniqueX2:
		return a.SqlUniqueX2
	case NewsIndexSqlUniqueX1X2:
		return a.SqlUniqueX1X2
	case NewsIndexSqlPrimary:
		return a.SqlPrimary
	case NewsIndexSqlJsonbIndex:
		return a.SqlJsonbIndex
	case NewsIndexTimeDuration:
		return a.TimeDuration
	case NewsIndexGoTypeIntToStrings:
		return a.GoTypeIntToStrings
	case NewsIndexPublicField1:
		return a.PublicField1
	case NewsIndexPublicField2:
		return a.PublicField2
	case NewsIndexPublicField3:
		return a.PublicField3
	case NewsIndexPublicFieldMe1:
		return a.PublicFieldMe1
	case NewsIndexPublicFieldMe2:
		return a.PublicFieldMe2
	case NewsIndexPublicFieldMe3:
		return a.PublicFieldMe3
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *News) sqlInsertArgs() []any {
	return []any{
		int(a.Role),
		a.Key,
		a.T,
		a.D,
		a.Ints,
		a.Ints8,
		a.Ints16,
		a.Ints32,
		a.Ints64,
		a.Uints,
		a.Uints8,
		a.Uints16,
		a.Uints32,
		a.Uints64,
		a.Floats32,
		a.Floats64,
		a.Bools,
		a.Byte1,
		a.Bytes,
		a.ListInts,
		a.ListString,
		a.ListFloat,
		a.MapStringString,
		a.MapStringBytes,
		a.MapStringBool,
		a.MapStringInt,
		a.MapStringFloat64,
		a.MapStringAny,
		a.MapIntString,
		a.MapIntInt,
		a.MapIntBool,
		a.RenameSQL,
		a.RenameGO_OK,
		a.RenameJS,
		a.MASTUPPERGO,
		a.IntToSmallInt,
		a.SqlUniqueU11,
		a.SqlUniqueU12,
		a.SqlIndex11,
		a.SqlIndex12,
		a.SqlIndex13,
		a.SqlKeys1,
		a.SqlKeys2,
		a.SqlKeys3,
		a.SqlSearch,
		a.SqlGet,
		a.SqlUniqueX1,
		a.SqlUniqueX2,
		a.SqlUniqueX1X2,
		a.SqlPrimary,
		a.SqlJsonbIndex,
		a.TimeDuration,
		a.GoTypeIntToStrings,
		a.PublicField1,
		a.PublicField2,
		a.PublicField3,
		a.PublicFieldMe1,
		a.PublicFieldMe2,
		a.PublicFieldMe3,
	}
}

// sqlAllArgs returns values of all columns
func (a *News) sqlAllArgs() []any {
	return []any{
		a.Inc,
		int(a.Role),
		a.Key,
		a.T,
		a.D,
		a.Ints,
		a.Ints8,
		a.Ints16,
		a.Ints32,
		a.Ints64,
		a.Uints,
		a.Uints8,
		a.Uints16,
		a.Uints32,
		a.Uints64,
		a.Floats32,
		a.Floats64,
		a.Bools,
		a.Byte1,
		a.Bytes,
		a.ListInts,
		a.ListString,
		a.ListFloat,
		a.MapStringString,
		a.MapStringBytes,
		a.MapStringBool,
		a.MapStringInt,
		a.MapStringFloat64,
		a.MapStringAny,
		a.MapIntString,
		a.MapIntInt,
		a.MapIntBool,
		a.RenameSQL,
		a.RenameGO_OK,
		a.RenameJS,
		a.MASTUPPERGO,
		a.IntToSmallInt,
		a.SqlUniqueU11,
		a.SqlUniqueU12,
		a.SqlIndex11,
		a.SqlIndex12,
		a.SqlIndex13,
		a.SqlKeys1,
		a.SqlKeys2,
		a.SqlKeys3,
		a.SqlSearch,
		a.SqlGet,
		a.SqlUniqueX1,
		a.SqlUniqueX2,
		a.SqlUniqueX1X2,
		a.SqlPrimary,
		a.SqlJsonbIndex,
		a.TimeDuration,
		a.GoTypeIntToStrings,
		a.PublicField1,
		a.PublicField2,
		a.PublicField3,
		a.PublicFieldMe1,
		a.PublicFieldMe2,
		a.PublicFieldMe3,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *NewsSQL) query(fields []NewsIndexTyped, tail string, args ...any) ([]News, error) {
	cols, fields, err := newsSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from news"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v News, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *NewsSQL) queryRow(fields []NewsIndexTyped, tail string, args ...any) (v News, err error) {
	cols, fields, err := newsSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from news"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by inc
func (a *NewsSQL) Get(id int, fields ...NewsIndexTyped) (News, error) {
	return a.queryRow(fields, " where inc = $1", id)
}

// GetMe returns NewsMe by inc
func (a *NewsSQL) GetMe(id int) (res NewsMe, err error) {
	v, err := a.Get(id, NewsMeList()...)
	if err != nil {
		return
	}
	return *v.NewsMe(), nil
}

// GetPublic returns NewsPublic by inc
func (a *NewsSQL) GetPublic(id int) (res NewsPublic, err error) {
	v, err := a.Get(id, NewsPublicList()...)
	if err != nil {
		return
	}
	return *v.NewsPublic(), nil
}

// Exists checks row by inc
func (a *NewsSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from news where inc = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *NewsSQL) GetBy(field NewsIndexTyped, v any, fields ...NewsIndexTyped) (res News, err error) {
	col, err := newsSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetBySqlSearch returns first row by sql_search
func (a *NewsSQL) GetBySqlSearch(v string, fields ...NewsIndexTyped) (News, error) {
	return a.queryRow(fields, " where sql_search = $1 limit 1", v)
}

// GetBySqlGet returns first row by sql_get
func (a *NewsSQL) GetBySqlGet(v string, fields ...NewsIndexTyped) (News, error) {
	return a.queryRow(fields, " where sql_get = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *NewsSQL) GetWhere(fields []NewsIndexTyped, where string, args ...any) (News, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *NewsSQL) Has(field NewsIndexTyped, v any) (has bool, err error) {
	col, err := newsSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from news where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *NewsSQL) All(fields ...NewsIndexTyped) ([]News, error) {
	return a.query(fields, " order by inc")
}

// List returns page of rows
func (a *NewsSQL) List(limit, offset int, fields ...NewsIndexTyped) ([]News, error) {
	return a.query(fields, " order by inc limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *NewsSQL) Select(fields []NewsIndexTyped, where string, args ...any) ([]News, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *NewsSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from news").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *NewsSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from news where "+where, args...).Scan(&count)
	return
}

// FullTextSearch full text search in search, best match first
//
//	db.FullTextSearch("red car -old", 20)
func (a *NewsSQL) FullTextSearch(text string, limit int, fields ...NewsIndexTyped) ([]News, error) {
	return a.query(fields, " where search @@ websearch_to_tsquery('simple', $1) order by ts_rank(search, websearch_to_tsquery('simple', $1)) desc limit $2", text, limit)
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const newsSQLInsert = "insert into news (role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59)"

const newsSQLInsertFull = "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60)"

// Insert adds row and sets generated v.Inc
func (a *NewsSQL) Insert(v *News) error {
	return a.db.QueryRow(context.Background(), newsSQLInsert+" returning inc", v.sqlInsertArgs()...).Scan(&v.Inc)
}

// InsertNoConflict adds row and sets v.Inc, false when row exists
func (a *NewsSQL) InsertNoConflict(v *News) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), newsSQLInsert+" on conflict do nothing returning inc", v.sqlInsertArgs()...).Scan(&v.Inc)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *NewsSQL) InsertFull(v *News) error {
	_, err := a.db.Exec(context.Background(), newsSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (sql_primary)
func (a *NewsSQL) Upsert(v *News) error {
	_, err := a.db.Exec(context.Background(), "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL_OK, renameGO, renameJS, mast_upper_go, intToSmallInt, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60) on conflict (sql_primary) do update set inc = excluded.inc, role = excluded.role, key = excluded.key, t = excluded.t, d = excluded.d, ints = excluded.ints, ints8 = excluded.ints8, ints16 = excluded.ints16, ints32 = excluded.ints32, ints64 = excluded.ints64, uints = excluded.uints, uints8 = excluded.uints8, uints16 = excluded.uints16, uints32 = excluded.uints32, uints64 = excluded.uints64, floats32 = excluded.floats32, floats64 = excluded.floats64, bools = excluded.bools, byte1 = excluded.byte1, bytes = excluded.bytes, list_ints = excluded.list_ints, list_string = excluded.list_string, list_float = excluded.list_float, map_string_string = excluded.map_string_string, map_string_bytes = excluded.map_string_bytes, map_string_bool = excluded.map_string_bool, map_string_int = excluded.map_string_int, map_string_float64 = excluded.map_string_float64, map_string_any = excluded.map_string_any, map_int_string = excluded.map_int_string, map_int_int = excluded.map_int_int, map_int_bool = excluded.map_int_bool, renameSQL_OK = excluded.renameSQL_OK, renameGO = excluded.renameGO, renameJS = excluded.renameJS, mast_upper_go = excluded.mast_upper_go, intToSmallInt = excluded.intToSmallInt, sql_unique_u1_1 = excluded.sql_unique_u1_1, sql_unique_u1_2 = excluded.sql_unique_u1_2, sql_index1_1 = excluded.sql_index1_1, sql_index1_2 = excluded.sql_index1_2, sql_index1_3 = excluded.sql_index1_3, sql_keys_1 = excluded.sql_keys_1, sql_keys_2 = excluded.sql_keys_2, sql_keys_3 = excluded.sql_keys_3, sql_search = excluded.sql_search, sql_get = excluded.sql_get, sql_unique_x1 = excluded.sql_unique_x1, sql_unique_x2 = excluded.sql_unique_x2, sql_unique_x1_x2 = excluded.sql_unique_x1_x2, sql_jsonb_index = excluded.sql_jsonb_index, time_duration = excluded.time_duration, go_type_int_to_strings = excluded.go_type_int_to_strings, public_field1 = excluded.public_field1, public_field2 = excluded.public_field2, public_field3 = excluded.public_field3, public_field_me1 = excluded.public_field_me1, public_field_me2 = excluded.public_field_me2, public_field_me3 = excluded.public_field_me3", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *NewsSQL) InsertMany(list []News) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"news"}, newsSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip and sets generated Inc
func (a *NewsSQL) InsertBatch(list []*News) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(newsSQLInsert+" returning inc", v.sqlInsertArgs()...)
	}
	res := a.db.SendBatch(context.Background(), b)
	for _, v := range list {
		if err := res.QueryRow().Scan(&v.Inc); err != nil {
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
func (a *NewsSQL) Update(id int, field NewsIndexTyped, v any) error {
	col, err := newsSQLColumn(field)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = $1 where inc = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *NewsSQL) Updates(id int, v map[NewsIndexTyped]any) error {
	set, args, err := newsSQLSet(v, 1)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+set+" where inc = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.Inc: listed fields or all columns
func (a *NewsSQL) Save(v *News, fields ...NewsIndexTyped) error {
	if len(fields) == 0 {
		fields = newsSQLFields
	}
	values := make(map[NewsIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != NewsIndexInc {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := newsSQLSet(values, 1)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+set+" where inc = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(NewsIndexInc))...))
}

// Inc adds v to number field: views = views + 1
func (a *NewsSQL) Inc(id int, field NewsIndexTyped, v any) error {
	col, err := newsSQLColumn(field)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = coalesce("+col+", 0) + $1 where inc = $2", v, id))
}

// Delete removes row by inc
func (a *NewsSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from news where inc = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *NewsSQL) UpdateWhere(field NewsIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := newsSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update news set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *NewsSQL) UpdatesWhere(v map[NewsIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := newsSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update news set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *NewsSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from news where "+where, args...)
	return tag.RowsAffected(), err
}

// AppendListInts adds values to array list_ints
func (a *NewsSQL) AppendListInts(id int, v ...int) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_ints = coalesce(list_ints, '{}') || $1::bigint[] where inc = $2", v, id))
}

// RemoveListInts removes all entries of v from array list_ints
func (a *NewsSQL) RemoveListInts(id int, v int) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_ints = array_remove(list_ints, $1::bigint) where inc = $2", v, id))
}

// AppendListString adds values to array list_string
func (a *NewsSQL) AppendListString(id int, v ...string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_string = coalesce(list_string, '{}') || $1::text[] where inc = $2", v, id))
}

// RemoveListString removes all entries of v from array list_string
func (a *NewsSQL) RemoveListString(id int, v string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_string = array_remove(list_string, $1::text) where inc = $2", v, id))
}

// AppendListFloat adds values to array list_float
func (a *NewsSQL) AppendListFloat(id int, v ...float64) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_float = coalesce(list_float, '{}') || $1::double precision[] where inc = $2", v, id))
}

// RemoveListFloat removes all entries of v from array list_float
func (a *NewsSQL) RemoveListFloat(id int, v float64) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set list_float = array_remove(list_float, $1::double precision) where inc = $2", v, id))
}

// UpdateMapStringString sets key of jsonb map_string_string
func (a *NewsSQL) UpdateMapStringString(id int, k string, v string) error {
	return a.jsonSet(id, "map_string_string", fmt.Sprint(k), v)
}

// MergeMapStringString adds keys to jsonb map_string_string: existing keys are replaced
func (a *NewsSQL) MergeMapStringString(id int, v map[string]string) error {
	return a.jsonMerge(id, "map_string_string", "{}", v)
}

// DeleteMapStringString removes key of jsonb map_string_string
func (a *NewsSQL) DeleteMapStringString(id int, k string) error {
	return a.jsonDelete(id, "map_string_string", fmt.Sprint(k))
}

// RenameMapStringString renames key of jsonb map_string_string, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringString(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_string", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapStringBytes sets key of jsonb map_string_bytes
func (a *NewsSQL) UpdateMapStringBytes(id int, k string, v []byte) error {
	return a.jsonSet(id, "map_string_bytes", fmt.Sprint(k), v)
}

// MergeMapStringBytes adds keys to jsonb map_string_bytes: existing keys are replaced
func (a *NewsSQL) MergeMapStringBytes(id int, v map[string][]byte) error {
	return a.jsonMerge(id, "map_string_bytes", "{}", v)
}

// DeleteMapStringBytes removes key of jsonb map_string_bytes
func (a *NewsSQL) DeleteMapStringBytes(id int, k string) error {
	return a.jsonDelete(id, "map_string_bytes", fmt.Sprint(k))
}

// RenameMapStringBytes renames key of jsonb map_string_bytes, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringBytes(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_bytes", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapStringBool sets key of jsonb map_string_bool
func (a *NewsSQL) UpdateMapStringBool(id int, k string, v bool) error {
	return a.jsonSet(id, "map_string_bool", fmt.Sprint(k), v)
}

// MergeMapStringBool adds keys to jsonb map_string_bool: existing keys are replaced
func (a *NewsSQL) MergeMapStringBool(id int, v map[string]bool) error {
	return a.jsonMerge(id, "map_string_bool", "{}", v)
}

// DeleteMapStringBool removes key of jsonb map_string_bool
func (a *NewsSQL) DeleteMapStringBool(id int, k string) error {
	return a.jsonDelete(id, "map_string_bool", fmt.Sprint(k))
}

// RenameMapStringBool renames key of jsonb map_string_bool, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringBool(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_bool", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapStringInt sets key of jsonb map_string_int
func (a *NewsSQL) UpdateMapStringInt(id int, k string, v int) error {
	return a.jsonSet(id, "map_string_int", fmt.Sprint(k), v)
}

// MergeMapStringInt adds keys to jsonb map_string_int: existing keys are replaced
func (a *NewsSQL) MergeMapStringInt(id int, v map[string]int) error {
	return a.jsonMerge(id, "map_string_int", "{}", v)
}

// DeleteMapStringInt removes key of jsonb map_string_int
func (a *NewsSQL) DeleteMapStringInt(id int, k string) error {
	return a.jsonDelete(id, "map_string_int", fmt.Sprint(k))
}

// RenameMapStringInt renames key of jsonb map_string_int, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringInt(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_int", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapStringFloat64 sets key of jsonb map_string_float64
func (a *NewsSQL) UpdateMapStringFloat64(id int, k string, v float64) error {
	return a.jsonSet(id, "map_string_float64", fmt.Sprint(k), v)
}

// MergeMapStringFloat64 adds keys to jsonb map_string_float64: existing keys are replaced
func (a *NewsSQL) MergeMapStringFloat64(id int, v map[string]float64) error {
	return a.jsonMerge(id, "map_string_float64", "{}", v)
}

// DeleteMapStringFloat64 removes key of jsonb map_string_float64
func (a *NewsSQL) DeleteMapStringFloat64(id int, k string) error {
	return a.jsonDelete(id, "map_string_float64", fmt.Sprint(k))
}

// RenameMapStringFloat64 renames key of jsonb map_string_float64, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringFloat64(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_float64", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapStringAny sets key of jsonb map_string_any
func (a *NewsSQL) UpdateMapStringAny(id int, k string, v any) error {
	return a.jsonSet(id, "map_string_any", fmt.Sprint(k), v)
}

// MergeMapStringAny adds keys to jsonb map_string_any: existing keys are replaced
func (a *NewsSQL) MergeMapStringAny(id int, v map[string]any) error {
	return a.jsonMerge(id, "map_string_any", "{}", v)
}

// DeleteMapStringAny removes key of jsonb map_string_any
func (a *NewsSQL) DeleteMapStringAny(id int, k string) error {
	return a.jsonDelete(id, "map_string_any", fmt.Sprint(k))
}

// RenameMapStringAny renames key of jsonb map_string_any, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapStringAny(id int, k, newK string) error {
	return a.jsonRename(id, "map_string_any", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapIntString sets key of jsonb map_int_string
func (a *NewsSQL) UpdateMapIntString(id int, k int, v string) error {
	return a.jsonSet(id, "map_int_string", fmt.Sprint(k), v)
}

// MergeMapIntString adds keys to jsonb map_int_string: existing keys are replaced
func (a *NewsSQL) MergeMapIntString(id int, v map[int]string) error {
	return a.jsonMerge(id, "map_int_string", "{}", v)
}

// DeleteMapIntString removes key of jsonb map_int_string
func (a *NewsSQL) DeleteMapIntString(id int, k int) error {
	return a.jsonDelete(id, "map_int_string", fmt.Sprint(k))
}

// RenameMapIntString renames key of jsonb map_int_string, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapIntString(id int, k, newK int) error {
	return a.jsonRename(id, "map_int_string", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapIntInt sets key of jsonb map_int_int
func (a *NewsSQL) UpdateMapIntInt(id int, k int, v int) error {
	return a.jsonSet(id, "map_int_int", fmt.Sprint(k), v)
}

// MergeMapIntInt adds keys to jsonb map_int_int: existing keys are replaced
func (a *NewsSQL) MergeMapIntInt(id int, v map[int]int) error {
	return a.jsonMerge(id, "map_int_int", "{}", v)
}

// DeleteMapIntInt removes key of jsonb map_int_int
func (a *NewsSQL) DeleteMapIntInt(id int, k int) error {
	return a.jsonDelete(id, "map_int_int", fmt.Sprint(k))
}

// RenameMapIntInt renames key of jsonb map_int_int, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapIntInt(id int, k, newK int) error {
	return a.jsonRename(id, "map_int_int", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateMapIntBool sets key of jsonb map_int_bool
func (a *NewsSQL) UpdateMapIntBool(id int, k int, v bool) error {
	return a.jsonSet(id, "map_int_bool", fmt.Sprint(k), v)
}

// MergeMapIntBool adds keys to jsonb map_int_bool: existing keys are replaced
func (a *NewsSQL) MergeMapIntBool(id int, v map[int]bool) error {
	return a.jsonMerge(id, "map_int_bool", "{}", v)
}

// DeleteMapIntBool removes key of jsonb map_int_bool
func (a *NewsSQL) DeleteMapIntBool(id int, k int) error {
	return a.jsonDelete(id, "map_int_bool", fmt.Sprint(k))
}

// RenameMapIntBool renames key of jsonb map_int_bool, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameMapIntBool(id int, k, newK int) error {
	return a.jsonRename(id, "map_int_bool", fmt.Sprint(k), fmt.Sprint(newK))
}

// UpdateSqlJsonbIndex sets key of jsonb sql_jsonb_index
func (a *NewsSQL) UpdateSqlJsonbIndex(id int, k string, v any) error {
	return a.jsonSet(id, "sql_jsonb_index", fmt.Sprint(k), v)
}

// MergeSqlJsonbIndex adds keys to jsonb sql_jsonb_index: existing keys are replaced
func (a *NewsSQL) MergeSqlJsonbIndex(id int, v map[string]any) error {
	return a.jsonMerge(id, "sql_jsonb_index", "{}", v)
}

// DeleteSqlJsonbIndex removes key of jsonb sql_jsonb_index
func (a *NewsSQL) DeleteSqlJsonbIndex(id int, k string) error {
	return a.jsonDelete(id, "sql_jsonb_index", fmt.Sprint(k))
}

// RenameSqlJsonbIndex renames key of jsonb sql_jsonb_index, pgx.ErrNoRows when key not found
func (a *NewsSQL) RenameSqlJsonbIndex(id int, k, newK string) error {
	return a.jsonRename(id, "sql_jsonb_index", fmt.Sprint(k), fmt.Sprint(newK))
}

// AppendGoTypeIntToStrings adds values to array go_type_int_to_strings
func (a *NewsSQL) AppendGoTypeIntToStrings(id int, v ...string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set go_type_int_to_strings = coalesce(go_type_int_to_strings, '{}') || $1::text[] where inc = $2", v, id))
}

// RemoveGoTypeIntToStrings removes all entries of v from array go_type_int_to_strings
func (a *NewsSQL) RemoveGoTypeIntToStrings(id int, v string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set go_type_int_to_strings = array_remove(go_type_int_to_strings, $1::text) where inc = $2", v, id))
}

// jsonSet sets key of jsonb column
func (a *NewsSQL) jsonSet(id int, col, k string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = jsonb_set(coalesce("+col+", '{}'::jsonb), array[$1::text], $2::jsonb, true) where inc = $3", k, b, id))
}

// jsonMerge concats json to jsonb column: object keys or array values
func (a *NewsSQL) jsonMerge(id int, col, empty string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = coalesce("+col+", '"+empty+"'::jsonb) || $1::jsonb where inc = $2", b, id))
}

// jsonDelete removes key of jsonb column
func (a *NewsSQL) jsonDelete(id int, col, k string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = "+col+" - $1::text where inc = $2", k, id))
}

// jsonRename renames key of jsonb column
func (a *NewsSQL) jsonRename(id int, col, k, newK string) error {
	return newsSQLAffected(a.db.Exec(context.Background(), "update news set "+col+" = ("+col+" - $1::text) || jsonb_build_object($2::text, "+col+" -> $1::text) where inc = $3 and "+col+" ? $1::text", k, newK, id))
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// NewsQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type NewsQuery struct {
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

// NewNewsQuery creates query with empty maps
func NewNewsQuery() *NewsQuery {
	return &NewsQuery{
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
func (q *NewsQuery) column(k string) (string, NewsIndexTyped, error) {
	if !NewsValidKey(k) {
		return "", 0, fmt.Errorf("news: unknown field %q", k)
	}
	f := NewsKeyIndex(k)
	col, err := newsSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *NewsQuery) Render() (tail string, fields []NewsIndexTyped, args []any, err error) {

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
func (a *NewsSQL) Search(q *NewsQuery) ([]News, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const newsSQLCreateTable = `create table if not exists news (
	inc                        bigint generated by default as identity,
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
	search                     tsvector generated always as (to_tsvector('simple', coalesce(sql_search::text, ''))) stored,
	unique (sql_unique_u1_1, sql_unique_u1_2),
	unique (sql_unique_x1, sql_unique_x1_x2),
	unique (sql_unique_x2, sql_unique_x1_x2)
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var newsSQLIndexes = []string{
	"create index if not exists gin_news_sql_jsonb_index on news using gin(sql_jsonb_index)",
	"create index if not exists idx_news_index1 on news (sql_index1_1, sql_index1_2, sql_index1_3)",
	"create index if not exists gin_news_search on news using gin(search)",
}

// CreateTable creates table if not exists
func (a *NewsSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), newsSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *NewsSQL) CreateIndexes() error {
	for _, q := range newsSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ResetSequence sets next inc to max(inc) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *NewsSQL) ResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('news', 'inc'), coalesce((select max(inc) from news), 0) + $1, false)", plus)
	return err
}

// SetSequence sets next inc
func (a *NewsSQL) SetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('news', 'inc'), $1, false)", v)
	return err
}

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
	NewsIndexListInts
	NewsIndexListString
	NewsIndexListFloat
	NewsIndexMapStringString
	NewsIndexMapStringBytes
	NewsIndexMapStringBool
	NewsIndexMapStringInt
	NewsIndexMapStringFloat64
	NewsIndexMapStringAny
	NewsIndexMapIntString
	NewsIndexMapIntInt
	NewsIndexMapIntBool
	NewsIndexRenameSQL
	NewsIndexRenameGO_OK
	NewsIndexRenameJS
	NewsIndexMASTUPPERGO
	NewsIndexIntToSmallInt
	NewsIndexSkip
	NewsIndexSqlUniqueU11
	NewsIndexSqlUniqueU12
	NewsIndexSqlIndex11
	NewsIndexSqlIndex12
	NewsIndexSqlIndex13
	NewsIndexSqlKeys1
	NewsIndexSqlKeys2
	NewsIndexSqlKeys3
	NewsIndexSqlSearch
	NewsIndexSqlGet
	NewsIndexSqlUniqueX1
	NewsIndexSqlUniqueX2
	NewsIndexSqlUniqueX1X2
	NewsIndexSqlPrimary
	NewsIndexSqlJsonbIndex
	NewsIndexTimeDuration
	NewsIndexGoTypeIntToStrings
	NewsIndexPublicField1
	NewsIndexPublicField2
	NewsIndexPublicField3
	NewsIndexPublicFieldMe1
	NewsIndexPublicFieldMe2
	NewsIndexPublicFieldMe3
)

// string index
const (
	NewsFieldInc                = "inc"                    // int sql{inc}  #readonly
	NewsFieldRole               = "role"                   // role.Role
	NewsFieldKey                = "key"                    // uuid.UUID
	NewsFieldT                  = "t"                      // time.Time
	NewsFieldD                  = "d"                      // time.Duration
	NewsFieldInts               = "ints"                   // int
	NewsFieldInts8              = "ints8"                  // int8
	NewsFieldInts16             = "ints16"                 // int16
	NewsFieldInts32             = "ints32"                 // int32
	NewsFieldInts64             = "ints64"                 // int64
	NewsFieldUints              = "uints"                  // uint
	NewsFieldUints8             = "uints8"                 // uint8
	NewsFieldUints16            = "uints16"                // uint16
	NewsFieldUints32            = "uints32"                // uint32
	NewsFieldUints64            = "uints64"                // uint64
	NewsFieldFloats32           = "floats32"               // float32
	NewsFieldFloats64           = "floats64"               // float64
	NewsFieldBools              = "bools"                  // bool
	NewsFieldByte1              = "byte1"                  // byte
	NewsFieldBytes              = "bytes"                  // []byte
	NewsFieldListInts           = "list_ints"              // []int
	NewsFieldListString         = "list_string"            // []string
	NewsFieldListFloat          = "list_float"             // []float64
	NewsFieldMapStringString    = "map_string_string"      // map[string]string
	NewsFieldMapStringBytes     = "map_string_bytes"       // map[string][]byte
	NewsFieldMapStringBool      = "map_string_bool"        // map[string]bool
	NewsFieldMapStringInt       = "map_string_int"         // map[string]int
	NewsFieldMapStringFloat64   = "map_string_float64"     // map[string]float64
	NewsFieldMapStringAny       = "map_string_any"         // map[string]any
	NewsFieldMapIntString       = "map_int_string"         // map[int]string
	NewsFieldMapIntInt          = "map_int_int"            // map[int]int
	NewsFieldMapIntBool         = "map_int_bool"           // map[int]bool
	NewsFieldRenameSQL          = "renameSQL"              // string sql{name="renameSQL_OK"}
	NewsFieldRenameGO_OK        = "renameGO"               // string go{name="RenameGO_OK"}
	NewsFieldRenameJS           = "renameJS_OK"            // string js{name="renameJS_OK"}
	NewsFieldMASTUPPERGO        = "mast_upper_go"          // string go{up}
	NewsFieldIntToSmallInt      = "intToSmallInt"          // int sql{type="smallint"} rename
	NewsFieldSkip               = "skip"                   // string sql{skip} swift{skip} js{skip}
	NewsFieldSqlUniqueU11       = "sql_unique_u1_1"        // int sql{unique="u1"}
	NewsFieldSqlUniqueU12       = "sql_unique_u1_2"        // int sql{unique="u1"}
	NewsFieldSqlIndex11         = "sql_index1_1"           // int sql{idx="index1"}
	NewsFieldSqlIndex12         = "sql_index1_2"           // int sql{idx="index1"}
	NewsFieldSqlIndex13         = "sql_index1_3"           // int sql{idx="index1"}
	NewsFieldSqlKeys1           = "sql_keys_1"             // int sql{keys="keys1"}
	NewsFieldSqlKeys2           = "sql_keys_2"             // int sql{keys="keys1"}
	NewsFieldSqlKeys3           = "sql_keys_3"             // int sql{keys="keys1"}
	NewsFieldSqlSearch          = "sql_search"             // string sql{search="search" get}
	NewsFieldSqlGet             = "sql_get"                // string sql{get}
	NewsFieldSqlUniqueX1        = "sql_unique_x1"          // int sql{unique="x1"}
	NewsFieldSqlUniqueX2        = "sql_unique_x2"          // int sql{unique="x2"}
	NewsFieldSqlUniqueX1X2      = "sql_unique_x1_x2"       // int sql{unique="x1", unique="x2"}
	NewsFieldSqlPrimary         = "sql_primary"            // float64 sql{primarykey}
	NewsFieldSqlJsonbIndex      = "sql_jsonb_index"        // map[string]any sql{index}
	NewsFieldTimeDuration       = "time_duration"          // time.Duration
	NewsFieldGoTypeIntToStrings = "go_type_int_to_strings" // int go{type="[]string"}
	NewsFieldPublicField1       = "public_field1"          // int @public
	NewsFieldPublicField2       = "public_field2"          // int @public
	NewsFieldPublicField3       = "public_field3"          // int @public
	NewsFieldPublicFieldMe1     = "public_field_me1"       // int @me
	NewsFieldPublicFieldMe2     = "public_field_me2"       // int @me
	NewsFieldPublicFieldMe3     = "public_field_me3"       // int @me
)

// index func
func NewsIndexes() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexListInts, NewsIndexListString, NewsIndexListFloat, NewsIndexMapStringString, NewsIndexMapStringBytes, NewsIndexMapStringBool, NewsIndexMapStringInt, NewsIndexMapStringFloat64, NewsIndexMapStringAny, NewsIndexMapIntString, NewsIndexMapIntInt, NewsIndexMapIntBool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMASTUPPERGO, NewsIndexIntToSmallInt, NewsIndexSkip, NewsIndexSqlUniqueU11, NewsIndexSqlUniqueU12, NewsIndexSqlIndex11, NewsIndexSqlIndex12, NewsIndexSqlIndex13, NewsIndexSqlKeys1, NewsIndexSqlKeys2, NewsIndexSqlKeys3, NewsIndexSqlSearch, NewsIndexSqlGet, NewsIndexSqlUniqueX1, NewsIndexSqlUniqueX2, NewsIndexSqlUniqueX1X2, NewsIndexSqlPrimary, NewsIndexSqlJsonbIndex, NewsIndexTimeDuration, NewsIndexGoTypeIntToStrings, NewsIndexPublicField1, NewsIndexPublicField2, NewsIndexPublicField3, NewsIndexPublicFieldMe1, NewsIndexPublicFieldMe2, NewsIndexPublicFieldMe3}

}

func NewsMeList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexPublicFieldMe1, NewsIndexPublicFieldMe2, NewsIndexPublicFieldMe3}
}

func NewsPublicList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexPublicField1, NewsIndexPublicField2, NewsIndexPublicField3}
}

func NewsReadonlyList() []NewsIndexTyped {
	return []NewsIndexTyped{NewsIndexInc}
}

func (a NewsIndexTyped) Me() bool {
	switch a {
	case NewsIndexPublicFieldMe1, NewsIndexPublicFieldMe2, NewsIndexPublicFieldMe3:
		return true
	default:
		return false
	}
}
func (a NewsIndexTyped) Public() bool {
	switch a {
	case NewsIndexPublicField1, NewsIndexPublicField2, NewsIndexPublicField3:
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
	case NewsIndexListInts:
		return "list_ints"
	case NewsIndexListString:
		return "list_string"
	case NewsIndexListFloat:
		return "list_float"
	case NewsIndexMapStringString:
		return "map_string_string"
	case NewsIndexMapStringBytes:
		return "map_string_bytes"
	case NewsIndexMapStringBool:
		return "map_string_bool"
	case NewsIndexMapStringInt:
		return "map_string_int"
	case NewsIndexMapStringFloat64:
		return "map_string_float64"
	case NewsIndexMapStringAny:
		return "map_string_any"
	case NewsIndexMapIntString:
		return "map_int_string"
	case NewsIndexMapIntInt:
		return "map_int_int"
	case NewsIndexMapIntBool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMASTUPPERGO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSqlUniqueU11:
		return "sql_unique_u1_1"
	case NewsIndexSqlUniqueU12:
		return "sql_unique_u1_2"
	case NewsIndexSqlIndex11:
		return "sql_index1_1"
	case NewsIndexSqlIndex12:
		return "sql_index1_2"
	case NewsIndexSqlIndex13:
		return "sql_index1_3"
	case NewsIndexSqlKeys1:
		return "sql_keys_1"
	case NewsIndexSqlKeys2:
		return "sql_keys_2"
	case NewsIndexSqlKeys3:
		return "sql_keys_3"
	case NewsIndexSqlSearch:
		return "sql_search"
	case NewsIndexSqlGet:
		return "sql_get"
	case NewsIndexSqlUniqueX1:
		return "sql_unique_x1"
	case NewsIndexSqlUniqueX2:
		return "sql_unique_x2"
	case NewsIndexSqlUniqueX1X2:
		return "sql_unique_x1_x2"
	case NewsIndexSqlPrimary:
		return "sql_primary"
	case NewsIndexSqlJsonbIndex:
		return "sql_jsonb_index"
	case NewsIndexTimeDuration:
		return "time_duration"
	case NewsIndexGoTypeIntToStrings:
		return "go_type_int_to_strings"
	case NewsIndexPublicField1:
		return "public_field1"
	case NewsIndexPublicField2:
		return "public_field2"
	case NewsIndexPublicField3:
		return "public_field3"
	case NewsIndexPublicFieldMe1:
		return "public_field_me1"
	case NewsIndexPublicFieldMe2:
		return "public_field_me2"
	case NewsIndexPublicFieldMe3:
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
	case NewsIndexListInts:
		return "list_ints"
	case NewsIndexListString:
		return "list_string"
	case NewsIndexListFloat:
		return "list_float"
	case NewsIndexMapStringString:
		return "map_string_string"
	case NewsIndexMapStringBytes:
		return "map_string_bytes"
	case NewsIndexMapStringBool:
		return "map_string_bool"
	case NewsIndexMapStringInt:
		return "map_string_int"
	case NewsIndexMapStringFloat64:
		return "map_string_float64"
	case NewsIndexMapStringAny:
		return "map_string_any"
	case NewsIndexMapIntString:
		return "map_int_string"
	case NewsIndexMapIntInt:
		return "map_int_int"
	case NewsIndexMapIntBool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL_OK"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMASTUPPERGO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSqlUniqueU11:
		return "sql_unique_u1_1"
	case NewsIndexSqlUniqueU12:
		return "sql_unique_u1_2"
	case NewsIndexSqlIndex11:
		return "sql_index1_1"
	case NewsIndexSqlIndex12:
		return "sql_index1_2"
	case NewsIndexSqlIndex13:
		return "sql_index1_3"
	case NewsIndexSqlKeys1:
		return "sql_keys_1"
	case NewsIndexSqlKeys2:
		return "sql_keys_2"
	case NewsIndexSqlKeys3:
		return "sql_keys_3"
	case NewsIndexSqlSearch:
		return "sql_search"
	case NewsIndexSqlGet:
		return "sql_get"
	case NewsIndexSqlUniqueX1:
		return "sql_unique_x1"
	case NewsIndexSqlUniqueX2:
		return "sql_unique_x2"
	case NewsIndexSqlUniqueX1X2:
		return "sql_unique_x1_x2"
	case NewsIndexSqlPrimary:
		return "sql_primary"
	case NewsIndexSqlJsonbIndex:
		return "sql_jsonb_index"
	case NewsIndexTimeDuration:
		return "time_duration"
	case NewsIndexGoTypeIntToStrings:
		return "go_type_int_to_strings"
	case NewsIndexPublicField1:
		return "public_field1"
	case NewsIndexPublicField2:
		return "public_field2"
	case NewsIndexPublicField3:
		return "public_field3"
	case NewsIndexPublicFieldMe1:
		return "public_field_me1"
	case NewsIndexPublicFieldMe2:
		return "public_field_me2"
	case NewsIndexPublicFieldMe3:
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
	case NewsIndexListInts:
		return "list_ints"
	case NewsIndexListString:
		return "list_string"
	case NewsIndexListFloat:
		return "list_float"
	case NewsIndexMapStringString:
		return "map_string_string"
	case NewsIndexMapStringBytes:
		return "map_string_bytes"
	case NewsIndexMapStringBool:
		return "map_string_bool"
	case NewsIndexMapStringInt:
		return "map_string_int"
	case NewsIndexMapStringFloat64:
		return "map_string_float64"
	case NewsIndexMapStringAny:
		return "map_string_any"
	case NewsIndexMapIntString:
		return "map_int_string"
	case NewsIndexMapIntInt:
		return "map_int_int"
	case NewsIndexMapIntBool:
		return "map_int_bool"
	case NewsIndexRenameSQL:
		return "renameSQL"
	case NewsIndexRenameGO_OK:
		return "renameGO"
	case NewsIndexRenameJS:
		return "renameJS"
	case NewsIndexMASTUPPERGO:
		return "mast_upper_go"
	case NewsIndexIntToSmallInt:
		return "intToSmallInt"
	case NewsIndexSkip:
		return "skip"
	case NewsIndexSqlUniqueU11:
		return "sql_unique_u1_1"
	case NewsIndexSqlUniqueU12:
		return "sql_unique_u1_2"
	case NewsIndexSqlIndex11:
		return "sql_index1_1"
	case NewsIndexSqlIndex12:
		return "sql_index1_2"
	case NewsIndexSqlIndex13:
		return "sql_index1_3"
	case NewsIndexSqlKeys1:
		return "sql_keys_1"
	case NewsIndexSqlKeys2:
		return "sql_keys_2"
	case NewsIndexSqlKeys3:
		return "sql_keys_3"
	case NewsIndexSqlSearch:
		return "sql_search"
	case NewsIndexSqlGet:
		return "sql_get"
	case NewsIndexSqlUniqueX1:
		return "sql_unique_x1"
	case NewsIndexSqlUniqueX2:
		return "sql_unique_x2"
	case NewsIndexSqlUniqueX1X2:
		return "sql_unique_x1_x2"
	case NewsIndexSqlPrimary:
		return "sql_primary"
	case NewsIndexSqlJsonbIndex:
		return "sql_jsonb_index"
	case NewsIndexTimeDuration:
		return "time_duration"
	case NewsIndexGoTypeIntToStrings:
		return "go_type_int_to_strings"
	case NewsIndexPublicField1:
		return "public_field1"
	case NewsIndexPublicField2:
		return "public_field2"
	case NewsIndexPublicField3:
		return "public_field3"
	case NewsIndexPublicFieldMe1:
		return "public_field_me1"
	case NewsIndexPublicFieldMe2:
		return "public_field_me2"
	case NewsIndexPublicFieldMe3:
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
		return "role.Role"
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
	case NewsIndexListInts:
		return "[]int"
	case NewsIndexListString:
		return "[]string"
	case NewsIndexListFloat:
		return "[]float64"
	case NewsIndexMapStringString:
		return "map[string]string"
	case NewsIndexMapStringBytes:
		return "map[string][]byte"
	case NewsIndexMapStringBool:
		return "map[string]bool"
	case NewsIndexMapStringInt:
		return "map[string]int"
	case NewsIndexMapStringFloat64:
		return "map[string]float64"
	case NewsIndexMapStringAny:
		return "map[string]any"
	case NewsIndexMapIntString:
		return "map[int]string"
	case NewsIndexMapIntInt:
		return "map[int]int"
	case NewsIndexMapIntBool:
		return "map[int]bool"
	case NewsIndexRenameSQL:
		return "string"
	case NewsIndexRenameGO_OK:
		return "string"
	case NewsIndexRenameJS:
		return "string"
	case NewsIndexMASTUPPERGO:
		return "string"
	case NewsIndexIntToSmallInt:
		return "int"
	case NewsIndexSkip:
		return "string"
	case NewsIndexSqlUniqueU11:
		return "int"
	case NewsIndexSqlUniqueU12:
		return "int"
	case NewsIndexSqlIndex11:
		return "int"
	case NewsIndexSqlIndex12:
		return "int"
	case NewsIndexSqlIndex13:
		return "int"
	case NewsIndexSqlKeys1:
		return "int"
	case NewsIndexSqlKeys2:
		return "int"
	case NewsIndexSqlKeys3:
		return "int"
	case NewsIndexSqlSearch:
		return "string"
	case NewsIndexSqlGet:
		return "string"
	case NewsIndexSqlUniqueX1:
		return "int"
	case NewsIndexSqlUniqueX2:
		return "int"
	case NewsIndexSqlUniqueX1X2:
		return "int"
	case NewsIndexSqlPrimary:
		return "float64"
	case NewsIndexSqlJsonbIndex:
		return "map[string]any"
	case NewsIndexTimeDuration:
		return "time.Duration"
	case NewsIndexGoTypeIntToStrings:
		return "int"
	case NewsIndexPublicField1:
		return "int"
	case NewsIndexPublicField2:
		return "int"
	case NewsIndexPublicField3:
		return "int"
	case NewsIndexPublicFieldMe1:
		return "int"
	case NewsIndexPublicFieldMe2:
		return "int"
	case NewsIndexPublicFieldMe3:
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
	case NewsIndexListInts:
		return "ListInts"
	case NewsIndexListString:
		return "ListString"
	case NewsIndexListFloat:
		return "ListFloat"
	case NewsIndexMapStringString:
		return "MapStringString"
	case NewsIndexMapStringBytes:
		return "MapStringBytes"
	case NewsIndexMapStringBool:
		return "MapStringBool"
	case NewsIndexMapStringInt:
		return "MapStringInt"
	case NewsIndexMapStringFloat64:
		return "MapStringFloat64"
	case NewsIndexMapStringAny:
		return "MapStringAny"
	case NewsIndexMapIntString:
		return "MapIntString"
	case NewsIndexMapIntInt:
		return "MapIntInt"
	case NewsIndexMapIntBool:
		return "MapIntBool"
	case NewsIndexRenameSQL:
		return "RenameSQL"
	case NewsIndexRenameGO_OK:
		return "RenameGO_OK"
	case NewsIndexRenameJS:
		return "RenameJS"
	case NewsIndexMASTUPPERGO:
		return "MASTUPPERGO"
	case NewsIndexIntToSmallInt:
		return "IntToSmallInt"
	case NewsIndexSkip:
		return "Skip"
	case NewsIndexSqlUniqueU11:
		return "SqlUniqueU11"
	case NewsIndexSqlUniqueU12:
		return "SqlUniqueU12"
	case NewsIndexSqlIndex11:
		return "SqlIndex11"
	case NewsIndexSqlIndex12:
		return "SqlIndex12"
	case NewsIndexSqlIndex13:
		return "SqlIndex13"
	case NewsIndexSqlKeys1:
		return "SqlKeys1"
	case NewsIndexSqlKeys2:
		return "SqlKeys2"
	case NewsIndexSqlKeys3:
		return "SqlKeys3"
	case NewsIndexSqlSearch:
		return "SqlSearch"
	case NewsIndexSqlGet:
		return "SqlGet"
	case NewsIndexSqlUniqueX1:
		return "SqlUniqueX1"
	case NewsIndexSqlUniqueX2:
		return "SqlUniqueX2"
	case NewsIndexSqlUniqueX1X2:
		return "SqlUniqueX1X2"
	case NewsIndexSqlPrimary:
		return "SqlPrimary"
	case NewsIndexSqlJsonbIndex:
		return "SqlJsonbIndex"
	case NewsIndexTimeDuration:
		return "TimeDuration"
	case NewsIndexGoTypeIntToStrings:
		return "GoTypeIntToStrings"
	case NewsIndexPublicField1:
		return "PublicField1"
	case NewsIndexPublicField2:
		return "PublicField2"
	case NewsIndexPublicField3:
		return "PublicField3"
	case NewsIndexPublicFieldMe1:
		return "PublicFieldMe1"
	case NewsIndexPublicFieldMe2:
		return "PublicFieldMe2"
	case NewsIndexPublicFieldMe3:
		return "PublicFieldMe3"
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
		return NewsIndexListInts
	case "list_string":
		return NewsIndexListString
	case "list_float":
		return NewsIndexListFloat
	case "map_string_string":
		return NewsIndexMapStringString
	case "map_string_bytes":
		return NewsIndexMapStringBytes
	case "map_string_bool":
		return NewsIndexMapStringBool
	case "map_string_int":
		return NewsIndexMapStringInt
	case "map_string_float64":
		return NewsIndexMapStringFloat64
	case "map_string_any":
		return NewsIndexMapStringAny
	case "map_int_string":
		return NewsIndexMapIntString
	case "map_int_int":
		return NewsIndexMapIntInt
	case "map_int_bool":
		return NewsIndexMapIntBool
	case "renameSQL":
		return NewsIndexRenameSQL
	case "renameGO":
		return NewsIndexRenameGO_OK
	case "renameJS":
		return NewsIndexRenameJS
	case "mast_upper_go":
		return NewsIndexMASTUPPERGO
	case "intToSmallInt":
		return NewsIndexIntToSmallInt
	case "skip":
		return NewsIndexSkip
	case "sql_unique_u1_1":
		return NewsIndexSqlUniqueU11
	case "sql_unique_u1_2":
		return NewsIndexSqlUniqueU12
	case "sql_index1_1":
		return NewsIndexSqlIndex11
	case "sql_index1_2":
		return NewsIndexSqlIndex12
	case "sql_index1_3":
		return NewsIndexSqlIndex13
	case "sql_keys_1":
		return NewsIndexSqlKeys1
	case "sql_keys_2":
		return NewsIndexSqlKeys2
	case "sql_keys_3":
		return NewsIndexSqlKeys3
	case "sql_search":
		return NewsIndexSqlSearch
	case "sql_get":
		return NewsIndexSqlGet
	case "sql_unique_x1":
		return NewsIndexSqlUniqueX1
	case "sql_unique_x2":
		return NewsIndexSqlUniqueX2
	case "sql_unique_x1_x2":
		return NewsIndexSqlUniqueX1X2
	case "sql_primary":
		return NewsIndexSqlPrimary
	case "sql_jsonb_index":
		return NewsIndexSqlJsonbIndex
	case "time_duration":
		return NewsIndexTimeDuration
	case "go_type_int_to_strings":
		return NewsIndexGoTypeIntToStrings
	case "public_field1":
		return NewsIndexPublicField1
	case "public_field2":
		return NewsIndexPublicField2
	case "public_field3":
		return NewsIndexPublicField3
	case "public_field_me1":
		return NewsIndexPublicFieldMe1
	case "public_field_me2":
		return NewsIndexPublicFieldMe2
	case "public_field_me3":
		return NewsIndexPublicFieldMe3
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

type NewsMe struct {
	PublicFieldMe1 int `json:"public_field_me1,omitempty" msg:"public_field_me1,omitempty"` // @me
	PublicFieldMe2 int `json:"public_field_me2,omitempty" msg:"public_field_me2,omitempty"` // @me
	PublicFieldMe3 int `json:"public_field_me3,omitempty" msg:"public_field_me3,omitempty"` // @me
}

func (a *News) NewsMe() (res *NewsMe) {
	return &NewsMe{
		PublicFieldMe1: a.PublicFieldMe1,
		PublicFieldMe2: a.PublicFieldMe2,
		PublicFieldMe3: a.PublicFieldMe3,
	}
}

func (a *NewsMe) News() (res *News) {
	return &News{
		PublicFieldMe1: a.PublicFieldMe1,
		PublicFieldMe2: a.PublicFieldMe2,
		PublicFieldMe3: a.PublicFieldMe3,
	}
}

func (a *NewsMe) Pack() (res []byte) {
	res, _ = gojson.Marshal(a)
	return
}

type NewsPublic struct {
	PublicField1 int `json:"public_field1,omitempty" msg:"public_field1,omitempty"` // @public
	PublicField2 int `json:"public_field2,omitempty" msg:"public_field2,omitempty"` // @public
	PublicField3 int `json:"public_field3,omitempty" msg:"public_field3,omitempty"` // @public
}

func (a *News) NewsPublic() (res *NewsPublic) {
	return &NewsPublic{
		PublicField1: a.PublicField1,
		PublicField2: a.PublicField2,
		PublicField3: a.PublicField3,
	}
}

func (a *NewsPublic) News() (res *News) {
	return &News{
		PublicField1: a.PublicField1,
		PublicField2: a.PublicField2,
		PublicField3: a.PublicField3,
	}
}

func (a *NewsPublic) Pack() (res []byte) {
	res, _ = gojson.Marshal(a)
	return
}

// Tuple create an array from struct
func (a *News) Tuple() (r []any) {
	return []any{a.Inc, a.Role, a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.ListInts, a.ListString, a.ListFloat, a.MapStringString, a.MapStringBytes, a.MapStringBool, a.MapStringInt, a.MapStringFloat64, a.MapStringAny, a.MapIntString, a.MapIntInt, a.MapIntBool, a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MASTUPPERGO, a.IntToSmallInt, a.Skip, a.SqlUniqueU11, a.SqlUniqueU12, a.SqlIndex11, a.SqlIndex12, a.SqlIndex13, a.SqlKeys1, a.SqlKeys2, a.SqlKeys3, a.SqlSearch, a.SqlGet, a.SqlUniqueX1, a.SqlUniqueX2, a.SqlUniqueX1X2, a.SqlPrimary, a.SqlJsonbIndex, a.TimeDuration, a.GoTypeIntToStrings, a.PublicField1, a.PublicField2, a.PublicField3, a.PublicFieldMe1, a.PublicFieldMe2, a.PublicFieldMe3}
}

// get struct value with function
func (a *News) Get(k string) (v any) {
	switch k {
	case "inc":
		return a.Inc //int
	case "role":
		return a.Role //role.Role
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
		return a.ListInts //[]int
	case "list_string":
		return a.ListString //[]string
	case "list_float":
		return a.ListFloat //[]float64
	case "map_string_string":
		return a.MapStringString //map[string]string
	case "map_string_bytes":
		return a.MapStringBytes //map[string][]byte
	case "map_string_bool":
		return a.MapStringBool //map[string]bool
	case "map_string_int":
		return a.MapStringInt //map[string]int
	case "map_string_float64":
		return a.MapStringFloat64 //map[string]float64
	case "map_string_any":
		return a.MapStringAny //map[string]any
	case "map_int_string":
		return a.MapIntString //map[int]string
	case "map_int_int":
		return a.MapIntInt //map[int]int
	case "map_int_bool":
		return a.MapIntBool //map[int]bool
	case "renameSQL":
		return a.RenameSQL //string
	case "renameGO":
		return a.RenameGO_OK //string
	case "renameJS":
		return a.RenameJS //string
	case "mast_upper_go":
		return a.MASTUPPERGO //string
	case "intToSmallInt":
		return a.IntToSmallInt //int
	case "skip":
		return a.Skip //string
	case "sql_unique_u1_1":
		return a.SqlUniqueU11 //int
	case "sql_unique_u1_2":
		return a.SqlUniqueU12 //int
	case "sql_index1_1":
		return a.SqlIndex11 //int
	case "sql_index1_2":
		return a.SqlIndex12 //int
	case "sql_index1_3":
		return a.SqlIndex13 //int
	case "sql_keys_1":
		return a.SqlKeys1 //int
	case "sql_keys_2":
		return a.SqlKeys2 //int
	case "sql_keys_3":
		return a.SqlKeys3 //int
	case "sql_search":
		return a.SqlSearch //string
	case "sql_get":
		return a.SqlGet //string
	case "sql_unique_x1":
		return a.SqlUniqueX1 //int
	case "sql_unique_x2":
		return a.SqlUniqueX2 //int
	case "sql_unique_x1_x2":
		return a.SqlUniqueX1X2 //int
	case "sql_primary":
		return a.SqlPrimary //float64
	case "sql_jsonb_index":
		return a.SqlJsonbIndex //map[string]any
	case "time_duration":
		return a.TimeDuration //time.Duration
	case "go_type_int_to_strings":
		return a.GoTypeIntToStrings //int
	case "public_field1":
		return a.PublicField1 //int
	case "public_field2":
		return a.PublicField2 //int
	case "public_field3":
		return a.PublicField3 //int
	case "public_field_me1":
		return a.PublicFieldMe1 //int
	case "public_field_me2":
		return a.PublicFieldMe2 //int
	case "public_field_me3":
		return a.PublicFieldMe3 //int
	}
	return
}

// get any struct value as string
func (a *News) String(k string) (v string) {
	switch k {
	case "inc":
		return fmt.Sprint(a.Inc) //int
	case "role":
		return fmt.Sprint(a.Role) //role.Role
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
		return fmt.Sprint(a.ListInts) //[]int
	case "list_string":
		return fmt.Sprint(a.ListString) //[]string
	case "list_float":
		return fmt.Sprint(a.ListFloat) //[]float64
	case "map_string_string":
		return fmt.Sprint(a.MapStringString) //map[string]string
	case "map_string_bytes":
		return fmt.Sprint(a.MapStringBytes) //map[string][]byte
	case "map_string_bool":
		return fmt.Sprint(a.MapStringBool) //map[string]bool
	case "map_string_int":
		return fmt.Sprint(a.MapStringInt) //map[string]int
	case "map_string_float64":
		return fmt.Sprint(a.MapStringFloat64) //map[string]float64
	case "map_string_any":
		return fmt.Sprint(a.MapStringAny) //map[string]any
	case "map_int_string":
		return fmt.Sprint(a.MapIntString) //map[int]string
	case "map_int_int":
		return fmt.Sprint(a.MapIntInt) //map[int]int
	case "map_int_bool":
		return fmt.Sprint(a.MapIntBool) //map[int]bool
	case "renameSQL":
		return fmt.Sprint(a.RenameSQL) //string
	case "renameGO":
		return fmt.Sprint(a.RenameGO_OK) //string
	case "renameJS":
		return fmt.Sprint(a.RenameJS) //string
	case "mast_upper_go":
		return fmt.Sprint(a.MASTUPPERGO) //string
	case "intToSmallInt":
		return fmt.Sprint(a.IntToSmallInt) //int
	case "skip":
		return fmt.Sprint(a.Skip) //string
	case "sql_unique_u1_1":
		return fmt.Sprint(a.SqlUniqueU11) //int
	case "sql_unique_u1_2":
		return fmt.Sprint(a.SqlUniqueU12) //int
	case "sql_index1_1":
		return fmt.Sprint(a.SqlIndex11) //int
	case "sql_index1_2":
		return fmt.Sprint(a.SqlIndex12) //int
	case "sql_index1_3":
		return fmt.Sprint(a.SqlIndex13) //int
	case "sql_keys_1":
		return fmt.Sprint(a.SqlKeys1) //int
	case "sql_keys_2":
		return fmt.Sprint(a.SqlKeys2) //int
	case "sql_keys_3":
		return fmt.Sprint(a.SqlKeys3) //int
	case "sql_search":
		return fmt.Sprint(a.SqlSearch) //string
	case "sql_get":
		return fmt.Sprint(a.SqlGet) //string
	case "sql_unique_x1":
		return fmt.Sprint(a.SqlUniqueX1) //int
	case "sql_unique_x2":
		return fmt.Sprint(a.SqlUniqueX2) //int
	case "sql_unique_x1_x2":
		return fmt.Sprint(a.SqlUniqueX1X2) //int
	case "sql_primary":
		return fmt.Sprint(a.SqlPrimary) //float64
	case "sql_jsonb_index":
		return fmt.Sprint(a.SqlJsonbIndex) //map[string]any
	case "time_duration":
		return fmt.Sprint(a.TimeDuration) //time.Duration
	case "go_type_int_to_strings":
		return fmt.Sprint(a.GoTypeIntToStrings) //int
	case "public_field1":
		return fmt.Sprint(a.PublicField1) //int
	case "public_field2":
		return fmt.Sprint(a.PublicField2) //int
	case "public_field3":
		return fmt.Sprint(a.PublicField3) //int
	case "public_field_me1":
		return fmt.Sprint(a.PublicFieldMe1) //int
	case "public_field_me2":
		return fmt.Sprint(a.PublicFieldMe2) //int
	case "public_field_me3":
		return fmt.Sprint(a.PublicFieldMe3) //int
	}
	return
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
		"list_ints":              a.ListInts,
		"list_string":            a.ListString,
		"list_float":             a.ListFloat,
		"map_string_string":      a.MapStringString,
		"map_string_bytes":       a.MapStringBytes,
		"map_string_bool":        a.MapStringBool,
		"map_string_int":         a.MapStringInt,
		"map_string_float64":     a.MapStringFloat64,
		"map_string_any":         a.MapStringAny,
		"map_int_string":         a.MapIntString,
		"map_int_int":            a.MapIntInt,
		"map_int_bool":           a.MapIntBool,
		"renameSQL":              a.RenameSQL,
		"renameGO":               a.RenameGO_OK,
		"renameJS_OK":            a.RenameJS,
		"mast_upper_go":          a.MASTUPPERGO,
		"intToSmallInt":          a.IntToSmallInt,
		"skip":                   a.Skip,
		"sql_unique_u1_1":        a.SqlUniqueU11,
		"sql_unique_u1_2":        a.SqlUniqueU12,
		"sql_index1_1":           a.SqlIndex11,
		"sql_index1_2":           a.SqlIndex12,
		"sql_index1_3":           a.SqlIndex13,
		"sql_keys_1":             a.SqlKeys1,
		"sql_keys_2":             a.SqlKeys2,
		"sql_keys_3":             a.SqlKeys3,
		"sql_search":             a.SqlSearch,
		"sql_get":                a.SqlGet,
		"sql_unique_x1":          a.SqlUniqueX1,
		"sql_unique_x2":          a.SqlUniqueX2,
		"sql_unique_x1_x2":       a.SqlUniqueX1X2,
		"sql_primary":            a.SqlPrimary,
		"sql_jsonb_index":        a.SqlJsonbIndex,
		"time_duration":          a.TimeDuration,
		"go_type_int_to_strings": a.GoTypeIntToStrings,
		"public_field1":          a.PublicField1,
		"public_field2":          a.PublicField2,
		"public_field3":          a.PublicField3,
		"public_field_me1":       a.PublicFieldMe1,
		"public_field_me2":       a.PublicFieldMe2,
		"public_field_me3":       a.PublicFieldMe3,
	}
}

// struct to map
func (a *News) Iterate(f func(k NewsIndexTyped, v any)) {
	for _, x := range NewsIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *News) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseNews(v []byte) (a News, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// gotiny marshal
func (a *News) MarshalGotiny() []byte {
	return gotiny.Marshal(&a.Inc, &a.Role, &a.Key, &a.T, &a.D, &a.Ints, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Floats32, &a.Floats64, &a.Bools, &a.Byte1, &a.Bytes, &a.ListInts, &a.ListString, &a.ListFloat, &a.MapStringString, &a.MapStringBytes, &a.MapStringBool, &a.MapStringInt, &a.MapStringFloat64, &a.MapStringAny, &a.MapIntString, &a.MapIntInt, &a.MapIntBool, &a.RenameSQL, &a.RenameGO_OK, &a.RenameJS, &a.MASTUPPERGO, &a.IntToSmallInt, &a.Skip, &a.SqlUniqueU11, &a.SqlUniqueU12, &a.SqlIndex11, &a.SqlIndex12, &a.SqlIndex13, &a.SqlKeys1, &a.SqlKeys2, &a.SqlKeys3, &a.SqlSearch, &a.SqlGet, &a.SqlUniqueX1, &a.SqlUniqueX2, &a.SqlUniqueX1X2, &a.SqlPrimary, &a.SqlJsonbIndex, &a.TimeDuration, &a.GoTypeIntToStrings, &a.PublicField1, &a.PublicField2, &a.PublicField3, &a.PublicFieldMe1, &a.PublicFieldMe2, &a.PublicFieldMe3)
}

// parse gotiny
func UnmarshalNewsGotiny(v []byte) (a News) {
	gotiny.Unmarshal(v, &a.Inc, &a.Role, &a.Key, &a.T, &a.D, &a.Ints, &a.Ints8, &a.Ints16, &a.Ints32, &a.Ints64, &a.Uints, &a.Uints8, &a.Uints16, &a.Uints32, &a.Uints64, &a.Floats32, &a.Floats64, &a.Bools, &a.Byte1, &a.Bytes, &a.ListInts, &a.ListString, &a.ListFloat, &a.MapStringString, &a.MapStringBytes, &a.MapStringBool, &a.MapStringInt, &a.MapStringFloat64, &a.MapStringAny, &a.MapIntString, &a.MapIntInt, &a.MapIntBool, &a.RenameSQL, &a.RenameGO_OK, &a.RenameJS, &a.MASTUPPERGO, &a.IntToSmallInt, &a.Skip, &a.SqlUniqueU11, &a.SqlUniqueU12, &a.SqlIndex11, &a.SqlIndex12, &a.SqlIndex13, &a.SqlKeys1, &a.SqlKeys2, &a.SqlKeys3, &a.SqlSearch, &a.SqlGet, &a.SqlUniqueX1, &a.SqlUniqueX2, &a.SqlUniqueX1X2, &a.SqlPrimary, &a.SqlJsonbIndex, &a.TimeDuration, &a.GoTypeIntToStrings, &a.PublicField1, &a.PublicField2, &a.PublicField3, &a.PublicFieldMe1, &a.PublicFieldMe2, &a.PublicFieldMe3)
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

// Tuple create an array from struct for clickhouse
func (a *News) clickhouseTuple() (r []any) {
	return []any{a.Inc, int(a.Role), a.Key, a.T, a.D, a.Ints, a.Ints8, a.Ints16, a.Ints32, a.Ints64, a.Uints, a.Uints8, a.Uints16, a.Uints32, a.Uints64, a.Floats32, a.Floats64, a.Bools, a.Byte1, a.Bytes, a.ListInts, a.ListString, a.ListFloat, a.MapStringString, a.MapStringBytes, a.MapStringBool, func() map[string]int64 {
		r := make(map[string]int64, len(a.MapStringInt))
		for k, v := range a.MapStringInt {
			r[string(k)] = int64(v)
		}
		return r
	}(), a.MapStringFloat64, a.MapStringAny, func() map[int64]string {
		r := make(map[int64]string, len(a.MapIntString))
		for k, v := range a.MapIntString {
			r[int64(k)] = string(v)
		}
		return r
	}(), func() map[int64]int64 {
		r := make(map[int64]int64, len(a.MapIntInt))
		for k, v := range a.MapIntInt {
			r[int64(k)] = int64(v)
		}
		return r
	}(), func() map[int64]bool {
		r := make(map[int64]bool, len(a.MapIntBool))
		for k, v := range a.MapIntBool {
			r[int64(k)] = bool(v)
		}
		return r
	}(), a.RenameSQL, a.RenameGO_OK, a.RenameJS, a.MASTUPPERGO, a.IntToSmallInt, a.Skip, a.SqlUniqueU11, a.SqlUniqueU12, a.SqlIndex11, a.SqlIndex12, a.SqlIndex13, a.SqlKeys1, a.SqlKeys2, a.SqlKeys3, a.SqlSearch, a.SqlGet, a.SqlUniqueX1, a.SqlUniqueX2, a.SqlUniqueX1X2, a.SqlPrimary, a.SqlJsonbIndex, a.TimeDuration, a.GoTypeIntToStrings, a.PublicField1, a.PublicField2, a.PublicField3, a.PublicFieldMe1, a.PublicFieldMe2, a.PublicFieldMe3}
}

// NewsCQL clickhouse queries of table news
//
// Rules:
//   - safe for concurrent use
//   - not found row is sql.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "country = $1 and views > $2"
//   - rows are only inserted, selected and deleted
type NewsCQL struct {
	conn driver.Conn
	mu   sync.Mutex
	list []News // buffer of Add
}

// NewNewsCQL creates queries on connection and table if not exists.
// Rows of Add are written every 10 minutes and by Push.
// Error of table is not returned: call CreateTable to see it
func NewNewsCQL(conn driver.Conn) *NewsCQL {
	a := &NewsCQL{conn: conn}
	_ = a.CreateTable()
	go a.flush()
	return a
}

// Conn returns connection, for own queries
func (a *NewsCQL) Conn() driver.Conn {
	return a.conn
}

// TableName returns table name
func (a *NewsCQL) TableName() string {
	return "news"
}

// Fields returns all columns
func (a *NewsCQL) Fields() []NewsIndexTyped {
	return slices.Clone(newsCHFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all columns
var newsCHFields = []NewsIndexTyped{NewsIndexInc, NewsIndexRole, NewsIndexKey, NewsIndexT, NewsIndexD, NewsIndexInts, NewsIndexInts8, NewsIndexInts16, NewsIndexInts32, NewsIndexInts64, NewsIndexUints, NewsIndexUints8, NewsIndexUints16, NewsIndexUints32, NewsIndexUints64, NewsIndexFloats32, NewsIndexFloats64, NewsIndexBools, NewsIndexByte1, NewsIndexBytes, NewsIndexListInts, NewsIndexListString, NewsIndexListFloat, NewsIndexMapStringString, NewsIndexMapStringBytes, NewsIndexMapStringBool, NewsIndexMapStringInt, NewsIndexMapStringFloat64, NewsIndexMapStringAny, NewsIndexMapIntString, NewsIndexMapIntInt, NewsIndexMapIntBool, NewsIndexRenameSQL, NewsIndexRenameGO_OK, NewsIndexRenameJS, NewsIndexMASTUPPERGO, NewsIndexIntToSmallInt, NewsIndexSkip, NewsIndexSqlUniqueU11, NewsIndexSqlUniqueU12, NewsIndexSqlIndex11, NewsIndexSqlIndex12, NewsIndexSqlIndex13, NewsIndexSqlKeys1, NewsIndexSqlKeys2, NewsIndexSqlKeys3, NewsIndexSqlSearch, NewsIndexSqlGet, NewsIndexSqlUniqueX1, NewsIndexSqlUniqueX2, NewsIndexSqlUniqueX1X2, NewsIndexSqlPrimary, NewsIndexSqlJsonbIndex, NewsIndexTimeDuration, NewsIndexGoTypeIntToStrings, NewsIndexPublicField1, NewsIndexPublicField2, NewsIndexPublicField3, NewsIndexPublicFieldMe1, NewsIndexPublicFieldMe2, NewsIndexPublicFieldMe3}

// column names
var newsCHColumns = [...]string{
	NewsIndexInc:                "inc",
	NewsIndexRole:               "role",
	NewsIndexKey:                "key",
	NewsIndexT:                  "t",
	NewsIndexD:                  "d",
	NewsIndexInts:               "ints",
	NewsIndexInts8:              "ints8",
	NewsIndexInts16:             "ints16",
	NewsIndexInts32:             "ints32",
	NewsIndexInts64:             "ints64",
	NewsIndexUints:              "uints",
	NewsIndexUints8:             "uints8",
	NewsIndexUints16:            "uints16",
	NewsIndexUints32:            "uints32",
	NewsIndexUints64:            "uints64",
	NewsIndexFloats32:           "floats32",
	NewsIndexFloats64:           "floats64",
	NewsIndexBools:              "bools",
	NewsIndexByte1:              "byte1",
	NewsIndexBytes:              "bytes",
	NewsIndexListInts:           "list_ints",
	NewsIndexListString:         "list_string",
	NewsIndexListFloat:          "list_float",
	NewsIndexMapStringString:    "map_string_string",
	NewsIndexMapStringBytes:     "map_string_bytes",
	NewsIndexMapStringBool:      "map_string_bool",
	NewsIndexMapStringInt:       "map_string_int",
	NewsIndexMapStringFloat64:   "map_string_float64",
	NewsIndexMapStringAny:       "map_string_any",
	NewsIndexMapIntString:       "map_int_string",
	NewsIndexMapIntInt:          "map_int_int",
	NewsIndexMapIntBool:         "map_int_bool",
	NewsIndexRenameSQL:          "renameSQL",
	NewsIndexRenameGO_OK:        "renameGO",
	NewsIndexRenameJS:           "renameJS",
	NewsIndexMASTUPPERGO:        "mast_upper_go",
	NewsIndexIntToSmallInt:      "intToSmallInt",
	NewsIndexSkip:               "skip",
	NewsIndexSqlUniqueU11:       "sql_unique_u1_1",
	NewsIndexSqlUniqueU12:       "sql_unique_u1_2",
	NewsIndexSqlIndex11:         "sql_index1_1",
	NewsIndexSqlIndex12:         "sql_index1_2",
	NewsIndexSqlIndex13:         "sql_index1_3",
	NewsIndexSqlKeys1:           "sql_keys_1",
	NewsIndexSqlKeys2:           "sql_keys_2",
	NewsIndexSqlKeys3:           "sql_keys_3",
	NewsIndexSqlSearch:          "sql_search",
	NewsIndexSqlGet:             "sql_get",
	NewsIndexSqlUniqueX1:        "sql_unique_x1",
	NewsIndexSqlUniqueX2:        "sql_unique_x2",
	NewsIndexSqlUniqueX1X2:      "sql_unique_x1_x2",
	NewsIndexSqlPrimary:         "sql_primary",
	NewsIndexSqlJsonbIndex:      "sql_jsonb_index",
	NewsIndexTimeDuration:       "time_duration",
	NewsIndexGoTypeIntToStrings: "go_type_int_to_strings",
	NewsIndexPublicField1:       "public_field1",
	NewsIndexPublicField2:       "public_field2",
	NewsIndexPublicField3:       "public_field3",
	NewsIndexPublicFieldMe1:     "public_field_me1",
	NewsIndexPublicFieldMe2:     "public_field_me2",
	NewsIndexPublicFieldMe3:     "public_field_me3",
}

const newsCHSelectAll = "inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL, renameGO, renameJS, mast_upper_go, intToSmallInt, skip, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3"

// mutations wait until the data is changed
const newsCHSync = " settings mutations_sync = 1"

// newsCHColumn returns column name of field
func newsCHColumn(f NewsIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(newsCHColumns) || newsCHColumns[f] == "" {
		return "", fmt.Errorf("news: field %d is not a clickhouse column", int(f))
	}
	return newsCHColumns[f], nil
}

// newsCHSelect returns select list of fields, all columns when empty
func newsCHSelect(fields []NewsIndexTyped) (string, []NewsIndexTyped, error) {
	if len(fields) == 0 {
		return newsCHSelectAll, newsCHFields, nil
	}
	list := make([]string, 0, len(fields))
	for _, f := range fields {
		col, err := newsCHColumn(f)
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
func (a *NewsCQL) Add(v ...News) {
	a.mu.Lock()
	a.list = append(a.list, v...)
	a.mu.Unlock()
}

// Len returns count of rows in buffer
func (a *NewsCQL) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.list)
}

// Push writes rows of buffer. Rows stay in buffer when the write fails
func (a *NewsCQL) Push() error {
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
func (a *NewsCQL) Insert(v ...News) error {
	if len(v) == 0 {
		return nil
	}
	return a.send(v)
}

// send writes rows by one batch
func (a *NewsCQL) send(list []News) error {
	batch, err := a.conn.PrepareBatch(context.Background(), "insert into news (inc, role, key, t, d, ints, ints8, ints16, ints32, ints64, uints, uints8, uints16, uints32, uints64, floats32, floats64, bools, byte1, bytes, list_ints, list_string, list_float, map_string_string, map_string_bytes, map_string_bool, map_string_int, map_string_float64, map_string_any, map_int_string, map_int_int, map_int_bool, renameSQL, renameGO, renameJS, mast_upper_go, intToSmallInt, skip, sql_unique_u1_1, sql_unique_u1_2, sql_index1_1, sql_index1_2, sql_index1_3, sql_keys_1, sql_keys_2, sql_keys_3, sql_search, sql_get, sql_unique_x1, sql_unique_x2, sql_unique_x1_x2, sql_primary, sql_jsonb_index, time_duration, go_type_int_to_strings, public_field1, public_field2, public_field3, public_field_me1, public_field_me2, public_field_me3)")
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
func (a *NewsCQL) flush() {
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
func (a *NewsCQL) query(fields []NewsIndexTyped, tail string, args ...any) (res []News, err error) {
	cols, fields, err := newsCHSelect(fields)
	if err != nil {
		return
	}
	rows, err := a.conn.Query(context.Background(), "select "+cols+" from news"+tail, args...)
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
		var v News
		for i, f := range fields {
			v.clickhouseSet(f, scan[i])
		}
		res = append(res, v)
	}
	return res, rows.Err()
}

// queryRow selects one row
func (a *NewsCQL) queryRow(fields []NewsIndexTyped, tail string, args ...any) (v News, err error) {
	list, err := a.query(fields, tail, args...)
	if err != nil {
		return
	}
	if len(list) == 0 {
		return v, sql.ErrNoRows
	}
	return list[0], nil
}

// Get returns row by inc
func (a *NewsCQL) Get(id int, fields ...NewsIndexTyped) (News, error) {
	return a.queryRow(fields, " where inc = $1 limit 1", id)
}

// Exists checks row by inc
func (a *NewsCQL) Exists(id int) (bool, error) {
	n, err := a.CountWhere("inc = $1", id)
	return n > 0, err
}

// GetBy returns first row with field = v
func (a *NewsCQL) GetBy(field NewsIndexTyped, v any, fields ...NewsIndexTyped) (res News, err error) {
	col, err := newsCHColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "country = $1 order by created desc", "DE")
func (a *NewsCQL) GetWhere(fields []NewsIndexTyped, where string, args ...any) (News, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *NewsCQL) Has(field NewsIndexTyped, v any) (bool, error) {
	col, err := newsCHColumn(field)
	if err != nil {
		return false, err
	}
	n, err := a.CountWhere(col+" = $1", v)
	return n > 0, err
}

// All returns all rows
func (a *NewsCQL) All(fields ...NewsIndexTyped) ([]News, error) {
	return a.query(fields, " order by inc")
}

// List returns page of rows
func (a *NewsCQL) List(limit, offset int, fields ...NewsIndexTyped) ([]News, error) {
	return a.query(fields, " order by inc limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *NewsCQL) Select(fields []NewsIndexTyped, where string, args ...any) ([]News, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *NewsCQL) Count() (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from news").Scan(&n)
	return int(n), err
}

// CountWhere returns count of rows by condition
func (a *NewsCQL) CountWhere(where string, args ...any) (int, error) {
	var n uint64
	err := a.conn.QueryRow(context.Background(), "select count() from news where "+where, args...).Scan(&n)
	return int(n), err
}

// Stat returns count of rows by values of field: {"DE": 425, "US": 134}
func (a *NewsCQL) Stat(field NewsIndexTyped) (map[string]int, error) {
	col, err := newsCHColumn(field)
	if err != nil {
		return nil, err
	}
	rows, err := a.conn.Query(context.Background(), "select toString("+col+"), count() from news group by "+col)
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

// Delete removes row by inc
func (a *NewsCQL) Delete(id int) error {
	return a.conn.Exec(context.Background(), "alter table news delete where inc = $1"+newsCHSync, id)
}

// DeleteWhere removes rows by condition
func (a *NewsCQL) DeleteWhere(where string, args ...any) error {
	return a.conn.Exec(context.Background(), "alter table news delete where "+where+newsCHSync, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const newsCHCreateTable = `create table if not exists news (
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

// CreateTable creates table if not exists
func (a *NewsCQL) CreateTable() error {
	return a.conn.Exec(context.Background(), newsCHCreateTable)
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

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
	case NewsIndexListInts:
		newsCHAssign(reflect.ValueOf(&a.ListInts).Elem(), reflect.ValueOf(v))
	case NewsIndexListString:
		newsCHAssign(reflect.ValueOf(&a.ListString).Elem(), reflect.ValueOf(v))
	case NewsIndexListFloat:
		newsCHAssign(reflect.ValueOf(&a.ListFloat).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringString:
		newsCHAssign(reflect.ValueOf(&a.MapStringString).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringBytes:
		newsCHAssign(reflect.ValueOf(&a.MapStringBytes).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringBool:
		newsCHAssign(reflect.ValueOf(&a.MapStringBool).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringInt:
		newsCHAssign(reflect.ValueOf(&a.MapStringInt).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringFloat64:
		newsCHAssign(reflect.ValueOf(&a.MapStringFloat64).Elem(), reflect.ValueOf(v))
	case NewsIndexMapStringAny:
		newsCHAssign(reflect.ValueOf(&a.MapStringAny).Elem(), reflect.ValueOf(v))
	case NewsIndexMapIntString:
		newsCHAssign(reflect.ValueOf(&a.MapIntString).Elem(), reflect.ValueOf(v))
	case NewsIndexMapIntInt:
		newsCHAssign(reflect.ValueOf(&a.MapIntInt).Elem(), reflect.ValueOf(v))
	case NewsIndexMapIntBool:
		newsCHAssign(reflect.ValueOf(&a.MapIntBool).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameSQL:
		newsCHAssign(reflect.ValueOf(&a.RenameSQL).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameGO_OK:
		newsCHAssign(reflect.ValueOf(&a.RenameGO_OK).Elem(), reflect.ValueOf(v))
	case NewsIndexRenameJS:
		newsCHAssign(reflect.ValueOf(&a.RenameJS).Elem(), reflect.ValueOf(v))
	case NewsIndexMASTUPPERGO:
		newsCHAssign(reflect.ValueOf(&a.MASTUPPERGO).Elem(), reflect.ValueOf(v))
	case NewsIndexIntToSmallInt:
		newsCHAssign(reflect.ValueOf(&a.IntToSmallInt).Elem(), reflect.ValueOf(v))
	case NewsIndexSkip:
		newsCHAssign(reflect.ValueOf(&a.Skip).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlUniqueU11:
		newsCHAssign(reflect.ValueOf(&a.SqlUniqueU11).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlUniqueU12:
		newsCHAssign(reflect.ValueOf(&a.SqlUniqueU12).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlIndex11:
		newsCHAssign(reflect.ValueOf(&a.SqlIndex11).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlIndex12:
		newsCHAssign(reflect.ValueOf(&a.SqlIndex12).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlIndex13:
		newsCHAssign(reflect.ValueOf(&a.SqlIndex13).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlKeys1:
		newsCHAssign(reflect.ValueOf(&a.SqlKeys1).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlKeys2:
		newsCHAssign(reflect.ValueOf(&a.SqlKeys2).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlKeys3:
		newsCHAssign(reflect.ValueOf(&a.SqlKeys3).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlSearch:
		newsCHAssign(reflect.ValueOf(&a.SqlSearch).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlGet:
		newsCHAssign(reflect.ValueOf(&a.SqlGet).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlUniqueX1:
		newsCHAssign(reflect.ValueOf(&a.SqlUniqueX1).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlUniqueX2:
		newsCHAssign(reflect.ValueOf(&a.SqlUniqueX2).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlUniqueX1X2:
		newsCHAssign(reflect.ValueOf(&a.SqlUniqueX1X2).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlPrimary:
		newsCHAssign(reflect.ValueOf(&a.SqlPrimary).Elem(), reflect.ValueOf(v))
	case NewsIndexSqlJsonbIndex:
		newsCHAssign(reflect.ValueOf(&a.SqlJsonbIndex).Elem(), reflect.ValueOf(v))
	case NewsIndexTimeDuration:
		newsCHAssign(reflect.ValueOf(&a.TimeDuration).Elem(), reflect.ValueOf(v))
	case NewsIndexGoTypeIntToStrings:
		newsCHAssign(reflect.ValueOf(&a.GoTypeIntToStrings).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicField1:
		newsCHAssign(reflect.ValueOf(&a.PublicField1).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicField2:
		newsCHAssign(reflect.ValueOf(&a.PublicField2).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicField3:
		newsCHAssign(reflect.ValueOf(&a.PublicField3).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicFieldMe1:
		newsCHAssign(reflect.ValueOf(&a.PublicFieldMe1).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicFieldMe2:
		newsCHAssign(reflect.ValueOf(&a.PublicFieldMe2).Elem(), reflect.ValueOf(v))
	case NewsIndexPublicFieldMe3:
		newsCHAssign(reflect.ValueOf(&a.PublicFieldMe3).Elem(), reflect.ValueOf(v))
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

	// Map(String, Int64) > map[string]int
	case d.Kind() == reflect.Map && s.Kind() == reflect.Map:
		r := reflect.MakeMapWithSize(d.Type(), s.Len())
		for it := s.MapRange(); it.Next(); {
			k := reflect.New(d.Type().Key()).Elem()
			v := reflect.New(d.Type().Elem()).Elem()
			newsCHAssign(k, it.Key())
			newsCHAssign(v, it.Value())
			r.SetMapIndex(k, v)
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
