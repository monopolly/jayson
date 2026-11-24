package alltypes

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	jsoniter "github.com/json-iterator/go"
	"github.com/monopolly/cast"
	"github.com/monopolly/jayson/test/role"
	"github.com/monopolly/jsons"
	"github.com/shopspring/decimal"
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
type AllIndexTyped int

// int index
const (
	AllIndexID = AllIndexTyped(iota)
	AllIndexB
	AllIndexS
	AllIndexI
	AllIndexI8
	AllIndexI16
	AllIndexI32
	AllIndexI64
	AllIndexU
	AllIndexU8
	AllIndexU16
	AllIndexU32
	AllIndexU64
	AllIndexF32
	AllIndexF64
	AllIndexR
	AllIndexBY
	AllIndexBytes
	AllIndexFixed
	AllIndexUID
	AllIndexPuid
	AllIndexRaw
	AllIndexMapany
	AllIndexMaps
	AllIndexConf
	AllIndexConfs
	AllIndexAnyv
	AllIndexT
	AllIndexDur
	AllIndexDate
	AllIndexPD
	AllIndexPts
	AllIndexPiv
	AllIndexLB
	AllIndexLS
	AllIndexLI
	AllIndexLi8
	AllIndexLi16
	AllIndexLi32
	AllIndexLu16
	AllIndexLu32
	AllIndexLu64
	AllIndexLf32
	AllIndexLT
	AllIndexLU
	AllIndexLs2
	AllIndexLi2
	AllIndexLbb
	AllIndexLM
	AllIndexLst
	AllIndexLrl
	AllIndexIP
	AllIndexPfx
	AllIndexNip
	AllIndexNnet
	AllIndexNum
	AllIndexDec
	AllIndexRng
	AllIndexMrng
	AllIndexPT
	AllIndexBits
	AllIndexPS
	AllIndexPI
	AllIndexPB
	AllIndexPF
	AllIndexPt2
	AllIndexPip
	AllIndexNS
	AllIndexNI
	AllIndexNB
	AllIndexNF
	AllIndexNT
	AllIndexPtx
	AllIndexPi8
	AllIndexPbo
	AllIndexST
	AllIndexProv
	AllIndexRL
	AllIndexNN
)

// string index
const (
	AllFieldID     = "id"     // int sql{inc}
	AllFieldB      = "b"      // bool
	AllFieldS      = "s"      // string
	AllFieldI      = "i"      // int
	AllFieldI8     = "i8"     // int8
	AllFieldI16    = "i16"    // int16
	AllFieldI32    = "i32"    // int32
	AllFieldI64    = "i64"    // int64
	AllFieldU      = "u"      // uint
	AllFieldU8     = "u8"     // uint8
	AllFieldU16    = "u16"    // uint16
	AllFieldU32    = "u32"    // uint32
	AllFieldU64    = "u64"    // uint64
	AllFieldF32    = "f32"    // float32
	AllFieldF64    = "f64"    // float64
	AllFieldR      = "r"      // rune
	AllFieldBY     = "by"     // byte
	AllFieldBytes  = "bytes"  // []byte
	AllFieldFixed  = "fixed"  // [16]byte
	AllFieldUID    = "uid"    // uuid.UUID
	AllFieldPuid   = "puid"   // pgtype.UUID
	AllFieldRaw    = "raw"    // json.RawMessage
	AllFieldMapany = "mapany" // map[string]any
	AllFieldMaps   = "maps"   // map[string]string
	AllFieldConf   = "conf"   // Config
	AllFieldConfs  = "confs"  // []Config
	AllFieldAnyv   = "anyv"   // any
	AllFieldT      = "t"      // time.Time
	AllFieldDur    = "dur"    // time.Duration
	AllFieldDate   = "date"   // time.Time sql{type="date"}
	AllFieldPD     = "pd"     // pgtype.Date
	AllFieldPts    = "pts"    // pgtype.Timestamp
	AllFieldPiv    = "piv"    // pgtype.Interval
	AllFieldLB     = "lb"     // []bool
	AllFieldLS     = "ls"     // []string
	AllFieldLI     = "li"     // []int
	AllFieldLi8    = "li8"    // []int8
	AllFieldLi16   = "li16"   // []int16
	AllFieldLi32   = "li32"   // []int32
	AllFieldLu16   = "lu16"   // []uint16
	AllFieldLu32   = "lu32"   // []uint32
	AllFieldLu64   = "lu64"   // []uint64
	AllFieldLf32   = "lf32"   // []float32
	AllFieldLT     = "lt"     // []time.Time
	AllFieldLU     = "lu"     // []uuid.UUID
	AllFieldLs2    = "ls2"    // [][]string
	AllFieldLi2    = "li2"    // [][]int64
	AllFieldLbb    = "lbb"    // [][]byte
	AllFieldLM     = "lm"     // []map[string]any
	AllFieldLst    = "lst"    // []Status
	AllFieldLrl    = "lrl"    // []role.Role
	AllFieldIP     = "ip"     // netip.Addr
	AllFieldPfx    = "pfx"    // netip.Prefix
	AllFieldNip    = "nip"    // net.IP
	AllFieldNnet   = "nnet"   // net.IPNet
	AllFieldNum    = "num"    // pgtype.Numeric
	AllFieldDec    = "dec"    // decimal.Decimal
	AllFieldRng    = "rng"    // pgtype.Range[pgtype.Int4]
	AllFieldMrng   = "mrng"   // pgtype.Multirange[pgtype.Range[pgtype.Int8]]
	AllFieldPT     = "pt"     // pgtype.Point
	AllFieldBits   = "bits"   // pgtype.Bits
	AllFieldPS     = "ps"     // *string
	AllFieldPI     = "pi"     // *int
	AllFieldPB     = "pb"     // *bool
	AllFieldPF     = "pf"     // *float64
	AllFieldPt2    = "pt2"    // *time.Time
	AllFieldPip    = "pip"    // *netip.Addr
	AllFieldNS     = "ns"     // sql.NullString
	AllFieldNI     = "ni"     // sql.NullInt64
	AllFieldNB     = "nb"     // sql.NullBool
	AllFieldNF     = "nf"     // sql.NullFloat64
	AllFieldNT     = "nt"     // sql.NullTime
	AllFieldPtx    = "ptx"    // pgtype.Text
	AllFieldPi8    = "pi8"    // pgtype.Int8
	AllFieldPbo    = "pbo"    // pgtype.Bool
	AllFieldST     = "st"     // Status
	AllFieldProv   = "prov"   // Provider
	AllFieldRL     = "rl"     // role.Role
	AllFieldNN     = "nn"     // string sql{notnull}
)

// index func
func AllIndexes() []AllIndexTyped {
	return []AllIndexTyped{AllIndexID, AllIndexB, AllIndexS, AllIndexI, AllIndexI8, AllIndexI16, AllIndexI32, AllIndexI64, AllIndexU, AllIndexU8, AllIndexU16, AllIndexU32, AllIndexU64, AllIndexF32, AllIndexF64, AllIndexR, AllIndexBY, AllIndexBytes, AllIndexFixed, AllIndexUID, AllIndexPuid, AllIndexRaw, AllIndexMapany, AllIndexMaps, AllIndexConf, AllIndexConfs, AllIndexAnyv, AllIndexT, AllIndexDur, AllIndexDate, AllIndexPD, AllIndexPts, AllIndexPiv, AllIndexLB, AllIndexLS, AllIndexLI, AllIndexLi8, AllIndexLi16, AllIndexLi32, AllIndexLu16, AllIndexLu32, AllIndexLu64, AllIndexLf32, AllIndexLT, AllIndexLU, AllIndexLs2, AllIndexLi2, AllIndexLbb, AllIndexLM, AllIndexLst, AllIndexLrl, AllIndexIP, AllIndexPfx, AllIndexNip, AllIndexNnet, AllIndexNum, AllIndexDec, AllIndexRng, AllIndexMrng, AllIndexPT, AllIndexBits, AllIndexPS, AllIndexPI, AllIndexPB, AllIndexPF, AllIndexPt2, AllIndexPip, AllIndexNS, AllIndexNI, AllIndexNB, AllIndexNF, AllIndexNT, AllIndexPtx, AllIndexPi8, AllIndexPbo, AllIndexST, AllIndexProv, AllIndexRL, AllIndexNN}

}

// 616 bytes (go padding)
//
//easyjson:json
type All struct {
	ID     int                                          `json:"id,omitempty"`     // sql{inc}
	B      bool                                         `json:"b,omitempty"`      //
	S      string                                       `json:"s,omitempty"`      //
	I      int                                          `json:"i,omitempty"`      //
	I8     int8                                         `json:"i8,omitempty"`     //
	I16    int16                                        `json:"i16,omitempty"`    //
	I32    int32                                        `json:"i32,omitempty"`    //
	I64    int64                                        `json:"i64,omitempty"`    //
	U      uint                                         `json:"u,omitempty"`      //
	U8     uint8                                        `json:"u8,omitempty"`     //
	U16    uint16                                       `json:"u16,omitempty"`    //
	U32    uint32                                       `json:"u32,omitempty"`    //
	U64    uint64                                       `json:"u64,omitempty"`    //
	F32    float32                                      `json:"f32,omitempty"`    //
	F64    float64                                      `json:"f64,omitempty"`    //
	R      rune                                         `json:"r,omitempty"`      //
	BY     byte                                         `json:"by,omitempty"`     //
	Bytes  []byte                                       `json:"bytes,omitempty"`  //
	Fixed  [16]byte                                     `json:"fixed,omitempty"`  //
	UID    uuid.UUID                                    `json:"uid,omitempty"`    //
	Puid   pgtype.UUID                                  `json:"puid,omitempty"`   //
	Raw    json.RawMessage                              `json:"raw,omitempty"`    //
	Mapany map[string]any                               `json:"mapany,omitempty"` //
	Maps   map[string]string                            `json:"maps,omitempty"`   //
	Conf   Config                                       `json:"conf,omitempty"`   //
	Confs  []Config                                     `json:"confs,omitempty"`  //
	Anyv   any                                          `json:"anyv,omitempty"`   //
	T      time.Time                                    `json:"t,omitempty"`      //
	Dur    time.Duration                                `json:"dur,omitempty"`    //
	Date   time.Time                                    `json:"date,omitempty"`   // sql{type="date"}
	PD     pgtype.Date                                  `json:"pd,omitempty"`     //
	Pts    pgtype.Timestamp                             `json:"pts,omitempty"`    //
	Piv    pgtype.Interval                              `json:"piv,omitempty"`    //
	LB     []bool                                       `json:"lb,omitempty"`     //
	LS     []string                                     `json:"ls,omitempty"`     //
	LI     []int                                        `json:"li,omitempty"`     //
	Li8    []int8                                       `json:"li8,omitempty"`    //
	Li16   []int16                                      `json:"li16,omitempty"`   //
	Li32   []int32                                      `json:"li32,omitempty"`   //
	Lu16   []uint16                                     `json:"lu16,omitempty"`   //
	Lu32   []uint32                                     `json:"lu32,omitempty"`   //
	Lu64   []uint64                                     `json:"lu64,omitempty"`   //
	Lf32   []float32                                    `json:"lf32,omitempty"`   //
	LT     []time.Time                                  `json:"lt,omitempty"`     //
	LU     []uuid.UUID                                  `json:"lu,omitempty"`     //
	Ls2    [][]string                                   `json:"ls2,omitempty"`    //
	Li2    [][]int64                                    `json:"li2,omitempty"`    //
	Lbb    [][]byte                                     `json:"lbb,omitempty"`    //
	LM     []map[string]any                             `json:"lm,omitempty"`     //
	Lst    []Status                                     `json:"lst,omitempty"`    //
	Lrl    []role.Role                                  `json:"lrl,omitempty"`    //
	IP     netip.Addr                                   `json:"ip,omitempty"`     //
	Pfx    netip.Prefix                                 `json:"pfx,omitempty"`    //
	Nip    net.IP                                       `json:"nip,omitempty"`    //
	Nnet   net.IPNet                                    `json:"nnet,omitempty"`   //
	Num    pgtype.Numeric                               `json:"num,omitempty"`    //
	Dec    decimal.Decimal                              `json:"dec,omitempty"`    //
	Rng    pgtype.Range[pgtype.Int4]                    `json:"rng,omitempty"`    //
	Mrng   pgtype.Multirange[pgtype.Range[pgtype.Int8]] `json:"mrng,omitempty"`   //
	PT     pgtype.Point                                 `json:"pt,omitempty"`     //
	Bits   pgtype.Bits                                  `json:"bits,omitempty"`   //
	PS     *string                                      `json:"ps,omitempty"`     //
	PI     *int                                         `json:"pi,omitempty"`     //
	PB     *bool                                        `json:"pb,omitempty"`     //
	PF     *float64                                     `json:"pf,omitempty"`     //
	Pt2    *time.Time                                   `json:"pt2,omitempty"`    //
	Pip    *netip.Addr                                  `json:"pip,omitempty"`    //
	NS     sql.NullString                               `json:"ns,omitempty"`     //
	NI     sql.NullInt64                                `json:"ni,omitempty"`     //
	NB     sql.NullBool                                 `json:"nb,omitempty"`     //
	NF     sql.NullFloat64                              `json:"nf,omitempty"`     //
	NT     sql.NullTime                                 `json:"nt,omitempty"`     //
	Ptx    pgtype.Text                                  `json:"ptx,omitempty"`    //
	Pi8    pgtype.Int8                                  `json:"pi8,omitempty"`    //
	Pbo    pgtype.Bool                                  `json:"pbo,omitempty"`    //
	ST     Status                                       `json:"st,omitempty"`     //
	Prov   Provider                                     `json:"prov,omitempty"`   //
	RL     role.Role                                    `json:"rl,omitempty"`     //
	NN     string                                       `json:"nn,omitempty"`     // sql{notnull}
}

// init struct
func NewAll() (a *All) {
	a = new(All)
	a.Mapany = make(map[string]any)
	a.Maps = make(map[string]string)
	return
}

// Parse []any to ID struct
func ParseAllToStruct(r []any) (a All) {

	for pos, x := range r {
		switch AllIndexTyped(pos) {
		case AllIndexID:
			cast.Convert(&a.ID, x) //int
		case AllIndexB:
			cast.Convert(&a.B, x) //bool
		case AllIndexS:
			cast.Convert(&a.S, x) //string
		case AllIndexI:
			cast.Convert(&a.I, x) //int
		case AllIndexI8:
			cast.Convert(&a.I8, x) //int8
		case AllIndexI16:
			cast.Convert(&a.I16, x) //int16
		case AllIndexI32:
			cast.Convert(&a.I32, x) //int32
		case AllIndexI64:
			cast.Convert(&a.I64, x) //int64
		case AllIndexU:
			if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
				b, _ := n.MarshalJSON()
				cast.Convert(&a.U, string(b))
			} else {
				cast.Convert(&a.U, x)
			} //uint
		case AllIndexU8:
			cast.Convert(&a.U8, x) //uint8
		case AllIndexU16:
			cast.Convert(&a.U16, x) //uint16
		case AllIndexU32:
			cast.Convert(&a.U32, x) //uint32
		case AllIndexU64:
			if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
				b, _ := n.MarshalJSON()
				cast.Convert(&a.U64, string(b))
			} else {
				cast.Convert(&a.U64, x)
			} //uint64
		case AllIndexF32:
			cast.Convert(&a.F32, x) //float32
		case AllIndexF64:
			cast.Convert(&a.F64, x) //float64
		case AllIndexR:
			cast.Convert(&a.R, x) //rune
		case AllIndexBY:
			cast.Convert(&a.BY, x) //byte
		case AllIndexBytes:
			cast.Convert(&a.Bytes, x) //[]byte
		case AllIndexFixed:
			switch v0 := x.(type) {
			case [16]byte:
				a.Fixed = v0
			case []byte:
				copy(a.Fixed[:], v0)
			case string:
				copy(a.Fixed[:], v0)
			} //[16]byte
		case AllIndexUID:
			switch v := x.(type) {
			case uuid.UUID:
				a.UID = v
			case [16]byte:
				a.UID = v
			case string:
				a.UID, _ = uuid.Parse(v)
			case []byte:
				if len(v) == 16 {
					a.UID, _ = uuid.FromBytes(v)
				} else {
					a.UID, _ = uuid.ParseBytes(v)
				}
			} //uuid.UUID
		case AllIndexPuid:
			switch v0 := x.(type) {
			case nil:
			case pgtype.UUID:
				a.Puid = v0
			case *pgtype.UUID:
				if v0 != nil {
					a.Puid = *v0
				}
			case [16]byte:
				a.Puid = pgtype.UUID{Bytes: v0, Valid: true}
			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Puid).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Puid)
				}
			} //pgtype.UUID
		case AllIndexRaw:
			switch v0 := x.(type) {
			case nil:
			case json.RawMessage:
				a.Raw = v0
			case *json.RawMessage:
				if v0 != nil {
					a.Raw = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Raw).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Raw)
				}
			} //json.RawMessage
		case AllIndexMapany:
			cast.Convert(&a.Mapany, x) //map[string]any
		case AllIndexMaps:
			cast.Convert(&a.Maps, x) //map[string]string
		case AllIndexConf:
			switch v0 := x.(type) {
			case nil:
			case Config:
				a.Conf = v0
			case *Config:
				if v0 != nil {
					a.Conf = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Conf).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Conf)
				}
			} //Config
		case AllIndexConfs:
			switch v0 := x.(type) {
			case nil:
			case []Config:
				a.Confs = v0
			case []any:
				r0 := make([]Config, 0, len(v0))
				for _, e0 := range v0 {
					var el0 Config
					switch v1 := e0.(type) {
					case nil:
					case Config:
						el0 = v1
					case *Config:
						if v1 != nil {
							el0 = *v1
						}

					default:
						// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
						if s, ok := any(&el0).(interface{ Scan(any) error }); !ok || s.Scan(v1) != nil {
							b, _ := jsoniter.Marshal(v1)
							_ = jsoniter.Unmarshal(b, &el0)
						}
					}
					r0 = append(r0, el0)
				}
				a.Confs = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Confs)
			} //[]Config
		case AllIndexAnyv:
			a.Anyv = x //any
		case AllIndexT:
			cast.Convert(&a.T, x) //time.Time
		case AllIndexDur:
			cast.Convert(&a.Dur, x) //time.Duration
		case AllIndexDate:
			cast.Convert(&a.Date, x) //time.Time
		case AllIndexPD:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Date:
				a.PD = v0
			case *pgtype.Date:
				if v0 != nil {
					a.PD = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.PD).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.PD)
				}
			} //pgtype.Date
		case AllIndexPts:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Timestamp:
				a.Pts = v0
			case *pgtype.Timestamp:
				if v0 != nil {
					a.Pts = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Pts).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Pts)
				}
			} //pgtype.Timestamp
		case AllIndexPiv:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Interval:
				a.Piv = v0
			case *pgtype.Interval:
				if v0 != nil {
					a.Piv = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Piv).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Piv)
				}
			} //pgtype.Interval
		case AllIndexLB:
			switch v0 := x.(type) {
			case nil:
			case []bool:
				a.LB = v0
			case []any:
				r0 := make([]bool, 0, len(v0))
				for _, e0 := range v0 {
					var el0 bool
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.LB = r0
			default:
				cast.Convert(&a.LB, v0)
			} //[]bool
		case AllIndexLS:
			switch v0 := x.(type) {
			case nil:
			case []string:
				a.LS = v0
			case []any:
				r0 := make([]string, 0, len(v0))
				for _, e0 := range v0 {
					var el0 string
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.LS = r0
			default:
				cast.Convert(&a.LS, v0)
			} //[]string
		case AllIndexLI:
			switch v0 := x.(type) {
			case nil:
			case []int:
				a.LI = v0
			case []any:
				r0 := make([]int, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.LI = r0
			default:
				cast.Convert(&a.LI, v0)
			} //[]int
		case AllIndexLi8:
			switch v0 := x.(type) {
			case nil:
			case []int8:
				a.Li8 = v0
			case []any:
				r0 := make([]int8, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int8
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Li8 = r0
			default:
				cast.Convert(&a.Li8, v0)
			} //[]int8
		case AllIndexLi16:
			switch v0 := x.(type) {
			case nil:
			case []int16:
				a.Li16 = v0
			case []any:
				r0 := make([]int16, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int16
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Li16 = r0
			default:
				cast.Convert(&a.Li16, v0)
			} //[]int16
		case AllIndexLi32:
			switch v0 := x.(type) {
			case nil:
			case []int32:
				a.Li32 = v0
			case []any:
				r0 := make([]int32, 0, len(v0))
				for _, e0 := range v0 {
					var el0 int32
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Li32 = r0
			default:
				cast.Convert(&a.Li32, v0)
			} //[]int32
		case AllIndexLu16:
			switch v0 := x.(type) {
			case nil:
			case []uint16:
				a.Lu16 = v0
			case []any:
				r0 := make([]uint16, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint16
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Lu16 = r0
			default:
				cast.Convert(&a.Lu16, v0)
			} //[]uint16
		case AllIndexLu32:
			switch v0 := x.(type) {
			case nil:
			case []uint32:
				a.Lu32 = v0
			case []any:
				r0 := make([]uint32, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uint32
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Lu32 = r0
			default:
				cast.Convert(&a.Lu32, v0)
			} //[]uint32
		case AllIndexLu64:
			switch v0 := x.(type) {
			case nil:
			case []uint64:
				a.Lu64 = v0
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
				a.Lu64 = r0
			default:
				cast.Convert(&a.Lu64, v0)
			} //[]uint64
		case AllIndexLf32:
			switch v0 := x.(type) {
			case nil:
			case []float32:
				a.Lf32 = v0
			case []any:
				r0 := make([]float32, 0, len(v0))
				for _, e0 := range v0 {
					var el0 float32
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Lf32 = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Lf32)
			} //[]float32
		case AllIndexLT:
			switch v0 := x.(type) {
			case nil:
			case []time.Time:
				a.LT = v0
			case []any:
				r0 := make([]time.Time, 0, len(v0))
				for _, e0 := range v0 {
					var el0 time.Time
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.LT = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.LT)
			} //[]time.Time
		case AllIndexLU:
			switch v0 := x.(type) {
			case nil:
			case []uuid.UUID:
				a.LU = v0
			case []any:
				r0 := make([]uuid.UUID, 0, len(v0))
				for _, e0 := range v0 {
					var el0 uuid.UUID
					switch v := e0.(type) {
					case uuid.UUID:
						el0 = v
					case [16]byte:
						el0 = v
					case string:
						el0, _ = uuid.Parse(v)
					case []byte:
						if len(v) == 16 {
							el0, _ = uuid.FromBytes(v)
						} else {
							el0, _ = uuid.ParseBytes(v)
						}
					}
					r0 = append(r0, el0)
				}
				a.LU = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.LU)
			} //[]uuid.UUID
		case AllIndexLs2:
			switch v0 := x.(type) {
			case nil:
			case [][]string:
				a.Ls2 = v0
			case []any:
				r0 := make([][]string, 0, len(v0))
				for _, e0 := range v0 {
					var el0 []string
					switch v1 := e0.(type) {
					case nil:
					case []string:
						el0 = v1
					case []any:
						r1 := make([]string, 0, len(v1))
						for _, e1 := range v1 {
							var el1 string
							cast.Convert(&el1, e1)
							r1 = append(r1, el1)
						}
						el0 = r1
					default:
						cast.Convert(&el0, v1)
					}
					r0 = append(r0, el0)
				}
				a.Ls2 = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Ls2)
			} //[][]string
		case AllIndexLi2:
			switch v0 := x.(type) {
			case nil:
			case [][]int64:
				a.Li2 = v0
			case []any:
				r0 := make([][]int64, 0, len(v0))
				for _, e0 := range v0 {
					var el0 []int64
					switch v1 := e0.(type) {
					case nil:
					case []int64:
						el0 = v1
					case []any:
						r1 := make([]int64, 0, len(v1))
						for _, e1 := range v1 {
							var el1 int64
							cast.Convert(&el1, e1)
							r1 = append(r1, el1)
						}
						el0 = r1
					default:
						cast.Convert(&el0, v1)
					}
					r0 = append(r0, el0)
				}
				a.Li2 = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Li2)
			} //[][]int64
		case AllIndexLbb:
			switch v0 := x.(type) {
			case nil:
			case [][]byte:
				a.Lbb = v0
			case []any:
				r0 := make([][]byte, 0, len(v0))
				for _, e0 := range v0 {
					var el0 []byte
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.Lbb = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Lbb)
			} //[][]byte
		case AllIndexLM:
			switch v0 := x.(type) {
			case nil:
			case []map[string]any:
				a.LM = v0
			case []any:
				r0 := make([]map[string]any, 0, len(v0))
				for _, e0 := range v0 {
					var el0 map[string]any
					cast.Convert(&el0, e0)
					r0 = append(r0, el0)
				}
				a.LM = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.LM)
			} //[]map[string]any
		case AllIndexLst:
			switch v0 := x.(type) {
			case nil:
			case []Status:
				a.Lst = v0
			case []any:
				r0 := make([]Status, 0, len(v0))
				for _, e0 := range v0 {
					var el0 Status
					var b1 int16
					cast.Convert(&b1, e0)
					el0 = Status(b1)
					r0 = append(r0, el0)
				}
				a.Lst = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Lst)
			} //[]Status
		case AllIndexLrl:
			switch v0 := x.(type) {
			case nil:
			case []role.Role:
				a.Lrl = v0
			case []any:
				r0 := make([]role.Role, 0, len(v0))
				for _, e0 := range v0 {
					var el0 role.Role
					var b1 int
					cast.Convert(&b1, e0)
					el0 = role.Role(b1)
					r0 = append(r0, el0)
				}
				a.Lrl = r0
			default:
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Lrl)
			} //[]role.Role
		case AllIndexIP:
			switch v0 := x.(type) {
			case nil:
			case netip.Addr:
				a.IP = v0
			case *netip.Addr:
				if v0 != nil {
					a.IP = *v0
				}
			case netip.Prefix:
				a.IP = v0.Addr()
			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.IP).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.IP)
				}
			} //netip.Addr
		case AllIndexPfx:
			switch v0 := x.(type) {
			case nil:
			case netip.Prefix:
				a.Pfx = v0
			case *netip.Prefix:
				if v0 != nil {
					a.Pfx = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Pfx).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Pfx)
				}
			} //netip.Prefix
		case AllIndexNip:
			switch v0 := x.(type) {
			case nil:
			case net.IP:
				a.Nip = v0
			case *net.IP:
				if v0 != nil {
					a.Nip = *v0
				}
			case netip.Prefix:
				a.Nip = net.IP(v0.Addr().AsSlice())
			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Nip).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Nip)
				}
			} //net.IP
		case AllIndexNnet:
			switch v0 := x.(type) {
			case nil:
			case net.IPNet:
				a.Nnet = v0
			case *net.IPNet:
				if v0 != nil {
					a.Nnet = *v0
				}
			case netip.Prefix:
				a.Nnet = net.IPNet{IP: v0.Masked().Addr().AsSlice(), Mask: net.CIDRMask(v0.Bits(), v0.Addr().BitLen())}
			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Nnet).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Nnet)
				}
			} //net.IPNet
		case AllIndexNum:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Numeric:
				a.Num = v0
			case *pgtype.Numeric:
				if v0 != nil {
					a.Num = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Num).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Num)
				}
			} //pgtype.Numeric
		case AllIndexDec:
			switch v0 := x.(type) {
			case nil:
			case decimal.Decimal:
				a.Dec = v0
			case *decimal.Decimal:
				if v0 != nil {
					a.Dec = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Dec).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Dec)
				}
			} //decimal.Decimal
		case AllIndexRng:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Range[pgtype.Int4]:
				a.Rng = v0
			case *pgtype.Range[pgtype.Int4]:
				if v0 != nil {
					a.Rng = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Rng).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Rng)
				}
			} //pgtype.Range[pgtype.Int4]
		case AllIndexMrng:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Multirange[pgtype.Range[pgtype.Int8]]:
				a.Mrng = v0
			case *pgtype.Multirange[pgtype.Range[pgtype.Int8]]:
				if v0 != nil {
					a.Mrng = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Mrng).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Mrng)
				}
			} //pgtype.Multirange[pgtype.Range[pgtype.Int8]]
		case AllIndexPT:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Point:
				a.PT = v0
			case *pgtype.Point:
				if v0 != nil {
					a.PT = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.PT).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.PT)
				}
			} //pgtype.Point
		case AllIndexBits:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Bits:
				a.Bits = v0
			case *pgtype.Bits:
				if v0 != nil {
					a.Bits = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Bits).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Bits)
				}
			} //pgtype.Bits
		case AllIndexPS:
			if x != nil {
				var p0 string
				cast.Convert(&p0, x)
				a.PS = &p0
			} //*string
		case AllIndexPI:
			if x != nil {
				var p0 int
				cast.Convert(&p0, x)
				a.PI = &p0
			} //*int
		case AllIndexPB:
			if x != nil {
				var p0 bool
				cast.Convert(&p0, x)
				a.PB = &p0
			} //*bool
		case AllIndexPF:
			if x != nil {
				var p0 float64
				cast.Convert(&p0, x)
				a.PF = &p0
			} //*float64
		case AllIndexPt2:
			if x != nil {
				var p0 time.Time
				cast.Convert(&p0, x)
				a.Pt2 = &p0
			} //*time.Time
		case AllIndexPip:
			if x != nil {
				var p0 netip.Addr
				switch v1 := x.(type) {
				case nil:
				case netip.Addr:
					p0 = v1
				case *netip.Addr:
					if v1 != nil {
						p0 = *v1
					}
				case netip.Prefix:
					p0 = v1.Addr()
				default:
					// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
					if s, ok := any(&p0).(interface{ Scan(any) error }); !ok || s.Scan(v1) != nil {
						b, _ := jsoniter.Marshal(v1)
						_ = jsoniter.Unmarshal(b, &p0)
					}
				}
				a.Pip = &p0
			} //*netip.Addr
		case AllIndexNS:
			switch v0 := x.(type) {
			case nil:
			case sql.NullString:
				a.NS = v0
			case *sql.NullString:
				if v0 != nil {
					a.NS = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.NS).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.NS)
				}
			} //sql.NullString
		case AllIndexNI:
			switch v0 := x.(type) {
			case nil:
			case sql.NullInt64:
				a.NI = v0
			case *sql.NullInt64:
				if v0 != nil {
					a.NI = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.NI).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.NI)
				}
			} //sql.NullInt64
		case AllIndexNB:
			switch v0 := x.(type) {
			case nil:
			case sql.NullBool:
				a.NB = v0
			case *sql.NullBool:
				if v0 != nil {
					a.NB = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.NB).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.NB)
				}
			} //sql.NullBool
		case AllIndexNF:
			switch v0 := x.(type) {
			case nil:
			case sql.NullFloat64:
				a.NF = v0
			case *sql.NullFloat64:
				if v0 != nil {
					a.NF = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.NF).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.NF)
				}
			} //sql.NullFloat64
		case AllIndexNT:
			switch v0 := x.(type) {
			case nil:
			case sql.NullTime:
				a.NT = v0
			case *sql.NullTime:
				if v0 != nil {
					a.NT = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.NT).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.NT)
				}
			} //sql.NullTime
		case AllIndexPtx:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Text:
				a.Ptx = v0
			case *pgtype.Text:
				if v0 != nil {
					a.Ptx = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Ptx).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Ptx)
				}
			} //pgtype.Text
		case AllIndexPi8:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Int8:
				a.Pi8 = v0
			case *pgtype.Int8:
				if v0 != nil {
					a.Pi8 = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Pi8).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Pi8)
				}
			} //pgtype.Int8
		case AllIndexPbo:
			switch v0 := x.(type) {
			case nil:
			case pgtype.Bool:
				a.Pbo = v0
			case *pgtype.Bool:
				if v0 != nil {
					a.Pbo = *v0
				}

			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&a.Pbo).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
					b, _ := jsoniter.Marshal(v0)
					_ = jsoniter.Unmarshal(b, &a.Pbo)
				}
			} //pgtype.Bool
		case AllIndexST:
			var v int16
			cast.Convert(&v, x)
			a.ST = Status(v) //Status
		case AllIndexProv:
			var v string
			cast.Convert(&v, x)
			a.Prov = Provider(v) //Provider
		case AllIndexRL:
			var v int
			cast.Convert(&v, x)
			a.RL = role.Role(v) //role.Role
		case AllIndexNN:
			cast.Convert(&a.NN, x) //string
		}
	}
	return
}

// Tuple create an array from struct
func (a *All) Tuple() (r []any) {
	return []any{a.ID, a.B, a.S, a.I, a.I8, a.I16, a.I32, a.I64, a.U, a.U8, a.U16, a.U32, a.U64, a.F32, a.F64, a.R, a.BY, a.Bytes, a.Fixed, a.UID, a.Puid, a.Raw, a.Mapany, a.Maps, a.Conf, a.Confs, a.Anyv, a.T, a.Dur, a.Date, a.PD, a.Pts, a.Piv, a.LB, a.LS, a.LI, a.Li8, a.Li16, a.Li32, a.Lu16, a.Lu32, a.Lu64, a.Lf32, a.LT, a.LU, a.Ls2, a.Li2, a.Lbb, a.LM, a.Lst, a.Lrl, a.IP, a.Pfx, a.Nip, a.Nnet, a.Num, a.Dec, a.Rng, a.Mrng, a.PT, a.Bits, a.PS, a.PI, a.PB, a.PF, a.Pt2, a.Pip, a.NS, a.NI, a.NB, a.NF, a.NT, a.Ptx, a.Pi8, a.Pbo, a.ST, a.Prov, a.RL, a.NN}
}

// Tuple create an array from struct for clickhouse
func (a *All) clickhouseTuple() (r []any) {
	return []any{a.ID, a.B, a.S, a.I, a.I8, a.I16, a.I32, a.I64, a.U, a.U8, a.U16, a.U32, a.U64, a.F32, a.F64, a.R, a.BY, a.Bytes, a.Fixed, a.UID, a.Puid, a.Raw, a.Mapany, a.Maps, a.Conf, a.Confs, a.Anyv, a.T, a.Dur, a.Date, a.PD, a.Pts, a.Piv, a.LB, a.LS, a.LI, a.Li8, a.Li16, a.Li32, a.Lu16, a.Lu32, a.Lu64, a.Lf32, a.LT, a.LU, a.Ls2, a.Li2, a.Lbb, a.LM, a.Lst, a.Lrl, a.IP, a.Pfx, a.Nip, a.Nnet, a.Num, a.Dec, a.Rng, a.Mrng, a.PT, a.Bits, a.PS, a.PI, a.PB, a.PF, a.Pt2, a.Pip, a.NS, a.NI, a.NB, a.NF, a.NT, a.Ptx, a.Pi8, a.Pbo, int16(a.ST), string(a.Prov), int(a.RL), a.NN}
}

// update struct with function
func (a *All) Update(k string, x any) {
	switch k {
	case "id":
		cast.Convert(&a.ID, x) //int
	case "b":
		cast.Convert(&a.B, x) //bool
	case "s":
		cast.Convert(&a.S, x) //string
	case "i":
		cast.Convert(&a.I, x) //int
	case "i8":
		cast.Convert(&a.I8, x) //int8
	case "i16":
		cast.Convert(&a.I16, x) //int16
	case "i32":
		cast.Convert(&a.I32, x) //int32
	case "i64":
		cast.Convert(&a.I64, x) //int64
	case "u":
		if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
			b, _ := n.MarshalJSON()
			cast.Convert(&a.U, string(b))
		} else {
			cast.Convert(&a.U, x)
		} //uint
	case "u8":
		cast.Convert(&a.U8, x) //uint8
	case "u16":
		cast.Convert(&a.U16, x) //uint16
	case "u32":
		cast.Convert(&a.U32, x) //uint32
	case "u64":
		if n, ok := x.(interface{ MarshalJSON() ([]byte, error) }); ok {
			b, _ := n.MarshalJSON()
			cast.Convert(&a.U64, string(b))
		} else {
			cast.Convert(&a.U64, x)
		} //uint64
	case "f32":
		cast.Convert(&a.F32, x) //float32
	case "f64":
		cast.Convert(&a.F64, x) //float64
	case "r":
		cast.Convert(&a.R, x) //rune
	case "by":
		cast.Convert(&a.BY, x) //byte
	case "bytes":
		cast.Convert(&a.Bytes, x) //[]byte
	case "fixed":
		switch v0 := x.(type) {
		case [16]byte:
			a.Fixed = v0
		case []byte:
			copy(a.Fixed[:], v0)
		case string:
			copy(a.Fixed[:], v0)
		} //[16]byte
	case "uid":
		switch v := x.(type) {
		case uuid.UUID:
			a.UID = v
		case [16]byte:
			a.UID = v
		case string:
			a.UID, _ = uuid.Parse(v)
		case []byte:
			if len(v) == 16 {
				a.UID, _ = uuid.FromBytes(v)
			} else {
				a.UID, _ = uuid.ParseBytes(v)
			}
		} //uuid.UUID
	case "puid":
		switch v0 := x.(type) {
		case nil:
		case pgtype.UUID:
			a.Puid = v0
		case *pgtype.UUID:
			if v0 != nil {
				a.Puid = *v0
			}
		case [16]byte:
			a.Puid = pgtype.UUID{Bytes: v0, Valid: true}
		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Puid).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Puid)
			}
		} //pgtype.UUID
	case "raw":
		switch v0 := x.(type) {
		case nil:
		case json.RawMessage:
			a.Raw = v0
		case *json.RawMessage:
			if v0 != nil {
				a.Raw = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Raw).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Raw)
			}
		} //json.RawMessage
	case "mapany":
		cast.Convert(&a.Mapany, x) //map[string]any
	case "maps":
		cast.Convert(&a.Maps, x) //map[string]string
	case "conf":
		switch v0 := x.(type) {
		case nil:
		case Config:
			a.Conf = v0
		case *Config:
			if v0 != nil {
				a.Conf = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Conf).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Conf)
			}
		} //Config
	case "confs":
		switch v0 := x.(type) {
		case nil:
		case []Config:
			a.Confs = v0
		case []any:
			r0 := make([]Config, 0, len(v0))
			for _, e0 := range v0 {
				var el0 Config
				switch v1 := e0.(type) {
				case nil:
				case Config:
					el0 = v1
				case *Config:
					if v1 != nil {
						el0 = *v1
					}

				default:
					// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
					if s, ok := any(&el0).(interface{ Scan(any) error }); !ok || s.Scan(v1) != nil {
						b, _ := jsoniter.Marshal(v1)
						_ = jsoniter.Unmarshal(b, &el0)
					}
				}
				r0 = append(r0, el0)
			}
			a.Confs = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Confs)
		} //[]Config
	case "anyv":
		a.Anyv = x //any
	case "t":
		cast.Convert(&a.T, x) //time.Time
	case "dur":
		cast.Convert(&a.Dur, x) //time.Duration
	case "date":
		cast.Convert(&a.Date, x) //time.Time
	case "pd":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Date:
			a.PD = v0
		case *pgtype.Date:
			if v0 != nil {
				a.PD = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.PD).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.PD)
			}
		} //pgtype.Date
	case "pts":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Timestamp:
			a.Pts = v0
		case *pgtype.Timestamp:
			if v0 != nil {
				a.Pts = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Pts).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Pts)
			}
		} //pgtype.Timestamp
	case "piv":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Interval:
			a.Piv = v0
		case *pgtype.Interval:
			if v0 != nil {
				a.Piv = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Piv).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Piv)
			}
		} //pgtype.Interval
	case "lb":
		switch v0 := x.(type) {
		case nil:
		case []bool:
			a.LB = v0
		case []any:
			r0 := make([]bool, 0, len(v0))
			for _, e0 := range v0 {
				var el0 bool
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.LB = r0
		default:
			cast.Convert(&a.LB, v0)
		} //[]bool
	case "ls":
		switch v0 := x.(type) {
		case nil:
		case []string:
			a.LS = v0
		case []any:
			r0 := make([]string, 0, len(v0))
			for _, e0 := range v0 {
				var el0 string
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.LS = r0
		default:
			cast.Convert(&a.LS, v0)
		} //[]string
	case "li":
		switch v0 := x.(type) {
		case nil:
		case []int:
			a.LI = v0
		case []any:
			r0 := make([]int, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.LI = r0
		default:
			cast.Convert(&a.LI, v0)
		} //[]int
	case "li8":
		switch v0 := x.(type) {
		case nil:
		case []int8:
			a.Li8 = v0
		case []any:
			r0 := make([]int8, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int8
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Li8 = r0
		default:
			cast.Convert(&a.Li8, v0)
		} //[]int8
	case "li16":
		switch v0 := x.(type) {
		case nil:
		case []int16:
			a.Li16 = v0
		case []any:
			r0 := make([]int16, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int16
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Li16 = r0
		default:
			cast.Convert(&a.Li16, v0)
		} //[]int16
	case "li32":
		switch v0 := x.(type) {
		case nil:
		case []int32:
			a.Li32 = v0
		case []any:
			r0 := make([]int32, 0, len(v0))
			for _, e0 := range v0 {
				var el0 int32
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Li32 = r0
		default:
			cast.Convert(&a.Li32, v0)
		} //[]int32
	case "lu16":
		switch v0 := x.(type) {
		case nil:
		case []uint16:
			a.Lu16 = v0
		case []any:
			r0 := make([]uint16, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint16
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Lu16 = r0
		default:
			cast.Convert(&a.Lu16, v0)
		} //[]uint16
	case "lu32":
		switch v0 := x.(type) {
		case nil:
		case []uint32:
			a.Lu32 = v0
		case []any:
			r0 := make([]uint32, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uint32
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Lu32 = r0
		default:
			cast.Convert(&a.Lu32, v0)
		} //[]uint32
	case "lu64":
		switch v0 := x.(type) {
		case nil:
		case []uint64:
			a.Lu64 = v0
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
			a.Lu64 = r0
		default:
			cast.Convert(&a.Lu64, v0)
		} //[]uint64
	case "lf32":
		switch v0 := x.(type) {
		case nil:
		case []float32:
			a.Lf32 = v0
		case []any:
			r0 := make([]float32, 0, len(v0))
			for _, e0 := range v0 {
				var el0 float32
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Lf32 = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Lf32)
		} //[]float32
	case "lt":
		switch v0 := x.(type) {
		case nil:
		case []time.Time:
			a.LT = v0
		case []any:
			r0 := make([]time.Time, 0, len(v0))
			for _, e0 := range v0 {
				var el0 time.Time
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.LT = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.LT)
		} //[]time.Time
	case "lu":
		switch v0 := x.(type) {
		case nil:
		case []uuid.UUID:
			a.LU = v0
		case []any:
			r0 := make([]uuid.UUID, 0, len(v0))
			for _, e0 := range v0 {
				var el0 uuid.UUID
				switch v := e0.(type) {
				case uuid.UUID:
					el0 = v
				case [16]byte:
					el0 = v
				case string:
					el0, _ = uuid.Parse(v)
				case []byte:
					if len(v) == 16 {
						el0, _ = uuid.FromBytes(v)
					} else {
						el0, _ = uuid.ParseBytes(v)
					}
				}
				r0 = append(r0, el0)
			}
			a.LU = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.LU)
		} //[]uuid.UUID
	case "ls2":
		switch v0 := x.(type) {
		case nil:
		case [][]string:
			a.Ls2 = v0
		case []any:
			r0 := make([][]string, 0, len(v0))
			for _, e0 := range v0 {
				var el0 []string
				switch v1 := e0.(type) {
				case nil:
				case []string:
					el0 = v1
				case []any:
					r1 := make([]string, 0, len(v1))
					for _, e1 := range v1 {
						var el1 string
						cast.Convert(&el1, e1)
						r1 = append(r1, el1)
					}
					el0 = r1
				default:
					cast.Convert(&el0, v1)
				}
				r0 = append(r0, el0)
			}
			a.Ls2 = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Ls2)
		} //[][]string
	case "li2":
		switch v0 := x.(type) {
		case nil:
		case [][]int64:
			a.Li2 = v0
		case []any:
			r0 := make([][]int64, 0, len(v0))
			for _, e0 := range v0 {
				var el0 []int64
				switch v1 := e0.(type) {
				case nil:
				case []int64:
					el0 = v1
				case []any:
					r1 := make([]int64, 0, len(v1))
					for _, e1 := range v1 {
						var el1 int64
						cast.Convert(&el1, e1)
						r1 = append(r1, el1)
					}
					el0 = r1
				default:
					cast.Convert(&el0, v1)
				}
				r0 = append(r0, el0)
			}
			a.Li2 = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Li2)
		} //[][]int64
	case "lbb":
		switch v0 := x.(type) {
		case nil:
		case [][]byte:
			a.Lbb = v0
		case []any:
			r0 := make([][]byte, 0, len(v0))
			for _, e0 := range v0 {
				var el0 []byte
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.Lbb = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Lbb)
		} //[][]byte
	case "lm":
		switch v0 := x.(type) {
		case nil:
		case []map[string]any:
			a.LM = v0
		case []any:
			r0 := make([]map[string]any, 0, len(v0))
			for _, e0 := range v0 {
				var el0 map[string]any
				cast.Convert(&el0, e0)
				r0 = append(r0, el0)
			}
			a.LM = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.LM)
		} //[]map[string]any
	case "lst":
		switch v0 := x.(type) {
		case nil:
		case []Status:
			a.Lst = v0
		case []any:
			r0 := make([]Status, 0, len(v0))
			for _, e0 := range v0 {
				var el0 Status
				var b1 int16
				cast.Convert(&b1, e0)
				el0 = Status(b1)
				r0 = append(r0, el0)
			}
			a.Lst = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Lst)
		} //[]Status
	case "lrl":
		switch v0 := x.(type) {
		case nil:
		case []role.Role:
			a.Lrl = v0
		case []any:
			r0 := make([]role.Role, 0, len(v0))
			for _, e0 := range v0 {
				var el0 role.Role
				var b1 int
				cast.Convert(&b1, e0)
				el0 = role.Role(b1)
				r0 = append(r0, el0)
			}
			a.Lrl = r0
		default:
			b, _ := jsoniter.Marshal(v0)
			_ = jsoniter.Unmarshal(b, &a.Lrl)
		} //[]role.Role
	case "ip":
		switch v0 := x.(type) {
		case nil:
		case netip.Addr:
			a.IP = v0
		case *netip.Addr:
			if v0 != nil {
				a.IP = *v0
			}
		case netip.Prefix:
			a.IP = v0.Addr()
		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.IP).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.IP)
			}
		} //netip.Addr
	case "pfx":
		switch v0 := x.(type) {
		case nil:
		case netip.Prefix:
			a.Pfx = v0
		case *netip.Prefix:
			if v0 != nil {
				a.Pfx = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Pfx).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Pfx)
			}
		} //netip.Prefix
	case "nip":
		switch v0 := x.(type) {
		case nil:
		case net.IP:
			a.Nip = v0
		case *net.IP:
			if v0 != nil {
				a.Nip = *v0
			}
		case netip.Prefix:
			a.Nip = net.IP(v0.Addr().AsSlice())
		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Nip).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Nip)
			}
		} //net.IP
	case "nnet":
		switch v0 := x.(type) {
		case nil:
		case net.IPNet:
			a.Nnet = v0
		case *net.IPNet:
			if v0 != nil {
				a.Nnet = *v0
			}
		case netip.Prefix:
			a.Nnet = net.IPNet{IP: v0.Masked().Addr().AsSlice(), Mask: net.CIDRMask(v0.Bits(), v0.Addr().BitLen())}
		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Nnet).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Nnet)
			}
		} //net.IPNet
	case "num":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Numeric:
			a.Num = v0
		case *pgtype.Numeric:
			if v0 != nil {
				a.Num = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Num).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Num)
			}
		} //pgtype.Numeric
	case "dec":
		switch v0 := x.(type) {
		case nil:
		case decimal.Decimal:
			a.Dec = v0
		case *decimal.Decimal:
			if v0 != nil {
				a.Dec = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Dec).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Dec)
			}
		} //decimal.Decimal
	case "rng":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Range[pgtype.Int4]:
			a.Rng = v0
		case *pgtype.Range[pgtype.Int4]:
			if v0 != nil {
				a.Rng = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Rng).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Rng)
			}
		} //pgtype.Range[pgtype.Int4]
	case "mrng":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Multirange[pgtype.Range[pgtype.Int8]]:
			a.Mrng = v0
		case *pgtype.Multirange[pgtype.Range[pgtype.Int8]]:
			if v0 != nil {
				a.Mrng = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Mrng).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Mrng)
			}
		} //pgtype.Multirange[pgtype.Range[pgtype.Int8]]
	case "pt":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Point:
			a.PT = v0
		case *pgtype.Point:
			if v0 != nil {
				a.PT = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.PT).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.PT)
			}
		} //pgtype.Point
	case "bits":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Bits:
			a.Bits = v0
		case *pgtype.Bits:
			if v0 != nil {
				a.Bits = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Bits).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Bits)
			}
		} //pgtype.Bits
	case "ps":
		if x != nil {
			var p0 string
			cast.Convert(&p0, x)
			a.PS = &p0
		} //*string
	case "pi":
		if x != nil {
			var p0 int
			cast.Convert(&p0, x)
			a.PI = &p0
		} //*int
	case "pb":
		if x != nil {
			var p0 bool
			cast.Convert(&p0, x)
			a.PB = &p0
		} //*bool
	case "pf":
		if x != nil {
			var p0 float64
			cast.Convert(&p0, x)
			a.PF = &p0
		} //*float64
	case "pt2":
		if x != nil {
			var p0 time.Time
			cast.Convert(&p0, x)
			a.Pt2 = &p0
		} //*time.Time
	case "pip":
		if x != nil {
			var p0 netip.Addr
			switch v1 := x.(type) {
			case nil:
			case netip.Addr:
				p0 = v1
			case *netip.Addr:
				if v1 != nil {
					p0 = *v1
				}
			case netip.Prefix:
				p0 = v1.Addr()
			default:
				// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
				if s, ok := any(&p0).(interface{ Scan(any) error }); !ok || s.Scan(v1) != nil {
					b, _ := jsoniter.Marshal(v1)
					_ = jsoniter.Unmarshal(b, &p0)
				}
			}
			a.Pip = &p0
		} //*netip.Addr
	case "ns":
		switch v0 := x.(type) {
		case nil:
		case sql.NullString:
			a.NS = v0
		case *sql.NullString:
			if v0 != nil {
				a.NS = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.NS).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.NS)
			}
		} //sql.NullString
	case "ni":
		switch v0 := x.(type) {
		case nil:
		case sql.NullInt64:
			a.NI = v0
		case *sql.NullInt64:
			if v0 != nil {
				a.NI = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.NI).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.NI)
			}
		} //sql.NullInt64
	case "nb":
		switch v0 := x.(type) {
		case nil:
		case sql.NullBool:
			a.NB = v0
		case *sql.NullBool:
			if v0 != nil {
				a.NB = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.NB).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.NB)
			}
		} //sql.NullBool
	case "nf":
		switch v0 := x.(type) {
		case nil:
		case sql.NullFloat64:
			a.NF = v0
		case *sql.NullFloat64:
			if v0 != nil {
				a.NF = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.NF).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.NF)
			}
		} //sql.NullFloat64
	case "nt":
		switch v0 := x.(type) {
		case nil:
		case sql.NullTime:
			a.NT = v0
		case *sql.NullTime:
			if v0 != nil {
				a.NT = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.NT).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.NT)
			}
		} //sql.NullTime
	case "ptx":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Text:
			a.Ptx = v0
		case *pgtype.Text:
			if v0 != nil {
				a.Ptx = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Ptx).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Ptx)
			}
		} //pgtype.Text
	case "pi8":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Int8:
			a.Pi8 = v0
		case *pgtype.Int8:
			if v0 != nil {
				a.Pi8 = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Pi8).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Pi8)
			}
		} //pgtype.Int8
	case "pbo":
		switch v0 := x.(type) {
		case nil:
		case pgtype.Bool:
			a.Pbo = v0
		case *pgtype.Bool:
			if v0 != nil {
				a.Pbo = *v0
			}

		default:
			// sql.Scanner: pgtype.*, sql.Null*, decimal.Decimal
			if s, ok := any(&a.Pbo).(interface{ Scan(any) error }); !ok || s.Scan(v0) != nil {
				b, _ := jsoniter.Marshal(v0)
				_ = jsoniter.Unmarshal(b, &a.Pbo)
			}
		} //pgtype.Bool
	case "st":
		var v int16
		cast.Convert(&v, x)
		a.ST = Status(v) //Status
	case "prov":
		var v string
		cast.Convert(&v, x)
		a.Prov = Provider(v) //Provider
	case "rl":
		var v int
		cast.Convert(&v, x)
		a.RL = role.Role(v) //role.Role
	case "nn":
		cast.Convert(&a.NN, x) //string
	}
}

// get struct value with function
func (a *All) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "b":
		return a.B //bool
	case "s":
		return a.S //string
	case "i":
		return a.I //int
	case "i8":
		return a.I8 //int8
	case "i16":
		return a.I16 //int16
	case "i32":
		return a.I32 //int32
	case "i64":
		return a.I64 //int64
	case "u":
		return a.U //uint
	case "u8":
		return a.U8 //uint8
	case "u16":
		return a.U16 //uint16
	case "u32":
		return a.U32 //uint32
	case "u64":
		return a.U64 //uint64
	case "f32":
		return a.F32 //float32
	case "f64":
		return a.F64 //float64
	case "r":
		return a.R //rune
	case "by":
		return a.BY //byte
	case "bytes":
		return a.Bytes //[]byte
	case "fixed":
		return a.Fixed //[16]byte
	case "uid":
		return a.UID //uuid.UUID
	case "puid":
		return a.Puid //pgtype.UUID
	case "raw":
		return a.Raw //json.RawMessage
	case "mapany":
		return a.Mapany //map[string]any
	case "maps":
		return a.Maps //map[string]string
	case "conf":
		return a.Conf //Config
	case "confs":
		return a.Confs //[]Config
	case "anyv":
		return a.Anyv //any
	case "t":
		return a.T //time.Time
	case "dur":
		return a.Dur //time.Duration
	case "date":
		return a.Date //time.Time
	case "pd":
		return a.PD //pgtype.Date
	case "pts":
		return a.Pts //pgtype.Timestamp
	case "piv":
		return a.Piv //pgtype.Interval
	case "lb":
		return a.LB //[]bool
	case "ls":
		return a.LS //[]string
	case "li":
		return a.LI //[]int
	case "li8":
		return a.Li8 //[]int8
	case "li16":
		return a.Li16 //[]int16
	case "li32":
		return a.Li32 //[]int32
	case "lu16":
		return a.Lu16 //[]uint16
	case "lu32":
		return a.Lu32 //[]uint32
	case "lu64":
		return a.Lu64 //[]uint64
	case "lf32":
		return a.Lf32 //[]float32
	case "lt":
		return a.LT //[]time.Time
	case "lu":
		return a.LU //[]uuid.UUID
	case "ls2":
		return a.Ls2 //[][]string
	case "li2":
		return a.Li2 //[][]int64
	case "lbb":
		return a.Lbb //[][]byte
	case "lm":
		return a.LM //[]map[string]any
	case "lst":
		return a.Lst //[]Status
	case "lrl":
		return a.Lrl //[]role.Role
	case "ip":
		return a.IP //netip.Addr
	case "pfx":
		return a.Pfx //netip.Prefix
	case "nip":
		return a.Nip //net.IP
	case "nnet":
		return a.Nnet //net.IPNet
	case "num":
		return a.Num //pgtype.Numeric
	case "dec":
		return a.Dec //decimal.Decimal
	case "rng":
		return a.Rng //pgtype.Range[pgtype.Int4]
	case "mrng":
		return a.Mrng //pgtype.Multirange[pgtype.Range[pgtype.Int8]]
	case "pt":
		return a.PT //pgtype.Point
	case "bits":
		return a.Bits //pgtype.Bits
	case "ps":
		return a.PS //*string
	case "pi":
		return a.PI //*int
	case "pb":
		return a.PB //*bool
	case "pf":
		return a.PF //*float64
	case "pt2":
		return a.Pt2 //*time.Time
	case "pip":
		return a.Pip //*netip.Addr
	case "ns":
		return a.NS //sql.NullString
	case "ni":
		return a.NI //sql.NullInt64
	case "nb":
		return a.NB //sql.NullBool
	case "nf":
		return a.NF //sql.NullFloat64
	case "nt":
		return a.NT //sql.NullTime
	case "ptx":
		return a.Ptx //pgtype.Text
	case "pi8":
		return a.Pi8 //pgtype.Int8
	case "pbo":
		return a.Pbo //pgtype.Bool
	case "st":
		return a.ST //Status
	case "prov":
		return a.Prov //Provider
	case "rl":
		return a.RL //role.Role
	case "nn":
		return a.NN //string
	}
	return
}

// get any struct value as string
func (a *All) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "b":
		return fmt.Sprint(a.B) //bool
	case "s":
		return fmt.Sprint(a.S) //string
	case "i":
		return fmt.Sprint(a.I) //int
	case "i8":
		return fmt.Sprint(a.I8) //int8
	case "i16":
		return fmt.Sprint(a.I16) //int16
	case "i32":
		return fmt.Sprint(a.I32) //int32
	case "i64":
		return fmt.Sprint(a.I64) //int64
	case "u":
		return fmt.Sprint(a.U) //uint
	case "u8":
		return fmt.Sprint(a.U8) //uint8
	case "u16":
		return fmt.Sprint(a.U16) //uint16
	case "u32":
		return fmt.Sprint(a.U32) //uint32
	case "u64":
		return fmt.Sprint(a.U64) //uint64
	case "f32":
		return fmt.Sprint(a.F32) //float32
	case "f64":
		return fmt.Sprint(a.F64) //float64
	case "r":
		return fmt.Sprint(a.R) //rune
	case "by":
		return fmt.Sprint(a.BY) //byte
	case "bytes":
		return fmt.Sprint(a.Bytes) //[]byte
	case "fixed":
		return fmt.Sprint(a.Fixed) //[16]byte
	case "uid":
		return fmt.Sprint(a.UID) //uuid.UUID
	case "puid":
		return fmt.Sprint(a.Puid) //pgtype.UUID
	case "raw":
		return fmt.Sprint(a.Raw) //json.RawMessage
	case "mapany":
		return fmt.Sprint(a.Mapany) //map[string]any
	case "maps":
		return fmt.Sprint(a.Maps) //map[string]string
	case "conf":
		return fmt.Sprint(a.Conf) //Config
	case "confs":
		return fmt.Sprint(a.Confs) //[]Config
	case "anyv":
		return fmt.Sprint(a.Anyv) //any
	case "t":
		return fmt.Sprint(a.T) //time.Time
	case "dur":
		return fmt.Sprint(a.Dur) //time.Duration
	case "date":
		return fmt.Sprint(a.Date) //time.Time
	case "pd":
		return fmt.Sprint(a.PD) //pgtype.Date
	case "pts":
		return fmt.Sprint(a.Pts) //pgtype.Timestamp
	case "piv":
		return fmt.Sprint(a.Piv) //pgtype.Interval
	case "lb":
		return fmt.Sprint(a.LB) //[]bool
	case "ls":
		return fmt.Sprint(a.LS) //[]string
	case "li":
		return fmt.Sprint(a.LI) //[]int
	case "li8":
		return fmt.Sprint(a.Li8) //[]int8
	case "li16":
		return fmt.Sprint(a.Li16) //[]int16
	case "li32":
		return fmt.Sprint(a.Li32) //[]int32
	case "lu16":
		return fmt.Sprint(a.Lu16) //[]uint16
	case "lu32":
		return fmt.Sprint(a.Lu32) //[]uint32
	case "lu64":
		return fmt.Sprint(a.Lu64) //[]uint64
	case "lf32":
		return fmt.Sprint(a.Lf32) //[]float32
	case "lt":
		return fmt.Sprint(a.LT) //[]time.Time
	case "lu":
		return fmt.Sprint(a.LU) //[]uuid.UUID
	case "ls2":
		return fmt.Sprint(a.Ls2) //[][]string
	case "li2":
		return fmt.Sprint(a.Li2) //[][]int64
	case "lbb":
		return fmt.Sprint(a.Lbb) //[][]byte
	case "lm":
		return fmt.Sprint(a.LM) //[]map[string]any
	case "lst":
		return fmt.Sprint(a.Lst) //[]Status
	case "lrl":
		return fmt.Sprint(a.Lrl) //[]role.Role
	case "ip":
		return fmt.Sprint(a.IP) //netip.Addr
	case "pfx":
		return fmt.Sprint(a.Pfx) //netip.Prefix
	case "nip":
		return fmt.Sprint(a.Nip) //net.IP
	case "nnet":
		return fmt.Sprint(a.Nnet) //net.IPNet
	case "num":
		return fmt.Sprint(a.Num) //pgtype.Numeric
	case "dec":
		return fmt.Sprint(a.Dec) //decimal.Decimal
	case "rng":
		return fmt.Sprint(a.Rng) //pgtype.Range[pgtype.Int4]
	case "mrng":
		return fmt.Sprint(a.Mrng) //pgtype.Multirange[pgtype.Range[pgtype.Int8]]
	case "pt":
		return fmt.Sprint(a.PT) //pgtype.Point
	case "bits":
		return fmt.Sprint(a.Bits) //pgtype.Bits
	case "ps":
		return fmt.Sprint(a.PS) //*string
	case "pi":
		return fmt.Sprint(a.PI) //*int
	case "pb":
		return fmt.Sprint(a.PB) //*bool
	case "pf":
		return fmt.Sprint(a.PF) //*float64
	case "pt2":
		return fmt.Sprint(a.Pt2) //*time.Time
	case "pip":
		return fmt.Sprint(a.Pip) //*netip.Addr
	case "ns":
		return fmt.Sprint(a.NS) //sql.NullString
	case "ni":
		return fmt.Sprint(a.NI) //sql.NullInt64
	case "nb":
		return fmt.Sprint(a.NB) //sql.NullBool
	case "nf":
		return fmt.Sprint(a.NF) //sql.NullFloat64
	case "nt":
		return fmt.Sprint(a.NT) //sql.NullTime
	case "ptx":
		return fmt.Sprint(a.Ptx) //pgtype.Text
	case "pi8":
		return fmt.Sprint(a.Pi8) //pgtype.Int8
	case "pbo":
		return fmt.Sprint(a.Pbo) //pgtype.Bool
	case "st":
		return fmt.Sprint(a.ST) //Status
	case "prov":
		return fmt.Sprint(a.Prov) //Provider
	case "rl":
		return fmt.Sprint(a.RL) //role.Role
	case "nn":
		return fmt.Sprint(a.NN) //string
	}
	return
}

// Struct to json
func (a *All) ToJson() (r []byte) {
	js := jsons.Create().
		Add(AllFieldID, a.ID).
		Add(AllFieldB, a.B).
		Add(AllFieldS, a.S).
		Add(AllFieldI, a.I).
		Add(AllFieldI8, a.I8).
		Add(AllFieldI16, a.I16).
		Add(AllFieldI32, a.I32).
		Add(AllFieldI64, a.I64).
		Add(AllFieldU, a.U).
		Add(AllFieldU8, a.U8).
		Add(AllFieldU16, a.U16).
		Add(AllFieldU32, a.U32).
		Add(AllFieldU64, a.U64).
		Add(AllFieldF32, a.F32).
		Add(AllFieldF64, a.F64).
		Add(AllFieldR, a.R).
		Add(AllFieldBY, a.BY).
		Add(AllFieldBytes, a.Bytes).
		Add(AllFieldFixed, a.Fixed).
		Add(AllFieldUID, a.UID).
		Add(AllFieldPuid, a.Puid).
		Add(AllFieldRaw, a.Raw).
		Add(AllFieldMapany, a.Mapany).
		Add(AllFieldMaps, a.Maps).
		Add(AllFieldConf, a.Conf).
		Add(AllFieldConfs, a.Confs).
		Add(AllFieldAnyv, a.Anyv).
		Add(AllFieldT, a.T).
		Add(AllFieldDur, a.Dur).
		Add(AllFieldDate, a.Date).
		Add(AllFieldPD, a.PD).
		Add(AllFieldPts, a.Pts).
		Add(AllFieldPiv, a.Piv).
		Add(AllFieldLB, a.LB).
		Add(AllFieldLS, a.LS).
		Add(AllFieldLI, a.LI).
		Add(AllFieldLi8, a.Li8).
		Add(AllFieldLi16, a.Li16).
		Add(AllFieldLi32, a.Li32).
		Add(AllFieldLu16, a.Lu16).
		Add(AllFieldLu32, a.Lu32).
		Add(AllFieldLu64, a.Lu64).
		Add(AllFieldLf32, a.Lf32).
		Add(AllFieldLT, a.LT).
		Add(AllFieldLU, a.LU).
		Add(AllFieldLs2, a.Ls2).
		Add(AllFieldLi2, a.Li2).
		Add(AllFieldLbb, a.Lbb).
		Add(AllFieldLM, a.LM).
		Add(AllFieldLst, a.Lst).
		Add(AllFieldLrl, a.Lrl).
		Add(AllFieldIP, a.IP).
		Add(AllFieldPfx, a.Pfx).
		Add(AllFieldNip, a.Nip).
		Add(AllFieldNnet, a.Nnet).
		Add(AllFieldNum, a.Num).
		Add(AllFieldDec, a.Dec).
		Add(AllFieldRng, a.Rng).
		Add(AllFieldMrng, a.Mrng).
		Add(AllFieldPT, a.PT).
		Add(AllFieldBits, a.Bits).
		Add(AllFieldPS, a.PS).
		Add(AllFieldPI, a.PI).
		Add(AllFieldPB, a.PB).
		Add(AllFieldPF, a.PF).
		Add(AllFieldPt2, a.Pt2).
		Add(AllFieldPip, a.Pip).
		Add(AllFieldNS, a.NS).
		Add(AllFieldNI, a.NI).
		Add(AllFieldNB, a.NB).
		Add(AllFieldNF, a.NF).
		Add(AllFieldNT, a.NT).
		Add(AllFieldPtx, a.Ptx).
		Add(AllFieldPi8, a.Pi8).
		Add(AllFieldPbo, a.Pbo).
		Add(AllFieldST, a.ST).
		Add(AllFieldProv, a.Prov).
		Add(AllFieldRL, a.RL).
		Add(AllFieldNN, a.NN)
	return js.Bytes()
}

// key index string
func (a AllIndexTyped) String() string {
	switch a {
	case AllIndexID:
		return "id"
	case AllIndexB:
		return "b"
	case AllIndexS:
		return "s"
	case AllIndexI:
		return "i"
	case AllIndexI8:
		return "i8"
	case AllIndexI16:
		return "i16"
	case AllIndexI32:
		return "i32"
	case AllIndexI64:
		return "i64"
	case AllIndexU:
		return "u"
	case AllIndexU8:
		return "u8"
	case AllIndexU16:
		return "u16"
	case AllIndexU32:
		return "u32"
	case AllIndexU64:
		return "u64"
	case AllIndexF32:
		return "f32"
	case AllIndexF64:
		return "f64"
	case AllIndexR:
		return "r"
	case AllIndexBY:
		return "by"
	case AllIndexBytes:
		return "bytes"
	case AllIndexFixed:
		return "fixed"
	case AllIndexUID:
		return "uid"
	case AllIndexPuid:
		return "puid"
	case AllIndexRaw:
		return "raw"
	case AllIndexMapany:
		return "mapany"
	case AllIndexMaps:
		return "maps"
	case AllIndexConf:
		return "conf"
	case AllIndexConfs:
		return "confs"
	case AllIndexAnyv:
		return "anyv"
	case AllIndexT:
		return "t"
	case AllIndexDur:
		return "dur"
	case AllIndexDate:
		return "date"
	case AllIndexPD:
		return "pd"
	case AllIndexPts:
		return "pts"
	case AllIndexPiv:
		return "piv"
	case AllIndexLB:
		return "lb"
	case AllIndexLS:
		return "ls"
	case AllIndexLI:
		return "li"
	case AllIndexLi8:
		return "li8"
	case AllIndexLi16:
		return "li16"
	case AllIndexLi32:
		return "li32"
	case AllIndexLu16:
		return "lu16"
	case AllIndexLu32:
		return "lu32"
	case AllIndexLu64:
		return "lu64"
	case AllIndexLf32:
		return "lf32"
	case AllIndexLT:
		return "lt"
	case AllIndexLU:
		return "lu"
	case AllIndexLs2:
		return "ls2"
	case AllIndexLi2:
		return "li2"
	case AllIndexLbb:
		return "lbb"
	case AllIndexLM:
		return "lm"
	case AllIndexLst:
		return "lst"
	case AllIndexLrl:
		return "lrl"
	case AllIndexIP:
		return "ip"
	case AllIndexPfx:
		return "pfx"
	case AllIndexNip:
		return "nip"
	case AllIndexNnet:
		return "nnet"
	case AllIndexNum:
		return "num"
	case AllIndexDec:
		return "dec"
	case AllIndexRng:
		return "rng"
	case AllIndexMrng:
		return "mrng"
	case AllIndexPT:
		return "pt"
	case AllIndexBits:
		return "bits"
	case AllIndexPS:
		return "ps"
	case AllIndexPI:
		return "pi"
	case AllIndexPB:
		return "pb"
	case AllIndexPF:
		return "pf"
	case AllIndexPt2:
		return "pt2"
	case AllIndexPip:
		return "pip"
	case AllIndexNS:
		return "ns"
	case AllIndexNI:
		return "ni"
	case AllIndexNB:
		return "nb"
	case AllIndexNF:
		return "nf"
	case AllIndexNT:
		return "nt"
	case AllIndexPtx:
		return "ptx"
	case AllIndexPi8:
		return "pi8"
	case AllIndexPbo:
		return "pbo"
	case AllIndexST:
		return "st"
	case AllIndexProv:
		return "prov"
	case AllIndexRL:
		return "rl"
	case AllIndexNN:
		return "nn"
	default:
		return ""
	}
}

// key index string
func (a AllIndexTyped) SQLName() string {
	switch a {
	case AllIndexID:
		return "id"
	case AllIndexB:
		return "b"
	case AllIndexS:
		return "s"
	case AllIndexI:
		return "i"
	case AllIndexI8:
		return "i8"
	case AllIndexI16:
		return "i16"
	case AllIndexI32:
		return "i32"
	case AllIndexI64:
		return "i64"
	case AllIndexU:
		return "u"
	case AllIndexU8:
		return "u8"
	case AllIndexU16:
		return "u16"
	case AllIndexU32:
		return "u32"
	case AllIndexU64:
		return "u64"
	case AllIndexF32:
		return "f32"
	case AllIndexF64:
		return "f64"
	case AllIndexR:
		return "r"
	case AllIndexBY:
		return "by"
	case AllIndexBytes:
		return "bytes"
	case AllIndexFixed:
		return "fixed"
	case AllIndexUID:
		return "uid"
	case AllIndexPuid:
		return "puid"
	case AllIndexRaw:
		return "raw"
	case AllIndexMapany:
		return "mapany"
	case AllIndexMaps:
		return "maps"
	case AllIndexConf:
		return "conf"
	case AllIndexConfs:
		return "confs"
	case AllIndexAnyv:
		return "anyv"
	case AllIndexT:
		return "t"
	case AllIndexDur:
		return "dur"
	case AllIndexDate:
		return "date"
	case AllIndexPD:
		return "pd"
	case AllIndexPts:
		return "pts"
	case AllIndexPiv:
		return "piv"
	case AllIndexLB:
		return "lb"
	case AllIndexLS:
		return "ls"
	case AllIndexLI:
		return "li"
	case AllIndexLi8:
		return "li8"
	case AllIndexLi16:
		return "li16"
	case AllIndexLi32:
		return "li32"
	case AllIndexLu16:
		return "lu16"
	case AllIndexLu32:
		return "lu32"
	case AllIndexLu64:
		return "lu64"
	case AllIndexLf32:
		return "lf32"
	case AllIndexLT:
		return "lt"
	case AllIndexLU:
		return "lu"
	case AllIndexLs2:
		return "ls2"
	case AllIndexLi2:
		return "li2"
	case AllIndexLbb:
		return "lbb"
	case AllIndexLM:
		return "lm"
	case AllIndexLst:
		return "lst"
	case AllIndexLrl:
		return "lrl"
	case AllIndexIP:
		return "ip"
	case AllIndexPfx:
		return "pfx"
	case AllIndexNip:
		return "nip"
	case AllIndexNnet:
		return "nnet"
	case AllIndexNum:
		return "num"
	case AllIndexDec:
		return "dec"
	case AllIndexRng:
		return "rng"
	case AllIndexMrng:
		return "mrng"
	case AllIndexPT:
		return "pt"
	case AllIndexBits:
		return "bits"
	case AllIndexPS:
		return "ps"
	case AllIndexPI:
		return "pi"
	case AllIndexPB:
		return "pb"
	case AllIndexPF:
		return "pf"
	case AllIndexPt2:
		return "pt2"
	case AllIndexPip:
		return "pip"
	case AllIndexNS:
		return "ns"
	case AllIndexNI:
		return "ni"
	case AllIndexNB:
		return "nb"
	case AllIndexNF:
		return "nf"
	case AllIndexNT:
		return "nt"
	case AllIndexPtx:
		return "ptx"
	case AllIndexPi8:
		return "pi8"
	case AllIndexPbo:
		return "pbo"
	case AllIndexST:
		return "st"
	case AllIndexProv:
		return "prov"
	case AllIndexRL:
		return "rl"
	case AllIndexNN:
		return "nn"
	default:
		return ""
	}
}

// key index clickhouse string
func (a AllIndexTyped) ClickhouseName() string {
	switch a {
	case AllIndexID:
		return "id"
	case AllIndexB:
		return "b"
	case AllIndexS:
		return "s"
	case AllIndexI:
		return "i"
	case AllIndexI8:
		return "i8"
	case AllIndexI16:
		return "i16"
	case AllIndexI32:
		return "i32"
	case AllIndexI64:
		return "i64"
	case AllIndexU:
		return "u"
	case AllIndexU8:
		return "u8"
	case AllIndexU16:
		return "u16"
	case AllIndexU32:
		return "u32"
	case AllIndexU64:
		return "u64"
	case AllIndexF32:
		return "f32"
	case AllIndexF64:
		return "f64"
	case AllIndexR:
		return "r"
	case AllIndexBY:
		return "by"
	case AllIndexBytes:
		return "bytes"
	case AllIndexFixed:
		return "fixed"
	case AllIndexUID:
		return "uid"
	case AllIndexPuid:
		return "puid"
	case AllIndexRaw:
		return "raw"
	case AllIndexMapany:
		return "mapany"
	case AllIndexMaps:
		return "maps"
	case AllIndexConf:
		return "conf"
	case AllIndexConfs:
		return "confs"
	case AllIndexAnyv:
		return "anyv"
	case AllIndexT:
		return "t"
	case AllIndexDur:
		return "dur"
	case AllIndexDate:
		return "date"
	case AllIndexPD:
		return "pd"
	case AllIndexPts:
		return "pts"
	case AllIndexPiv:
		return "piv"
	case AllIndexLB:
		return "lb"
	case AllIndexLS:
		return "ls"
	case AllIndexLI:
		return "li"
	case AllIndexLi8:
		return "li8"
	case AllIndexLi16:
		return "li16"
	case AllIndexLi32:
		return "li32"
	case AllIndexLu16:
		return "lu16"
	case AllIndexLu32:
		return "lu32"
	case AllIndexLu64:
		return "lu64"
	case AllIndexLf32:
		return "lf32"
	case AllIndexLT:
		return "lt"
	case AllIndexLU:
		return "lu"
	case AllIndexLs2:
		return "ls2"
	case AllIndexLi2:
		return "li2"
	case AllIndexLbb:
		return "lbb"
	case AllIndexLM:
		return "lm"
	case AllIndexLst:
		return "lst"
	case AllIndexLrl:
		return "lrl"
	case AllIndexIP:
		return "ip"
	case AllIndexPfx:
		return "pfx"
	case AllIndexNip:
		return "nip"
	case AllIndexNnet:
		return "nnet"
	case AllIndexNum:
		return "num"
	case AllIndexDec:
		return "dec"
	case AllIndexRng:
		return "rng"
	case AllIndexMrng:
		return "mrng"
	case AllIndexPT:
		return "pt"
	case AllIndexBits:
		return "bits"
	case AllIndexPS:
		return "ps"
	case AllIndexPI:
		return "pi"
	case AllIndexPB:
		return "pb"
	case AllIndexPF:
		return "pf"
	case AllIndexPt2:
		return "pt2"
	case AllIndexPip:
		return "pip"
	case AllIndexNS:
		return "ns"
	case AllIndexNI:
		return "ni"
	case AllIndexNB:
		return "nb"
	case AllIndexNF:
		return "nf"
	case AllIndexNT:
		return "nt"
	case AllIndexPtx:
		return "ptx"
	case AllIndexPi8:
		return "pi8"
	case AllIndexPbo:
		return "pbo"
	case AllIndexST:
		return "st"
	case AllIndexProv:
		return "prov"
	case AllIndexRL:
		return "rl"
	case AllIndexNN:
		return "nn"
	default:
		return ""
	}
}

// key index type
func (a AllIndexTyped) Type() string {
	switch a {
	case AllIndexID:
		return "int"
	case AllIndexB:
		return "bool"
	case AllIndexS:
		return "string"
	case AllIndexI:
		return "int"
	case AllIndexI8:
		return "int8"
	case AllIndexI16:
		return "int16"
	case AllIndexI32:
		return "int32"
	case AllIndexI64:
		return "int64"
	case AllIndexU:
		return "uint"
	case AllIndexU8:
		return "uint8"
	case AllIndexU16:
		return "uint16"
	case AllIndexU32:
		return "uint32"
	case AllIndexU64:
		return "uint64"
	case AllIndexF32:
		return "float32"
	case AllIndexF64:
		return "float64"
	case AllIndexR:
		return "rune"
	case AllIndexBY:
		return "byte"
	case AllIndexBytes:
		return "[]byte"
	case AllIndexFixed:
		return "[16]byte"
	case AllIndexUID:
		return "uuid.UUID"
	case AllIndexPuid:
		return "pgtype.UUID"
	case AllIndexRaw:
		return "json.RawMessage"
	case AllIndexMapany:
		return "map[string]any"
	case AllIndexMaps:
		return "map[string]string"
	case AllIndexConf:
		return "Config"
	case AllIndexConfs:
		return "[]Config"
	case AllIndexAnyv:
		return "any"
	case AllIndexT:
		return "time.Time"
	case AllIndexDur:
		return "time.Duration"
	case AllIndexDate:
		return "time.Time"
	case AllIndexPD:
		return "pgtype.Date"
	case AllIndexPts:
		return "pgtype.Timestamp"
	case AllIndexPiv:
		return "pgtype.Interval"
	case AllIndexLB:
		return "[]bool"
	case AllIndexLS:
		return "[]string"
	case AllIndexLI:
		return "[]int"
	case AllIndexLi8:
		return "[]int8"
	case AllIndexLi16:
		return "[]int16"
	case AllIndexLi32:
		return "[]int32"
	case AllIndexLu16:
		return "[]uint16"
	case AllIndexLu32:
		return "[]uint32"
	case AllIndexLu64:
		return "[]uint64"
	case AllIndexLf32:
		return "[]float32"
	case AllIndexLT:
		return "[]time.Time"
	case AllIndexLU:
		return "[]uuid.UUID"
	case AllIndexLs2:
		return "[][]string"
	case AllIndexLi2:
		return "[][]int64"
	case AllIndexLbb:
		return "[][]byte"
	case AllIndexLM:
		return "[]map[string]any"
	case AllIndexLst:
		return "[]Status"
	case AllIndexLrl:
		return "[]role.Role"
	case AllIndexIP:
		return "netip.Addr"
	case AllIndexPfx:
		return "netip.Prefix"
	case AllIndexNip:
		return "net.IP"
	case AllIndexNnet:
		return "net.IPNet"
	case AllIndexNum:
		return "pgtype.Numeric"
	case AllIndexDec:
		return "decimal.Decimal"
	case AllIndexRng:
		return "pgtype.Range[pgtype.Int4]"
	case AllIndexMrng:
		return "pgtype.Multirange[pgtype.Range[pgtype.Int8]]"
	case AllIndexPT:
		return "pgtype.Point"
	case AllIndexBits:
		return "pgtype.Bits"
	case AllIndexPS:
		return "*string"
	case AllIndexPI:
		return "*int"
	case AllIndexPB:
		return "*bool"
	case AllIndexPF:
		return "*float64"
	case AllIndexPt2:
		return "*time.Time"
	case AllIndexPip:
		return "*netip.Addr"
	case AllIndexNS:
		return "sql.NullString"
	case AllIndexNI:
		return "sql.NullInt64"
	case AllIndexNB:
		return "sql.NullBool"
	case AllIndexNF:
		return "sql.NullFloat64"
	case AllIndexNT:
		return "sql.NullTime"
	case AllIndexPtx:
		return "pgtype.Text"
	case AllIndexPi8:
		return "pgtype.Int8"
	case AllIndexPbo:
		return "pgtype.Bool"
	case AllIndexST:
		return "Status"
	case AllIndexProv:
		return "Provider"
	case AllIndexRL:
		return "role.Role"
	case AllIndexNN:
		return "string"
	default:
		return ""
	}
}

// custom title
func (a AllIndexTyped) Title() string {
	switch a {
	case AllIndexID:
		return "ID"
	case AllIndexB:
		return "B"
	case AllIndexS:
		return "S"
	case AllIndexI:
		return "I"
	case AllIndexI8:
		return "I8"
	case AllIndexI16:
		return "I16"
	case AllIndexI32:
		return "I32"
	case AllIndexI64:
		return "I64"
	case AllIndexU:
		return "U"
	case AllIndexU8:
		return "U8"
	case AllIndexU16:
		return "U16"
	case AllIndexU32:
		return "U32"
	case AllIndexU64:
		return "U64"
	case AllIndexF32:
		return "F32"
	case AllIndexF64:
		return "F64"
	case AllIndexR:
		return "R"
	case AllIndexBY:
		return "BY"
	case AllIndexBytes:
		return "Bytes"
	case AllIndexFixed:
		return "Fixed"
	case AllIndexUID:
		return "UID"
	case AllIndexPuid:
		return "Puid"
	case AllIndexRaw:
		return "Raw"
	case AllIndexMapany:
		return "Mapany"
	case AllIndexMaps:
		return "Maps"
	case AllIndexConf:
		return "Conf"
	case AllIndexConfs:
		return "Confs"
	case AllIndexAnyv:
		return "Anyv"
	case AllIndexT:
		return "T"
	case AllIndexDur:
		return "Dur"
	case AllIndexDate:
		return "Date"
	case AllIndexPD:
		return "PD"
	case AllIndexPts:
		return "Pts"
	case AllIndexPiv:
		return "Piv"
	case AllIndexLB:
		return "LB"
	case AllIndexLS:
		return "LS"
	case AllIndexLI:
		return "LI"
	case AllIndexLi8:
		return "Li8"
	case AllIndexLi16:
		return "Li16"
	case AllIndexLi32:
		return "Li32"
	case AllIndexLu16:
		return "Lu16"
	case AllIndexLu32:
		return "Lu32"
	case AllIndexLu64:
		return "Lu64"
	case AllIndexLf32:
		return "Lf32"
	case AllIndexLT:
		return "LT"
	case AllIndexLU:
		return "LU"
	case AllIndexLs2:
		return "Ls2"
	case AllIndexLi2:
		return "Li2"
	case AllIndexLbb:
		return "Lbb"
	case AllIndexLM:
		return "LM"
	case AllIndexLst:
		return "Lst"
	case AllIndexLrl:
		return "Lrl"
	case AllIndexIP:
		return "IP"
	case AllIndexPfx:
		return "Pfx"
	case AllIndexNip:
		return "Nip"
	case AllIndexNnet:
		return "Nnet"
	case AllIndexNum:
		return "Num"
	case AllIndexDec:
		return "Dec"
	case AllIndexRng:
		return "Rng"
	case AllIndexMrng:
		return "Mrng"
	case AllIndexPT:
		return "PT"
	case AllIndexBits:
		return "Bits"
	case AllIndexPS:
		return "PS"
	case AllIndexPI:
		return "PI"
	case AllIndexPB:
		return "PB"
	case AllIndexPF:
		return "PF"
	case AllIndexPt2:
		return "Pt2"
	case AllIndexPip:
		return "Pip"
	case AllIndexNS:
		return "NS"
	case AllIndexNI:
		return "NI"
	case AllIndexNB:
		return "NB"
	case AllIndexNF:
		return "NF"
	case AllIndexNT:
		return "NT"
	case AllIndexPtx:
		return "Ptx"
	case AllIndexPi8:
		return "Pi8"
	case AllIndexPbo:
		return "Pbo"
	case AllIndexST:
		return "ST"
	case AllIndexProv:
		return "Prov"
	case AllIndexRL:
		return "RL"
	case AllIndexNN:
		return "NN"
	default:
		return ""
	}
}

// custom desc
func (a AllIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func AllKeyIndex(key string) AllIndexTyped {
	switch key {
	case "id":
		return AllIndexID
	case "b":
		return AllIndexB
	case "s":
		return AllIndexS
	case "i":
		return AllIndexI
	case "i8":
		return AllIndexI8
	case "i16":
		return AllIndexI16
	case "i32":
		return AllIndexI32
	case "i64":
		return AllIndexI64
	case "u":
		return AllIndexU
	case "u8":
		return AllIndexU8
	case "u16":
		return AllIndexU16
	case "u32":
		return AllIndexU32
	case "u64":
		return AllIndexU64
	case "f32":
		return AllIndexF32
	case "f64":
		return AllIndexF64
	case "r":
		return AllIndexR
	case "by":
		return AllIndexBY
	case "bytes":
		return AllIndexBytes
	case "fixed":
		return AllIndexFixed
	case "uid":
		return AllIndexUID
	case "puid":
		return AllIndexPuid
	case "raw":
		return AllIndexRaw
	case "mapany":
		return AllIndexMapany
	case "maps":
		return AllIndexMaps
	case "conf":
		return AllIndexConf
	case "confs":
		return AllIndexConfs
	case "anyv":
		return AllIndexAnyv
	case "t":
		return AllIndexT
	case "dur":
		return AllIndexDur
	case "date":
		return AllIndexDate
	case "pd":
		return AllIndexPD
	case "pts":
		return AllIndexPts
	case "piv":
		return AllIndexPiv
	case "lb":
		return AllIndexLB
	case "ls":
		return AllIndexLS
	case "li":
		return AllIndexLI
	case "li8":
		return AllIndexLi8
	case "li16":
		return AllIndexLi16
	case "li32":
		return AllIndexLi32
	case "lu16":
		return AllIndexLu16
	case "lu32":
		return AllIndexLu32
	case "lu64":
		return AllIndexLu64
	case "lf32":
		return AllIndexLf32
	case "lt":
		return AllIndexLT
	case "lu":
		return AllIndexLU
	case "ls2":
		return AllIndexLs2
	case "li2":
		return AllIndexLi2
	case "lbb":
		return AllIndexLbb
	case "lm":
		return AllIndexLM
	case "lst":
		return AllIndexLst
	case "lrl":
		return AllIndexLrl
	case "ip":
		return AllIndexIP
	case "pfx":
		return AllIndexPfx
	case "nip":
		return AllIndexNip
	case "nnet":
		return AllIndexNnet
	case "num":
		return AllIndexNum
	case "dec":
		return AllIndexDec
	case "rng":
		return AllIndexRng
	case "mrng":
		return AllIndexMrng
	case "pt":
		return AllIndexPT
	case "bits":
		return AllIndexBits
	case "ps":
		return AllIndexPS
	case "pi":
		return AllIndexPI
	case "pb":
		return AllIndexPB
	case "pf":
		return AllIndexPF
	case "pt2":
		return AllIndexPt2
	case "pip":
		return AllIndexPip
	case "ns":
		return AllIndexNS
	case "ni":
		return AllIndexNI
	case "nb":
		return AllIndexNB
	case "nf":
		return AllIndexNF
	case "nt":
		return AllIndexNT
	case "ptx":
		return AllIndexPtx
	case "pi8":
		return AllIndexPi8
	case "pbo":
		return AllIndexPbo
	case "st":
		return AllIndexST
	case "prov":
		return AllIndexProv
	case "rl":
		return AllIndexRL
	case "nn":
		return AllIndexNN
	default:
		return 0
	}
}

// valid struct key check
func AllValidKey(key string) bool {
	switch key {
	case "id", "b", "s", "i", "i8", "i16", "i32", "i64", "u", "u8", "u16", "u32", "u64", "f32", "f64", "r", "by", "bytes", "fixed", "uid", "puid", "raw", "mapany", "maps", "conf", "confs", "anyv", "t", "dur", "date", "pd", "pts", "piv", "lb", "ls", "li", "li8", "li16", "li32", "lu16", "lu32", "lu64", "lf32", "lt", "lu", "ls2", "li2", "lbb", "lm", "lst", "lrl", "ip", "pfx", "nip", "nnet", "num", "dec", "rng", "mrng", "pt", "bits", "ps", "pi", "pb", "pf", "pt2", "pip", "ns", "ni", "nb", "nf", "nt", "ptx", "pi8", "pbo", "st", "prov", "rl", "nn":
		return true
	default:
		return false
	}
}

// struct to map
func (a *All) Map() map[string]any {
	return map[string]any{
		"id":     a.ID,
		"b":      a.B,
		"s":      a.S,
		"i":      a.I,
		"i8":     a.I8,
		"i16":    a.I16,
		"i32":    a.I32,
		"i64":    a.I64,
		"u":      a.U,
		"u8":     a.U8,
		"u16":    a.U16,
		"u32":    a.U32,
		"u64":    a.U64,
		"f32":    a.F32,
		"f64":    a.F64,
		"r":      a.R,
		"by":     a.BY,
		"bytes":  a.Bytes,
		"fixed":  a.Fixed,
		"uid":    a.UID,
		"puid":   a.Puid,
		"raw":    a.Raw,
		"mapany": a.Mapany,
		"maps":   a.Maps,
		"conf":   a.Conf,
		"confs":  a.Confs,
		"anyv":   a.Anyv,
		"t":      a.T,
		"dur":    a.Dur,
		"date":   a.Date,
		"pd":     a.PD,
		"pts":    a.Pts,
		"piv":    a.Piv,
		"lb":     a.LB,
		"ls":     a.LS,
		"li":     a.LI,
		"li8":    a.Li8,
		"li16":   a.Li16,
		"li32":   a.Li32,
		"lu16":   a.Lu16,
		"lu32":   a.Lu32,
		"lu64":   a.Lu64,
		"lf32":   a.Lf32,
		"lt":     a.LT,
		"lu":     a.LU,
		"ls2":    a.Ls2,
		"li2":    a.Li2,
		"lbb":    a.Lbb,
		"lm":     a.LM,
		"lst":    a.Lst,
		"lrl":    a.Lrl,
		"ip":     a.IP,
		"pfx":    a.Pfx,
		"nip":    a.Nip,
		"nnet":   a.Nnet,
		"num":    a.Num,
		"dec":    a.Dec,
		"rng":    a.Rng,
		"mrng":   a.Mrng,
		"pt":     a.PT,
		"bits":   a.Bits,
		"ps":     a.PS,
		"pi":     a.PI,
		"pb":     a.PB,
		"pf":     a.PF,
		"pt2":    a.Pt2,
		"pip":    a.Pip,
		"ns":     a.NS,
		"ni":     a.NI,
		"nb":     a.NB,
		"nf":     a.NF,
		"nt":     a.NT,
		"ptx":    a.Ptx,
		"pi8":    a.Pi8,
		"pbo":    a.Pbo,
		"st":     a.ST,
		"prov":   a.Prov,
		"rl":     a.RL,
		"nn":     a.NN,
	}
}

// struct to map
func (a *All) Iterate(f func(k AllIndexTyped, v any)) {
	for _, x := range AllIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *All) Pack() []byte {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	b, _ := jsoner.Marshal(a)
	return b
}

// fast json unmarshal
func ParseAll(v []byte) (a All, err error) {
	var jsoner = jsoniter.ConfigCompatibleWithStandardLibrary
	err = jsoner.Unmarshal(v, &a)
	return
}
