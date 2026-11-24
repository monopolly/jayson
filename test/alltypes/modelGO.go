package alltypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/jayson/test/role"
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

// Tuple create an array from struct
func (a *All) Tuple() (r []any) {
	return []any{a.ID, a.B, a.S, a.I, a.I8, a.I16, a.I32, a.I64, a.U, a.U8, a.U16, a.U32, a.U64, a.F32, a.F64, a.R, a.BY, a.Bytes, a.Fixed, a.UID, a.Puid, a.Raw, a.Mapany, a.Maps, a.Conf, a.Confs, a.Anyv, a.T, a.Dur, a.Date, a.PD, a.Pts, a.Piv, a.LB, a.LS, a.LI, a.Li8, a.Li16, a.Li32, a.Lu16, a.Lu32, a.Lu64, a.Lf32, a.LT, a.LU, a.Ls2, a.Li2, a.Lbb, a.LM, a.Lst, a.Lrl, a.IP, a.Pfx, a.Nip, a.Nnet, a.Num, a.Dec, a.Rng, a.Mrng, a.PT, a.Bits, a.PS, a.PI, a.PB, a.PF, a.Pt2, a.Pip, a.NS, a.NI, a.NB, a.NF, a.NT, a.Ptx, a.Pi8, a.Pbo, a.ST, a.Prov, a.RL, a.NN}
}

// Tuple create an array from struct for clickhouse
func (a *All) clickhouseTuple() (r []any) {
	return []any{a.ID, a.B, a.S, a.I, a.I8, a.I16, a.I32, a.I64, a.U, a.U8, a.U16, a.U32, a.U64, a.F32, a.F64, a.R, a.BY, a.Bytes, a.Fixed, a.UID, a.Puid, a.Raw, a.Mapany, a.Maps, a.Conf, a.Confs, a.Anyv, a.T, a.Dur, a.Date, a.PD, a.Pts, a.Piv, a.LB, a.LS, a.LI, a.Li8, a.Li16, a.Li32, a.Lu16, a.Lu32, a.Lu64, a.Lf32, a.LT, a.LU, a.Ls2, a.Li2, a.Lbb, a.LM, a.Lst, a.Lrl, a.IP, a.Pfx, a.Nip, a.Nnet, a.Num, a.Dec, a.Rng, a.Mrng, a.PT, a.Bits, a.PS, a.PI, a.PB, a.PF, a.Pt2, a.Pip, a.NS, a.NI, a.NB, a.NF, a.NT, a.Ptx, a.Pi8, a.Pbo, int16(a.ST), string(a.Prov), int(a.RL), a.NN}
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
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseAll(v []byte) (a All, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}

// allSQLDB runs queries: pool or transaction
type allSQLDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// AllSQL sql queries of table alltypes
//
// Rules:
//   - safe for concurrent use: queries run on pgxpool, the class keeps no state
//   - not found row is pgx.ErrNoRows
//   - fields: columns to select, all columns when empty
//   - where: sql condition with placeholders $1, $2... for args: "title = $1 and views > $2"
type AllSQL struct {
	pool *pgxpool.Pool
	db   allSQLDB // pool, or transaction after TX
}

// NewAllSQL creates queries on pool, table and indexes if not exist
func NewAllSQL(pool *pgxpool.Pool) (*AllSQL, error) {
	a := &AllSQL{pool: pool, db: pool}
	if err := a.CreateTable(); err != nil {
		return nil, err
	}
	return a, a.CreateIndexes()
}

// Pool returns pool
func (a *AllSQL) Pool() *pgxpool.Pool {
	return a.pool
}

// Conn runs f on a connection of pool, for own queries.
// Connection is always taken from pool: it is out of transaction of TX.
// Own methods of AllSQL can use a.db: pool, or transaction after TX
//
//	err := db.Conn(func(conn *pgxpool.Conn) error {
//		_, err := conn.Exec(context.Background(), "vacuum alltypes")
//		return err
//	})
func (a *AllSQL) Conn(f func(conn *pgxpool.Conn) error) error {
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
func (a *AllSQL) Begin(f func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(context.Background(), a.db, f)
}

// TX returns queries in transaction tx
func (a *AllSQL) TX(tx pgx.Tx) *AllSQL {
	return &AllSQL{pool: a.pool, db: tx}
}

// TableName returns sql table name
func (a *AllSQL) TableName() string {
	return "alltypes"
}

// Fields returns all sql columns
func (a *AllSQL) Fields() []AllIndexTyped {
	return slices.Clone(allSQLFields)
}

// ---------------------------------------------------------------------------
// columns
// ---------------------------------------------------------------------------

// all sql columns
var allSQLFields = []AllIndexTyped{AllIndexID, AllIndexB, AllIndexS, AllIndexI, AllIndexI8, AllIndexI16, AllIndexI32, AllIndexI64, AllIndexU, AllIndexU8, AllIndexU16, AllIndexU32, AllIndexU64, AllIndexF32, AllIndexF64, AllIndexR, AllIndexBY, AllIndexBytes, AllIndexFixed, AllIndexUID, AllIndexPuid, AllIndexRaw, AllIndexMapany, AllIndexMaps, AllIndexConf, AllIndexConfs, AllIndexAnyv, AllIndexT, AllIndexDur, AllIndexDate, AllIndexPD, AllIndexPts, AllIndexPiv, AllIndexLB, AllIndexLS, AllIndexLI, AllIndexLi8, AllIndexLi16, AllIndexLi32, AllIndexLu16, AllIndexLu32, AllIndexLu64, AllIndexLf32, AllIndexLT, AllIndexLU, AllIndexLs2, AllIndexLi2, AllIndexLbb, AllIndexLM, AllIndexLst, AllIndexLrl, AllIndexIP, AllIndexPfx, AllIndexNip, AllIndexNnet, AllIndexNum, AllIndexDec, AllIndexRng, AllIndexMrng, AllIndexPT, AllIndexBits, AllIndexPS, AllIndexPI, AllIndexPB, AllIndexPF, AllIndexPt2, AllIndexPip, AllIndexNS, AllIndexNI, AllIndexNB, AllIndexNF, AllIndexNT, AllIndexPtx, AllIndexPi8, AllIndexPbo, AllIndexST, AllIndexProv, AllIndexRL, AllIndexNN}

// columns of insert
var allSQLInsertColumns = []string{"b", "s", "i", "i8", "i16", "i32", "i64", "u", "u8", "u16", "u32", "u64", "f32", "f64", "r", "by", "bytes", "fixed", "uid", "puid", "raw", "mapany", "maps", "conf", "confs", "anyv", "t", "dur", "date", "pd", "pts", "piv", "lb", "ls", "li", "li8", "li16", "li32", "lu16", "lu32", "lu64", "lf32", "lt", "lu", "ls2", "li2", "lbb", "lm", "lst", "lrl", "ip", "pfx", "nip", "nnet", "num", "dec", "rng", "mrng", "pt", "bits", "ps", "pi", "pb", "pf", "pt2", "pip", "ns", "ni", "nb", "nf", "nt", "ptx", "pi8", "pbo", "st", "prov", "rl", "nn"}

// sql column names
var allSQLColumns = [...]string{
	AllIndexID:     "id",
	AllIndexB:      "b",
	AllIndexS:      "s",
	AllIndexI:      "i",
	AllIndexI8:     "i8",
	AllIndexI16:    "i16",
	AllIndexI32:    "i32",
	AllIndexI64:    "i64",
	AllIndexU:      "u",
	AllIndexU8:     "u8",
	AllIndexU16:    "u16",
	AllIndexU32:    "u32",
	AllIndexU64:    "u64",
	AllIndexF32:    "f32",
	AllIndexF64:    "f64",
	AllIndexR:      "r",
	AllIndexBY:     "by",
	AllIndexBytes:  "bytes",
	AllIndexFixed:  "fixed",
	AllIndexUID:    "uid",
	AllIndexPuid:   "puid",
	AllIndexRaw:    "raw",
	AllIndexMapany: "mapany",
	AllIndexMaps:   "maps",
	AllIndexConf:   "conf",
	AllIndexConfs:  "confs",
	AllIndexAnyv:   "anyv",
	AllIndexT:      "t",
	AllIndexDur:    "dur",
	AllIndexDate:   "date",
	AllIndexPD:     "pd",
	AllIndexPts:    "pts",
	AllIndexPiv:    "piv",
	AllIndexLB:     "lb",
	AllIndexLS:     "ls",
	AllIndexLI:     "li",
	AllIndexLi8:    "li8",
	AllIndexLi16:   "li16",
	AllIndexLi32:   "li32",
	AllIndexLu16:   "lu16",
	AllIndexLu32:   "lu32",
	AllIndexLu64:   "lu64",
	AllIndexLf32:   "lf32",
	AllIndexLT:     "lt",
	AllIndexLU:     "lu",
	AllIndexLs2:    "ls2",
	AllIndexLi2:    "li2",
	AllIndexLbb:    "lbb",
	AllIndexLM:     "lm",
	AllIndexLst:    "lst",
	AllIndexLrl:    "lrl",
	AllIndexIP:     "ip",
	AllIndexPfx:    "pfx",
	AllIndexNip:    "nip",
	AllIndexNnet:   "nnet",
	AllIndexNum:    "num",
	AllIndexDec:    "dec",
	AllIndexRng:    "rng",
	AllIndexMrng:   "mrng",
	AllIndexPT:     "pt",
	AllIndexBits:   "bits",
	AllIndexPS:     "ps",
	AllIndexPI:     "pi",
	AllIndexPB:     "pb",
	AllIndexPF:     "pf",
	AllIndexPt2:    "pt2",
	AllIndexPip:    "pip",
	AllIndexNS:     "ns",
	AllIndexNI:     "ni",
	AllIndexNB:     "nb",
	AllIndexNF:     "nf",
	AllIndexNT:     "nt",
	AllIndexPtx:    "ptx",
	AllIndexPi8:    "pi8",
	AllIndexPbo:    "pbo",
	AllIndexST:     "st",
	AllIndexProv:   "prov",
	AllIndexRL:     "rl",
	AllIndexNN:     "nn",
}

// select expressions: null of scalar column is zero value
var allSQLSelects = [...]string{
	AllIndexID:     "coalesce(id, 0)",
	AllIndexB:      "coalesce(b, false)",
	AllIndexS:      "coalesce(s, '')",
	AllIndexI:      "coalesce(i, 0)",
	AllIndexI8:     "coalesce(i8, 0)",
	AllIndexI16:    "coalesce(i16, 0)",
	AllIndexI32:    "coalesce(i32, 0)",
	AllIndexI64:    "coalesce(i64, 0)",
	AllIndexU:      "coalesce(u, 0)",
	AllIndexU8:     "coalesce(u8, 0)",
	AllIndexU16:    "coalesce(u16, 0)",
	AllIndexU32:    "coalesce(u32, 0)",
	AllIndexU64:    "coalesce(u64, 0)",
	AllIndexF32:    "coalesce(f32, 0)",
	AllIndexF64:    "coalesce(f64, 0)",
	AllIndexR:      "coalesce(r, 0)",
	AllIndexBY:     "coalesce(by, 0)",
	AllIndexBytes:  "bytes",
	AllIndexFixed:  "fixed",
	AllIndexUID:    "uid",
	AllIndexPuid:   "puid",
	AllIndexRaw:    "raw",
	AllIndexMapany: "mapany",
	AllIndexMaps:   "maps",
	AllIndexConf:   "coalesce(conf, 'null'::jsonb)",
	AllIndexConfs:  "confs",
	AllIndexAnyv:   "anyv",
	AllIndexT:      "coalesce(t, '0001-01-01 00:00:00+00')",
	AllIndexDur:    "coalesce(dur, 0)",
	AllIndexDate:   "coalesce(date, '0001-01-01 00:00:00+00')",
	AllIndexPD:     "pd",
	AllIndexPts:    "pts",
	AllIndexPiv:    "piv",
	AllIndexLB:     "lb",
	AllIndexLS:     "ls",
	AllIndexLI:     "li",
	AllIndexLi8:    "li8",
	AllIndexLi16:   "li16",
	AllIndexLi32:   "li32",
	AllIndexLu16:   "lu16",
	AllIndexLu32:   "lu32",
	AllIndexLu64:   "lu64",
	AllIndexLf32:   "lf32",
	AllIndexLT:     "lt",
	AllIndexLU:     "lu",
	AllIndexLs2:    "ls2",
	AllIndexLi2:    "li2",
	AllIndexLbb:    "lbb",
	AllIndexLM:     "lm",
	AllIndexLst:    "lst",
	AllIndexLrl:    "lrl",
	AllIndexIP:     "ip",
	AllIndexPfx:    "pfx",
	AllIndexNip:    "nip",
	AllIndexNnet:   "nnet",
	AllIndexNum:    "num",
	AllIndexDec:    "coalesce(dec, 0)",
	AllIndexRng:    "rng",
	AllIndexMrng:   "mrng",
	AllIndexPT:     "pt",
	AllIndexBits:   "bits",
	AllIndexPS:     "ps",
	AllIndexPI:     "pi",
	AllIndexPB:     "pb",
	AllIndexPF:     "pf",
	AllIndexPt2:    "pt2",
	AllIndexPip:    "pip",
	AllIndexNS:     "ns",
	AllIndexNI:     "ni",
	AllIndexNB:     "nb",
	AllIndexNF:     "nf",
	AllIndexNT:     "nt",
	AllIndexPtx:    "ptx",
	AllIndexPi8:    "pi8",
	AllIndexPbo:    "pbo",
	AllIndexST:     "coalesce(st, 0)",
	AllIndexProv:   "coalesce(prov, '')",
	AllIndexRL:     "coalesce(rl, 0)",
	AllIndexNN:     "coalesce(nn, '')",
}

// sql column types
var allSQLTypes = [...]string{
	AllIndexID:     "bigint",
	AllIndexB:      "boolean",
	AllIndexS:      "text",
	AllIndexI:      "bigint",
	AllIndexI8:     "smallint",
	AllIndexI16:    "smallint",
	AllIndexI32:    "integer",
	AllIndexI64:    "bigint",
	AllIndexU:      "numeric(20,0)",
	AllIndexU8:     "smallint",
	AllIndexU16:    "integer",
	AllIndexU32:    "bigint",
	AllIndexU64:    "numeric(20,0)",
	AllIndexF32:    "real",
	AllIndexF64:    "double precision",
	AllIndexR:      "integer",
	AllIndexBY:     "smallint",
	AllIndexBytes:  "bytea",
	AllIndexFixed:  "bytea",
	AllIndexUID:    "uuid",
	AllIndexPuid:   "uuid",
	AllIndexRaw:    "jsonb",
	AllIndexMapany: "jsonb",
	AllIndexMaps:   "jsonb",
	AllIndexConf:   "jsonb",
	AllIndexConfs:  "jsonb",
	AllIndexAnyv:   "jsonb",
	AllIndexT:      "timestamptz",
	AllIndexDur:    "bigint",
	AllIndexDate:   "date",
	AllIndexPD:     "date",
	AllIndexPts:    "timestamp",
	AllIndexPiv:    "interval",
	AllIndexLB:     "boolean[]",
	AllIndexLS:     "text[]",
	AllIndexLI:     "bigint[]",
	AllIndexLi8:    "smallint[]",
	AllIndexLi16:   "smallint[]",
	AllIndexLi32:   "integer[]",
	AllIndexLu16:   "integer[]",
	AllIndexLu32:   "bigint[]",
	AllIndexLu64:   "numeric(20,0)[]",
	AllIndexLf32:   "real[]",
	AllIndexLT:     "timestamptz[]",
	AllIndexLU:     "uuid[]",
	AllIndexLs2:    "jsonb",
	AllIndexLi2:    "jsonb",
	AllIndexLbb:    "bytea[]",
	AllIndexLM:     "jsonb",
	AllIndexLst:    "smallint[]",
	AllIndexLrl:    "bigint[]",
	AllIndexIP:     "inet",
	AllIndexPfx:    "cidr",
	AllIndexNip:    "inet",
	AllIndexNnet:   "cidr",
	AllIndexNum:    "numeric",
	AllIndexDec:    "numeric",
	AllIndexRng:    "int4range",
	AllIndexMrng:   "int8multirange",
	AllIndexPT:     "point",
	AllIndexBits:   "varbit",
	AllIndexPS:     "text",
	AllIndexPI:     "bigint",
	AllIndexPB:     "boolean",
	AllIndexPF:     "double precision",
	AllIndexPt2:    "timestamptz",
	AllIndexPip:    "inet",
	AllIndexNS:     "text",
	AllIndexNI:     "bigint",
	AllIndexNB:     "boolean",
	AllIndexNF:     "double precision",
	AllIndexNT:     "timestamptz",
	AllIndexPtx:    "text",
	AllIndexPi8:    "bigint",
	AllIndexPbo:    "boolean",
	AllIndexST:     "smallint",
	AllIndexProv:   "text",
	AllIndexRL:     "bigint",
	AllIndexNN:     "text",
}

const allSQLSelectAll = "coalesce(id, 0), coalesce(b, false), coalesce(s, ''), coalesce(i, 0), coalesce(i8, 0), coalesce(i16, 0), coalesce(i32, 0), coalesce(i64, 0), coalesce(u, 0), coalesce(u8, 0), coalesce(u16, 0), coalesce(u32, 0), coalesce(u64, 0), coalesce(f32, 0), coalesce(f64, 0), coalesce(r, 0), coalesce(by, 0), bytes, fixed, uid, puid, raw, mapany, maps, coalesce(conf, 'null'::jsonb), confs, anyv, coalesce(t, '0001-01-01 00:00:00+00'), coalesce(dur, 0), coalesce(date, '0001-01-01 00:00:00+00'), pd, pts, piv, lb, ls, li, li8, li16, li32, lu16, lu32, lu64, lf32, lt, lu, ls2, li2, lbb, lm, lst, lrl, ip, pfx, nip, nnet, num, coalesce(dec, 0), rng, mrng, pt, bits, ps, pi, pb, pf, pt2, pip, ns, ni, nb, nf, nt, ptx, pi8, pbo, coalesce(st, 0), coalesce(prov, ''), coalesce(rl, 0), coalesce(nn, '')"

// allSQLColumn returns sql column name of field
func allSQLColumn(f AllIndexTyped) (string, error) {
	if int(f) < 0 || int(f) >= len(allSQLColumns) || allSQLColumns[f] == "" {
		return "", fmt.Errorf("alltypes: field %d is not a sql column", int(f))
	}
	return allSQLColumns[f], nil
}

// allSQLSelect returns select list of fields, all columns when empty
func allSQLSelect(fields []AllIndexTyped) (string, []AllIndexTyped, error) {
	if len(fields) == 0 {
		return allSQLSelectAll, allSQLFields, nil
	}
	var b strings.Builder
	for i, f := range fields {
		if _, err := allSQLColumn(f); err != nil {
			return "", nil, err
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(allSQLSelects[f])
	}
	return b.String(), fields, nil
}

// allSQLSet returns "a = $1, b = $2" in stable order, placeholders from $start
func allSQLSet(v map[AllIndexTyped]any, start int) (string, []any, error) {
	if len(v) == 0 {
		return "", nil, errors.New("alltypes: no values")
	}
	var b strings.Builder
	args := make([]any, 0, len(v))
	for i, f := range slices.Sorted(maps.Keys(v)) {
		col, err := allSQLColumn(f)
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

// allSQLAffected returns pgx.ErrNoRows when nothing was changed
func allSQLAffected(tag pgconn.CommandTag, err error) error {
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// allSQLScanBytes scans bytea to byte array
type allSQLScanBytes func(b []byte)

func (f allSQLScanBytes) ScanBytes(b []byte) error {
	f(b)
	return nil
}

// allSQLScanPrefix scans cidr, null is invalid prefix
type allSQLScanPrefix func(p netip.Prefix)

func (f allSQLScanPrefix) ScanNetipPrefix(p netip.Prefix) error {
	f(p)
	return nil
}

// ---------------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------------

// sqlDest returns scan targets of fields
func (a *All) sqlDest(fields []AllIndexTyped) []any {
	dest := make([]any, len(fields))
	for i, f := range fields {
		switch f {
		case AllIndexID:
			dest[i] = &a.ID
		case AllIndexB:
			dest[i] = &a.B
		case AllIndexS:
			dest[i] = &a.S
		case AllIndexI:
			dest[i] = &a.I
		case AllIndexI8:
			dest[i] = &a.I8
		case AllIndexI16:
			dest[i] = &a.I16
		case AllIndexI32:
			dest[i] = &a.I32
		case AllIndexI64:
			dest[i] = &a.I64
		case AllIndexU:
			dest[i] = &a.U
		case AllIndexU8:
			dest[i] = &a.U8
		case AllIndexU16:
			dest[i] = &a.U16
		case AllIndexU32:
			dest[i] = &a.U32
		case AllIndexU64:
			dest[i] = &a.U64
		case AllIndexF32:
			dest[i] = &a.F32
		case AllIndexF64:
			dest[i] = &a.F64
		case AllIndexR:
			dest[i] = &a.R
		case AllIndexBY:
			dest[i] = &a.BY
		case AllIndexBytes:
			dest[i] = &a.Bytes
		case AllIndexFixed:
			dest[i] = allSQLScanBytes(func(b []byte) { copy(a.Fixed[:], b) })
		case AllIndexUID:
			dest[i] = &a.UID
		case AllIndexPuid:
			dest[i] = &a.Puid
		case AllIndexRaw:
			dest[i] = &a.Raw
		case AllIndexMapany:
			dest[i] = &a.Mapany
		case AllIndexMaps:
			dest[i] = &a.Maps
		case AllIndexConf:
			dest[i] = &a.Conf
		case AllIndexConfs:
			dest[i] = &a.Confs
		case AllIndexAnyv:
			dest[i] = &a.Anyv
		case AllIndexT:
			dest[i] = &a.T
		case AllIndexDur:
			dest[i] = &a.Dur
		case AllIndexDate:
			dest[i] = &a.Date
		case AllIndexPD:
			dest[i] = &a.PD
		case AllIndexPts:
			dest[i] = &a.Pts
		case AllIndexPiv:
			dest[i] = &a.Piv
		case AllIndexLB:
			dest[i] = &a.LB
		case AllIndexLS:
			dest[i] = &a.LS
		case AllIndexLI:
			dest[i] = &a.LI
		case AllIndexLi8:
			dest[i] = &a.Li8
		case AllIndexLi16:
			dest[i] = &a.Li16
		case AllIndexLi32:
			dest[i] = &a.Li32
		case AllIndexLu16:
			dest[i] = &a.Lu16
		case AllIndexLu32:
			dest[i] = &a.Lu32
		case AllIndexLu64:
			dest[i] = &a.Lu64
		case AllIndexLf32:
			dest[i] = &a.Lf32
		case AllIndexLT:
			dest[i] = &a.LT
		case AllIndexLU:
			dest[i] = &a.LU
		case AllIndexLs2:
			dest[i] = &a.Ls2
		case AllIndexLi2:
			dest[i] = &a.Li2
		case AllIndexLbb:
			dest[i] = &a.Lbb
		case AllIndexLM:
			dest[i] = &a.LM
		case AllIndexLst:
			dest[i] = &a.Lst
		case AllIndexLrl:
			dest[i] = &a.Lrl
		case AllIndexIP:
			dest[i] = &a.IP
		case AllIndexPfx:
			dest[i] = &a.Pfx
		case AllIndexNip:
			dest[i] = &a.Nip
		case AllIndexNnet:
			dest[i] = allSQLScanPrefix(func(p netip.Prefix) {
				a.Nnet = net.IPNet{}
				if p.IsValid() {
					a.Nnet = net.IPNet{IP: p.Masked().Addr().AsSlice(), Mask: net.CIDRMask(p.Bits(), p.Addr().BitLen())}
				}
			})
		case AllIndexNum:
			dest[i] = &a.Num
		case AllIndexDec:
			dest[i] = &a.Dec
		case AllIndexRng:
			dest[i] = &a.Rng
		case AllIndexMrng:
			dest[i] = &a.Mrng
		case AllIndexPT:
			dest[i] = &a.PT
		case AllIndexBits:
			dest[i] = &a.Bits
		case AllIndexPS:
			dest[i] = &a.PS
		case AllIndexPI:
			dest[i] = &a.PI
		case AllIndexPB:
			dest[i] = &a.PB
		case AllIndexPF:
			dest[i] = &a.PF
		case AllIndexPt2:
			dest[i] = &a.Pt2
		case AllIndexPip:
			dest[i] = &a.Pip
		case AllIndexNS:
			dest[i] = &a.NS
		case AllIndexNI:
			dest[i] = &a.NI
		case AllIndexNB:
			dest[i] = &a.NB
		case AllIndexNF:
			dest[i] = &a.NF
		case AllIndexNT:
			dest[i] = &a.NT
		case AllIndexPtx:
			dest[i] = &a.Ptx
		case AllIndexPi8:
			dest[i] = &a.Pi8
		case AllIndexPbo:
			dest[i] = &a.Pbo
		case AllIndexST:
			dest[i] = &a.ST
		case AllIndexProv:
			dest[i] = &a.Prov
		case AllIndexRL:
			dest[i] = &a.RL
		case AllIndexNN:
			dest[i] = &a.NN
		}
	}
	return dest
}

// sqlValue returns value of field for query
func (a *All) sqlValue(f AllIndexTyped) any {
	switch f {
	case AllIndexID:
		return a.ID
	case AllIndexB:
		return a.B
	case AllIndexS:
		return a.S
	case AllIndexI:
		return a.I
	case AllIndexI8:
		return a.I8
	case AllIndexI16:
		return a.I16
	case AllIndexI32:
		return a.I32
	case AllIndexI64:
		return a.I64
	case AllIndexU:
		return a.U
	case AllIndexU8:
		return a.U8
	case AllIndexU16:
		return a.U16
	case AllIndexU32:
		return a.U32
	case AllIndexU64:
		return a.U64
	case AllIndexF32:
		return a.F32
	case AllIndexF64:
		return a.F64
	case AllIndexR:
		return a.R
	case AllIndexBY:
		return a.BY
	case AllIndexBytes:
		return a.Bytes
	case AllIndexFixed:
		return a.Fixed[:]
	case AllIndexUID:
		return a.UID
	case AllIndexPuid:
		return a.Puid
	case AllIndexRaw:
		return a.Raw
	case AllIndexMapany:
		return a.Mapany
	case AllIndexMaps:
		return a.Maps
	case AllIndexConf:
		return a.Conf
	case AllIndexConfs:
		return a.Confs
	case AllIndexAnyv:
		return func() []byte { b, _ := gojson.Marshal(a.Anyv); return b }()
	case AllIndexT:
		return a.T
	case AllIndexDur:
		return a.Dur
	case AllIndexDate:
		return a.Date
	case AllIndexPD:
		return a.PD
	case AllIndexPts:
		return a.Pts
	case AllIndexPiv:
		return a.Piv
	case AllIndexLB:
		return a.LB
	case AllIndexLS:
		return a.LS
	case AllIndexLI:
		return a.LI
	case AllIndexLi8:
		return a.Li8
	case AllIndexLi16:
		return a.Li16
	case AllIndexLi32:
		return a.Li32
	case AllIndexLu16:
		return a.Lu16
	case AllIndexLu32:
		return a.Lu32
	case AllIndexLu64:
		return a.Lu64
	case AllIndexLf32:
		return a.Lf32
	case AllIndexLT:
		return a.LT
	case AllIndexLU:
		return a.LU
	case AllIndexLs2:
		return a.Ls2
	case AllIndexLi2:
		return a.Li2
	case AllIndexLbb:
		return a.Lbb
	case AllIndexLM:
		return a.LM
	case AllIndexLst:
		return a.Lst
	case AllIndexLrl:
		return a.Lrl
	case AllIndexIP:
		return a.IP
	case AllIndexPfx:
		return a.Pfx
	case AllIndexNip:
		return a.Nip
	case AllIndexNnet:
		return func() any {
			if a.Nnet.IP == nil {
				return nil
			}
			return a.Nnet
		}()
	case AllIndexNum:
		return a.Num
	case AllIndexDec:
		return a.Dec
	case AllIndexRng:
		return a.Rng
	case AllIndexMrng:
		return a.Mrng
	case AllIndexPT:
		return a.PT
	case AllIndexBits:
		return a.Bits
	case AllIndexPS:
		return a.PS
	case AllIndexPI:
		return a.PI
	case AllIndexPB:
		return a.PB
	case AllIndexPF:
		return a.PF
	case AllIndexPt2:
		return a.Pt2
	case AllIndexPip:
		return a.Pip
	case AllIndexNS:
		return a.NS
	case AllIndexNI:
		return a.NI
	case AllIndexNB:
		return a.NB
	case AllIndexNF:
		return a.NF
	case AllIndexNT:
		return a.NT
	case AllIndexPtx:
		return a.Ptx
	case AllIndexPi8:
		return a.Pi8
	case AllIndexPbo:
		return a.Pbo
	case AllIndexST:
		return int16(a.ST)
	case AllIndexProv:
		return string(a.Prov)
	case AllIndexRL:
		return int(a.RL)
	case AllIndexNN:
		return a.NN
	}
	return nil
}

// sqlInsertArgs returns values of insert
func (a *All) sqlInsertArgs() []any {
	return []any{
		a.B,
		a.S,
		a.I,
		a.I8,
		a.I16,
		a.I32,
		a.I64,
		a.U,
		a.U8,
		a.U16,
		a.U32,
		a.U64,
		a.F32,
		a.F64,
		a.R,
		a.BY,
		a.Bytes,
		a.Fixed[:],
		a.UID,
		a.Puid,
		a.Raw,
		a.Mapany,
		a.Maps,
		a.Conf,
		a.Confs,
		func() []byte { b, _ := gojson.Marshal(a.Anyv); return b }(),
		a.T,
		a.Dur,
		a.Date,
		a.PD,
		a.Pts,
		a.Piv,
		a.LB,
		a.LS,
		a.LI,
		a.Li8,
		a.Li16,
		a.Li32,
		a.Lu16,
		a.Lu32,
		a.Lu64,
		a.Lf32,
		a.LT,
		a.LU,
		a.Ls2,
		a.Li2,
		a.Lbb,
		a.LM,
		a.Lst,
		a.Lrl,
		a.IP,
		a.Pfx,
		a.Nip,
		func() any {
			if a.Nnet.IP == nil {
				return nil
			}
			return a.Nnet
		}(),
		a.Num,
		a.Dec,
		a.Rng,
		a.Mrng,
		a.PT,
		a.Bits,
		a.PS,
		a.PI,
		a.PB,
		a.PF,
		a.Pt2,
		a.Pip,
		a.NS,
		a.NI,
		a.NB,
		a.NF,
		a.NT,
		a.Ptx,
		a.Pi8,
		a.Pbo,
		int16(a.ST),
		string(a.Prov),
		int(a.RL),
		a.NN,
	}
}

// sqlAllArgs returns values of all columns
func (a *All) sqlAllArgs() []any {
	return []any{
		a.ID,
		a.B,
		a.S,
		a.I,
		a.I8,
		a.I16,
		a.I32,
		a.I64,
		a.U,
		a.U8,
		a.U16,
		a.U32,
		a.U64,
		a.F32,
		a.F64,
		a.R,
		a.BY,
		a.Bytes,
		a.Fixed[:],
		a.UID,
		a.Puid,
		a.Raw,
		a.Mapany,
		a.Maps,
		a.Conf,
		a.Confs,
		func() []byte { b, _ := gojson.Marshal(a.Anyv); return b }(),
		a.T,
		a.Dur,
		a.Date,
		a.PD,
		a.Pts,
		a.Piv,
		a.LB,
		a.LS,
		a.LI,
		a.Li8,
		a.Li16,
		a.Li32,
		a.Lu16,
		a.Lu32,
		a.Lu64,
		a.Lf32,
		a.LT,
		a.LU,
		a.Ls2,
		a.Li2,
		a.Lbb,
		a.LM,
		a.Lst,
		a.Lrl,
		a.IP,
		a.Pfx,
		a.Nip,
		func() any {
			if a.Nnet.IP == nil {
				return nil
			}
			return a.Nnet
		}(),
		a.Num,
		a.Dec,
		a.Rng,
		a.Mrng,
		a.PT,
		a.Bits,
		a.PS,
		a.PI,
		a.PB,
		a.PF,
		a.Pt2,
		a.Pip,
		a.NS,
		a.NI,
		a.NB,
		a.NF,
		a.NT,
		a.Ptx,
		a.Pi8,
		a.Pbo,
		int16(a.ST),
		string(a.Prov),
		int(a.RL),
		a.NN,
	}
}

// ---------------------------------------------------------------------------
// select
// ---------------------------------------------------------------------------

// query selects rows: tail is sql after table name
func (a *AllSQL) query(fields []AllIndexTyped, tail string, args ...any) ([]All, error) {
	cols, fields, err := allSQLSelect(fields)
	if err != nil {
		return nil, err
	}
	rows, err := a.db.Query(context.Background(), "select "+cols+" from alltypes"+tail, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (v All, err error) {
		err = row.Scan(v.sqlDest(fields)...)
		return
	})
}

// queryRow selects one row
func (a *AllSQL) queryRow(fields []AllIndexTyped, tail string, args ...any) (v All, err error) {
	cols, fields, err := allSQLSelect(fields)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select "+cols+" from alltypes"+tail, args...).Scan(v.sqlDest(fields)...)
	return
}

// Get returns row by id
func (a *AllSQL) Get(id int, fields ...AllIndexTyped) (All, error) {
	return a.queryRow(fields, " where id = $1", id)
}

// Exists checks row by id
func (a *AllSQL) Exists(id int) (has bool, err error) {
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from alltypes where id = $1)", id).Scan(&has)
	return
}

// GetBy returns first row with field = v
func (a *AllSQL) GetBy(field AllIndexTyped, v any, fields ...AllIndexTyped) (res All, err error) {
	col, err := allSQLColumn(field)
	if err != nil {
		return
	}
	return a.queryRow(fields, " where "+col+" = $1 limit 1", v)
}

// GetWhere returns first row by condition
//
//	db.GetWhere(nil, "title = $1 order by created desc", "James")
func (a *AllSQL) GetWhere(fields []AllIndexTyped, where string, args ...any) (All, error) {
	return a.queryRow(fields, " where "+where+" limit 1", args...)
}

// Has checks row with field = v
func (a *AllSQL) Has(field AllIndexTyped, v any) (has bool, err error) {
	col, err := allSQLColumn(field)
	if err != nil {
		return
	}
	err = a.db.QueryRow(context.Background(), "select exists (select 1 from alltypes where "+col+" = $1)", v).Scan(&has)
	return
}

// All returns all rows
func (a *AllSQL) All(fields ...AllIndexTyped) ([]All, error) {
	return a.query(fields, " order by id")
}

// List returns page of rows
func (a *AllSQL) List(limit, offset int, fields ...AllIndexTyped) ([]All, error) {
	return a.query(fields, " order by id limit $1 offset $2", limit, offset)
}

// Select returns rows by condition, all rows when where is empty
//
//	db.Select(nil, "views > $1 order by views desc limit 10", 100)
func (a *AllSQL) Select(fields []AllIndexTyped, where string, args ...any) ([]All, error) {
	if where != "" {
		where = " where " + where
	}
	return a.query(fields, where, args...)
}

// Count returns count of rows
func (a *AllSQL) Count() (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from alltypes").Scan(&count)
	return
}

// CountWhere returns count of rows by condition
func (a *AllSQL) CountWhere(where string, args ...any) (count int, err error) {
	err = a.db.QueryRow(context.Background(), "select count(*) from alltypes where "+where, args...).Scan(&count)
	return
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

const allSQLInsert = "insert into alltypes (b, s, i, i8, i16, i32, i64, u, u8, u16, u32, u64, f32, f64, r, by, bytes, fixed, uid, puid, raw, mapany, maps, conf, confs, anyv, t, dur, date, pd, pts, piv, lb, ls, li, li8, li16, li32, lu16, lu32, lu64, lf32, lt, lu, ls2, li2, lbb, lm, lst, lrl, ip, pfx, nip, nnet, num, dec, rng, mrng, pt, bits, ps, pi, pb, pf, pt2, pip, ns, ni, nb, nf, nt, ptx, pi8, pbo, st, prov, rl, nn) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78)"

const allSQLInsertFull = "insert into alltypes (id, b, s, i, i8, i16, i32, i64, u, u8, u16, u32, u64, f32, f64, r, by, bytes, fixed, uid, puid, raw, mapany, maps, conf, confs, anyv, t, dur, date, pd, pts, piv, lb, ls, li, li8, li16, li32, lu16, lu32, lu64, lf32, lt, lu, ls2, li2, lbb, lm, lst, lrl, ip, pfx, nip, nnet, num, dec, rng, mrng, pt, bits, ps, pi, pb, pf, pt2, pip, ns, ni, nb, nf, nt, ptx, pi8, pbo, st, prov, rl, nn) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79)"

// Insert adds row and sets generated v.ID
func (a *AllSQL) Insert(v *All) error {
	return a.db.QueryRow(context.Background(), allSQLInsert+" returning id", v.sqlInsertArgs()...).Scan(&v.ID)
}

// InsertNoConflict adds row and sets v.ID, false when row exists
func (a *AllSQL) InsertNoConflict(v *All) (inserted bool, err error) {
	err = a.db.QueryRow(context.Background(), allSQLInsert+" on conflict do nothing returning id", v.sqlInsertArgs()...).Scan(&v.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// InsertFull adds row with all columns as is: generated and noinsert columns too
func (a *AllSQL) InsertFull(v *All) error {
	_, err := a.db.Exec(context.Background(), allSQLInsertFull, v.sqlAllArgs()...)
	return err
}

// Upsert adds row with all columns or updates existing row by (id)
func (a *AllSQL) Upsert(v *All) error {
	_, err := a.db.Exec(context.Background(), "insert into alltypes (id, b, s, i, i8, i16, i32, i64, u, u8, u16, u32, u64, f32, f64, r, by, bytes, fixed, uid, puid, raw, mapany, maps, conf, confs, anyv, t, dur, date, pd, pts, piv, lb, ls, li, li8, li16, li32, lu16, lu32, lu64, lf32, lt, lu, ls2, li2, lbb, lm, lst, lrl, ip, pfx, nip, nnet, num, dec, rng, mrng, pt, bits, ps, pi, pb, pf, pt2, pip, ns, ni, nb, nf, nt, ptx, pi8, pbo, st, prov, rl, nn) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79) on conflict (id) do update set b = excluded.b, s = excluded.s, i = excluded.i, i8 = excluded.i8, i16 = excluded.i16, i32 = excluded.i32, i64 = excluded.i64, u = excluded.u, u8 = excluded.u8, u16 = excluded.u16, u32 = excluded.u32, u64 = excluded.u64, f32 = excluded.f32, f64 = excluded.f64, r = excluded.r, by = excluded.by, bytes = excluded.bytes, fixed = excluded.fixed, uid = excluded.uid, puid = excluded.puid, raw = excluded.raw, mapany = excluded.mapany, maps = excluded.maps, conf = excluded.conf, confs = excluded.confs, anyv = excluded.anyv, t = excluded.t, dur = excluded.dur, date = excluded.date, pd = excluded.pd, pts = excluded.pts, piv = excluded.piv, lb = excluded.lb, ls = excluded.ls, li = excluded.li, li8 = excluded.li8, li16 = excluded.li16, li32 = excluded.li32, lu16 = excluded.lu16, lu32 = excluded.lu32, lu64 = excluded.lu64, lf32 = excluded.lf32, lt = excluded.lt, lu = excluded.lu, ls2 = excluded.ls2, li2 = excluded.li2, lbb = excluded.lbb, lm = excluded.lm, lst = excluded.lst, lrl = excluded.lrl, ip = excluded.ip, pfx = excluded.pfx, nip = excluded.nip, nnet = excluded.nnet, num = excluded.num, dec = excluded.dec, rng = excluded.rng, mrng = excluded.mrng, pt = excluded.pt, bits = excluded.bits, ps = excluded.ps, pi = excluded.pi, pb = excluded.pb, pf = excluded.pf, pt2 = excluded.pt2, pip = excluded.pip, ns = excluded.ns, ni = excluded.ni, nb = excluded.nb, nf = excluded.nf, nt = excluded.nt, ptx = excluded.ptx, pi8 = excluded.pi8, pbo = excluded.pbo, st = excluded.st, prov = excluded.prov, rl = excluded.rl, nn = excluded.nn", v.sqlAllArgs()...)
	return err
}

// InsertMany adds rows by copy protocol: fast, without conflict checks and generated ids
func (a *AllSQL) InsertMany(list []All) (int64, error) {
	return a.db.CopyFrom(context.Background(), pgx.Identifier{"alltypes"}, allSQLInsertColumns, pgx.CopyFromSlice(len(list), func(i int) ([]any, error) {
		return list[i].sqlInsertArgs(), nil
	}))
}

// InsertBatch adds rows in one round trip and sets generated ID
func (a *AllSQL) InsertBatch(list []*All) error {
	if len(list) == 0 {
		return nil
	}
	b := &pgx.Batch{}
	for _, v := range list {
		b.Queue(allSQLInsert+" returning id", v.sqlInsertArgs()...)
	}
	res := a.db.SendBatch(context.Background(), b)
	for _, v := range list {
		if err := res.QueryRow().Scan(&v.ID); err != nil {
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
func (a *AllSQL) Update(id int, field AllIndexTyped, v any) error {
	col, err := allSQLColumn(field)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = $1 where id = $2", v, id))
}

// Updates sets fields of row, pgx.ErrNoRows when row not found
func (a *AllSQL) Updates(id int, v map[AllIndexTyped]any) error {
	set, args, err := allSQLSet(v, 1)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, id)...))
}

// Save updates row from struct by v.ID: listed fields or all columns
func (a *AllSQL) Save(v *All, fields ...AllIndexTyped) error {
	if len(fields) == 0 {
		fields = allSQLFields
	}
	values := make(map[AllIndexTyped]any, len(fields))
	for _, f := range fields {
		if f != AllIndexID {
			values[f] = v.sqlValue(f)
		}
	}
	set, args, err := allSQLSet(values, 1)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+set+" where id = $"+strconv.Itoa(len(args)+1), append(args, v.sqlValue(AllIndexID))...))
}

// Inc adds v to number field: views = views + 1
func (a *AllSQL) Inc(id int, field AllIndexTyped, v any) error {
	col, err := allSQLColumn(field)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = coalesce("+col+", 0) + $1 where id = $2", v, id))
}

// Delete removes row by id
func (a *AllSQL) Delete(id int) error {
	_, err := a.db.Exec(context.Background(), "delete from alltypes where id = $1", id)
	return err
}

// UpdateWhere sets field of rows by condition, returns count of rows
//
//	db.UpdateWhere(IndexActive, false, "created < $1", date)
func (a *AllSQL) UpdateWhere(field AllIndexTyped, v any, where string, args ...any) (int64, error) {
	col, err := allSQLColumn(field)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update alltypes set "+col+" = $"+strconv.Itoa(len(args)+1)+" where "+where, append(slices.Clone(args), v)...)
	return tag.RowsAffected(), err
}

// UpdatesWhere sets fields of rows by condition, returns count of rows
func (a *AllSQL) UpdatesWhere(v map[AllIndexTyped]any, where string, args ...any) (int64, error) {
	set, vals, err := allSQLSet(v, len(args)+1)
	if err != nil {
		return 0, err
	}
	tag, err := a.db.Exec(context.Background(), "update alltypes set "+set+" where "+where, append(slices.Clone(args), vals...)...)
	return tag.RowsAffected(), err
}

// DeleteWhere removes rows by condition, returns count of rows
func (a *AllSQL) DeleteWhere(where string, args ...any) (int64, error) {
	tag, err := a.db.Exec(context.Background(), "delete from alltypes where "+where, args...)
	return tag.RowsAffected(), err
}

// RawUpdate sets key of jsonb raw
func (a *AllSQL) RawUpdate(id int, k string, v any) error {
	return a.jsonSet(id, "raw", k, v)
}

// RawMerge adds keys of object v to jsonb raw: existing keys are replaced
func (a *AllSQL) RawMerge(id int, v any) error {
	return a.jsonMerge(id, "raw", "{}", v)
}

// RawDelete removes key of jsonb raw
func (a *AllSQL) RawDelete(id int, k string) error {
	return a.jsonDelete(id, "raw", k)
}

// RawRename renames key of jsonb raw, pgx.ErrNoRows when key not found
func (a *AllSQL) RawRename(id int, k, newK string) error {
	return a.jsonRename(id, "raw", k, newK)
}

// MapanyUpdate sets key of jsonb mapany
func (a *AllSQL) MapanyUpdate(id int, k string, v any) error {
	return a.jsonSet(id, "mapany", fmt.Sprint(k), v)
}

// MapanyMerge adds keys to jsonb mapany: existing keys are replaced
func (a *AllSQL) MapanyMerge(id int, v map[string]any) error {
	return a.jsonMerge(id, "mapany", "{}", v)
}

// MapanyDelete removes key of jsonb mapany
func (a *AllSQL) MapanyDelete(id int, k string) error {
	return a.jsonDelete(id, "mapany", fmt.Sprint(k))
}

// MapanyRename renames key of jsonb mapany, pgx.ErrNoRows when key not found
func (a *AllSQL) MapanyRename(id int, k, newK string) error {
	return a.jsonRename(id, "mapany", fmt.Sprint(k), fmt.Sprint(newK))
}

// MapsUpdate sets key of jsonb maps
func (a *AllSQL) MapsUpdate(id int, k string, v string) error {
	return a.jsonSet(id, "maps", fmt.Sprint(k), v)
}

// MapsMerge adds keys to jsonb maps: existing keys are replaced
func (a *AllSQL) MapsMerge(id int, v map[string]string) error {
	return a.jsonMerge(id, "maps", "{}", v)
}

// MapsDelete removes key of jsonb maps
func (a *AllSQL) MapsDelete(id int, k string) error {
	return a.jsonDelete(id, "maps", fmt.Sprint(k))
}

// MapsRename renames key of jsonb maps, pgx.ErrNoRows when key not found
func (a *AllSQL) MapsRename(id int, k, newK string) error {
	return a.jsonRename(id, "maps", fmt.Sprint(k), fmt.Sprint(newK))
}

// ConfUpdate sets key of jsonb conf
func (a *AllSQL) ConfUpdate(id int, k string, v any) error {
	return a.jsonSet(id, "conf", k, v)
}

// ConfMerge adds keys of object v to jsonb conf: existing keys are replaced
func (a *AllSQL) ConfMerge(id int, v any) error {
	return a.jsonMerge(id, "conf", "{}", v)
}

// ConfDelete removes key of jsonb conf
func (a *AllSQL) ConfDelete(id int, k string) error {
	return a.jsonDelete(id, "conf", k)
}

// ConfRename renames key of jsonb conf, pgx.ErrNoRows when key not found
func (a *AllSQL) ConfRename(id int, k, newK string) error {
	return a.jsonRename(id, "conf", k, newK)
}

// ConfsAppend adds values to jsonb array confs
func (a *AllSQL) ConfsAppend(id int, v ...Config) error {
	return a.jsonMerge(id, "confs", "[]", v)
}

// AnyvUpdate sets key of jsonb anyv
func (a *AllSQL) AnyvUpdate(id int, k string, v any) error {
	return a.jsonSet(id, "anyv", k, v)
}

// AnyvMerge adds keys of object v to jsonb anyv: existing keys are replaced
func (a *AllSQL) AnyvMerge(id int, v any) error {
	return a.jsonMerge(id, "anyv", "{}", v)
}

// AnyvDelete removes key of jsonb anyv
func (a *AllSQL) AnyvDelete(id int, k string) error {
	return a.jsonDelete(id, "anyv", k)
}

// AnyvRename renames key of jsonb anyv, pgx.ErrNoRows when key not found
func (a *AllSQL) AnyvRename(id int, k, newK string) error {
	return a.jsonRename(id, "anyv", k, newK)
}

// LBAppend adds values to array lb
func (a *AllSQL) LBAppend(id int, v ...bool) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lb = coalesce(lb, '{}') || $1::boolean[] where id = $2", v, id))
}

// LBRemove removes all entries of v from array lb
func (a *AllSQL) LBRemove(id int, v bool) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lb = array_remove(lb, $1::boolean) where id = $2", v, id))
}

// LSAppend adds values to array ls
func (a *AllSQL) LSAppend(id int, v ...string) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set ls = coalesce(ls, '{}') || $1::text[] where id = $2", v, id))
}

// LSRemove removes all entries of v from array ls
func (a *AllSQL) LSRemove(id int, v string) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set ls = array_remove(ls, $1::text) where id = $2", v, id))
}

// LIAppend adds values to array li
func (a *AllSQL) LIAppend(id int, v ...int) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li = coalesce(li, '{}') || $1::bigint[] where id = $2", v, id))
}

// LIRemove removes all entries of v from array li
func (a *AllSQL) LIRemove(id int, v int) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li = array_remove(li, $1::bigint) where id = $2", v, id))
}

// Li8Append adds values to array li8
func (a *AllSQL) Li8Append(id int, v ...int8) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li8 = coalesce(li8, '{}') || $1::smallint[] where id = $2", v, id))
}

// Li8Remove removes all entries of v from array li8
func (a *AllSQL) Li8Remove(id int, v int8) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li8 = array_remove(li8, $1::smallint) where id = $2", v, id))
}

// Li16Append adds values to array li16
func (a *AllSQL) Li16Append(id int, v ...int16) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li16 = coalesce(li16, '{}') || $1::smallint[] where id = $2", v, id))
}

// Li16Remove removes all entries of v from array li16
func (a *AllSQL) Li16Remove(id int, v int16) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li16 = array_remove(li16, $1::smallint) where id = $2", v, id))
}

// Li32Append adds values to array li32
func (a *AllSQL) Li32Append(id int, v ...int32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li32 = coalesce(li32, '{}') || $1::integer[] where id = $2", v, id))
}

// Li32Remove removes all entries of v from array li32
func (a *AllSQL) Li32Remove(id int, v int32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set li32 = array_remove(li32, $1::integer) where id = $2", v, id))
}

// Lu16Append adds values to array lu16
func (a *AllSQL) Lu16Append(id int, v ...uint16) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu16 = coalesce(lu16, '{}') || $1::integer[] where id = $2", v, id))
}

// Lu16Remove removes all entries of v from array lu16
func (a *AllSQL) Lu16Remove(id int, v uint16) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu16 = array_remove(lu16, $1::integer) where id = $2", v, id))
}

// Lu32Append adds values to array lu32
func (a *AllSQL) Lu32Append(id int, v ...uint32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu32 = coalesce(lu32, '{}') || $1::bigint[] where id = $2", v, id))
}

// Lu32Remove removes all entries of v from array lu32
func (a *AllSQL) Lu32Remove(id int, v uint32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu32 = array_remove(lu32, $1::bigint) where id = $2", v, id))
}

// Lu64Append adds values to array lu64
func (a *AllSQL) Lu64Append(id int, v ...uint64) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu64 = coalesce(lu64, '{}') || $1::numeric(20,0)[] where id = $2", v, id))
}

// Lu64Remove removes all entries of v from array lu64
func (a *AllSQL) Lu64Remove(id int, v uint64) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu64 = array_remove(lu64, $1::numeric(20,0)) where id = $2", v, id))
}

// Lf32Append adds values to array lf32
func (a *AllSQL) Lf32Append(id int, v ...float32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lf32 = coalesce(lf32, '{}') || $1::real[] where id = $2", v, id))
}

// Lf32Remove removes all entries of v from array lf32
func (a *AllSQL) Lf32Remove(id int, v float32) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lf32 = array_remove(lf32, $1::real) where id = $2", v, id))
}

// LTAppend adds values to array lt
func (a *AllSQL) LTAppend(id int, v ...time.Time) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lt = coalesce(lt, '{}') || $1::timestamptz[] where id = $2", v, id))
}

// LTRemove removes all entries of v from array lt
func (a *AllSQL) LTRemove(id int, v time.Time) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lt = array_remove(lt, $1::timestamptz) where id = $2", v, id))
}

// LUAppend adds values to array lu
func (a *AllSQL) LUAppend(id int, v ...uuid.UUID) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu = coalesce(lu, '{}') || $1::uuid[] where id = $2", v, id))
}

// LURemove removes all entries of v from array lu
func (a *AllSQL) LURemove(id int, v uuid.UUID) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lu = array_remove(lu, $1::uuid) where id = $2", v, id))
}

// Ls2Append adds values to jsonb array ls2
func (a *AllSQL) Ls2Append(id int, v ...[]string) error {
	return a.jsonMerge(id, "ls2", "[]", v)
}

// Li2Append adds values to jsonb array li2
func (a *AllSQL) Li2Append(id int, v ...[]int64) error {
	return a.jsonMerge(id, "li2", "[]", v)
}

// LbbAppend adds values to array lbb
func (a *AllSQL) LbbAppend(id int, v ...[]byte) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lbb = coalesce(lbb, '{}') || $1::bytea[] where id = $2", v, id))
}

// LbbRemove removes all entries of v from array lbb
func (a *AllSQL) LbbRemove(id int, v []byte) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lbb = array_remove(lbb, $1::bytea) where id = $2", v, id))
}

// LMAppend adds values to jsonb array lm
func (a *AllSQL) LMAppend(id int, v ...map[string]any) error {
	return a.jsonMerge(id, "lm", "[]", v)
}

// LstAppend adds values to array lst
func (a *AllSQL) LstAppend(id int, v ...Status) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lst = coalesce(lst, '{}') || $1::smallint[] where id = $2", v, id))
}

// LstRemove removes all entries of v from array lst
func (a *AllSQL) LstRemove(id int, v Status) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lst = array_remove(lst, $1::smallint) where id = $2", v, id))
}

// LrlAppend adds values to array lrl
func (a *AllSQL) LrlAppend(id int, v ...role.Role) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lrl = coalesce(lrl, '{}') || $1::bigint[] where id = $2", v, id))
}

// LrlRemove removes all entries of v from array lrl
func (a *AllSQL) LrlRemove(id int, v role.Role) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set lrl = array_remove(lrl, $1::bigint) where id = $2", v, id))
}

// jsonSet sets key of jsonb column
func (a *AllSQL) jsonSet(id int, col, k string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = jsonb_set(coalesce("+col+", '{}'::jsonb), array[$1::text], $2::jsonb, true) where id = $3", k, b, id))
}

// jsonMerge concats json to jsonb column: object keys or array values
func (a *AllSQL) jsonMerge(id int, col, empty string, v any) error {
	b, err := gojson.Marshal(v)
	if err != nil {
		return err
	}
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = coalesce("+col+", '"+empty+"'::jsonb) || $1::jsonb where id = $2", b, id))
}

// jsonDelete removes key of jsonb column
func (a *AllSQL) jsonDelete(id int, col, k string) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = "+col+" - $1::text where id = $2", k, id))
}

// jsonRename renames key of jsonb column
func (a *AllSQL) jsonRename(id int, col, k, newK string) error {
	return allSQLAffected(a.db.Exec(context.Background(), "update alltypes set "+col+" = ("+col+" - $1::text) || jsonb_build_object($2::text, "+col+" -> $1::text) where id = $3 and "+col+" ? $1::text", k, newK, id))
}

// ---------------------------------------------------------------------------
// query
// ---------------------------------------------------------------------------

// AllQuery is a search query, safe to fill from json: keys are checked, values are sql parameters
type AllQuery struct {
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

// NewAllQuery creates query with empty maps
func NewAllQuery() *AllQuery {
	return &AllQuery{
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
func (q *AllQuery) column(k string) (string, AllIndexTyped, error) {
	if !AllValidKey(k) {
		return "", 0, fmt.Errorf("alltypes: unknown field %q", k)
	}
	f := AllKeyIndex(k)
	col, err := allSQLColumn(f)
	return col, f, err
}

// Render returns sql after table name (where, order, limit), fields and args.
// Sql text is stable for the same set of keys
func (q *AllQuery) Render() (tail string, fields []AllIndexTyped, args []any, err error) {

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
func (a *AllSQL) Search(q *AllQuery) ([]All, error) {
	tail, fields, args, err := q.Render()
	if err != nil {
		return nil, err
	}
	return a.query(fields, tail, args...)
}

// ---------------------------------------------------------------------------
// table
// ---------------------------------------------------------------------------

const allSQLCreateTable = `create table if not exists alltypes (
	id         bigint generated by default as identity primary key,
	b          boolean,
	s          text,
	i          bigint,
	i8         smallint,
	i16        smallint,
	i32        integer,
	i64        bigint,
	u          numeric(20,0),
	u8         smallint,
	u16        integer,
	u32        bigint,
	u64        numeric(20,0),
	f32        real,
	f64        double precision,
	r          integer,
	by         smallint,
	bytes      bytea,
	fixed      bytea,
	uid        uuid,
	puid       uuid,
	raw        jsonb default '{}'::jsonb,
	mapany     jsonb default '{}'::jsonb,
	maps       jsonb default '{}'::jsonb,
	conf       jsonb default '{}'::jsonb,
	confs      jsonb default '[]'::jsonb,
	anyv       jsonb default '{}'::jsonb,
	t          timestamptz,
	dur        bigint,
	date       date,
	pd         date,
	pts        timestamp,
	piv        interval,
	lb         boolean[] default '{}',
	ls         text[] default '{}',
	li         bigint[] default '{}',
	li8        smallint[] default '{}',
	li16       smallint[] default '{}',
	li32       integer[] default '{}',
	lu16       integer[] default '{}',
	lu32       bigint[] default '{}',
	lu64       numeric(20,0)[] default '{}',
	lf32       real[] default '{}',
	lt         timestamptz[] default '{}',
	lu         uuid[] default '{}',
	ls2        jsonb default '[]'::jsonb,
	li2        jsonb default '[]'::jsonb,
	lbb        bytea[] default '{}',
	lm         jsonb default '[]'::jsonb,
	lst        smallint[] default '{}',
	lrl        bigint[] default '{}',
	ip         inet,
	pfx        cidr,
	nip        inet,
	nnet       cidr,
	num        numeric,
	dec        numeric,
	rng        int4range,
	mrng       int8multirange,
	pt         point,
	bits       varbit,
	ps         text,
	pi         bigint,
	pb         boolean,
	pf         double precision,
	pt2        timestamptz,
	pip        inet,
	ns         text,
	ni         bigint,
	nb         boolean,
	nf         double precision,
	nt         timestamptz,
	ptx        text,
	pi8        bigint,
	pbo        boolean,
	st         smallint,
	prov       text,
	rl         bigint,
	nn         text not null
)`

// indexes from tags: sql{index}, sql{idx="group"}, sql{search="group"}
var allSQLIndexes = []string{}

// CreateTable creates table if not exists
func (a *AllSQL) CreateTable() error {
	_, err := a.db.Exec(context.Background(), allSQLCreateTable)
	return err
}

// CreateIndexes creates indexes if not exist
func (a *AllSQL) CreateIndexes() error {
	for _, q := range allSQLIndexes {
		if _, err := a.db.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}

// ResetSequence sets next id to max(id) + add (1 by default).
// Useful after insert with own ids: InsertFull, Upsert, InsertMany
func (a *AllSQL) ResetSequence(add ...int64) error {
	plus := int64(1)
	if len(add) > 0 {
		plus = add[0]
	}
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('alltypes', 'id'), coalesce((select max(id) from alltypes), 0) + $1, false)", plus)
	return err
}

// SetSequence sets next id
func (a *AllSQL) SetSequence(v int64) error {
	_, err := a.db.Exec(context.Background(), "select setval(pg_get_serial_sequence('alltypes', 'id'), $1, false)", v)
	return err
}
