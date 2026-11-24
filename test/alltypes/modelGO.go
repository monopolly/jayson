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
