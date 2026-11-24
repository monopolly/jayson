package examples

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	gojson "github.com/goccy/go-json"
	"github.com/niubaoshu/gotiny"
	"github.com/vmihailenco/msgpack/v5"
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
type StatIndexTyped int

// int index
const (
	StatIndexID = StatIndexTyped(iota)
	StatIndexName
	StatIndexValues
)

// string index
const (
	StatFieldID     = "id"     // int
	StatFieldName   = "name"   // string
	StatFieldValues = "values" // []uint64
)

// index func
func StatIndexes() []StatIndexTyped {
	return []StatIndexTyped{StatIndexID, StatIndexName, StatIndexValues}

}

// 32 bytes (go padding)
//
//easyjson:json
type Stat struct {
	ID     int      `json:"id,omitempty" msg:"id,omitempty"`         //
	Name   string   `json:"name,omitempty" msg:"name,omitempty"`     //
	Values []uint64 `json:"values,omitempty" msg:"values,omitempty"` //
}

// Tuple create an array from struct
func (a *Stat) Tuple() (r []any) {
	return []any{a.ID, a.Name, a.Values}
}

// Tuple create an array from struct for clickhouse
func (a *Stat) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Name, a.Values}
}

// get struct value with function
func (a *Stat) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "name":
		return a.Name //string
	case "values":
		return a.Values //[]uint64
	}
	return
}

// get any struct value as string
func (a *Stat) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "name":
		return fmt.Sprint(a.Name) //string
	case "values":
		return fmt.Sprint(a.Values) //[]uint64
	}
	return
}

// key index string
func (a StatIndexTyped) String() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexName:
		return "name"
	case StatIndexValues:
		return "values"
	default:
		return ""
	}
}

// key index string
func (a StatIndexTyped) SQLName() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexName:
		return "name"
	case StatIndexValues:
		return "values"
	default:
		return ""
	}
}

// key index clickhouse string
func (a StatIndexTyped) ClickhouseName() string {
	switch a {
	case StatIndexID:
		return "id"
	case StatIndexName:
		return "name"
	case StatIndexValues:
		return "values"
	default:
		return ""
	}
}

// key index type
func (a StatIndexTyped) Type() string {
	switch a {
	case StatIndexID:
		return "int"
	case StatIndexName:
		return "string"
	case StatIndexValues:
		return "[]uint64"
	default:
		return ""
	}
}

// custom title
func (a StatIndexTyped) Title() string {
	switch a {
	case StatIndexID:
		return "ID"
	case StatIndexName:
		return "Name"
	case StatIndexValues:
		return "Values"
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
	case "name":
		return StatIndexName
	case "values":
		return StatIndexValues
	default:
		return 0
	}
}

// valid struct key check
func StatValidKey(key string) bool {
	switch key {
	case "id", "name", "values":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Stat) Map() map[string]any {
	return map[string]any{
		"id":     a.ID,
		"name":   a.Name,
		"values": a.Values,
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
	return gotiny.Marshal(&a.ID, &a.Name, &a.Values)
}

// parse gotiny
func UnmarshalStatGotiny(v []byte) (a Stat) {
	gotiny.Unmarshal(v, &a.ID, &a.Name, &a.Values)
	return
}

// msgp marshal
func (a *Stat) MessagePack() []byte {
	b, _ := msgpack.Marshal(a)
	return b
}

// msgp unmarshal
func ParseStatMessagePack(v []byte) (a Stat, err error) {
	err = msgpack.Unmarshal(v, &a)
	return
}

// fast json marshal
func (a *Stat) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseStat(v []byte) (a Stat, err error) {
	err = gojson.Unmarshal(v, &a)
	return
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
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Values        []uint64               `protobuf:"varint,3,rep,packed,name=values,proto3" json:"values,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *StatProto) Reset() {
	*x = StatProto{}
	mi := &file_stat_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *StatProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatProto) ProtoMessage() {}

func (x *StatProto) ProtoReflect() protoreflect.Message {
	mi := &file_stat_proto_msgTypes[0]
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
	return file_stat_proto_rawDescGZIP(), []int{0}
}

func (x *StatProto) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *StatProto) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *StatProto) GetValues() []uint64 {
	if x != nil {
		return x.Values
	}
	return nil
}

var File_stat_proto protoreflect.FileDescriptor

const file_stat_proto_rawDesc = "" +
	"\n" +
	"\n" +
	"stat.proto\x12\bexamples\"G\n" +
	"\tStatProto\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x03R\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12\x16\n" +
	"\x06values\x18\x03 \x03(\x04R\x06valuesB\rZ\v./;examplesb\x06proto3"

var (
	file_stat_proto_rawDescOnce sync.Once
	file_stat_proto_rawDescData []byte
)

func file_stat_proto_rawDescGZIP() []byte {
	file_stat_proto_rawDescOnce.Do(func() {
		file_stat_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_stat_proto_rawDesc), len(file_stat_proto_rawDesc)))
	})
	return file_stat_proto_rawDescData
}

var file_stat_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_stat_proto_goTypes = []any{
	(*StatProto)(nil), // 0: examples.StatProto
}
var file_stat_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

var _ = initStatProto()

func initStatProto() struct{} {
	file_stat_proto_init()
	return struct{}{}
}
func file_stat_proto_init() {
	if File_stat_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_stat_proto_rawDesc), len(file_stat_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_stat_proto_goTypes,
		DependencyIndexes: file_stat_proto_depIdxs,
		MessageInfos:      file_stat_proto_msgTypes,
	}.Build()
	File_stat_proto = out.File
	file_stat_proto_goTypes = nil
	file_stat_proto_depIdxs = nil
}
