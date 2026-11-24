package alltypes

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
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
type TagIndexTyped int

// int index
const (
	TagIndexID = TagIndexTyped(iota)
	TagIndexName
	TagIndexOwner
	TagIndexMeta
)

// string index
const (
	TagFieldID    = "id"    // int sql{inc}
	TagFieldName  = "name"  // string
	TagFieldOwner = "owner" // int
	TagFieldMeta  = "meta"  // map[string]int
)

// index func
func TagIndexes() []TagIndexTyped {
	return []TagIndexTyped{TagIndexID, TagIndexName, TagIndexOwner, TagIndexMeta}

}

// 40 bytes (go padding)
//
//easyjson:json
type Tag struct {
	ID    int            `json:"id,omitempty"`    // sql{inc}
	Name  string         `json:"name,omitempty"`  //
	Owner int            `json:"owner,omitempty"` //
	Meta  map[string]int `json:"meta,omitempty"`  //
}

// init struct
func NewTag() (a *Tag) {
	a = new(Tag)
	a.Meta = make(map[string]int)
	return
}

// Tuple create an array from struct
func (a *Tag) Tuple() (r []any) {
	return []any{a.ID, a.Name, a.Owner, a.Meta}
}

// Tuple create an array from struct for clickhouse
func (a *Tag) clickhouseTuple() (r []any) {
	return []any{a.ID, a.Name, a.Owner, a.Meta}
}

// get struct value with function
func (a *Tag) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "name":
		return a.Name //string
	case "owner":
		return a.Owner //int
	case "meta":
		return a.Meta //map[string]int
	}
	return
}

// get any struct value as string
func (a *Tag) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "name":
		return fmt.Sprint(a.Name) //string
	case "owner":
		return fmt.Sprint(a.Owner) //int
	case "meta":
		return fmt.Sprint(a.Meta) //map[string]int
	}
	return
}

// key index string
func (a TagIndexTyped) String() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	default:
		return ""
	}
}

// key index string
func (a TagIndexTyped) SQLName() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	default:
		return ""
	}
}

// key index clickhouse string
func (a TagIndexTyped) ClickhouseName() string {
	switch a {
	case TagIndexID:
		return "id"
	case TagIndexName:
		return "name"
	case TagIndexOwner:
		return "owner"
	case TagIndexMeta:
		return "meta"
	default:
		return ""
	}
}

// key index type
func (a TagIndexTyped) Type() string {
	switch a {
	case TagIndexID:
		return "int"
	case TagIndexName:
		return "string"
	case TagIndexOwner:
		return "int"
	case TagIndexMeta:
		return "map[string]int"
	default:
		return ""
	}
}

// custom title
func (a TagIndexTyped) Title() string {
	switch a {
	case TagIndexID:
		return "ID"
	case TagIndexName:
		return "Name"
	case TagIndexOwner:
		return "Owner"
	case TagIndexMeta:
		return "Meta"
	default:
		return ""
	}
}

// custom desc
func (a TagIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func TagKeyIndex(key string) TagIndexTyped {
	switch key {
	case "id":
		return TagIndexID
	case "name":
		return TagIndexName
	case "owner":
		return TagIndexOwner
	case "meta":
		return TagIndexMeta
	default:
		return 0
	}
}

// valid struct key check
func TagValidKey(key string) bool {
	switch key {
	case "id", "name", "owner", "meta":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Tag) Map() map[string]any {
	return map[string]any{
		"id":    a.ID,
		"name":  a.Name,
		"owner": a.Owner,
		"meta":  a.Meta,
	}
}

// struct to map
func (a *Tag) Iterate(f func(k TagIndexTyped, v any)) {
	for _, x := range TagIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Tag) Pack() []byte {
	b, _ := jsoniter.Marshal(a)
	return b
}

// fast json unmarshal
func ParseTag(v []byte) (a Tag, err error) {
	err = jsoniter.Unmarshal(v, &a)
	return
}

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TagProto struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            int64                  `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	Name          string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Owner         int64                  `protobuf:"varint,3,opt,name=owner,proto3" json:"owner,omitempty"`
	Meta          map[string]int64       `protobuf:"bytes,4,rep,name=meta,proto3" json:"meta,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"varint,2,opt,name=value"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TagProto) Reset() {
	*x = TagProto{}
	mi := &file_tag_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TagProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TagProto) ProtoMessage() {}

func (x *TagProto) ProtoReflect() protoreflect.Message {
	mi := &file_tag_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TagProto.ProtoReflect.Descriptor instead.
func (*TagProto) Descriptor() ([]byte, []int) {
	return file_tag_proto_rawDescGZIP(), []int{0}
}

func (x *TagProto) GetId() int64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *TagProto) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TagProto) GetOwner() int64 {
	if x != nil {
		return x.Owner
	}
	return 0
}

func (x *TagProto) GetMeta() map[string]int64 {
	if x != nil {
		return x.Meta
	}
	return nil
}

var File_tag_proto protoreflect.FileDescriptor

const file_tag_proto_rawDesc = "" +
	"\n" +
	"\ttag.proto\x12\balltypes\"\xaf\x01\n" +
	"\bTagProto\x12\x0e\n" +
	"\x02id\x18\x01 \x01(\x03R\x02id\x12\x12\n" +
	"\x04name\x18\x02 \x01(\tR\x04name\x12\x14\n" +
	"\x05owner\x18\x03 \x01(\x03R\x05owner\x120\n" +
	"\x04meta\x18\x04 \x03(\v2\x1c.alltypes.TagProto.MetaEntryR\x04meta\x1a7\n" +
	"\tMetaEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\x03R\x05value:\x028\x01B\rZ\v./;alltypesb\x06proto3"

var (
	file_tag_proto_rawDescOnce sync.Once
	file_tag_proto_rawDescData []byte
)

func file_tag_proto_rawDescGZIP() []byte {
	file_tag_proto_rawDescOnce.Do(func() {
		file_tag_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_tag_proto_rawDesc), len(file_tag_proto_rawDesc)))
	})
	return file_tag_proto_rawDescData
}

var file_tag_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_tag_proto_goTypes = []any{
	(*TagProto)(nil), // 0: alltypes.TagProto
	nil,              // 1: alltypes.TagProto.MetaEntry
}
var file_tag_proto_depIdxs = []int32{
	1, // 0: alltypes.TagProto.meta:type_name -> alltypes.TagProto.MetaEntry
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

var _ = initTagProto()

func initTagProto() struct{} {
	file_tag_proto_init()
	return struct{}{}
}
func file_tag_proto_init() {
	if File_tag_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_tag_proto_rawDesc), len(file_tag_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tag_proto_goTypes,
		DependencyIndexes: file_tag_proto_depIdxs,
		MessageInfos:      file_tag_proto_msgTypes,
	}.Build()
	File_tag_proto = out.File
	file_tag_proto_goTypes = nil
	file_tag_proto_depIdxs = nil
}
