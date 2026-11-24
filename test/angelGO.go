package news

import (
	"fmt"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/monopolly/jayson/test/role"
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
type AngelIndexTyped int

// int index
const (
	AngelIndexID = AngelIndexTyped(iota)
	AngelIndexRole
	AngelIndexKey
	AngelIndexCreated
	AngelIndexSmalls
	AngelIndexMusic
	AngelIndexLinks
	AngelIndexStats
)

// string index
const (
	AngelFieldID      = "id"      // int sql{primary}  #readonly
	AngelFieldRole    = "role"    // role.Role sql{unique="css"}
	AngelFieldKey     = "key"     // uuid.UUID sql{unique="css"}
	AngelFieldCreated = "created" // time.Time
	AngelFieldSmalls  = "smalls"  // uint8
	AngelFieldMusic   = "music"   // []int
	AngelFieldLinks   = "links"   // map[string]string
	AngelFieldStats   = "stats"   // map[string]int
)

// index func
func AngelIndexes() []AngelIndexTyped {
	return []AngelIndexTyped{AngelIndexID, AngelIndexRole, AngelIndexKey, AngelIndexCreated, AngelIndexSmalls, AngelIndexMusic, AngelIndexLinks, AngelIndexStats}

}

// 64 bytes (go padding)
//
//easyjson:json
type Angel struct {
	ID      int               `json:"id,omitempty"`      // sql{primary}  #readonly
	Role    role.Role         `json:"role,omitempty"`    // sql{unique="css"}
	Key     uuid.UUID         `json:"key,omitempty"`     // sql{unique="css"}
	Created time.Time         `json:"created,omitempty"` //
	Smalls  uint8             `json:"smalls,omitempty"`  //
	Music   []int             `json:"music,omitempty"`   //
	Links   map[string]string `json:"links,omitempty"`   //
	Stats   map[string]int    `json:"stats,omitempty"`   //
}

// init struct
func NewAngel() (a *Angel) {
	a = new(Angel)
	a.Links = make(map[string]string)
	a.Stats = make(map[string]int)
	return
}

// Tuple create an array from struct
func (a *Angel) Tuple() (r []any) {
	return []any{a.ID, a.Role, a.Key, a.Created, a.Smalls, a.Music, a.Links, a.Stats}
}

// Tuple create an array from struct for clickhouse
func (a *Angel) clickhouseTuple() (r []any) {
	return []any{a.ID, int(a.Role), a.Key, a.Created, a.Smalls, a.Music, a.Links, a.Stats}
}

// get struct value with function
func (a *Angel) Get(k string) (v any) {
	switch k {
	case "id":
		return a.ID //int
	case "role":
		return a.Role //role.Role
	case "key":
		return a.Key //uuid.UUID
	case "created":
		return a.Created //time.Time
	case "smalls":
		return a.Smalls //uint8
	case "music":
		return a.Music //[]int
	case "links":
		return a.Links //map[string]string
	case "stats":
		return a.Stats //map[string]int
	}
	return
}

// get any struct value as string
func (a *Angel) String(k string) (v string) {
	switch k {
	case "id":
		return fmt.Sprint(a.ID) //int
	case "role":
		return fmt.Sprint(a.Role) //role.Role
	case "key":
		return fmt.Sprint(a.Key) //uuid.UUID
	case "created":
		return fmt.Sprint(a.Created) //time.Time
	case "smalls":
		return fmt.Sprint(a.Smalls) //uint8
	case "music":
		return fmt.Sprint(a.Music) //[]int
	case "links":
		return fmt.Sprint(a.Links) //map[string]string
	case "stats":
		return fmt.Sprint(a.Stats) //map[string]int
	}
	return
}

func AngelReadonlyList() []AngelIndexTyped {
	return []AngelIndexTyped{AngelIndexID}
}

func (a AngelIndexTyped) Readonly() bool {
	switch a {
	case AngelIndexID:
		return true
	default:
		return false
	}
}

// key index string
func (a AngelIndexTyped) String() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index string
func (a AngelIndexTyped) SQLName() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index clickhouse string
func (a AngelIndexTyped) ClickhouseName() string {
	switch a {
	case AngelIndexID:
		return "id"
	case AngelIndexRole:
		return "role"
	case AngelIndexKey:
		return "key"
	case AngelIndexCreated:
		return "created"
	case AngelIndexSmalls:
		return "smalls"
	case AngelIndexMusic:
		return "music"
	case AngelIndexLinks:
		return "links"
	case AngelIndexStats:
		return "stats"
	default:
		return ""
	}
}

// key index type
func (a AngelIndexTyped) Type() string {
	switch a {
	case AngelIndexID:
		return "int"
	case AngelIndexRole:
		return "role.Role"
	case AngelIndexKey:
		return "uuid.UUID"
	case AngelIndexCreated:
		return "time.Time"
	case AngelIndexSmalls:
		return "uint8"
	case AngelIndexMusic:
		return "[]int"
	case AngelIndexLinks:
		return "map[string]string"
	case AngelIndexStats:
		return "map[string]int"
	default:
		return ""
	}
}

// custom title
func (a AngelIndexTyped) Title() string {
	switch a {
	case AngelIndexID:
		return "ID"
	case AngelIndexRole:
		return "Role"
	case AngelIndexKey:
		return "Key"
	case AngelIndexCreated:
		return "Created"
	case AngelIndexSmalls:
		return "Smalls"
	case AngelIndexMusic:
		return "Music"
	case AngelIndexLinks:
		return "Links"
	case AngelIndexStats:
		return "Stats"
	default:
		return ""
	}
}

// custom desc
func (a AngelIndexTyped) Desc() string {
	switch a {
	default:
		return ""
	}
}

// struct key to index
func AngelKeyIndex(key string) AngelIndexTyped {
	switch key {
	case "id":
		return AngelIndexID
	case "role":
		return AngelIndexRole
	case "key":
		return AngelIndexKey
	case "created":
		return AngelIndexCreated
	case "smalls":
		return AngelIndexSmalls
	case "music":
		return AngelIndexMusic
	case "links":
		return AngelIndexLinks
	case "stats":
		return AngelIndexStats
	default:
		return 0
	}
}

// valid struct key check
func AngelValidKey(key string) bool {
	switch key {
	case "id", "role", "key", "created", "smalls", "music", "links", "stats":
		return true
	default:
		return false
	}
}

// struct to map
func (a *Angel) Map() map[string]any {
	return map[string]any{
		"id":      a.ID,
		"role":    a.Role,
		"key":     a.Key,
		"created": a.Created,
		"smalls":  a.Smalls,
		"music":   a.Music,
		"links":   a.Links,
		"stats":   a.Stats,
	}
}

// struct to map
func (a *Angel) Iterate(f func(k AngelIndexTyped, v any)) {
	for _, x := range AngelIndexes() {
		f(x, a.Get(x.String()))
	}
}

// fast json marshal
func (a *Angel) Pack() []byte {
	b, _ := gojson.Marshal(a)
	return b
}

// fast json unmarshal
func ParseAngel(v []byte) (a Angel, err error) {
	err = gojson.Unmarshal(v, &a)
	return
}
