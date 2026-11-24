package main

type Field struct {
	// ID        string // original struct name: types
	Name      string // json struct name: type
	Type      string //[]int
	Comment   string //nice touch
	FieldName string // fieldType for field index

	Json struct {
		Name string // json field name: js{name="sid"}
	}

	Proto struct {
		Name string
		Type string
		Skip bool
	}

	Go struct {
		Index      string   // IndexType for ordinal index: IndexType = 0, etc.
		Name       string   // Created, ID
		Type       string   // []*List
		Base       string   // underlying type of custom type: role.Role > int
		BaseImport string   // custom type package: github.com/x/role
		Imports    []string // packages of field type: *netip.Addr > net/netip

		UpperCase     bool            //sid > SID
		Must          bool            //special function for validation field valuer
		Tags          map[string]bool //#private #visible
		CustomStructs map[string]bool //@public > AccountPublic{}

		Title string // type title: dims > Dimension
		Desc  string // desc{Nice}

		UserLists map[string][]string // list{title="Required", art, cars, any}

		Bits  int //1,2,8...
		Align int //1,2,8...

	}

	SQL struct {
		Name     string //sql name
		Replace  string // replaces sql type
		Append   string // appends sql
		NoInsert bool   // skip in sql insert
		Type     string //new sql type jsonb
		Array    bool   // native postgres array: bigint[], text[]
		NotNull  bool   // not null column
		Get      bool   //need golang sql methods GetName(v type)

		Index struct {
			Simple bool     // create index for field
			Search []string // create text index for search
			Group  []string
		}
		Default struct {
			Value    string
			Simple   bool
			Unixtime bool
		}
		Inc        bool     //"bigserial primary key" noinsert
		Skip       bool     // skip in sql table
		Unique     []string //unique fields
		Primary    bool     //primary key fields
		AlterTable string   //version || 1
		Rename     string   //rename="oldname"
	}

	Clickhouse struct {
		Name    string //sql name id, name...
		Type    string //new sql type https://clickhouse.com/docs/sql-reference/data-types/int-uint
		Skip    bool   //skip in sql table
		Primary bool   //primary key fields PRIMARY KEY(category_id, sale_date)
		Unix    bool   //primary key fields PRIMARY KEY(category_id, sale_date)
		Low     bool   //low cardinality
		IP      bool   //IPv4/IPv6 field
	}
}
