package main

import (
	"fmt"
	"regexp"
	"strings"
)

// sql file
func (a *Clickhouse) File() []byte {
	return []byte(a.createTableSQL() + ";\n")
}

func (a *Clickhouse) createTableSQL() (res string) {
	var list, fieldList []string
	primary := a.primaryFields()

	list = append(list, fmt.Sprintf(`create table if not exists %s (`, settings.Clickhouse.Table))

	var pad int
	for _, x := range fields {
		if len(x.Clickhouse.Name) > pad {
			pad = len(x.Clickhouse.Name)
		}
	}

	for _, x := range fields {
		if x.Clickhouse.Skip {
			continue
		}
		fieldList = append(fieldList, fmt.Sprintf("\t%s%s%s", x.Clickhouse.Name, strings.Repeat(" ", (pad+5)-len(x.Clickhouse.Name)), a.fieldType(x)))
	}

	list = append(list, strings.Join(fieldList, ",\n"))
	list = append(list, ")")
	list = append(list, a.engineSQL(primary))

	return strings.Join(list, "\n")
}

func (a *Clickhouse) fieldType(x *Field) string {
	if !x.Clickhouse.Low {
		return x.Clickhouse.Type
	}
	if strings.HasPrefix(x.Clickhouse.Type, "LowCardinality(") {
		return x.Clickhouse.Type
	}
	return fmt.Sprintf("LowCardinality(%s)", x.Clickhouse.Type)
}

func (a *Clickhouse) fieldNames() (res []string) {
	for _, x := range fields {
		if x.Clickhouse.Skip {
			continue
		}
		res = append(res, x.Clickhouse.Name)
	}
	return
}

func (a *Clickhouse) primaryFields() (res []string) {
	for _, x := range fields {
		if x.Clickhouse.Skip || !x.Clickhouse.Primary {
			continue
		}
		res = append(res, x.Clickhouse.Name)
	}
	return
}

// clickhouse engines for help: only with option ch
var clickhouseEngines = [][2]string{
	{"MergeTree", "default: rows sorted by key, for most tables"},
	{"ReplacingMergeTree(ver)", "removes duplicates of key on merge, keeps the row with max ver (or the last one)"},
	{"SummingMergeTree(a, b)", "sums number columns of rows with the same key"},
	{"AggregatingMergeTree", "merges states of aggregate functions, for materialized views"},
	{"CollapsingMergeTree(sign)", "pair of rows with sign 1 and -1 is removed: change of row without update"},
	{"VersionedCollapsingMergeTree(sign, ver)", "the same for inserts in any order"},
	{"GraphiteMergeTree('config')", "rollup of graphite metrics"},
	{"ReplicatedMergeTree('/path', '{replica}')", "any MergeTree engine with replication: Replicated + name"},
	{"Log, TinyLog, StripeLog", "small tables without key and index, written once"},
	{"Memory", "rows in memory, lost on restart"},
	{"Null", "rows are dropped, source for materialized views"},
}

// engine of table: engine=ReplacingMergeTree(updated)
func (a *Clickhouse) engine() string {
	engine := settings.Clickhouse.Engine
	if engine == "" {
		return "MergeTree()"
	}
	if strings.Contains(engine, "(") {
		return engine
	}
	return engine + "()"
}

// fields of sorting key: ch{order}
func (a *Clickhouse) orderFields() (res []string) {
	for _, x := range fields {
		if x.Clickhouse.Skip || !x.Clickhouse.Order {
			continue
		}
		res = append(res, x.Clickhouse.Name)
	}
	return
}

// engine, primary key, order by, partition by
func (a *Clickhouse) engineSQL(primary []string) string {
	engine := a.engine()
	list := []string{"engine = " + engine}

	name, _, _ := strings.Cut(engine, "(")
	order := a.orderFields()

	// keys only for MergeTree family: Log, Memory and Null have no keys
	if strings.HasSuffix(strings.TrimSpace(name), "MergeTree") {
		if len(primary) > 0 {
			list = append(list, fmt.Sprintf("primary key (%s)", strings.Join(primary, ", ")))
		}
		if len(order) > 0 {
			list = append(list, fmt.Sprintf("order by (%s)", strings.Join(order, ", ")))
		}
		// MergeTree needs a key: without it all rows are equal, ReplacingMergeTree keeps one row
		if len(primary) == 0 && len(order) == 0 {
			panic(fmt.Sprintf("clickhouse %s: engine %s needs a key, add ch{primary} or ch{order} to fields. Ex: id int // ch{primary order}", settings.Clickhouse.Table, name))
		}
	}
	if settings.Clickhouse.Partition != "" {
		list = append(list, "partition by "+settings.Clickhouse.Partition)
	}
	if ttl := a.ttlSQL(); ttl != "" {
		list = append(list, ttl)
	}
	return strings.Join(list, "\n")
}

// units of ch{ttl=30d}
var clickhouseTTLUnits = map[string]string{
	"s": "second", "m": "minute", "h": "hour", "d": "day", "w": "week", "M": "month", "y": "year",
}

var clickhouseTTL = regexp.MustCompile(`^(\d+)([smhdwMy])$`)

// ttl created + interval 30 day
func (a *Clickhouse) ttlSQL() (res string) {
	for _, x := range fields {
		if x.Clickhouse.Skip || x.Clickhouse.TTL == "" {
			continue
		}
		if res != "" {
			panic(fmt.Sprintf("clickhouse %s: only one field with ch{ttl}", settings.Clickhouse.Table))
		}
		p := clickhouseTTL.FindStringSubmatch(x.Clickhouse.TTL)
		if p == nil {
			panic(fmt.Sprintf("clickhouse field %s: ttl=%s is wrong, use number and unit: 30s, 10m, 1h, 1d, 2w, 6M, 1y", x.Name, x.Clickhouse.TTL))
		}
		if !strings.HasPrefix(x.Clickhouse.Type, "Date") {
			panic(fmt.Sprintf("clickhouse field %s: ttl needs a time field (time.Time or ch{unix}), type is %s", x.Name, x.Clickhouse.Type))
		}
		res = fmt.Sprintf("ttl %s + interval %s %s", x.Clickhouse.Name, p[1], clickhouseTTLUnits[p[2]])
	}
	return
}
