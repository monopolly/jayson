package main

import (
	"fmt"
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

// engine, order by, partition by, ttl
func (a *Clickhouse) engineSQL(primary []string) string {
	engine := a.engine()
	list := []string{"engine = " + engine}

	name, _, _ := strings.Cut(engine, "(")
	upper := strings.ToUpper(engine)

	// key only for MergeTree family: Log, Memory and Null have no key
	if strings.HasSuffix(strings.TrimSpace(name), "MergeTree") && !strings.Contains(upper, "ORDER BY") {
		switch len(primary) {
		case 0:
			list = append(list, "order by tuple()")
		default:
			keys := strings.Join(primary, ", ")
			list = append(list, fmt.Sprintf("order by (%s)", keys), fmt.Sprintf("primary key (%s)", keys))
		}
	}
	if settings.Clickhouse.Partition != "" && !strings.Contains(upper, "PARTITION BY") {
		list = append(list, "partition by "+settings.Clickhouse.Partition)
	}
	if settings.Clickhouse.TTL != "" {
		list = append(list, "ttl "+settings.Clickhouse.TTL)
	}
	return strings.Join(list, "\n")
}
