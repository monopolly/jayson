package main

import (
	"strings"

	"github.com/monopolly/jayson/tools"
)

// clickhouse options: ch{primary} ch{order} ch{low} ch{ip} ch{ip6} ch{unix} ch{skip} ch{type="UInt8"} ch{name="col"}
func (a *Field) parseClickhouseOptions(v string) {

	// flags and values: primary low type="UInt8"
	flags := map[string]bool{}
	for _, x := range strings.FieldsFunc(sqlTagValue.ReplaceAllString(v, " "), func(r rune) bool { return r == ' ' || r == ',' || r == '\t' }) {
		flags[x] = true
	}

	a.Clickhouse.Skip = flags["skip"]
	a.Clickhouse.Low = flags["low"]
	a.Clickhouse.Primary = flags["primary"] || flags["!!"]
	a.Clickhouse.Order = flags["order"]
	a.Clickhouse.Unix = flags["unix"]
	a.Clickhouse.Type = tools.Between(v, `type="`, `"`)

	// ttl of rows by time field: ch{ttl=30d}
	a.Clickhouse.TTL = tools.Between(v, `ttl="`, `"`)
	for x := range flags {
		if value, ok := strings.CutPrefix(x, "ttl="); ok {
			a.Clickhouse.TTL = value
		}
	}

	if name := tools.Between(v, `name="`, `"`); name != "" {
		a.Clickhouse.Name = name
	}

	// ip address: ch{ip} IPv4, ch{ip6} IPv6, net.IP and netip.Addr without tag are IPv4
	goType := strings.TrimPrefix(a.GoType(), "[]")
	switch {
	case flags["ip6"]:
		a.Clickhouse.IP, a.Clickhouse.Type = true, "IPv6"
	case flags["ip"], a.Clickhouse.Type == "" && (goType == "net.IP" || goType == "netip.Addr"):
		a.Clickhouse.IP, a.Clickhouse.Type = true, "IPv4"
	}
	if a.Clickhouse.IP && strings.HasPrefix(a.GoType(), "[]") {
		a.Clickhouse.Type = "Array(" + a.Clickhouse.Type + ")"
	}

	// go type to clickhouse type
	if a.Clickhouse.Type == "" {
		a.Clickhouse.Type = clickhouse.Type(a)
	}

	helps.Clickhouse["primary"] = "ch{primary} Primary key, several fields make a composite key"
	helps.Clickhouse["order"] = "ch{order} Sorting key: order by (name). No order by without the tag"
	helps.Clickhouse["low"] = "ch{low} Wrap field type in LowCardinality(...)"
	helps.Clickhouse["ip"] = "ch{ip} IPv4 column for ip address"
	helps.Clickhouse["ip6"] = "ch{ip6} IPv6 column for ip address"
	helps.Clickhouse["ttl=30d"] = "ch{ttl=30d} Rows are removed after the time from this time field: 30s, 10m, 1h, 1d, 2w, 6M, 1y"
	helps.Clickhouse["unix"] = "ch{unix} DateTime column for number field with unix time"
	helps.Clickhouse[`type=""`] = `ch{type="UInt8"} Clickhouse type of column`
	helps.Clickhouse[`name=""`] = `ch{name="col"} Name of column`
	helps.Clickhouse["skip"] = "ch{skip} No column for the field"
}

func hasClickhouseIPFields() bool {
	for _, x := range fields {
		if !x.Clickhouse.Skip && x.Clickhouse.IP {
			return true
		}
	}
	return false
}
