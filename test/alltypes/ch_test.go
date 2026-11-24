package alltypes

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// JAYSON_TEST_CH=clickhouse://default:pass@127.0.0.1:9000/default go test ./test/alltypes/
func TestClickhouse(t *testing.T) {
	url := os.Getenv("JAYSON_TEST_CH")
	if url == "" {
		t.Skip("JAYSON_TEST_CH is not set")
	}
	opt, err := clickhouse.ParseDSN(url)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(context.Background(), "drop table if exists hits"); err != nil {
		t.Fatal(err)
	}

	db := NewHitCQL(conn)
	if err := db.CreateTable(); err != nil {
		t.Fatal("table:", err)
	}
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	want := Hit{
		ID: 1, User: 7, Country: "DE", IP: "10.0.0.1", Path: "/a", Ok: true, Views: 10, Created: now,
		Tags: []string{"a", "b"}, Codes: []int{1, 2}, Sizes: []uint{3},
		Attrs: map[string]string{"k": "v"}, Counts: map[string]int{"n": 1}, From: "google",
	}

	// buffer
	db.Add(want, Hit{ID: 2, User: 8, Country: "US", IP: "10.0.0.2", Path: "/b"})
	if db.Len() != 2 {
		t.Fatal(db.Len())
	}
	check(db.Push())
	if db.Len() != 0 {
		t.Fatal(db.Len())
	}
	// direct insert
	check(db.Insert(Hit{ID: 3, User: 9, Country: "DE", IP: "10.0.0.3", Path: "/c", Views: 5}))

	// read
	got, err := db.Get(1)
	check(err)
	got.Created = got.Created.UTC()
	if !reflect.DeepEqual(want, got) {
		t.Errorf("\nwant %+v\ngot  %+v", want, got)
	}
	if _, err := db.Get(100); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("not found:", err)
	}
	if v, err := db.Get(2, HitIndexCountry, HitIndexIP); err != nil || v.Country != "US" || v.IP != "10.0.0.2" || v.Path != "" {
		t.Fatal(v, err)
	}
	if v, err := db.GetBy(HitIndexUser, 9); err != nil || v.ID != 3 {
		t.Fatal(v.ID, err)
	}
	if v, err := db.GetWhere(nil, "country = $1 order by id desc", "DE"); err != nil || v.ID != 3 {
		t.Fatal(v.ID, err)
	}
	if list, err := db.All(HitIndexID); err != nil || len(list) != 3 || list[2].ID != 3 {
		t.Fatal(list, err)
	}
	if list, err := db.List(1, 1); err != nil || len(list) != 1 || list[0].ID != 2 {
		t.Fatal(list, err)
	}
	if list, err := db.Select([]HitIndexTyped{HitIndexID}, "country = $1 and views >= $2 order by id", "DE", 5); err != nil || len(list) != 2 {
		t.Fatal(list, err)
	}
	if n, err := db.Count(); err != nil || n != 3 {
		t.Fatal(n, err)
	}
	if n, err := db.CountWhere("country = $1", "DE"); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if ok, err := db.Has(HitIndexFrom, "google"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := db.Exists(50); err != nil || ok {
		t.Fatal(ok, err)
	}
	if st, err := db.Stat(HitIndexCountry); err != nil || st["DE"] != 2 || st["US"] != 1 {
		t.Fatal(st, err)
	}

	// own functions of interface hitCQL
	if list, err := db.ByCountry("DE"); err != nil || len(list) != 2 {
		t.Fatal(list, err)
	}
	if v, err := db.ByPath("/none"); err != nil || v.ID != 0 {
		t.Fatal(v, err)
	}
	if p, err := db.Path(2); err != nil || p != "/b" {
		t.Fatal(p, err)
	}
	if _, err := db.Path(100); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if n, err := db.Views(100); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	if ids, err := db.IDs("DE"); err != nil || !reflect.DeepEqual(ids, []int64{1, 3}) {
		t.Fatal(ids, err)
	}
	if tags, err := db.Tags(1); err != nil || len(tags) != 2 {
		t.Fatal(tags, err)
	}

	// delete
	check(db.Delete(3))
	check(db.DeleteWhere("country = $1", "US"))
	check(db.DropCountry("FR"))
	if n, err := db.Count(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

// option ee: errors of clickhouse are errors.E
func TestClickhouseErrors(t *testing.T) {
	url := os.Getenv("JAYSON_TEST_CH")
	if url == "" {
		t.Skip("JAYSON_TEST_CH is not set")
	}
	opt, _ := clickhouse.ParseDSN(url)
	conn, err := clickhouse.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.Exec(context.Background(), "drop table if exists clicks")

	db := NewClickCQL(conn)
	if e := db.CreateTable(); e != nil {
		t.Fatal(e)
	}
	if e := db.Insert(Click{ID: 1, Link: "a", Tags: []string{"x"}}); e != nil {
		t.Fatal(e)
	}
	if v, e := db.Get(1); e != nil || v.Link != "a" {
		t.Fatal(v, e)
	}
	if _, e := db.Get(2); e == nil || e.Code() != 404 {
		t.Fatal(e)
	}
	if _, e := db.Select(nil, "nocolumn = 1"); e == nil {
		t.Fatal("error expected")
	}
	if e := db.Delete(1); e != nil {
		t.Fatal(e)
	}
}
