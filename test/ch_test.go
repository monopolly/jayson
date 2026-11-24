package news

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// JAYSON_TEST_CH=clickhouse://default:test@127.0.0.1:9000/default go test ./test/
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
	ctx := context.Background()
	if err := conn.Exec(ctx, "drop table if exists stat"); err != nil {
		t.Fatal(err)
	}

	db := NewStatCQL(conn)
	if err := db.CreateTable(); err != nil {
		t.Fatal(err)
	}

	want := Stat{
		ID: 1, UID: 7, Type: 3, Name: "name", Source: "src", IP: "10.0.0.1", Image: true,
		Stack: []string{"a", "b"}, Uints: []uint{1, 2}, Uints8: []uint8{3}, Uints16: []uint16{4},
		Uints32: []uint32{5}, Uints64: []uint64{6}, Ints8: []int8{-7}, Ints16: []int16{-8},
		Ints32: []int32{-9}, Ints64: []int64{-10},
	}
	db.Add(want, Stat{ID: 2, UID: 8, IP: "10.0.0.2"})
	if err := db.Push(); err != nil {
		t.Fatal("push:", err)
	}

	got, err := db.GetWhere("uid = 7")
	if err != nil {
		t.Fatal("get:", err)
	}
	if !reflect.DeepEqual(want, *got) {
		t.Errorf("\nwant %+v\ngot  %+v", want, *got)
	}

	list, err := db.Select("uid > 0 order by uid", StatIndexUID, StatIndexIP, StatIndexStack)
	if err != nil || len(list) != 2 || list[1].UID != 8 || list[1].IP != "10.0.0.2" || len(list[0].Stack) != 2 {
		t.Fatal(list, err)
	}
	if n, err := db.Count(); err != nil || n != 2 {
		t.Fatal(n, err)
	}
}
