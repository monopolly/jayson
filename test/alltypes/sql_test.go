package alltypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/netip"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/monopolly/jayson/test/role"
	"github.com/shopspring/decimal"
)

// JAYSON_TEST_DB=postgres://postgres@127.0.0.1:5432/postgres go test ./test/alltypes/
func testDB(t *testing.T) (*AllSQL, context.Context) {
	url := os.Getenv("JAYSON_TEST_DB")
	if url == "" {
		t.Skip("JAYSON_TEST_DB is not set")
	}
	time.Local = time.UTC
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "drop table if exists alltypes"); err != nil {
		t.Fatal(err)
	}
	db := NewAllSQL(pool)
	if err := db.Init(ctx); err != nil {
		t.Fatal("init:", err)
	}
	return db, ctx
}

func testItem() All {
	now := time.Now().UTC().Truncate(time.Microsecond)
	day := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	s, n, bt, f := "str", 7, true, 1.5
	ip := netip.MustParseAddr("10.0.0.1")
	_, ipnet, _ := net.ParseCIDR("10.1.0.0/16")
	var num pgtype.Numeric
	_ = num.Scan("12345.678")

	return All{
		B: true, S: "s", I: -1, I8: -8, I16: -16, I32: -32, I64: math.MinInt64,
		U: math.MaxUint64, U8: 255, U16: 65535, U32: math.MaxUint32, U64: math.MaxUint64,
		F32: 1.25, F64: 2.5, R: 'ж', BY: 9,
		Bytes: []byte{1, 2, 3}, Fixed: [16]byte{1, 2}, UID: uuid.New(),
		Puid:   pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Raw:    json.RawMessage(`{"a": 1}`),
		Mapany: map[string]any{"x": "y"}, Maps: map[string]string{"k": "v"},
		Conf: Config{A: 3}, Confs: []Config{{A: 1}, {A: 2}}, Anyv: "any",
		T: now, Dur: 3 * time.Second, Date: day,
		PD:   pgtype.Date{Time: day, Valid: true},
		Pts:  pgtype.Timestamp{Time: now, Valid: true},
		Piv:  pgtype.Interval{Days: 2, Microseconds: 5, Valid: true},
		LB:   []bool{true, false}, LS: []string{"a", "b"}, LI: []int{1, -2},
		Li8:  []int8{-1, 2}, Li16: []int16{3}, Li32: []int32{4}, Lu16: []uint16{65535},
		Lu32: []uint32{math.MaxUint32}, Lu64: []uint64{math.MaxUint64, 1}, Lf32: []float32{1.5},
		LT: []time.Time{now}, LU: []uuid.UUID{uuid.New(), uuid.New()},
		Ls2: [][]string{{"a", "b"}, {"c"}}, Li2: [][]int64{{1, 2}, {3, 4}},
		Lbb: [][]byte{{1}, {2, 3}}, LM: []map[string]any{{"a": "b"}},
		Lst: []Status{1, 2}, Lrl: []role.Role{3, 4},
		IP: ip, Pfx: netip.MustParsePrefix("10.0.0.0/8"), Nip: net.ParseIP("192.168.1.1").To4(), Nnet: *ipnet,
		Num: num, Dec: decimal.RequireFromString("99.125"),
		Rng:  pgtype.Range[pgtype.Int4]{Lower: pgtype.Int4{Int32: 1, Valid: true}, Upper: pgtype.Int4{Int32: 5, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		Mrng: pgtype.Multirange[pgtype.Range[pgtype.Int8]]{{Lower: pgtype.Int8{Int64: 1, Valid: true}, Upper: pgtype.Int8{Int64: 3, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}},
		PT:   pgtype.Point{P: pgtype.Vec2{X: 1, Y: 2}, Valid: true},
		Bits: pgtype.Bits{Bytes: []byte{0xF0}, Len: 4, Valid: true},
		PS:   &s, PI: &n, PB: &bt, PF: &f, Pt2: &now, Pip: &ip,
		NS: sql.NullString{String: "ns", Valid: true}, NI: sql.NullInt64{Int64: 5, Valid: true},
		NB: sql.NullBool{Bool: true, Valid: true}, NF: sql.NullFloat64{Float64: 1.5, Valid: true},
		NT:   sql.NullTime{Time: now, Valid: true},
		Ptx:  pgtype.Text{String: "pt", Valid: true}, Pi8: pgtype.Int8{Int64: 8, Valid: true}, Pbo: pgtype.Bool{Bool: true, Valid: true},
		ST: 200, Prov: "google", RL: 5, NN: "notnull",
	}
}

// compare by fields: same value or same json
func testEqual(t *testing.T, want, got All) {
	t.Helper()
	rv, rg := reflect.ValueOf(want), reflect.ValueOf(got)
	for i := 0; i < rv.NumField(); i++ {
		a, b := rv.Field(i).Interface(), rg.Field(i).Interface()
		if reflect.DeepEqual(a, b) {
			continue
		}
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if string(ja) != string(jb) {
			t.Errorf("%s:\n  want %s\n  got  %s", rv.Type().Field(i).Name, ja, jb)
		}
	}
}

func TestTypes(t *testing.T) {
	db, ctx := testDB(t)

	v := testItem()
	if err := db.Insert(ctx, &v); err != nil {
		t.Fatal("insert:", err)
	}
	if v.ID == 0 {
		t.Fatal("id is not set")
	}
	got, err := db.Get(ctx, v.ID)
	if err != nil {
		t.Fatal("get:", err)
	}
	testEqual(t, v, got)

	// row of nulls: zero values
	var id int
	if err := db.DB().QueryRow(ctx, "insert into alltypes (nn) values ('x') returning id").Scan(&id); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get(ctx, id)
	if err != nil {
		t.Fatal("get nulls:", err)
	}
	testEqual(t, All{ID: id, NN: "x", Raw: json.RawMessage("{}"), Mapany: map[string]any{}, Maps: map[string]string{},
		LB: []bool{}, LS: []string{}, LI: []int{}, Li8: []int8{}, Li16: []int16{}, Li32: []int32{}, Lu16: []uint16{}, Lu32: []uint32{}, Lu64: []uint64{},
		Lf32: []float32{}, LT: []time.Time{}, LU: []uuid.UUID{}, Lbb: [][]byte{}, Lst: []Status{}, Lrl: []role.Role{},
		Confs: []Config{}, Ls2: [][]string{}, Li2: [][]int64{}, LM: []map[string]any{}, Anyv: map[string]any{}}, got)

	// every column is null
	for _, f := range db.Fields() {
		if f == AllIndexID || f == AllIndexNN {
			continue
		}
		if _, err := db.DB().Exec(ctx, "update alltypes set "+f.SQLName()+" = null where id = $1", id); err != nil {
			t.Fatal(f.SQLName(), err)
		}
	}
	got, err = db.Get(ctx, id)
	if err != nil {
		t.Fatal("get all nulls:", err)
	}
	testEqual(t, All{ID: id, NN: "x"}, got)

	// empty struct
	empty := All{NN: "e"}
	if err := db.Insert(ctx, &empty); err != nil {
		t.Fatal("insert empty:", err)
	}
	if _, err := db.Get(ctx, empty.ID); err != nil {
		t.Fatal("get empty:", err)
	}
}

func TestSelect(t *testing.T) {
	db, ctx := testDB(t)

	var list []*All
	for i := 1; i <= 5; i++ {
		list = append(list, &All{S: "name", I: i, NN: "n", B: i%2 == 0})
	}
	if err := db.InsertBatch(ctx, list); err != nil {
		t.Fatal(err)
	}
	if list[4].ID != 5 {
		t.Fatal("batch ids", list[4].ID)
	}

	if _, err := db.Get(ctx, 100); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("not found:", err)
	}

	// partial fields
	v, err := db.Get(ctx, 3, AllIndexI, AllIndexS)
	if err != nil || v.I != 3 || v.S != "name" || v.NN != "" {
		t.Fatal(v.I, v.S, v.NN, err)
	}
	if v, err = db.GetBy(ctx, AllIndexI, 4); err != nil || v.ID != 4 {
		t.Fatal(v.ID, err)
	}
	if v, err = db.GetWhere(ctx, nil, "i > $1 order by i desc", 1); err != nil || v.I != 5 {
		t.Fatal(v.I, err)
	}

	all, err := db.All(ctx)
	if err != nil || len(all) != 5 {
		t.Fatal(len(all), err)
	}
	page, err := db.List(ctx, 2, 2, AllIndexID)
	if err != nil || len(page) != 2 || page[0].ID != 3 {
		t.Fatal(page, err)
	}
	sel, err := db.Select(ctx, []AllIndexTyped{AllIndexI}, "i >= $1 and s = $2 order by i", 4, "name")
	if err != nil || len(sel) != 2 || sel[0].I != 4 {
		t.Fatal(sel, err)
	}

	if n, err := db.Count(ctx); err != nil || n != 5 {
		t.Fatal(n, err)
	}
	if n, err := db.CountWhere(ctx, "i < $1", 3); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if has, err := db.Has(ctx, AllIndexI, 5); err != nil || !has {
		t.Fatal(has, err)
	}
	if has, err := db.Exists(ctx, 50); err != nil || has {
		t.Fatal(has, err)
	}

	// query from json
	q := NewAllQuery()
	if err := json.Unmarshal([]byte(`{"eq":{"s":"name","b":false},"gt":{"i":1},"in":{"i":[1,3,5]},"like":{"s":"NAM"},"sort":"i","desc":true,"limit":10,"fields":["i"]}`), q); err != nil {
		t.Fatal(err)
	}
	res, err := db.Search(ctx, q)
	if err != nil || len(res) != 2 || res[0].I != 5 || res[1].I != 3 {
		t.Fatal(res, err)
	}
	q.EQ["drop table"] = 1
	if _, err := db.Search(ctx, q); err == nil {
		t.Fatal("unknown key must be an error")
	}

	// trusted where with args before generated params
	q = &AllQuery{Where: "i = $1 or i = $2", Args: []any{1, 2}, EQ: map[string]any{"s": "name"}}
	if res, err = db.Search(ctx, q); err != nil || len(res) != 2 {
		t.Fatal(res, err)
	}
}

func TestUpdate(t *testing.T) {
	db, ctx := testDB(t)

	v := All{S: "a", I: 1, NN: "n", LI: []int{1, 2}, LS: []string{"x"}, Maps: map[string]string{"k": "v"}}
	if err := db.Insert(ctx, &v); err != nil {
		t.Fatal(err)
	}
	id := v.ID

	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	check(db.Update(ctx, id, AllIndexS, "b"))
	check(db.Updates(ctx, id, map[AllIndexTyped]any{AllIndexI: 10, AllIndexB: true}))
	check(db.Increment(ctx, id, AllIndexI, 5))
	check(db.Increment(ctx, id, AllIndexI64, -2)) // null + delta
	if err := db.Update(ctx, 999, AllIndexS, "b"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("update of missing row:", err)
	}

	check(db.ArrayAppend(ctx, id, AllIndexLI, []int{3, 4}))
	check(db.ArrayRemove(ctx, id, AllIndexLI, 2))
	check(db.ArrayAppend(ctx, id, AllIndexLU, []uuid.UUID{uuid.Nil}))
	if err := db.ArrayAppend(ctx, id, AllIndexS, []int{1}); err == nil {
		t.Fatal("not array")
	}

	check(db.JSONSet(ctx, id, AllIndexMaps, "a", "b"))
	check(db.JSONMerge(ctx, id, AllIndexMaps, map[string]string{"c": "d", "e": "f"}))
	check(db.JSONDeleteKey(ctx, id, AllIndexMaps, "c"))
	check(db.JSONRenameKey(ctx, id, AllIndexMaps, "e", "g"))
	check(db.JSONAppend(ctx, id, AllIndexConfs, Config{A: 7}, Config{A: 8}))
	check(db.JSONSet(ctx, id, AllIndexMapany, "it's", `quote " $$ ' test`))

	got, err := db.Get(ctx, id)
	check(err)
	if got.S != "b" || got.I != 15 || !got.B || got.I64 != -2 {
		t.Error(got.S, got.I, got.B, got.I64)
	}
	if !reflect.DeepEqual(got.LI, []int{1, 3, 4}) || len(got.LU) != 1 {
		t.Error(got.LI, got.LU)
	}
	if !reflect.DeepEqual(got.Maps, map[string]string{"k": "v", "a": "b", "g": "f"}) {
		t.Error(got.Maps)
	}
	if !reflect.DeepEqual(got.Confs, []Config{{A: 7}, {A: 8}}) || got.Mapany["it's"] != `quote " $$ ' test` {
		t.Error(got.Confs, got.Mapany)
	}

	// save struct
	got.S, got.F64, got.LS = "saved", 9.5, []string{"y", "z"}
	check(db.Save(ctx, &got, AllIndexS, AllIndexF64))
	got2, _ := db.Get(ctx, id)
	if got2.S != "saved" || got2.F64 != 9.5 || len(got2.LS) != 1 {
		t.Error(got2.S, got2.F64, got2.LS)
	}
	check(db.Save(ctx, &got))
	got2, _ = db.Get(ctx, id)
	testEqual(t, got, got2)

	if n, err := db.UpdateWhere(ctx, AllIndexS, "w", "i = $1", 15); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := db.UpdatesWhere(ctx, map[AllIndexTyped]any{AllIndexS: "ww", AllIndexI: 1}, "s = $1", "w"); err != nil || n != 1 {
		t.Fatal(n, err)
	}

	// upsert, full insert, sequence
	check(db.Upsert(ctx, &All{ID: id, S: "upsert", NN: "n"}))
	check(db.Upsert(ctx, &All{ID: 77, S: "new", NN: "n"}))
	check(db.InsertFull(ctx, &All{ID: 78, NN: "n"}))
	check(db.ResetSequence(ctx))
	next := All{NN: "n"}
	check(db.Insert(ctx, &next))
	if got, _ = db.Get(ctx, id); got.S != "upsert" || next.ID != 79 {
		t.Error(got.S, next.ID)
	}

	// copy
	n, err := db.InsertMany(ctx, []All{testItem(), testItem(), {NN: "copy"}})
	if err != nil || n != 3 {
		t.Fatal("copy:", n, err)
	}

	check(db.Delete(ctx, id))
	if cnt, err := db.DeleteWhere(ctx, "nn = $1", "copy"); err != nil || cnt != 1 {
		t.Fatal(cnt, err)
	}
}

func TestTx(t *testing.T) {
	db, ctx := testDB(t)

	fail := errors.New("fail")
	err := db.Tx(ctx, func(tx *AllSQL) error {
		if err := tx.Insert(ctx, &All{NN: "rollback"}); err != nil {
			return err
		}
		return fail
	})
	if !errors.Is(err, fail) {
		t.Fatal(err)
	}

	err = db.Tx(ctx, func(tx *AllSQL) error {
		v := All{NN: "commit"}
		if err := tx.Insert(ctx, &v); err != nil {
			return err
		}
		// savepoint
		_ = tx.Tx(ctx, func(tx *AllSQL) error {
			_ = tx.Insert(ctx, &All{NN: "nested"})
			return fail
		})
		return tx.Update(ctx, v.ID, AllIndexS, "tx")
	})
	if err != nil {
		t.Fatal(err)
	}

	list, err := db.All(ctx)
	if err != nil || len(list) != 1 || list[0].NN != "commit" || list[0].S != "tx" {
		t.Fatal(list, err)
	}

	if ok, err := db.InsertNoConflict(ctx, &All{NN: "x"}); err != nil || !ok {
		t.Fatal(ok, err)
	}
}
