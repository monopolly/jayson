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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	merrors "github.com/monopolly/errors"
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
	db, err := NewAllSQL(pool)
	if err != nil {
		t.Fatal("new:", err)
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
		F32: 1.25, F64: 2.5, R: 'ж', By: 9,
		Bytes: []byte{1, 2, 3}, Fixed: [16]byte{1, 2}, UID: uuid.New(),
		Puid:   pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Raw:    json.RawMessage(`{"a": 1}`),
		Mapany: map[string]any{"x": "y"}, Maps: map[string]string{"k": "v"},
		Conf: Config{A: 3}, Confs: []Config{{A: 1}, {A: 2}}, Anyv: "any",
		T: now, Dur: 3 * time.Second, Date: day,
		Pd:  pgtype.Date{Time: day, Valid: true},
		Pts: pgtype.Timestamp{Time: now, Valid: true},
		Piv: pgtype.Interval{Days: 2, Microseconds: 5, Valid: true},
		Lb:  []bool{true, false}, Ls: []string{"a", "b"}, Li: []int{1, -2},
		Li8: []int8{-1, 2}, Li16: []int16{3}, Li32: []int32{4}, Lu16: []uint16{65535},
		Lu32: []uint32{math.MaxUint32}, Lu64: []uint64{math.MaxUint64, 1}, Lf32: []float32{1.5},
		Lt: []time.Time{now}, Lu: []uuid.UUID{uuid.New(), uuid.New()},
		Ls2: [][]string{{"a", "b"}, {"c"}}, Li2: [][]int64{{1, 2}, {3, 4}},
		Lbb: [][]byte{{1}, {2, 3}}, Lm: []map[string]any{{"a": "b"}},
		Lst: []Status{1, 2}, Lrl: []role.Role{3, 4},
		IP: ip, Pfx: netip.MustParsePrefix("10.0.0.0/8"), Nip: net.ParseIP("192.168.1.1").To4(), Nnet: *ipnet,
		Num: num, Dec: decimal.RequireFromString("99.125"),
		Rng:  pgtype.Range[pgtype.Int4]{Lower: pgtype.Int4{Int32: 1, Valid: true}, Upper: pgtype.Int4{Int32: 5, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true},
		Mrng: pgtype.Multirange[pgtype.Range[pgtype.Int8]]{{Lower: pgtype.Int8{Int64: 1, Valid: true}, Upper: pgtype.Int8{Int64: 3, Valid: true}, LowerType: pgtype.Inclusive, UpperType: pgtype.Exclusive, Valid: true}},
		Pt:   pgtype.Point{P: pgtype.Vec2{X: 1, Y: 2}, Valid: true},
		Bits: pgtype.Bits{Bytes: []byte{0xF0}, Len: 4, Valid: true},
		Ps:   &s, Pi: &n, Pb: &bt, Pf: &f, Pt2: &now, Pip: &ip,
		Ns: sql.NullString{String: "ns", Valid: true}, Ni: sql.NullInt64{Int64: 5, Valid: true},
		Nb: sql.NullBool{Bool: true, Valid: true}, Nf: sql.NullFloat64{Float64: 1.5, Valid: true},
		Nt:  sql.NullTime{Time: now, Valid: true},
		Ptx: pgtype.Text{String: "pt", Valid: true}, Pi8: pgtype.Int8{Int64: 8, Valid: true}, Pbo: pgtype.Bool{Bool: true, Valid: true},
		St: 200, Prov: "google", Rl: 5, Nn: "notnull",
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
	if err := db.Insert(&v); err != nil {
		t.Fatal("insert:", err)
	}
	if v.ID == 0 {
		t.Fatal("id is not set")
	}
	got, err := db.Get(v.ID)
	if err != nil {
		t.Fatal("get:", err)
	}
	testEqual(t, v, got)

	// row of nulls: zero values
	var id int
	if err := db.Pool().QueryRow(ctx, "insert into alltypes (nn) values ('x') returning id").Scan(&id); err != nil {
		t.Fatal(err)
	}
	got, err = db.Get(id)
	if err != nil {
		t.Fatal("get nulls:", err)
	}
	testEqual(t, All{ID: id, Nn: "x", Raw: json.RawMessage("{}"), Mapany: map[string]any{}, Maps: map[string]string{},
		Lb: []bool{}, Ls: []string{}, Li: []int{}, Li8: []int8{}, Li16: []int16{}, Li32: []int32{}, Lu16: []uint16{}, Lu32: []uint32{}, Lu64: []uint64{},
		Lf32: []float32{}, Lt: []time.Time{}, Lu: []uuid.UUID{}, Lbb: [][]byte{}, Lst: []Status{}, Lrl: []role.Role{},
		Confs: []Config{}, Ls2: [][]string{}, Li2: [][]int64{}, Lm: []map[string]any{}, Anyv: map[string]any{}}, got)

	// every column is null
	for _, f := range db.Fields() {
		if f == AllIndexID || f == AllIndexNn {
			continue
		}
		if _, err := db.Pool().Exec(ctx, "update alltypes set "+f.SQLName()+" = null where id = $1", id); err != nil {
			t.Fatal(f.SQLName(), err)
		}
	}
	got, err = db.Get(id)
	if err != nil {
		t.Fatal("get all nulls:", err)
	}
	testEqual(t, All{ID: id, Nn: "x"}, got)

	// empty struct
	empty := All{Nn: "e"}
	if err := db.Insert(&empty); err != nil {
		t.Fatal("insert empty:", err)
	}
	if _, err := db.Get(empty.ID); err != nil {
		t.Fatal("get empty:", err)
	}
}

func TestSelect(t *testing.T) {
	db, _ := testDB(t)

	var list []*All
	for i := 1; i <= 5; i++ {
		list = append(list, &All{S: "name", I: i, Nn: "n", B: i%2 == 0})
	}
	if err := db.InsertBatch(list); err != nil {
		t.Fatal(err)
	}
	if list[4].ID != 5 {
		t.Fatal("batch ids", list[4].ID)
	}

	if _, err := db.Get(100); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("not found:", err)
	}

	// partial fields
	v, err := db.Get(3, AllIndexI, AllIndexS)
	if err != nil || v.I != 3 || v.S != "name" || v.Nn != "" {
		t.Fatal(v.I, v.S, v.Nn, err)
	}
	if v, err = db.GetBy(AllIndexI, 4); err != nil || v.ID != 4 {
		t.Fatal(v.ID, err)
	}
	if v, err = db.GetWhere(nil, "i > $1 order by i desc", 1); err != nil || v.I != 5 {
		t.Fatal(v.I, err)
	}

	all, err := db.All()
	if err != nil || len(all) != 5 {
		t.Fatal(len(all), err)
	}
	page, err := db.List(2, 2, AllIndexID)
	if err != nil || len(page) != 2 || page[0].ID != 3 {
		t.Fatal(page, err)
	}
	sel, err := db.Select([]AllIndexTyped{AllIndexI}, "i >= $1 and s = $2 order by i", 4, "name")
	if err != nil || len(sel) != 2 || sel[0].I != 4 {
		t.Fatal(sel, err)
	}

	if n, err := db.Count(); err != nil || n != 5 {
		t.Fatal(n, err)
	}
	if n, err := db.CountWhere("i < $1", 3); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if has, err := db.Has(AllIndexI, 5); err != nil || !has {
		t.Fatal(has, err)
	}
	if has, err := db.Exists(50); err != nil || has {
		t.Fatal(has, err)
	}

	// query from json
	q := NewAllQuery()
	if err := json.Unmarshal([]byte(`{"eq":{"s":"name","b":false},"gt":{"i":1},"in":{"i":[1,3,5]},"like":{"s":"NAM"},"sort":"i","desc":true,"limit":10,"fields":["i"]}`), q); err != nil {
		t.Fatal(err)
	}
	res, err := db.Search(q)
	if err != nil || len(res) != 2 || res[0].I != 5 || res[1].I != 3 {
		t.Fatal(res, err)
	}
	q.EQ["drop table"] = 1
	if _, err := db.Search(q); err == nil {
		t.Fatal("unknown key must be an error")
	}

	// trusted where with args before generated params
	q = &AllQuery{Where: "i = $1 or i = $2", Args: []any{1, 2}, EQ: map[string]any{"s": "name"}}
	if res, err = db.Search(q); err != nil || len(res) != 2 {
		t.Fatal(res, err)
	}
}

func TestUpdate(t *testing.T) {
	db, _ := testDB(t)

	v := All{S: "a", I: 1, Nn: "n", Li: []int{1, 2}, Ls: []string{"x"}, Maps: map[string]string{"k": "v"}}
	if err := db.Insert(&v); err != nil {
		t.Fatal(err)
	}
	id := v.ID

	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	check(db.Update(id, AllIndexS, "b"))
	check(db.Updates(id, map[AllIndexTyped]any{AllIndexI: 10, AllIndexB: true}))
	check(db.Inc(id, AllIndexI, 5))
	check(db.Inc(id, AllIndexI64, -2)) // null + delta
	if err := db.Update(999, AllIndexS, "b"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("update of missing row:", err)
	}

	check(db.AppendLi(id, 3, 4))
	check(db.RemoveLi(id, 2))
	check(db.AppendLu(id, uuid.Nil))

	check(db.UpdateMaps(id, "a", "b"))
	check(db.MergeMaps(id, map[string]string{"c": "d", "e": "f"}))
	check(db.DeleteMaps(id, "c"))
	check(db.RenameMaps(id, "e", "g"))
	if err := db.RenameMaps(id, "nokey", "x"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("rename of missing key:", err)
	}
	check(db.AppendConfs(id, Config{A: 7}, Config{A: 8}))
	check(db.UpdateMapany(id, "it's", `quote " $$ ' test`))
	check(db.UpdateConf(id, "A", 9))

	got, err := db.Get(id)
	check(err)
	if got.S != "b" || got.I != 15 || !got.B || got.I64 != -2 {
		t.Error(got.S, got.I, got.B, got.I64)
	}
	if !reflect.DeepEqual(got.Li, []int{1, 3, 4}) || len(got.Lu) != 1 {
		t.Error(got.Li, got.Lu)
	}
	if !reflect.DeepEqual(got.Maps, map[string]string{"k": "v", "a": "b", "g": "f"}) {
		t.Error(got.Maps)
	}
	if !reflect.DeepEqual(got.Confs, []Config{{A: 7}, {A: 8}}) || got.Mapany["it's"] != `quote " $$ ' test` || got.Conf.A != 9 {
		t.Error(got.Confs, got.Mapany)
	}

	// save struct
	got.S, got.F64, got.Ls = "saved", 9.5, []string{"y", "z"}
	check(db.Save(&got, AllIndexS, AllIndexF64))
	got2, _ := db.Get(id)
	if got2.S != "saved" || got2.F64 != 9.5 || len(got2.Ls) != 1 {
		t.Error(got2.S, got2.F64, got2.Ls)
	}
	check(db.Save(&got))
	got2, _ = db.Get(id)
	testEqual(t, got, got2)

	if n, err := db.UpdateWhere(AllIndexS, "w", "i = $1", 15); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if n, err := db.UpdatesWhere(map[AllIndexTyped]any{AllIndexS: "ww", AllIndexI: 1}, "s = $1", "w"); err != nil || n != 1 {
		t.Fatal(n, err)
	}

	// upsert, full insert, sequence
	check(db.Upsert(&All{ID: id, S: "upsert", Nn: "n"}))
	check(db.Upsert(&All{ID: 77, S: "new", Nn: "n"}))
	check(db.InsertFull(&All{ID: 78, Nn: "n"}))
	check(db.ResetSequence())
	next := All{Nn: "n"}
	check(db.Insert(&next))
	if got, _ = db.Get(id); got.S != "upsert" || next.ID != 79 {
		t.Error(got.S, next.ID)
	}

	// copy
	n, err := db.InsertMany([]All{testItem(), testItem(), {Nn: "copy"}})
	if err != nil || n != 3 {
		t.Fatal("copy:", n, err)
	}

	check(db.Delete(id))
	if cnt, err := db.DeleteWhere("nn = $1", "copy"); err != nil || cnt != 1 {
		t.Fatal(cnt, err)
	}
}

func TestTx(t *testing.T) {
	db, ctx := testDB(t)
	if _, err := db.Pool().Exec(ctx, "drop table if exists tags"); err != nil {
		t.Fatal(err)
	}
	tags, e := NewTagSQL(db.Pool())
	if e != nil {
		t.Fatal(e)
	}

	// rollback of two models
	fail := errors.New("fail")
	err := db.Begin(func(tx pgx.Tx) error {
		v := All{Nn: "rollback"}
		if err := db.TX(tx).Insert(&v); err != nil {
			return err
		}
		if err := tags.TX(tx).Insert(&Tag{Name: "rollback", Owner: v.ID}); err != nil {
			return err
		}
		return fail
	})
	if !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if n, _ := db.Count(); n != 0 {
		t.Fatal("rollback all:", n)
	}
	if n, _ := tags.Count(); n != 0 {
		t.Fatal("rollback tags:", n)
	}

	// commit of two models
	var v All
	err = db.Begin(func(tx pgx.Tx) error {
		v = All{Nn: "commit"}
		if err := db.TX(tx).Insert(&v); err != nil {
			return err
		}
		tag := Tag{Name: "commit", Owner: v.ID}
		if err := tags.TX(tx).Insert(&tag); err != nil {
			return err
		}
		// savepoint
		_ = db.TX(tx).Begin(func(sp pgx.Tx) error {
			_ = db.TX(sp).Insert(&All{Nn: "nested"})
			return fail
		})
		// not visible out of transaction
		if n, _ := tags.Count(); n != 0 {
			t.Error("isolation:", n)
		}
		// errors.E is not error: empty errors.E returned as error is not nil
		if err := tags.TX(tx).UpdateMeta(tag.ID, "views", 5); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	list, err := db.All()
	if err != nil || len(list) != 1 || list[0].Nn != "commit" {
		t.Fatal(list, err)
	}
	// own variable: errors.E assigned to variable of type error is never nil
	got, e := tags.GetBy(TagIndexOwner, v.ID)
	if e != nil || got.Name != "commit" || got.Meta["views"] != 5 {
		t.Fatal(got, e)
	}

	// own query on connection of pool
	var one int
	err = db.Conn(func(conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "select 1").Scan(&one)
	})
	if err != nil || one != 1 {
		t.Fatal(one, err)
	}

	if ok, err := db.InsertNoConflict(&All{Nn: "x"}); err != nil || !ok {
		t.Fatal(ok, err)
	}
}

// queries from many goroutines on one class
func TestParallel(t *testing.T) {
	db, _ := testDB(t)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := All{Nn: "p", I: i, Li: []int{i}}
			if err := db.Insert(&v); err != nil {
				t.Error(err)
				return
			}
			if err := db.AppendLi(v.ID, i+1); err != nil {
				t.Error(err)
			}
			if err := db.Updates(v.ID, map[AllIndexTyped]any{AllIndexS: "s", AllIndexB: true}); err != nil {
				t.Error(err)
			}
			got, err := db.Get(v.ID, AllIndexI, AllIndexLi, AllIndexS)
			if err != nil || got.I != i || len(got.Li) != 2 || got.S != "s" {
				t.Error(got.I, got.Li, got.S, err)
			}
			if _, err := db.Search(&AllQuery{EQ: map[string]any{"i": i}, Fields: []string{"i"}}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n, _ := db.Count(); n != 50 {
		t.Fatal(n)
	}
}

// own functions from interface tagSQL
func TestFuncs(t *testing.T) {
	db, ctx := testDB(t)
	if _, err := db.Pool().Exec(ctx, "drop table if exists tags"); err != nil {
		t.Fatal(err)
	}
	tags, err := NewTagSQL(db.Pool())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []Tag{{Name: "a", Owner: 1}, {Name: "b", Owner: 1}, {Name: "c", Owner: 2}} {
		if err := tags.Insert(&v); err != nil {
			t.Fatal(err)
		}
	}

	if list, err := tags.ByOwner(1); err != nil || len(list) != 2 || list[1].Name != "b" {
		t.Fatal(list, err)
	}
	if v, err := tags.ByName("c"); err != nil || v.Owner != 2 {
		t.Fatal(v, err)
	}
	// !404: empty value without error
	if v, err := tags.ByName("none"); err != nil || v.ID != 0 {
		t.Fatal(v, err)
	}
	if n, err := tags.Owner(100); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	// without !404
	// option ee: errors.E, not found is 404
	if _, err := tags.Name(100); err == nil || err.Code() != 404 {
		t.Fatal(err)
	}
	// unique violation is exist
	if err := tags.InsertFull(&Tag{ID: 1}); err == nil || err.ID() != "exist" || err.Comment() != "tags_pkey" {
		t.Fatal(err)
	}
	// other errors of postgres are database errors
	if _, err := tags.Select(nil, "nocolumn = 1"); err == nil || err.Code() != 500 {
		t.Fatal(err)
	}
	if err := tags.Update(100, TagIndexName, "x"); err == nil || err.Code() != 404 {
		t.Fatal(err)
	}
	if name, err := tags.Name(1); err != nil || name != "a" {
		t.Fatal(name, err)
	}
	if ids, err := tags.IDs([]int{1, 2}); err != nil || !reflect.DeepEqual(ids, []int{1, 2, 3}) {
		t.Fatal(ids, err)
	}
	if n, err := tags.Rename(1, "x"); err != nil || n != 2 {
		t.Fatal(n, err)
	}

	// reserved sql words: go From, json from, column ifrom
	rw := Tag{Name: "rw", Owner: 9, From: 5, To: 6}
	if err := tags.Insert(&rw); err != nil {
		t.Fatal(err)
	}
	if v, err := tags.GetBy(TagIndexFrom, 5); err != nil || v.To != 6 || TagIndexFrom.SQLName() != "ifrom" {
		t.Fatal(v, err)
	}
	if list, err := tags.Search(&TagQuery{EQ: map[string]any{"from": 5}, Sort: "to"}); err != nil || len(list) != 1 {
		t.Fatal(list, err)
	}
	if err := tags.Delete(rw.ID); err != nil {
		t.Fatal(err)
	}

	// all or nothing
	err = tags.Begin(func(tx pgx.Tx) merrors.E {
		if err := tags.TX(tx).Drop(3); err != nil {
			return err
		}
		_, err := tags.TX(tx).Name(100) // not found: rollback
		return err
	})
	if err == nil || err.Code() != 404 {
		t.Fatal(err)
	}
	// commit
	err = tags.Begin(func(tx pgx.Tx) merrors.E {
		return tags.TX(tx).Update(1, TagIndexName, "tx")
	})
	if name, _ := tags.Name(1); err != nil || name != "tx" {
		t.Fatal(name, err)
	}
	if err := tags.Conn(func(conn *pgxpool.Conn) merrors.E { return nil }); err != nil {
		t.Fatal(err)
	}
	if n, _ := tags.Count(); n != 3 {
		t.Fatal("rollback:", n)
	}
	if err := tags.Drop(3); err != nil {
		t.Fatal(err)
	}
	if n, _ := tags.Count(); n != 2 {
		t.Fatal(n)
	}
}
