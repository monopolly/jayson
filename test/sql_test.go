package news

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JAYSON_TEST_DB=postgres://postgres@127.0.0.1:5432/postgres go test ./test/
func TestSQL(t *testing.T) {
	url := os.Getenv("JAYSON_TEST_DB")
	if url == "" {
		t.Skip("JAYSON_TEST_DB is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	for _, table := range []string{"news", NewStatSQL(pool).TableName(), NewStat2SQL(pool).TableName()} {
		if _, err := pool.Exec(ctx, "drop table if exists "+table); err != nil {
			t.Fatal(err)
		}
	}

	// tables and indexes of all models
	if err := NewStatSQL(pool).Init(ctx); err != nil {
		t.Fatal("stat:", err)
	}
	if err := NewStat2SQL(pool).Init(ctx); err != nil {
		t.Fatal("stat2:", err)
	}
	db := NewNewsSQL(pool)
	if err := db.Init(ctx); err != nil {
		t.Fatal("news:", err)
	}
	if err := db.Init(ctx); err != nil {
		t.Fatal("news again:", err)
	}

	a := News{Sql_search: "red fast car", Sql_get: "a", Sql_primary: 1, Sql_unique_u1_1: 1, Sql_unique_x1: 1, Sql_unique_x2: 1, Public_field1: 5}
	b := News{Sql_search: "blue slow boat", Sql_get: "b", Sql_primary: 2, Sql_unique_u1_1: 2, Sql_unique_x1: 2, Sql_unique_x2: 2}
	if err := db.Insert(ctx, &a); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert(ctx, &b); err != nil {
		t.Fatal(err)
	}

	// unique conflict
	c := b
	if ok, err := db.InsertNoConflict(ctx, &c); err != nil || ok {
		t.Fatal(ok, err)
	}

	list, err := db.FullTextSearch(ctx, "car -boat", 10)
	if err != nil || len(list) != 1 || list[0].Inc != a.Inc {
		t.Fatal(list, err)
	}
	if v, err := db.GetBySql_get(ctx, "b"); err != nil || v.Inc != b.Inc {
		t.Fatal(v.Inc, err)
	}
	if p, err := db.GetPublic(ctx, a.Inc); err != nil || p.Public_field1 != 5 {
		t.Fatal(p, err)
	}

	// upsert by primary key
	b.Sql_get = "upsert"
	if err := db.Upsert(ctx, &b); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(ctx, b.Inc, NewsIndexSql_get); err != nil || v.Sql_get != "upsert" {
		t.Fatal(v.Sql_get, err)
	}
	if n, _ := db.Count(ctx); n != 2 {
		t.Fatal(n)
	}
}
