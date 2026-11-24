<p align="center">
  <img src="logo.svg" width="360" alt="jayson">
</p>

<p align="center">
  Describe a Go struct once. Get PostgreSQL, ClickHouse, protobuf and fast JSON code for it.
</p>

---

`jayson` is a code generator. It reads a plain `.go` file with one struct and a few short tags in comments,
and writes ready-to-use Go code next to it: the exported struct, typed PostgreSQL queries on
[pgx](https://github.com/jackc/pgx), a ClickHouse buffer, protobuf, binary and JSON encoding, and the DDL files.

```go
// go sql
type account struct {
	id      int               // sql{inc}
	email   string            // sql{unique="email"}
	name    string            // sql{index}
	balance int64
	tags    []string
	meta    map[string]string
}
```

```bash
jayson account.go
```

```go
db := NewAccountSQL(pool) // creates the table and indexes if they do not exist

acc := Account{Email: "kate@example.com", Name: "Kate", Tags: []string{"new"}}
acc.ID, err = db.Insert(acc)                 // returns the generated id
err = db.Inc(acc.ID, AccountIndexBalance, 50) // balance = balance + 50
err = db.AppendTags(acc.ID, "vip")           // tags = tags || '{vip}'
err = db.UpdateMeta(acc.ID, "lang", "en")    // jsonb_set(meta, '{lang}', '"en"')

rich, err := db.Select(nil, "balance > $1 order by balance desc limit 10", 100)
```

## Why

- **One source of truth.** The struct, the table, the indexes, the queries and the wire formats come from the same
  ten lines. Add a field, run the generator, everything follows.
- **Native pgx, no ORM.** Rows are scanned straight into struct fields with `Scan`, in the binary protocol,
  without reflection or `map[string]any` in between. Go slices are real PostgreSQL arrays (`bigint[]`, `text[]`),
  not JSON.
- **No runtime dependency.** The generated file is self-contained: it imports pgx and nothing from this
  repository. You can read it, debug it and commit it.
- **SQL stays SQL.** Conditions are plain SQL with parameters. Your own queries are declared as a Go interface
  and become typed methods.
- **Safe by construction.** Values are always query parameters. Column names come from generated constants,
  never from strings.
- **Typed helpers for every array and jsonb field**: `AppendTags`, `RemoveTags`, `UpdateMeta`, `MergeMeta`...
- **Transactions across models** in one function.
- **Fast JSON.** `Pack` and `Parse` use [goccy/go-json](https://github.com/goccy/go-json).
- **Tested against real databases.** The generated code is covered by tests on PostgreSQL and ClickHouse:
  every supported type, `NULL` handling, transactions and parallel use under the race detector.

## Install

```bash
go install github.com/monopolly/jayson@latest
```

For protobuf you also need `protoc` and `protoc-gen-go`:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

Run it on a model file. The output is written next to it:

```bash
jayson account.go
```

| File | Content |
| --- | --- |
| `accountGO.go` | `Account` struct, field constants, JSON, `AccountSQL`, `AccountCQL`, protobuf code |
| `account.sql` | PostgreSQL table, indexes, migration lines |
| `account.cql` | ClickHouse table (option `ch`) |
| `account.proto` | protobuf schema (option `proto`) |
| `accountGO_test.go` | tests and benchmarks (option `test`) |

Use one model per source file. Several models can share a package: every generated name carries the struct name.

## Examples

The models and generated files of these examples are in [examples/](examples/).

### 1. CRUD

```go
pool, _ := pgxpool.New(ctx, dsn)
db := NewAccountSQL(pool)

// insert
acc := Account{Email: "kate@example.com", Name: "Kate"}
acc.ID, err = db.Insert(acc)              // returns the generated id
id, ok, err := db.InsertNoConflict(acc)   // ok is false when the row exists
err = db.Upsert(acc)                      // insert or update by primary key
n, err := db.InsertMany(list)             // COPY protocol, for bulk loads

// read: all columns, or only the listed ones
acc, err = db.Get(acc.ID)
acc, err = db.Get(acc.ID, AccountIndexName, AccountIndexBalance)
acc, err = db.GetBy(AccountIndexEmail, "kate@example.com")
if errors.Is(err, pgx.ErrNoRows) {
	// not found
}

list, err := db.Select(nil, "balance > $1 order by balance desc", 100)
page, err := db.List(20, 40) // limit, offset
total, err := db.Count()

// update
err = db.Update(acc.ID, AccountIndexName, "Max")
err = db.Updates(acc.ID, map[AccountIndexTyped]any{AccountIndexName: "Max", AccountIndexBalance: 0})
err = db.Inc(acc.ID, AccountIndexBalance, -10)
acc.Name = "Kate"
err = db.Save(acc, AccountIndexName)  // write listed fields of the struct
count, err := db.UpdateWhere(AccountIndexBalance, 0, "balance < $1", 0)

// delete
err = db.Delete(acc.ID)
count, err = db.DeleteWhere("email like $1", "%@spam.com")
```

`fields` selects columns, all columns when empty. `where` is an SQL condition with `$1, $2...` placeholders.
A missing row is `pgx.ErrNoRows`.

### 2. Arrays and jsonb

Every array and jsonb field gets its own typed methods, so you do not have to remember operators.

```go
type account struct {
	id       int            // sql{inc}
	tags     []string       // text[]
	scores   []int          // bigint[]
	settings map[string]int // jsonb
	items    []Item         // jsonb array
}
```

```go
err = db.AppendTags(id, "a", "b")       // tags = tags || '{a,b}'
err = db.RemoveTags(id, "a")            // array_remove(tags, 'a')
err = db.AppendScores(id, 10, 20)

err = db.UpdateSettings(id, "volume", 7)                // set one key
err = db.MergeSettings(id, map[string]int{"a": 1, "b": 2})
err = db.RenameSettings(id, "a", "c")
err = db.DeleteSettings(id, "b")

err = db.AppendItems(id, Item{Name: "book"})
```

### 3. Your own queries

Declare an interface named `<struct>SQL` in the model file. The comment above a method is its SQL.
The generator turns every method into a typed method of `AccountSQL`.

```go
type accountSQL interface {
	// where balance > @min order by balance desc
	Rich(min int64) ([]Account, error)

	// where email = @email
	ByEmail(email string) (Account, error) // !404

	// select balance from accounts where id = @id
	Balance(id int) (int64, error)

	// select id from accounts where name = any(@names)
	IDs(names []string) ([]int, error)

	// update accounts set balance = 0 where balance < @min
	Reset(min int64) (int64, error)

	// delete from accounts where id = @id
	Drop(id int) error
}
```

```go
rich, err := db.Rich(1000)
acc, err := db.ByEmail("nobody@example.com") // zero value, no error: !404
```

| Result | Generated |
| --- | --- |
| `([]Account, error)`, `(Account, error)` | Rows of the model. Start the SQL with `where`, `order` or `limit`: columns are added for you. |
| `(T, error)` | One value. |
| `([]T, error)` | A column of values. One array value when the query selects an array column of the model or has the `!array` flag. |
| `(int64, error)` with `update`/`delete`/`insert` | Count of affected rows. |
| `error` | Exec. |

`@name` is a parameter of the function. A name that is missing in the signature, or a parameter that is not used,
is a generation error, not a runtime surprise. `!404` at the end of the line returns the zero value and `nil`
instead of `pgx.ErrNoRows`.

### 4. Transactions

`Begin` opens a transaction, commits when the function returns `nil` and rolls everything back on an error.
`TX(tx)` runs the queries of any model inside it.

```go
err := accounts.Begin(func(tx pgx.Tx) error {
	if err := accounts.TX(tx).Inc(from, AccountIndexBalance, -50); err != nil {
		return err
	}
	if err := accounts.TX(tx).Inc(to, AccountIndexBalance, 50); err != nil {
		return err
	}
	_, err := orders.TX(tx).Insert(Order{Sender: from, Receiver: to, Sum: 50})
	return err
})
```

`AccountSQL` keeps no state and works on `pgxpool`, so one instance is safe to use from any number of goroutines.

For queries of your own, write methods on `AccountSQL` and use its `db` field: it is the pool, or the transaction
after `TX(tx)`. `Conn(f)` gives a dedicated connection of the pool.

### 5. Search from JSON

`AccountQuery` can be filled straight from a request body. Keys are checked against the model,
values are query parameters, an unknown key is an error.

```go
var q AccountQuery
json.Unmarshal(body, &q)
// {"eq": {"name": "Kate"}, "gt": {"balance": 10}, "in": {"id": [1, 2, 3]},
//  "like": {"email": "example"}, "sort": "balance", "desc": true, "limit": 20}

list, err := db.Search(&q)
```

### 6. Full text search

```go
type article struct {
	id    int    // sql{inc}
	title string // sql{search="text"}
	body  string // sql{search="text"}
}
```

This adds a generated `tsvector` column, a GIN index and a method:

```go
list, err := db.FullTextText("postgres index -mysql", 20) // best match first
```

### 7. Views of a struct

Mark fields with `@name` to get a smaller struct with only those fields, and with `#name` to get field lists.

```go
type user struct {
	id       int    // sql{inc} @public #readonly
	name     string // @public
	password string
}
```

```go
pub, err := db.GetPublic(id)   // UserPublic{ID, Name}: selects only these columns
view := user.UserPublic()      // the same from a loaded struct
fields := UserReadonlyList()   // []UserIndexTyped{UserIndexID}
```

### 8. ClickHouse

The same model, the same style of API. `EventCQL` also buffers rows in memory and writes them in batches.

```go
// go ch=events engine=ReplacingMergeTree(id) partition=intDiv(id,1000000)
type event struct {
	id      uint64 // ch{primary order}
	country string // ch{low order}
	ip      string // ch{ip}
	path    string
	tags    []string
}

type eventCQL interface {
	// where country = @country order by id desc limit 10
	Top(country string) ([]Event, error)
}
```

```go
ch := NewEventCQL(conn) // creates the table if it does not exist

// batch insert through the buffer: written every 10 minutes and by Push
ch.Add(Event{ID: 1, Country: "DE", IP: "10.0.0.1", Path: "/"})
err := ch.Push()

// batch insert now, without the buffer
err = ch.Insert(list...)

ev, err := ch.Get(1)
found, err := ch.Select(nil, "country = $1 order by id limit 10", "DE")
n, err := ch.CountWhere("country = $1", "DE")
stats, err := ch.Stat(EventIndexCountry) // map[string]int{"DE": 425, "US": 134}
top, err := ch.Top("DE")                 // own query

err = ch.Delete(1)
```

The table is described by field tags and two struct options:

| Where | Tag | Result |
| --- | --- | --- |
| field | `ch{primary}` | `primary key (id)`, several fields make a composite key |
| field | `ch{order}` | `order by (id, country)`, no `order by` without the tag |
| field | `ch{low}` | `LowCardinality(String)` |
| field | `ch{ip}`, `ch{ip6}` | `IPv4`, `IPv6` |
| field | `ch{ttl=30d}` | `ttl created + interval 30 day` on a time field: `30s`, `10m`, `1h`, `1d`, `2w`, `6M`, `1y` |
| struct | `engine=ReplacingMergeTree(id)` | engine, `MergeTree` by default |
| struct | `partition=toYYYYMM(created)` | `partition by` |

Engines of the `MergeTree` family need a key: generation stops when no field has `ch{primary}` or `ch{order}`.
In ClickHouse the primary key must be a prefix of the sorting key: put `order` on the `primary` fields first.
The header of every generated file with the `ch` option lists the engines with a short description.

ClickHouse is an insert and select database: there are no update methods. `Delete` is a mutation and waits
until the data is removed. A missing row is `sql.ErrNoRows`. Own queries live in the interface `<struct>CQL`
and follow the rules of example 3.

### 9. JSON, binary formats and protobuf

```go
// go proto gotiny msgp
type stat struct {
	id     int
	name   string
	values []uint64
}
```

```go
item := Stat{ID: 1, Name: "views", Values: []uint64{1, 2, 3}}

b := item.Pack()                 // JSON
item, err := ParseStat(b)

b = item.MarshalGotiny()         // gotiny
item = UnmarshalStatGotiny(b)

b = item.MessagePack()           // msgpack
item, err = ParseStatMessagePack(b)

m := item.Map()                  // map[string]any
v := item.Get("name")            // value by json key
```

`StatProto` and `stat.proto` are generated with the `proto` option.

JSON speed for a struct with strings, slices and maps, Apple M1 Pro:

| | Marshal | Unmarshal |
| --- | --- | --- |
| `encoding/json` | 1711 ns | 5734 ns |
| generated `Pack` / `Parse` | 645 ns | 1301 ns |

### 10. Errors as values of your own type

With the `ee` option every method of `AccountSQL` and `AccountCQL` returns `errors.E` from
[monopolly/errors](https://github.com/monopolly/errors): not found is 404, a unique violation is `exist`
with the constraint name.

```go
// go sql ee
```

```go
func (a *AccountSQL) Get(id int, fields ...AccountIndexTyped) (Account, errors.E)
func (a *AccountSQL) Begin(f func(tx pgx.Tx) errors.E) errors.E
```

`errors.E` is a concrete type, not the `error` interface. Do not store it in a variable of type `error`:
an empty `errors.E` stored as `error` is not `nil`.

## Reference

### Struct options

Options are written in the comment above the struct.

```go
// go=News sql=news ch=news proto test
type news struct {
```

| Option | Description |
| --- | --- |
| `go` / `go=News` | Name of the generated struct. |
| `sql` / `sql=news` | PostgreSQL class `NewsSQL` and the `.sql` file. |
| `ch` / `ch=news` | ClickHouse class `NewsCQL` and the `.cql` file. |
| `engine=ReplacingMergeTree(updated)` | ClickHouse engine, `MergeTree` by default. |
| `partition=toYYYYMM(created)` | ClickHouse `partition by`. |
| `proto` / `proto=NewsProto` | `.proto` file and protobuf Go code. |
| `ee` | PostgreSQL and ClickHouse methods return `errors.E` instead of `error`. |
| `gotiny`, `msgp` | gotiny and MessagePack encoding. |
| `test` | `GO_test.go` with tests and benchmarks. |
| `optimize` | Reorder fields to reduce struct padding. |
| `noinit` | Do not generate `New...()`. |
| `!omit` | No `omitempty` in JSON tags. |
| `lock` | Refuse to regenerate the model. |

### Field tags

Tags are written in the comment of a field.

```go
title string // sql{index} js{name="headline"} #readonly @public
```

| Tag | Description |
| --- | --- |
| `sql{inc}` | Identity column, usually `id int`. |
| `sql{primary}` | Primary key. Several fields make a composite key. |
| `sql{index}` | Index. GIN for jsonb and arrays. |
| `sql{idx="group"}` | Composite index of a group of fields. |
| `sql{unique="group"}` | Unique constraint of a group of fields. |
| `sql{search="group"}` | Full text search column, index and method. |
| `sql{get}` | Typed getter `GetByName(v)`. |
| `sql{type="date"}` | PostgreSQL type: `date`, `varchar(64)`, `numeric(18,2)`... |
| `sql{notnull}`, `sql{defaults}`, `sql{unix}` | `not null`, default by type, default unix time. |
| `sql{name="col"}` | Column name. |
| `sql{noinsert}` | Do not send the field in `Insert`. |
| `sql{ver="2"}`, `sql{renames="old"}` | Migration lines in the `.sql` file. |
| `sql{skip}` | No column for the field. |
| `ch{primary}`, `ch{order}`, `ch{low}`, `ch{ip}`, `ch{ip6}`, `ch{ttl=30d}` | ClickHouse primary key, sorting key, `LowCardinality`, `IPv4`, `IPv6`, life time of rows. |
| `ch{type="UInt8"}`, `ch{name="col"}`, `ch{unix}`, `ch{skip}` | ClickHouse type, column name, `DateTime` for unix time, skip. |
| `proto{name="..."}`, `proto{type="..."}`, `proto{skip}` | Protobuf field name, type, skip. |
| `go{name="..."}`, `go{type="..."}` | Go field name and type. |
| `js{name="..."}` | JSON key. |
| `#name` | Field list `NewsNameList()`. |
| `@name` | Struct `NewsName` with the marked fields. |

Go names have no underscores: the field `user_id` becomes `UserID`, the column stays `user_id`.

A field named as a reserved SQL word keeps its Go name and gets the prefix `i` in the column name:
`from int` is `From` in Go and `ifrom` in the table. The generator prints every such rename.

### Go to PostgreSQL types

| Go | PostgreSQL |
| --- | --- |
| `bool`, `string` | `boolean`, `text` |
| `int8`, `int16`, `uint8` | `smallint` |
| `int32`, `uint16`, `rune` | `integer` |
| `int`, `int64`, `uint32`, `time.Duration` | `bigint` |
| `uint`, `uint64` | `numeric(20,0)` |
| `float32`, `float64` | `real`, `double precision` |
| `time.Time` | `timestamptz` |
| `[]byte`, `[16]byte` | `bytea` |
| `uuid.UUID`, `pgtype.UUID` | `uuid` |
| `netip.Addr`, `net.IP` / `netip.Prefix`, `net.IPNet` | `inet` / `cidr` |
| `decimal.Decimal`, `pgtype.Numeric` | `numeric` |
| `[]T` | `T[]`, a native array |
| `*T`, `sql.Null*`, `pgtype.*` | type of `T`, nullable |
| `type Status int16` | type of the underlying type |
| `map`, `struct`, `any`, `json.RawMessage`, `[]struct`, `[][]T` | `jsonb` |

`NULL` in a scalar column is read as the zero value. Use a pointer or `sql.Null*` to tell `NULL` from zero.

### Methods of `AccountSQL`

| Group | Methods |
| --- | --- |
| Handle | `NewAccountSQL(pool)`, `Pool()`, `Conn(f)`, `TableName()`, `Fields()` |
| Transactions | `Begin(f)`, `TX(tx)` |
| Read | `Get`, `GetBy`, `GetWhere`, `Exists`, `Has`, `All`, `List`, `Select`, `Search`, `Count`, `CountWhere` |
| Insert | `Insert`, `InsertNoConflict`, `InsertFull`, `Upsert`, `InsertBatch`, `InsertMany` |
| Update | `Update`, `Updates`, `Save`, `Inc`, `UpdateWhere`, `UpdatesWhere` |
| Delete | `Delete`, `DeleteWhere` |
| Table | `CreateTable`, `CreateIndexes`, `ResetSequence`, `SetSequence` |

`NewAccountSQL` creates the table and indexes and never fails. To see an error of creation call `CreateTable()` or `CreateIndexes()`.

Models are passed and returned by value, never by pointer. With `sql{inc}` `Insert` returns the generated id, `InsertBatch` writes the ids into the items of the list.

Methods by id need a row id: `sql{inc}`, a single `sql{primary}` or a field named `id`.
`Update`, `Updates`, `Save`, `Inc` and the field methods return `pgx.ErrNoRows` when the row does not exist.

### Methods of `AccountCQL`

| Group | Methods |
| --- | --- |
| Handle | `NewAccountCQL(conn)`, `Conn()`, `TableName()`, `Fields()` |
| Insert | `Add`, `Len`, `Push`, `Insert` |
| Read | `Get`, `GetBy`, `GetWhere`, `Exists`, `Has`, `All`, `List`, `Select`, `Count`, `CountWhere`, `Stat` |
| Delete | `Delete`, `DeleteWhere` |
| Table | `CreateTable` |

The row id is `ch{primary}`, `sql{inc}` or a field named `id`.

Queries run on `context.Background()`. Limit their time with `statement_timeout` or pool settings.

## Tests

```bash
go test ./...
```

The database tests are skipped unless connection strings are set:

```bash
JAYSON_TEST_DB=postgres://postgres@127.0.0.1:5432/postgres \
JAYSON_TEST_CH=clickhouse://default:pass@127.0.0.1:9000/default \
go test ./test/alltypes/
```

## Notes

- Do not edit generated files. Change the model and run the generator again.
- Commit generated files if you want builds without running the generator.
