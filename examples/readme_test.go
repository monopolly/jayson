package examples

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	merrors "github.com/monopolly/errors"
)

// every snippet of README.md: it must compile
func use(pool *pgxpool.Pool, conn driver.Conn, body []byte, list []Account, from, to, id int) {
	db := NewAccountSQL(pool)
	var err error
	acc := Account{Email: "kate@example.com", Name: "Kate", Tags: []string{"new"}}
	acc.ID, err = db.Insert(acc)
	err = db.Inc(acc.ID, AccountIndexBalance, 50)
	err = db.AppendTags(acc.ID, "vip")
	err = db.UpdateMeta(acc.ID, "lang", "en")
	rich, err := db.Select(nil, "balance > $1 order by balance desc limit 10", 100)

	id, ok, err := db.InsertNoConflict(acc)
	err = db.Upsert(acc)
	n, err := db.InsertMany(list)
	acc, err = db.Get(acc.ID)
	acc, err = db.Get(acc.ID, AccountIndexName, AccountIndexBalance)
	acc, err = db.GetBy(AccountIndexEmail, "kate@example.com")
	if errors.Is(err, pgx.ErrNoRows) {
	}
	page, err := db.List(20, 40)
	total, err := db.Count()
	err = db.Update(acc.ID, AccountIndexName, "Max")
	err = db.Updates(acc.ID, map[AccountIndexTyped]any{AccountIndexName: "Max", AccountIndexBalance: 0})
	err = db.Inc(acc.ID, AccountIndexBalance, -10)
	err = db.Save(acc, AccountIndexName)
	count, err := db.UpdateWhere(AccountIndexBalance, 0, "balance < $1", 0)
	err = db.Delete(acc.ID)
	count, err = db.DeleteWhere("email like $1", "%@spam.com")

	err = db.AppendTags(id, "a", "b")
	err = db.RemoveTags(id, "a")
	err = db.AppendScores(id, 10, 20)
	err = db.UpdateSettings(id, "volume", 7)
	err = db.MergeSettings(id, map[string]int{"a": 1, "b": 2})
	err = db.RenameSettings(id, "a", "c")
	err = db.DeleteSettings(id, "b")
	err = db.AppendItems(id, Item{Name: "book"})

	rich, err = db.Rich(1000)
	acc, err = db.ByEmail("nobody@example.com")
	bal, err := db.Balance(id)
	ids, err := db.IDs([]string{"a"})
	n, err = db.Reset(0)
	err = db.Drop(id)

	accounts := db
	orders := NewOrderSQL(pool)
	err = accounts.Begin(func(tx pgx.Tx) error {
		if err := accounts.TX(tx).Inc(from, AccountIndexBalance, -50); err != nil {
			return err
		}
		return accounts.TX(tx).Inc(to, AccountIndexBalance, 50)
	})
	var e merrors.E = orders.Begin(func(tx pgx.Tx) merrors.E {
		_, err := orders.TX(tx).Insert(Order{Sender: from, Receiver: to, Sum: 50})
		return err
	})
	var o Order
	o, e = orders.Get(1)

	var q AccountQuery
	json.Unmarshal(body, &q)
	rich, err = db.Search(&q)

	articles := NewArticleSQL(pool)
	found, err := articles.FullTextText("postgres index -mysql", 20)

	users := NewUserSQL(pool)
	pub, err := users.GetPublic(id)
	u, _ := users.Get(id)
	view := u.UserPublic()
	fields := UserReadonlyList()

	ch := NewEventCQL(conn)
	ch.Add(Event{ID: 1, Country: "DE", IP: "10.0.0.1", Path: "/"})
	err = ch.Push()
	err = ch.Insert(Event{ID: 2, Country: "US"})
	ev, err := ch.Get(1)
	events, err := ch.Select(nil, "country = $1 order by id limit 10", "DE")
	cnt, err := ch.CountWhere("country = $1", "DE")
	stats, err := ch.Stat(EventIndexCountry)
	err = ch.Delete(1)
	top, err := ch.Top("DE")
	_, _, _ = ev, cnt, top

	item := Stat{ID: 1, Name: "views", Values: []uint64{1, 2, 3}}
	b := item.Pack()
	item, err = ParseStat(b)
	b = item.MarshalGotiny()
	item = UnmarshalStatGotiny(b)
	b = item.MessagePack()
	item, err = ParseStatMessagePack(b)
	m := item.Map()
	v := item.Get("name")
	var _ StatProto

	_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ = rich, ok, n, page, total, count, bal, ids, e, o, found, pub, view, fields, events, stats, m, v, err, acc, list
}

func TestReadme(t *testing.T) {}
