package news

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)

// go test ./test/ -run xxx -bench Angel -benchmem

func benchAngel() Angel {
	return Angel{
		ID:      100500,
		Role:    3,
		Key:     uuid.MustParse("8e1f2a4c-6b3d-4f7e-9a0c-2d5b8e1f3a6c"),
		Created: time.Date(2026, 11, 24, 10, 30, 0, 0, time.UTC),
		Smalls:  200,
		Name:    "Gabriel the Messenger",
		About:   []string{"guardian of the east gate", "plays the horn", "speaks nine languages", "never sleeps"},
		Music:   []int{101, 202, 303, 404, 505, 606, 707, 808, 909, 1010},
		Links: map[string]string{
			"site":    "https://example.com/angels/gabriel",
			"twitter": "@gabriel",
			"email":   "gabriel@example.com",
		},
		Stats: map[string]int{"views": 4315411, "likes": 62400, "followers": 1200, "posts": 87},
	}
}

func TestAngelJSON(t *testing.T) {
	item := benchAngel()
	for name, f := range map[string]func() (Angel, error){
		"std":      func() (v Angel, err error) { b, _ := json.Marshal(&item); err = json.Unmarshal(b, &v); return },
		"jsoniter": func() (v Angel, err error) { b, _ := jsoniter.Marshal(&item); err = jsoniter.Unmarshal(b, &v); return },
		"goccy":    func() (v Angel, err error) { b, _ := gojson.Marshal(&item); err = gojson.Unmarshal(b, &v); return },
		"pack":     func() (Angel, error) { return ParseAngel(item.Pack()) },
	} {
		got, err := f()
		if err != nil || !reflect.DeepEqual(item, got) {
			t.Errorf("%s: %v\nwant %+v\ngot  %+v", name, err, item, got)
		}
	}
}

func BenchmarkAngelMarshal(b *testing.B) {
	item := benchAngel()
	b.Run("std", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = json.Marshal(&item)
		}
	})
	b.Run("jsoniter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = jsoniter.Marshal(&item)
		}
	})
	b.Run("goccy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = gojson.Marshal(&item)
		}
	})
	b.Run("pack", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = item.Pack()
		}
	})
}

func BenchmarkAngelUnmarshal(b *testing.B) {
	item := benchAngel()
	data, _ := json.Marshal(&item)
	b.Run("std", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var v Angel
			_ = json.Unmarshal(data, &v)
		}
	})
	b.Run("jsoniter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var v Angel
			_ = jsoniter.Unmarshal(data, &v)
		}
	})
	b.Run("goccy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var v Angel
			_ = gojson.Unmarshal(data, &v)
		}
	})
	b.Run("parse", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, _ = ParseAngel(data)
		}
	})
}
