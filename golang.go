package main

import (
	"bytes"
)

var golang Golang

type Golang int

// Head renders constructor and struct: the top of generated file
func (a *Golang) Head() []byte {
	var res bytes.Buffer
	res.Write(a.New())
	res.Write(a.Struct())
	return res.Bytes()
}

// Fields renders indexes and names of fields.
// It runs first: other generators use names of indexes
func (a *Golang) Fields() []byte {
	var res bytes.Buffer
	res.Write(a.Index())
	res.Write(a.Tags())
	res.Write(a.IndexType())
	res.Write(a.IndexKeys())
	return res.Bytes()
}

// Methods renders methods of struct
func (a *Golang) Methods() []byte {
	var res bytes.Buffer
	res.Write(a.StructCustom())
	res.Write(a.IDStructToTuple())
	res.Write(a.Get())
	res.Write(a.GetString())
	res.Write(a.Valid())
	res.Write(a.Map())
	res.Write(a.Iterate())
	res.Write(a.FastJson())
	res.Write(a.FastJsonUnmarshall())
	res.Write(a.Gotiny())
	res.Write(a.GotinyUnmarshall())
	res.Write(a.Msgp())
	res.Write(a.MsgpUnmarshall())
	return res.Bytes()
}
