package examples

// go sql
type user struct {
	id       int    // sql{inc} @public #readonly
	name     string // @public
	password string
}
