package examples

// go sql
type article struct {
	id    int    // sql{inc}
	title string // sql{search="text"}
	body  string // sql{search="text"}
}
