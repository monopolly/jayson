package alltypes

// go=Click ch=clicks sql=clicks ee
type click struct {
	id   int // sql{inc} ch{primary}
	link string
	tags []string
}
