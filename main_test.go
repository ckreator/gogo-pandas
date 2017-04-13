package main

import (
	"io/ioutil"
	"testing"
)

func sufferIf(e error) {
	if e != nil {
		panic(e)
	}
}

/*func TestParseSimple(t *testing.T) {
	dat, err := ParseCSV("./test_data/simple.csv")
	sufferIf(err)
	fmt.Print(dat)
}*/

var tab *Table

var file = readf("./test_data/simple.csv")

func readf(fname string) string {
	dat, err := ioutil.ReadFile(fname)
	if err != nil {
		return ""
	}
	return string(dat)
}

func BenchmarkParse(b *testing.B) {
	var t *Table
	for n := 0; n < b.N; n++ {
		t, _ = ParseCSV("./test_data/simple.csv")
	}
	tab = t
}
