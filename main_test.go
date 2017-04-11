package main

import (
	"fmt"
	"testing"
)

func sufferIf(e error) {
	if e != nil {
		panic(e)
	}
}

func TestParseSimple(t *testing.T) {
	dat, err := ParseCSV("./test_data/simple.csv")
	sufferIf(err)
	fmt.Print(dat)
}
