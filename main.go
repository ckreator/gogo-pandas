package main

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
)

type Row []interface{}

type Table []Line

// TODO: maybe use another goroutine to load the file from disk? Not sure what's faster...
func ParseCSV(fname string) (*Table, error) {
	dat, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	input := string(dat)
	//fmt.Println("INPUT:", input)

	// prepare channels here and then wait for the 'done' signal to come
	charStream := make(chan string)
	tokStream := make(chan token)
	done := make(chan *Table)

	go tokenize(charStream, tokStream)
	go build(tokStream, done)

	for _, v := range input {
		charStream <- string(v)
	}
	// close the first stream
	close(charStream)

	for table := range done {
		return table, nil
	}
	return nil, nil //errors.New("Not implemented")
}

type token struct {
	Value string
}

func tokenize(chars <-chan string, toks chan<- token) {
	curr := ""
	for c := range chars {
		if c == "," {
			toks <- token{curr}
			curr = ""
		} else if c == "\n" {
			toks <- token{curr}
			// send the builder a newline token
			toks <- token{"\n"}
			curr = ""
		} else {
			curr += c
		}
	}
	// send last message
	toks <- token{curr}
	// close the token channel
	close(toks)
}

type Line struct {
	A string  `json:"str"`
	B int64   `json:"int"`
	C float64 `json:"float"`
}

func toStr(str string) interface{} {
	return str
}

func toInt(str string) interface{} {
	i, _ := strconv.ParseInt(str, 10, 64)
	return i
}

func toFloat(str string) interface{} {
	f, _ := strconv.ParseFloat(str, 64)
	return f
}

type converter func(string) interface{}

var order = []string{"str", "int", "float"}
var conv = []converter{toStr, toInt, toFloat}

func build(toks <-chan token, done chan<- *Table) {
	index := 0
	tab := Table{}
	mp := map[string]interface{}{}
	for t := range toks {
		if t.Value == "\n" {
			//	fmt.Println("GOT LINE", mp)
			tab = append(tab, makeStruct(mp))
			//	fmt.Println("STRUCT:", s)
			mp = map[string]interface{}{}
			index = 0
		} else {
			mp[order[index]] = conv[index](t.Value)
			index++
		}
	}
	done <- &tab
	close(done)
}

func makeStruct(mp map[string]interface{}) Line {
	var l Line
	b, _ := json.Marshal(mp)
	_ = json.Unmarshal(b, &l)
	return l
}

func main() {
	//fmt.Println("vim-go")
	ParseCSV("./test_data/simple.csv")
}
