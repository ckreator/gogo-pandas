package main

import (
	"fmt"
	"io/ioutil"
)

type Row []interface{}

type Table []Row

// TODO: maybe use another goroutine to load the file from disk? Not sure what's faster...
func ParseCSV(fname string) (*Table, error) {
	dat, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	input := string(dat)
	fmt.Println("INPUT:", input)

	// prepare channels here and then wait for the 'done' signal to come
	charStream := make(chan string)
	tokStream := make(chan token)
	done := make(chan *Table)

	go tokenize(charStream, tokStream)
	go build(tokStream, done)

	for _, v := range input {
		charStream <- string(v)
		fmt.Println("STRING:", string(v))
	}
	// close the first stream
	close(charStream)

	for table := range done {
		fmt.Println("GOT TABLE:", table)
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

func build(toks <-chan token, done chan<- *Table) {
	for t := range toks {
		fmt.Println("TOKEN:", t)
	}
	done <- &Table{}
	close(done)
}

func main() {
	fmt.Println("vim-go")
}
