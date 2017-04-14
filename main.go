package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type Row []interface{}

type Table []Line

// TODO: maybe use another goroutine to load the file from disk? Not sure what's faster...
func ParseCSV(fname string) (Table, error) {
	var err error
	var read *bufio.Reader

	f, _ := os.Open("./test_data/simple.csv")
	read = bufio.NewReader(f)

	// prepare channels here and then wait for the 'done' signal to come
	/*charStream := make(chan byte)
	tokStream := make(chan token)
	done := make(chan *Table)

	go tokenize(charStream, tokStream)
	go build(tokStream, done)*/

	/*	for _, v := range input {
		charStream <- string(v)
	}*/

	//fmt.Println("START READING FILE")

	err = nil
	//curr := make([]byte, 100)
	pindex := 0
	index := 0
	currTok := 0
	//line := make([]byte, 100)
	var line []byte
	//var c byte
	tokens := make([]token, 10000)

	// if char, just move on
	// if comma, set pindex to index and move on
	// if newline, push token from [pindex:index], reset index + pindex

	for err == nil {
		//fmt.Println("READ FILE:")
		if line, err = read.ReadSlice('\n'); err == nil {
			//fmt.Print("LINE:", string(line))
			index = 0
			pindex = 0
			for i := 0; i < len(line); i++ {
				if line[i] == ',' {
					//				fmt.Println("COMMA:", pindex, index)
					tokens[currTok] = token{line[pindex:index]}
					currTok++
					pindex = index + 1
				}
				index++
			}
			// set last token
			tokens[currTok] = token{line[pindex : index-1]}
			currTok++
			// send the builder a newline token
			//tokens = append(tokens, token{[]byte{'\n'}})
			// send last message
			//tokens = append(tokens, token{curr[0:index]})
			// close the token channel
		}
	}
	// close the first stream
	//fmt.Println("GOT TOKENS:", tokens[0:1000])

	// now we have the tokens, let's parse the structure
	tab := Table{}
	for i := 0; i < currTok; i += 3 {
		t1 := tokens[i]
		t2 := tokens[i+1]
		t3 := tokens[i+2]

		tab = append(tab, Line{
			A: toStr(t1.Value),
			B: toInt(t2.Value),
			C: toFloat(t3.Value),
		})
	}

	//fmt.Println("GOT TAB:", len(tab))

	return tab, nil //errors.New("Not implemented")
}

type token struct {
	Value []byte
}

func tokenize(chars <-chan byte, toks chan<- token) {
	curr := make([]byte, 100)
	index := 0
	var c byte
	for c = range chars {
		if c == ',' {
			toks <- token{curr[0:index]}
			index = 0
		} else if c == '\n' {
			toks <- token{curr[0:index]}
			// send the builder a newline token
			toks <- token{[]byte{'\n'}}
			index = 0
		} else {
			curr[index] = c
			index++
		}
	}
	// send last message
	toks <- token{curr[0:index]}
	// close the token channel
	close(toks)
}

type Line struct {
	A string  `json:"str"`
	B int64   `json:"int"`
	C float64 `json:"float"`
}

func toStr(str []byte) string {
	return string(str)
}

func toInt(str []byte) int64 {
	i, _ := strconv.Atoi(string(str))
	return int64(i)
}

func toFloat(str []byte) float64 {
	f, _ := strconv.ParseFloat(string(str), 64)
	fmt.Println("FLOAT:", string(str), f)
	return f
}

type converter func([]byte) interface{}

var order = []string{"str", "int", "float"}

/*var conv = []converter{toStr, toInt, toFloat}

func build(toks <-chan token, done chan<- *Table) {
	//index := 0
	tab := Table{}
	//mp := map[string]interface{}{}
	for _ = range toks {
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
*/
func makeStruct(mp map[string]interface{}) Line {
	var l Line
	b, _ := json.Marshal(mp)
	_ = json.Unmarshal(b, &l)
	return l
}

func main() {
	//fmt.Println("vim-go")
	var tabs []Table
	N := 1
	for i := 0; i < N; i++ {
		tab, _ := ParseCSV("./test_data/simple.csv")
		fmt.Println("I:", i, tab[0])
		tabs = append(tabs, tab)
	}
	fmt.Println("TAB FINAL:", len(tabs))
	/*buff, err := read.ReadString(byte('\n'))
	fmt.Println("BUFF", buff, err)
	f.Close()*/

	/*for i := 0; i < N; i++ {
		f, _ := os.Open("./test_data/simple.csv")
		read := bufio.NewReader(f)

		//fmt.Println("START READING FILE")

		var b string
		var err error

		b, err = read.ReadString('\n')
		//fmt.Println("FIRST READ", b, err, err == nil)

		for err == nil {
			if b, err = read.ReadString('\n'); err == nil {
				//		fmt.Println("READ BYTE:", b)
			}
		}
		fmt.Println("B:", b, i)
	}*/
}
