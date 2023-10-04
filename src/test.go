package main

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
)

type A struct {
	Test string
}

type TestS struct {
	M map[string]*A
	B []byte
}

func main() {
	t := TestS{}
	t.M = map[string]*A{}
	t.B = []byte{0, 1, 2}

	t.M["m"] = &A{Test:"asdf"}

	w := &bytes.Buffer{}
	encoder := labgob.NewEncoder(w)
	encoder.Encode(&t)

	r := &bytes.Buffer{}
	r.Write(w.Bytes())

	m2 := TestS{}
	decoder := labgob.NewDecoder(r)
	decoder.Decode(&m2)

	fmt.Println(m2.M["m"], m2.B)
}