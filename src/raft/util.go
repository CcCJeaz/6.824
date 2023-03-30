package raft

import (
	"flag"
	"fmt"
	"log"
	"strings"
)

// Debugging
var Debug = false

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
	flag.BoolVar(&Debug, "debug", false, "为true则打印debug日志")
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Zero[T any]() T {
	var zero T
	return zero
}

type Buffer[T any] struct {
	buf         []T
	startIndex  int
	front, rear int
}

func NewBuffer[T any]() *Buffer[T] {
	return &Buffer[T]{
		buf:        make([]T, 1),
		startIndex: 0,
		front:      0,
		rear:       0,
	}
}

func (b *Buffer[T]) cap() int {
	return len(b.buf)
}

func (b *Buffer[T]) IsEmpty() bool {
	return b.rear == b.front
}

func (b *Buffer[T]) IsFull() bool {
	return (b.rear+1)%b.cap() == b.front
}

func (b *Buffer[T]) grow() {
	originCap := b.cap()
	newCap := originCap * 2
	if originCap >= 1024 {
		newCap = originCap + originCap/4
	}
	newBuf := make([]T, newCap)

	// 迁移数据
	for i := 0; i < originCap-1; i++ {
		newBuf[i] = b.buf[(b.front+i)%originCap]
		b.buf[(b.front+i)%originCap] = Zero[T]()
	}

	b.buf = newBuf
	b.front = 0
	b.rear = originCap - 1
}

func (b *Buffer[T]) Push(data T) {
	if b.IsFull() {
		b.grow()
	}
	b.buf[b.rear] = data
	b.rear = (b.rear + 1) % b.cap()
}

func (b *Buffer[T]) Pop() {
	if b.IsEmpty() {
		panic("Buffer is empty")
	}
	b.buf[b.front] = Zero[T]()
	b.front = (b.front + 1) % b.cap()
	b.startIndex++
}

func (b *Buffer[T]) Len() int {
	return (b.cap() + b.rear - b.front) % b.cap()
}

func (b *Buffer[T]) FrontIndex() int {
	return b.startIndex
}

func (b *Buffer[T]) BackIndex() int {
	return b.startIndex + b.Len() - 1
}

func (b *Buffer[T]) Front() T {
	return b.buf[b.front]
}

func (b *Buffer[T]) Back() T {
	return b.buf[(b.cap()+b.rear-1)%b.cap()]
}

// buf = buf[l:r]
func (b *Buffer[T]) Cut(l, r int) {
	if l > r || l < b.FrontIndex() || r > b.BackIndex()+1 {
		panic(fmt.Sprintf("buf[%d:%d] out of range: [%d, %d]\n", l, r, b.FrontIndex(), b.BackIndex()))
	}

	for i := b.FrontIndex(); i < l; i++ {
		b.buf[(i-b.startIndex+b.front)%b.cap()] = Zero[T]()
	}

	for i := r; i <= b.BackIndex(); i++ {
		b.buf[(i-b.startIndex+b.front)%b.cap()] = Zero[T]()
	}

	b.rear = (b.front + r - b.startIndex) % b.cap()
	b.front = (b.front + l - b.startIndex) % b.cap()

	b.startIndex = l
}

func (b *Buffer[T]) Get(index int) T {
	if index < b.FrontIndex() || index > b.BackIndex() {
		panic(fmt.Sprintf("buf[%d] out of range: [%d, %d]\n", index, b.FrontIndex(), b.BackIndex()))
	}
	return b.buf[(index-b.startIndex+b.front)%b.cap()]
}

func (b *Buffer[T]) Set(index int, data T) {
	if index < b.FrontIndex() || index > b.BackIndex() {
		panic(fmt.Sprintf("buf[%d] out of range: [%d, %d]\n", index, b.FrontIndex(), b.BackIndex()))
	}
	b.buf[(index-b.startIndex+b.front)%b.cap()] = data
}

func (b *Buffer[T]) String() string {
	var builder strings.Builder
	builder.WriteByte('[')
	for i := b.FrontIndex(); i <= b.BackIndex(); i++ {
		builder.WriteString(fmt.Sprint(b.Get(i), " "))
	}
	builder.WriteByte(']')

	return builder.String()
}
