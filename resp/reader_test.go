// MIT License
//
// Copyright (c) 2017 Ryan Fowler
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package resp_test

import (
	"strings"
	"testing"

	"github.com/ryanfowler/red/resp"
)

func TestDiscard(t *testing.T) {
	// Simple string.
	s := strings.NewReader("+Hi there\r\n")
	r := resp.NewReader(s)
	err := r.Discard()
	if err != nil {
		t.Fatalf("Unexpected error discarding simple string: %s", err.Error())
	}
	if s.Len() != 0 {
		t.Fatal("Unexpected short read for simple string")
	}

	// Integer.
	s = strings.NewReader(":1\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err != nil {
		t.Fatalf("Unexpected error discarding integer: %s", err.Error())
	}
	if s.Len() != 0 {
		t.Fatal("Unexpected short read for integer")
	}

	// Bulk string.
	s = strings.NewReader("$4\r\ntest\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err != nil {
		t.Fatalf("Unexpected error discarding bulk string: %s", err.Error())
	}
	if s.Len() != 0 {
		t.Fatal("Unexpected short read for bulk string")
	}

	// Bulk string error.
	s = strings.NewReader("$hi\r\ntest\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: bulk string length: unable to parse integer 'hi'" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}

	// Error.
	s = strings.NewReader("-ERR test error\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: received error: ERR test error" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}
	if s.Len() != 0 {
		t.Fatal("Unexpected short read for bulk string")
	}

	// Array with error.
	s = strings.NewReader("*3\r\n+Hi\r\n-ERR test error\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: unexpected error at array index 1: ERR test error" {
		t.Fatalf("Unexpected error discarding array: %s", err.Error())
	}

	// Array length error.
	s = strings.NewReader("*hi\r\n+Hi\r\n-ERR test error\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: array length: unable to parse integer 'hi'" {
		t.Fatalf("Unexpected error discarding array: %s", err.Error())
	}

	// Array.
	s = strings.NewReader("*4\r\n+Hi\r\n$4\r\ntest\r\n:100\r\n*1\r\n+Hi again\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err != nil {
		t.Fatalf("Unexpected error discarding array: %s", err.Error())
	}
	if s.Len() != 0 {
		t.Fatal("Unexpected short read for array")
	}

	// Invalid.
	s = strings.NewReader("blahblah\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: invalid type byte 'b'" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}

	// Zero length.
	s = strings.NewReader("\r\n")
	r = resp.NewReader(s)
	err = r.Discard()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if err.Error() != "resp: unexpected empty read" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}
}

func TestReadSimpleString(t *testing.T) {
	s := strings.NewReader("+Hello there\r\n+How are you?\r\n")
	r := resp.NewReader(s)
	tp, err := r.NextType()
	if err != nil {
		t.Fatalf("Unexpected error getting next type: %s", err.Error())
	}
	if tp != resp.SimpleStringType {
		t.Fatalf("Unexpected type byte: %s", resp.DataTypeString(tp))
	}

	ss, err := r.ReadSimpleString()
	if err != nil {
		t.Fatalf("Unexpected error reading simple string: %s", err.Error())
	}
	if ss != "Hello there" {
		t.Fatalf("Unexpected simple string: '%s'", ss)
	}

	b, err := r.ReadSimpleStringBytes()
	if err != nil {
		t.Fatalf("Unexpected error reading simple string bytes: %s", err.Error())
	}
	if string(b) != "How are you?" {
		t.Fatalf("Unexpected simple string bytes: %s", b)
	}
}

func TestReadBulkString(t *testing.T) {
	s := strings.NewReader("$11\r\nHello there\r\n$-1\r\n\r\n")
	r := resp.NewReader(s)
	tp, err := r.NextType()
	if err != nil {
		t.Fatalf("Unexpected error getting next type: %s", err.Error())
	}
	if tp != resp.BulkStringType {
		t.Fatalf("Unexpected type byte: %s", resp.DataTypeString(tp))
	}

	b, err := r.ReadBulkStringBytes()
	if err != nil {
		t.Fatalf("Unexpected error reading bulk string bytes: %s", err.Error())
	}
	if string(b) != "Hello there" {
		t.Fatalf("Unexpected bulk string bytes: %s", b)
	}

	ss, ok, err := r.ReadBulkString()
	if err != nil {
		t.Fatalf("Unexpected error reading bulk string: %s", err.Error())
	}
	if ok {
		t.Fatalf("Unexpected bulk string existence: %v", ok)
	}
	if ss != "" {
		t.Fatalf("Unexpected bulk string: '%s'", ss)
	}
}

func TestReadArrayLength(t *testing.T) {
	s := strings.NewReader("*2\r\n+hi\r\n+there\r\n")
	r := resp.NewReader(s)
	tp, err := r.NextType()
	if err != nil {
		t.Fatalf("Unexpected error getting next type: %s", err.Error())
	}
	if tp != resp.ArrayType {
		t.Fatalf("Unexpected type byte: %s", resp.DataTypeString(tp))
	}

	length, err := r.ReadArrayLength()
	if err != nil {
		t.Fatalf("Unexpected error reading array length: %s", err.Error())
	}
	if length != 2 {
		t.Fatalf("Unexpected array length: %d", length)
	}
}

func TestReadInteger(t *testing.T) {
	s := strings.NewReader(":55\r\n")
	r := resp.NewReader(s)
	tp, err := r.NextType()
	if err != nil {
		t.Fatalf("Unexpected error getting next type: %s", err.Error())
	}
	if tp != resp.IntegerType {
		t.Fatalf("Unexpected type byte: %s", resp.DataTypeString(tp))
	}

	i, err := r.ReadInteger()
	if err != nil {
		t.Fatalf("Unexpected error reading integer: %s", err.Error())
	}
	if i != 55 {
		t.Fatalf("Unexpected integer: %d", i)
	}

	s = strings.NewReader(":-100\r\n")
	r = resp.NewReader(s)
	b, err := r.ReadIntegerBytes()
	if err != nil {
		t.Fatalf("Unexpected error reading integer bytes: %s", err.Error())
	}
	if string(b) != "-100" {
		t.Fatalf("Unexpected integer bytes: %s", b)
	}
}

func TestReadError(t *testing.T) {
	s := strings.NewReader("-ERR test error\r\n")
	r := resp.NewReader(s)
	tp, err := r.NextType()
	if err != nil {
		t.Fatalf("Unexpected error getting next type: %s", err.Error())
	}
	if tp != resp.ErrorType {
		t.Fatalf("Unexpected type byte: %s", resp.DataTypeString(tp))
	}

	err = r.ReadError()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if resp.IsFatalError(err) {
		t.Fatalf("Unexpected fatal error: %s", err.Error())
	}
	if err.Error() != "resp: received error: ERR test error" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}

	s = strings.NewReader("+Hi\r\n")
	r = resp.NewReader(s)
	err = r.ReadError()
	if err == nil {
		t.Fatal("Unexpected nil error")
	}
	if !resp.IsFatalError(err) {
		t.Fatalf("Unexpected non-fatal error: %s", err.Error())
	}
	if err.Error() != "resp: expecting type 'error'; received 'simple string'" {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}
}
