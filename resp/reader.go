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

package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Reader represents a buffered reader for a RESP (REdis Serialization Protocol)
// client. Methods on a reader CANNOT be called concurrently.
type Reader struct {
	r *bufio.Reader
}

// NewReader returns a new RESP Reader using the provided io.Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// NewReaderSize returns a new RESP Reader using the provided io.Reader and
// buffer size.
func NewReaderSize(r io.Reader, bufferSize int) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, bufferSize)}
}

// NewReaderBuf returns a new RESP Reader using the provided io.Reader and
// existing bufio,Reader.
func NewReaderBuf(r io.Reader, br *bufio.Reader) *Reader {
	br.Reset(r)
	return &Reader{r: br}
}

// NextType returns the DataType of the next value to be read.
func (r *Reader) NextType() (DataType, error) {
	b, err := r.r.Peek(1)
	if err != nil {
		return 0, readError(fmt.Sprintf("next type: %s", err.Error()))
	}
	switch DataType(b[0]) {
	case ArrayType:
		return ArrayType, nil
	case BulkStringType:
		return BulkStringType, nil
	case ErrorType:
		return ErrorType, nil
	case IntegerType:
		return IntegerType, nil
	case SimpleStringType:
		return SimpleStringType, nil
	default:
		return 0, readError(fmt.Sprintf("invalid type byte: '%s'", b))
	}
}

// ReadArrayLength parses an array value and returns its length.
func (r *Reader) ReadArrayLength() (int, error) {
	b, err := r.readAndAssert(ArrayType)
	if err != nil {
		return 0, err
	}
	i, err := parseInt(b)
	if err != nil {
		return 0, readError(err.Error())
	}
	return int(i), nil
}

var (
	okBytes    = []byte{'O', 'K'}
	okString   = "OK"
	pongBytes  = []byte{'P', 'O', 'N', 'G'}
	pongString = "PONG"
)

// ReadSimpleString parses a simple string and returns the value.
func (r *Reader) ReadSimpleString() (string, error) {
	b, err := r.readAndAssert(SimpleStringType)
	if bytes.Equal(b, okBytes) {
		return okString, err
	}
	if bytes.Equal(b, pongBytes) {
		return pongString, err
	}
	return string(b), err
}

// ReadSimpleStringBytes parses a simple string and returns the value as a byte
// slice.
func (r *Reader) ReadSimpleStringBytes() ([]byte, error) {
	b, err := r.readAndAssert(SimpleStringType)
	return clone(b), err
}

// ReadBulkString parses a bulk string and returns the value and a boolean
// indicatin whether the values exists (not null).
func (r *Reader) ReadBulkString() (string, bool, error) {
	length, err := r.readBulkStringLength()
	if err != nil {
		return "", false, err
	}
	if length < 0 {
		return "", false, nil
	}
	s, err := r.readString(int(length) + 2)
	if err != nil {
		return "", false, r.readBulkStringErr(err.Error())
	}
	if !strings.HasSuffix(s, "\r\n") {
		return "", false, readError("reading bulk string: invalid CRLF")
	}
	return s[:len(s)-2], true, nil
}

func (r *Reader) readString(length int) (string, error) {
	b, err := r.r.Peek(length)
	if err == nil {
		_, err = r.r.Discard(length)
		return string(b), err
	}
	if err != bufio.ErrBufferFull {
		return "", readError(err.Error())
	}
	b = make([]byte, length)
	_, err = io.ReadFull(r.r, b)
	if err != nil {
		return "", readError(err.Error())
	}
	return string(b), nil
}

// ReadBulkStringBytes parses a bulk string and returns the value as a byte
// slice. If the value of the bulk string is null, nil is returned.
func (r *Reader) ReadBulkStringBytes() ([]byte, error) {
	length, err := r.readBulkStringLength()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	s := make([]byte, length+2)
	_, err = io.ReadFull(r.r, s)
	if err != nil {
		return nil, r.readBulkStringErr(err.Error())
	}
	if !bytes.HasSuffix(s, crlf) {
		return nil, readError("bulk string: invalid CRLF")
	}
	return s[:len(s)-2], nil
}

func (r *Reader) readBulkStringErr(msg string) error {
	return readError(fmt.Sprintf("bulk string: %s", msg))
}

func (r *Reader) readBulkStringLength() (int64, error) {
	b, err := r.readAndAssert(BulkStringType)
	if err != nil {
		return 0, err
	}
	length, err := parseInt(b)
	if err != nil {
		return 0, readError(fmt.Sprintf("parsing bulk string length: %s", err.Error()))
	}
	return length, nil
}

// ReadInteger parses an integer and returns the value.
func (r *Reader) ReadInteger() (int64, error) {
	b, err := r.readAndAssert(IntegerType)
	if err != nil {
		return 0, err
	}
	i, err := parseInt(b)
	if err != nil {
		return 0, readError(err.Error())
	}
	return i, nil
}

// ReadIntegerBytes parses an integer and returns the value as a byte slice.
func (r *Reader) ReadIntegerBytes() ([]byte, error) {
	b, err := r.readAndAssert(IntegerType)
	return clone(b), err
}

// ReadError parses an error value and returns the result as an error.
func (r *Reader) ReadError() (string, error) {
	b, err := r.readAndAssert(ErrorType)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Discard reads the next value and discards the result, returning any error
// encountered.
func (r *Reader) Discard() error {
	b, err := r.readLine()
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return readError("unexpected empty read")
	}
	t := DataType(b[0])
	b = b[1:]

	switch t {
	case ErrorType:
		return fmt.Errorf("%s", b)
	case IntegerType, SimpleStringType:
		return nil
	case ArrayType:
		return r.discardArray(b)
	case BulkStringType:
		return r.discardBulkString(b)
	default:
		return readError(fmt.Sprintf("invalid type byte '%s'", string(t)))
	}
}

func (r *Reader) discardArray(b []byte) error {
	length, err := parseInt(b)
	if err != nil {
		return readError(fmt.Sprintf("array length: %s", err.Error()))
	}
	var i int64
	for i = 0; i < length; i++ {
		err = r.Discard()
		if err == nil {
			continue
		}
		if _, ok := err.(*Error); ok {
			return err
		}
		return readError(fmt.Sprintf("unexpected error at array index %d: %s", i, err.Error()))
	}
	return nil
}

func (r *Reader) discardBulkString(b []byte) error {
	i, err := parseInt(b)
	if err != nil {
		return readError(fmt.Sprintf("bulk string length: %s", err.Error()))
	}
	if i < 0 {
		return nil
	}
	if i > 0 {
		_, err = r.r.Discard(int(i))
		if err != nil {
			return readError(fmt.Sprintf("read: %s", err.Error()))
		}
	}
	for _, p := range crlf {
		err = r.readAndAssertByte(p)
		if err != nil {
			return readError(err.Error())
		}
	}
	return nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	p := make([]byte, len(b))
	copy(p, b)
	return p
}

func (r *Reader) readAndAssert(t DataType) ([]byte, error) {
	b, err := r.readLine()
	if err != nil {
		return nil, err
	}
	err = assertType(b, t)
	if err != nil {
		return nil, err
	}
	return b[1:], nil
}

func (r *Reader) readLine() ([]byte, error) {
	b, err := r.r.ReadSlice('\n')
	if err != nil {
		return nil, readError(fmt.Sprintf("read line: %s", err.Error()))
	}
	if !checkCarriage(b) {
		return nil, readError("invalid CRLF")
	}
	return b[:len(b)-2], nil
}

func assertType(b []byte, t DataType) error {
	if len(b) < 1 {
		return readError("unexpected empty line")
	}
	if byte(t) == b[0] {
		return nil
	}
	if b[0] == byte(ErrorType) {
		return fmt.Errorf("%s", b[1:])
	}
	expected := DataTypeString(t)
	received := DataTypeString(DataType(b[0]))
	return readError(fmt.Sprintf("expecting type '%s'; received '%s'", expected, received))
}

func (r *Reader) readAndAssertByte(b byte) error {
	p, err := r.r.ReadByte()
	if err != nil {
		return err
	}
	if p != b {
		return fmt.Errorf("unexpected byte: '%s'", string(p))
	}
	return nil
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, parseIntErr(b)
	}

	var negative bool
	if b[0] == '-' {
		negative = true
		b = b[1:]
		if len(b) == 0 {
			return 0, parseIntErr(b)
		}
	}

	var dec int64
	for _, p := range b {
		if p < '0' || p > '9' {
			return 0, parseIntErr(b)
		}
		dec = (dec * 10) + int64(p-'0')
	}
	if negative {
		dec *= -1
	}

	return dec, nil
}

func parseIntErr(b []byte) error {
	return fmt.Errorf("unable to parse integer '%s'", b)
}

func checkCarriage(b []byte) bool {
	return len(b) >= 2 && b[len(b)-1] == '\n' && b[len(b)-2] == '\r'
}
