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

package red

import (
	"errors"
	"fmt"

	"github.com/ryanfowler/red/resp"
)

var errTooManyReads = errors.New("red: too many reads on an array")

// ArrayRes represents an iterator for a RESP array. The Close method must be
// called when the ArrayRes is depleted or is no longer needed.
type ArrayRes struct {
	c      *conn
	err    error
	length int
	cursor int
}

// Length returns the length of the array. A length of -1 indicates that the
// array is null.
func (ar *ArrayRes) Length() int {
	return ar.length
}

// Close cleans up the ArrayRes and sets an error on the underlying Conn if
// needed.
func (ar *ArrayRes) Close() error {
	if ar.length == ar.cursor {
		return nil
	}
	if ar.err != nil {
		return nil
	}
	err := fmt.Errorf("array: short read: %d of %d values", ar.cursor, ar.length)
	ar.err = err
	ar.c.forceConnError(err)
	return nil
}

// Cursor returns the number of values that have been read from the array.
func (ar *ArrayRes) Cursor() int {
	return ar.cursor
}

// More returns true if there are additional values to read from the array.
func (ar *ArrayRes) More() bool {
	return ar.err == nil && ar.cursor < ar.length
}

// NextType returns the RESP DataType of the next value to be read.
func (ar *ArrayRes) NextType() (resp.DataType, error) {
	if err := ar.peekNext(); err != nil {
		return 0, err
	}
	t, err := ar.c.NextType()
	ar.inspectErr(err)
	return t, err
}

// String returns the next value in the array as a string.
func (ar *ArrayRes) String() (string, error) {
	if err := ar.next(); err != nil {
		return "", err
	}
	s, err := ar.c.ReadString()
	ar.inspectErr(err)
	return s, err
}

// NullString returns the next value in the array as a NullString.
func (ar *ArrayRes) NullString() (NullString, error) {
	if err := ar.next(); err != nil {
		return NullString{}, err
	}
	ns, err := ar.c.ReadNullString()
	ar.inspectErr(err)
	return ns, err
}

// Bytes returns the next value in the array as a byte slice.
func (ar *ArrayRes) Bytes() ([]byte, error) {
	if err := ar.next(); err != nil {
		return nil, err
	}
	b, err := ar.c.ReadBytes()
	ar.inspectErr(err)
	return b, err
}

// Integer returns the next value in the array as an integer.
func (ar *ArrayRes) Integer() (int64, error) {
	if err := ar.next(); err != nil {
		return 0, err
	}
	i, err := ar.c.ReadInteger()
	ar.inspectErr(err)
	return i, err
}

// Array returns the next value in the array as an ArrayRes.
func (ar *ArrayRes) Array() (*ArrayRes, error) {
	if err := ar.next(); err != nil {
		return nil, err
	}
	aRes, err := ar.c.ReadArray()
	ar.inspectErr(err)
	return aRes, err
}

func (ar *ArrayRes) peekNext() error {
	if ar.cursor >= ar.length {
		return errTooManyReads
	}
	if ar.err != nil {
		return ar.err
	}
	return nil
}

func (ar *ArrayRes) next() error {
	err := ar.peekNext()
	if err == nil {
		ar.cursor++
	}
	return err
}

func (ar *ArrayRes) inspectErr(err error) {
	if err == nil {
		return
	}
	if !resp.IsFatalError(err) {
		err = fmt.Errorf("unexpected error in array: %s", err.Error())
	}
	ar.c.forceConnError(err)
	ar.err = err
}
