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

// Array represents an iterator for a RESP array. The Close method must be
// called when the Array is depleted or is no longer needed.
type Array struct {
	c      *conn
	err    error
	length int
	cursor int
}

// Length returns the length of the array. A length of -1 indicates that the
// array is null.
func (a *Array) Length() int {
	return a.length
}

// Close cleans up the Array and sets an error on the underlying Conn if
// needed.
func (a *Array) Close() error {
	if a.length == a.cursor {
		return nil
	}
	if a.err != nil {
		return nil
	}
	err := fmt.Errorf("array: short read: %d of %d values", a.cursor, a.length)
	a.err = err
	a.c.forceConnError(err)
	return nil
}

// Cursor returns the number of values that have been read from the array.
func (a *Array) Cursor() int {
	return a.cursor
}

// More returns true if there are additional values to read from the array.
func (a *Array) More() bool {
	return a.err == nil && a.cursor < a.length
}

// NextType returns the RESP DataType of the next value to be read.
func (a *Array) NextType() (resp.DataType, error) {
	if err := a.peekNext(); err != nil {
		return 0, err
	}
	t, err := a.c.NextType()
	a.inspectErr(err)
	return t, err
}

// String returns the next value in the array as a string.
func (a *Array) String() (string, error) {
	if err := a.next(); err != nil {
		return "", err
	}
	s, err := a.c.ReadString()
	a.inspectErr(err)
	return s, err
}

// NullString returns the next value in the array as a NullString.
func (a *Array) NullString() (NullString, error) {
	if err := a.next(); err != nil {
		return NullString{}, err
	}
	ns, err := a.c.ReadNullString()
	a.inspectErr(err)
	return ns, err
}

// Bytes returns the next value in the array as a byte slice.
func (a *Array) Bytes() ([]byte, error) {
	if err := a.next(); err != nil {
		return nil, err
	}
	b, err := a.c.ReadBytes()
	a.inspectErr(err)
	return b, err
}

// Integer returns the next value in the array as an integer.
func (a *Array) Integer() (int64, error) {
	if err := a.next(); err != nil {
		return 0, err
	}
	i, err := a.c.ReadInteger()
	a.inspectErr(err)
	return i, err
}

// Array returns the next value in the array as an Array.
func (a *Array) Array() (*Array, error) {
	if err := a.next(); err != nil {
		return nil, err
	}
	aRes, err := a.c.ReadArray()
	a.inspectErr(err)
	return aRes, err
}

func (a *Array) peekNext() error {
	if a.cursor >= a.length {
		return errTooManyReads
	}
	if a.err != nil {
		return a.err
	}
	return nil
}

func (a *Array) next() error {
	err := a.peekNext()
	if err == nil {
		a.cursor++
	}
	return err
}

func (a *Array) inspectErr(err error) {
	if err == nil {
		return
	}
	if !resp.IsFatalError(err) {
		err = fmt.Errorf("unexpected error in array: %s", err.Error())
	}
	a.c.forceConnError(err)
	a.err = err
}
