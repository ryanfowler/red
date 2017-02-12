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
	"fmt"
)

// DataType represents a RESP data type, using the value of its leading byte.
// See: https://redis.io/topics/protocol
type DataType byte

const (
	// ArrayType represents the array data type.
	ArrayType DataType = '*'
	// BulkStringType represents the bulk string data type.
	BulkStringType DataType = '$'
	// ErrorType represents the error data type.
	ErrorType DataType = '-'
	// IntegerType represents the integer data type
	IntegerType DataType = ':'
	// SimpleStringType represents the simple string data type.
	SimpleStringType DataType = '+'
)

var crlf = []byte{'\r', '\n'}

// DataTypeString returns the string representation of the provided DataType.
func DataTypeString(t DataType) string {
	switch t {
	case ArrayType:
		return "array"
	case BulkStringType:
		return "bulk string"
	case IntegerType:
		return "integer"
	case ErrorType:
		return "error"
	case SimpleStringType:
		return "simple string"
	default:
		return fmt.Sprintf("unknown type '%s'", string(t))
	}
}

// IsFatalError returns true if the provided error is fatal (a non-recoverable
// protocol error).
func IsFatalError(err error) bool {
	if _, ok := err.(*receivedError); ok {
		return false
	}
	return true
}

type receivedError struct {
	msg string
}

func (e *receivedError) Error() string {
	return fmt.Sprintf("resp: received error: %s", e.msg)
}

func formatError(msg string) error {
	return fmt.Errorf("resp: %s", msg)
}
