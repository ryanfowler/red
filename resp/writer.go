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
	"io"
	"strconv"
)

// Writer represents a buffered writer for a RESP (REdis Serialization Protocol)
// client. Methods on a writer CANNOT be called concurrently.
type Writer struct {
	w       *bufio.Writer
	scratch [64]byte
}

// NewWriter returns a new RESP Writer using the provided io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// NewWriterSize returns a new RESP Writer using the provided io.Writer and
// buffer size.
func NewWriterSize(w io.Writer, bufferSize int) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, bufferSize)}
}

// NewWriterBuf returns a new RESP Writer using the provided io.Writer and
// existing bufio.Writer.
func NewWriterBuf(w io.Writer, bw *bufio.Writer) *Writer {
	bw.Reset(w)
	return &Writer{w: bw}
}

// Flush writes any buffered data to the Writer's underlying io.Writer.
func (w *Writer) Flush() error {
	err := w.w.Flush()
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteArrayLength writes an array header with the provided length.
func (w *Writer) WriteArrayLength(n int) error {
	err := w.writeLength(ArrayType, int64(n))
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteSimpleString writes the provided string to the underlying io.Writer as
// a simple string.
func (w *Writer) WriteSimpleString(s string) error {
	w.w.WriteByte(byte(SimpleStringType))
	w.w.WriteString(s)
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteSimpleStringBytes writes the provided byte slice to the underlying
// io.Writer as a simple string.
func (w *Writer) WriteSimpleStringBytes(b []byte) error {
	w.w.WriteByte(byte(SimpleStringType))
	w.w.Write(b)
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteBulkString writes the provided string to the underlying io.Writer as a
// bulk string.
func (w *Writer) WriteBulkString(s string) error {
	w.writeLength(BulkStringType, int64(len(s)))
	w.w.WriteString(s)
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteBulkStringBytes writes the provided byte slice to the underlying
// io.Writer as a bulk string.
func (w *Writer) WriteBulkStringBytes(b []byte) error {
	if b == nil {
		w.writeLength(BulkStringType, -1)
	} else {
		w.writeLength(BulkStringType, int64(len(b)))
		w.w.Write(b)
	}
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteInteger writes the provided integer to the underlying io.Writer.
func (w *Writer) WriteInteger(i int64) error {
	err := w.writeLength(IntegerType, i)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteError writes the provided message to the underlying io.Writer as an
// error.
func (w *Writer) WriteError(msg string) error {
	w.w.WriteByte(byte(ErrorType))
	w.w.WriteString(msg)
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

// WriteErrorBytes writes the provided message to the underlying io.Writer as
// an error.
func (w *Writer) WriteErrorBytes(msg []byte) error {
	w.w.WriteByte(byte(ErrorType))
	w.w.Write(msg)
	_, err := w.w.Write(crlf)
	if err != nil {
		return writeError(err.Error())
	}
	return nil
}

func (w *Writer) writeLength(lead DataType, n int64) error {
	w.scratch[0] = byte(lead)
	buf := strconv.AppendUint(w.scratch[:1], uint64(n), 10)
	buf = append(buf, crlf...)
	_, err := w.w.Write(buf)
	return err
}
