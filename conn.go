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
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ryanfowler/red/resp"
)

const (
	defaultDialTimeout = 30 * time.Second
	defaultBufferSize  = 1 << 13
)

// Dial returns a new Conn using the the TCP protocol and the provided address
// and connection timeout.
func Dial(addr string, timeout time.Duration) (Conn, error) {
	return DialNetwork("tcp", addr, timeout)
}

// DialNetwork returns a new Conn using the provided network, address, and
// connection timeout.
func DialNetwork(network, addr string, timeout time.Duration) (Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	tcpConn, err := dialer.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("red: %s", err.Error())
	}
	return NewConn(tcpConn), nil
}

func DialNetworkSize(network, addr string, timeout time.Duration, bufferSize int) (Conn, error) {
	return nil, nil
}

// conn represents the actual implementation of the Conn interface.
type conn struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer

	scratch [64]byte

	mu  sync.Mutex
	err error
}

// NewConn returns an initialized Conn using the provided net.Conn.
func NewConn(c net.Conn) Conn {
	return NewConnSize(c, defaultBufferSize)
}

// NewConnSize returns an initialized Conn using the provided net.Conn and
// read/write buffer size.
func NewConnSize(c net.Conn, bufferSize int) Conn {
	return &conn{
		c: c,
		r: resp.NewReaderSize(c, bufferSize),
		w: resp.NewWriterSize(c, bufferSize),
	}
}

// NetConn returns the Conn's underlying net.Conn.
func (c *conn) NetConn() net.Conn {
	return c.c
}

// Close closes the underlying net.Conn. A Conn cannot be used after being
// closed.
func (c *conn) Close() error {
	c.mu.Lock()
	if c.err != nil {
		c.err = errors.New("red: connection closed")
	}
	c.mu.Unlock()
	return c.c.Close()
}

// Err returns any fatal error that the Conn has experienced.
func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

// Cmd issues the provided Redis command and arguments. Send must be called
// before reading any data off of the Conn to flush any buffered data to the
// Redis server.
func (c *conn) Cmd(cmd string, args ...interface{}) error {
	err := c.cmd(cmd, args...)
	if err != nil {
		c.connError(err)
	}
	return err
}

func (c *conn) cmd(cmd string, args ...interface{}) error {
	c.w.WriteArrayLength(len(args) + 1)
	err := c.w.WriteBulkString(cmd)
	if err != nil {
		return err
	}
	for _, arg := range args {
		err = c.writeArg(arg)
		if err != nil {
			return err
		}
	}
	return nil
}

// Send flushes any buffered data to the Redis server.
func (c *conn) Send() error {
	err := c.w.Flush()
	if err != nil {
		c.connError(err)
	}
	return err
}

// NextType returns the RESP DataType of the next value to be read.
func (c *conn) NextType() (resp.DataType, error) {
	t, err := c.r.NextType()
	if err != nil {
		c.connError(err)
	}
	return t, err
}

// ReadDiscard reads (and discards) the next value and returns any error
// encountered.
func (c *conn) ReadDiscard() error {
	err := c.r.Discard()
	if err != nil {
		c.connError(err)
	}
	return err
}

// ReadString reads the next value and returns it as a string.
//
// - Bulk string: returned "as-is". If the value is null, an empty string is
// returned.
// - Simple string: returned "as-is".
// - Integer: returned as the string representation of the integer.
// - Array, Error: an error is returned.
func (c *conn) ReadString() (string, error) {
	s, _, err := c.readString()
	if err != nil {
		c.connError(err)
	}
	return s, err
}

// ReadNullString reads the next value and returns it as a NullString.
//
// - Bulk string: returned "as-is". If the value is null, the NullString.Valid
// is set to false.
// - Simple string: returned "as-is".
// - Integer: returned as the string representation of the integer.
// - Array, Error: an error is returned.
func (c *conn) ReadNullString() (NullString, error) {
	s, ok, err := c.readString()
	if err != nil {
		c.connError(err)
	}
	return NullString{
		String: s,
		Valid:  ok,
	}, err
}

// ReadInteger reads the next value and returns it as an integer.
//
// - Integer: returned as the string representation of the integer.
// - Bulk string, Simple string: attempt to parse as an integer.
// - Array, Error: an error is returned.
func (c *conn) ReadInteger() (int64, error) {
	i, err := c.readInteger()
	if err != nil {
		c.connError(err)
	}
	return i, err
}

// ReadBytes reads the next value and returns it as a byte slice.
//
// - Bulk string: returned "as-is". If the value is null, nil is returned.
// - Simple string: returned "as-is".
// - Integer: returned as the byte slice representation of the integer.
// - Array, Error: an error is returned.
func (c *conn) ReadBytes() ([]byte, error) {
	b, err := c.readBytes()
	if err != nil {
		c.connError(err)
	}
	return b, err
}

// ReadArray reads the next value and returns an ArrayRes.
//
// - Array: returns an ArrayRes.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *conn) ReadArray() (*Array, error) {
	length, err := c.r.ReadArrayLength()
	if err != nil {
		c.connError(err)
		return nil, err
	}
	return &Array{
		c:      c,
		length: length,
	}, nil
}

// ReadStringArray reads the next value as a slice of strings.
//
// - Array: returns the array values as strings.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *conn) ReadStringArray() ([]string, error) {
	ar, err := c.ReadArray()
	if err != nil {
		return nil, err
	}
	defer ar.Close()
	if ar.Length() < 0 {
		return nil, nil
	}
	ss := make([]string, 0, ar.Length())
	for ar.More() {
		s, err := ar.String()
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	return ss, nil
}

// ReadNullStringArray reads the next value as a slice of NullStrings.
//
// - Array: returns the array values as NullStrings.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *conn) ReadNullStringArray() ([]NullString, error) {
	ar, err := c.ReadArray()
	if err != nil {
		return nil, err
	}
	defer ar.Close()
	if ar.Length() < 0 {
		return nil, nil
	}
	ss := make([]NullString, 0, ar.Length())
	for ar.More() {
		s, err := ar.NullString()
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	return ss, nil
}

// ReadBytesArray reads the next value as a slice of byte slices.
//
// - Array: returns the array values as byte slices.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *conn) ReadBytesArray() ([][]byte, error) {
	ar, err := c.ReadArray()
	if err != nil {
		return nil, err
	}
	defer ar.Close()
	if ar.Length() < 0 {
		return nil, nil
	}
	bs := make([][]byte, 0, ar.Length())
	for ar.More() {
		b, err := ar.Bytes()
		if err != nil {
			return nil, err
		}
		bs = append(bs, b)
	}
	return bs, nil
}

// ReadIntegerArray reads the next value as a slice of integers.
//
// - Array: returns the array values as integers.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *conn) ReadIntegerArray() ([]int64, error) {
	ar, err := c.ReadArray()
	if err != nil {
		return nil, err
	}
	defer ar.Close()
	if ar.Length() < 0 {
		return nil, nil
	}
	is := make([]int64, 0, ar.Length())
	for ar.More() {
		i, err := ar.Integer()
		if err != nil {
			return nil, err
		}
		is = append(is, i)
	}
	return is, nil
}

func (c *conn) readString() (string, bool, error) {
	t, err := c.r.NextType()
	if err != nil {
		return "", false, err
	}
	switch t {
	case resp.BulkStringType:
		return c.r.ReadBulkString()
	case resp.SimpleStringType:
		s, err := c.r.ReadSimpleString()
		return s, err == nil, err
	case resp.IntegerType:
		b, err := c.r.ReadIntegerBytes()
		return string(b), err == nil, err
	case resp.ErrorType:
		return "", false, c.r.ReadError()
	case resp.ArrayType:
		return "", false, parsingError("string", "array")
	default:
		return "", false, invalidTypeError(t)
	}
}

func (c *conn) readInteger() (int64, error) {
	t, err := c.r.NextType()
	if err != nil {
		return 0, err
	}
	var s string
	switch t {
	case resp.IntegerType:
		return c.r.ReadInteger()
	case resp.SimpleStringType:
		s, err = c.r.ReadSimpleString()
	case resp.BulkStringType:
		s, _, err = c.r.ReadBulkString()
	case resp.ErrorType:
		return 0, c.r.ReadError()
	case resp.ArrayType:
		return 0, parsingError("integer", "array")
	default:
		return 0, invalidTypeError(t)
	}
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// TODO: this is a non-fatal error.
		return 0, fmt.Errorf("red: unable to parse integer: '%s'", s)
	}
	return i, nil
}

func (c *conn) readBytes() ([]byte, error) {
	t, err := c.r.NextType()
	if err != nil {
		return nil, err
	}
	switch t {
	case resp.BulkStringType:
		return c.r.ReadBulkStringBytes()
	case resp.SimpleStringType:
		return c.r.ReadSimpleStringBytes()
	case resp.IntegerType:
		return c.r.ReadIntegerBytes()
	case resp.ErrorType:
		return nil, c.r.ReadError()
	case resp.ArrayType:
		return nil, parsingError("bytes", "array")
	default:
		return nil, invalidTypeError(t)
	}
}

func parsingError(exp, got string) error {
	return fmt.Errorf("red: unable to parse type '%s' as '%s'", got, exp)
}

func invalidTypeError(t resp.DataType) error {
	return fmt.Errorf("red: invalid data type byte: '%s'", string(t))
}

func (c *conn) connError(err error) {
	if !resp.IsFatalError(err) {
		return
	}
	c.forceConnError(err)
}

func (c *conn) forceConnError(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}

func (c *conn) writeArg(arg interface{}) error {
	switch v := arg.(type) {
	case string:
		return c.w.WriteBulkString(v)
	case []byte:
		return c.w.WriteBulkStringBytes(v)
	case int:
		return c.writeInt(int64(v))
	case int8:
		return c.writeInt(int64(v))
	case int16:
		return c.writeInt(int64(v))
	case int32:
		return c.writeInt(int64(v))
	case int64:
		return c.writeInt(v)
	case uint:
		return c.writeUint(uint64(v))
	case uint8:
		return c.writeUint(uint64(v))
	case uint16:
		return c.writeUint(uint64(v))
	case uint32:
		return c.writeUint(uint64(v))
	case uint64:
		return c.writeUint(v)
	case float32:
		return c.writeFloat(float64(v))
	case float64:
		return c.writeFloat(v)
	case bool:
		if v {
			return c.w.WriteBulkString("1")
		}
		return c.w.WriteBulkString("0")
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, v)
		return c.w.WriteBulkStringBytes(buf.Bytes())
	}
}

func (c *conn) writeInt(i int64) error {
	b := strconv.AppendInt(c.scratch[:0], i, 10)
	return c.w.WriteBulkStringBytes(b)
}

func (c *conn) writeUint(i uint64) error {
	b := strconv.AppendUint(c.scratch[:0], i, 10)
	return c.w.WriteBulkStringBytes(b)
}

func (c *conn) writeFloat(f float64) error {
	b := strconv.AppendFloat(c.scratch[:0], f, 'f', -1, 64)
	return c.w.WriteBulkStringBytes(b)
}
