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
	DefaultBufferSize  = 1 << 13
	defaultDialTimeout = 30 * time.Second
)

// Dial returns a new Conn using the the TCP protocol and the provided address
// and connection timeout.
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	return DialNetwork("tcp", addr, timeout)
}

// DialNetwork returns a new Conn using the provided network, address, and
// connection timeout.
func DialNetwork(network, addr string, timeout time.Duration) (*Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	tcpConn, err := dialer.Dial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("red: %s", err.Error())
	}
	return NewConn(tcpConn), nil
}

// Conn represents a single connection to a Redis server.
//
// Only one method on a Conn should be called at a time, with the only exception
// being the Err method.
type Conn struct {
	c net.Conn
	r *resp.Reader
	w *resp.Writer

	scratch [64]byte

	mu  sync.Mutex
	err error
}

// NewConn returns an initialized Conn using the provided net.Conn.
func NewConn(c net.Conn) *Conn {
	return NewConnSize(c, DefaultBufferSize)
}

// NewConnSize returns an initialized Conn using the provided net.Conn and
// read/write buffer size.
func NewConnSize(c net.Conn, bufferSize int) *Conn {
	const minBufSize = 1 << 10
	if bufferSize < minBufSize {
		bufferSize = minBufSize
	}
	return &Conn{
		c: c,
		r: resp.NewReaderSize(c, bufferSize),
		w: resp.NewWriterSize(c, bufferSize),
	}
}

// NetConn returns the Conn's underlying net.Conn.
func (c *Conn) NetConn() net.Conn {
	return c.c
}

// Close closes the underlying net.Conn. A Conn cannot be used after being
// closed.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.err != nil {
		c.err = errors.New("red: connection closed")
	}
	c.mu.Unlock()
	return c.c.Close()
}

// Err returns any fatal error that the Conn has experienced. If the returned
// error is non-nil, the connection should NOT be reused.
func (c *Conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

// Pipeline returns a new Pipeline using the given Redis connection.
func (c *Conn) Pipeline() *Pipeline {
	return &Pipeline{c: c}
}

// PubSub returns a new PubSub using the given Redis connection.
func (c *Conn) PubSub() *PubSub {
	return &PubSub{c: c}
}

func (c *Conn) cmdSend(cmd string, args ...interface{}) error {
	err := c.cmd(cmd, args...)
	if err != nil {
		return err
	}
	return c.exec()
}

func (c *Conn) cmd(cmd string, args ...interface{}) error {
	c.w.WriteArrayLength(len(args) + 1)
	err := c.w.WriteBulkString(cmd)
	if err != nil {
		c.connError(err)
		return err
	}
	for _, arg := range args {
		err = c.writeArg(arg)
		if err != nil {
			c.connError(err)
			return err
		}
	}
	return nil
}

// exec flushes any buffered data to the Redis server.
func (c *Conn) exec() error {
	err := c.w.Flush()
	if err != nil {
		c.connError(err)
	}
	return err
}

// nextType returns the RESP DataType of the next value to be read.
func (c *Conn) nextType() (resp.DataType, error) {
	t, err := c.r.NextType()
	if err != nil {
		c.connError(err)
	}
	return t, err
}

// ExecDiscard executes the provided command and discards the next value,
// returning any error encountered.
func (c *Conn) ExecDiscard(cmd string, args ...interface{}) error {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return err
	}
	return c.readDiscard()
}

func (c *Conn) readDiscard() error {
	t, err := c.nextType()
	if err != nil {
		return err
	}
	if t != resp.ErrorType {
		err = c.r.Discard()
		if err != nil {
			c.connError(err)
		}
		return err
	}
	msg, err := c.r.ReadError()
	if err != nil {
		c.connError(err)
		return err
	}
	return &RedisError{msg: msg}
}

// ExecString executes the provided command and returns the response as a string.
//
// - Bulk string: returned "as-is". If the value is null, an empty string is
// returned.
// - Simple string: returned "as-is".
// - Integer: returned as the string representation of the integer.
// - Array, Error: an error is returned.
func (c *Conn) ExecString(cmd string, args ...interface{}) (string, error) {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return "", err
	}
	s, _, err := c.readString()
	return s, err
}

// ExecNullString executes the provided command and returns the response as a
// NullString.
//
// - Bulk string: returned "as-is". If the value is null, the NullString.Valid
// is set to false.
// - Simple string: returned "as-is".
// - Integer: returned as the string representation of the integer.
// - Array, Error: an error is returned.
func (c *Conn) ExecNullString(cmd string, args ...interface{}) (NullString, error) {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return NullString{}, err
	}
	s, ok, err := c.readString()
	return NullString{
		String: s,
		Valid:  ok,
	}, err
}

// ExecInteger executes the provided command and returns the response as an
// integer.
//
// - Integer: returned as the string representation of the integer.
// - Bulk string, Simple string: attempt to parse as an integer.
// - Array, Error: an error is returned.
func (c *Conn) ExecInteger(cmd string, args ...interface{}) (int64, error) {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return 0, err
	}
	return c.readInteger()
}

// ExecBytes executes the provided command and returns the response as a byte
// slice.
//
// - Bulk string: returned "as-is". If the value is null, nil is returned.
// - Simple string: returned "as-is".
// - Integer: returned as the byte slice representation of the integer.
// - Array, Error: an error is returned.
func (c *Conn) ExecBytes(cmd string, args ...interface{}) ([]byte, error) {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return nil, err
	}
	return c.readBytes()
}

// ExecArray executes the provided command and returns the response as an
// Array.
//
// - Array: returns an ArrayRes.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *Conn) ExecArray(cmd string, args ...interface{}) (*Array, error) {
	err := c.cmdSend(cmd, args...)
	if err != nil {
		return nil, err
	}
	return c.readArray()
}

func (c *Conn) readArray() (*Array, error) {
	t, err := c.nextType()
	if err != nil {
		return nil, err
	}
	switch t {
	case resp.ArrayType:
	default:
		return nil, c.readDiscard()
	}
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

// ExecStringArray executes the provided command and returns the response as a
// slice of strings.
//
// - Array: returns the array values as strings.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *Conn) ExecStringArray(cmd string, args ...interface{}) ([]string, error) {
	ar, err := c.ExecArray(cmd, args...)
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

// ExecNullStringArray executes the provided command and returns the response as
// a slice of NullStrings.
//
// - Array: returns the array values as NullStrings.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *Conn) ExecNullStringArray(cmd string, args ...interface{}) ([]NullString, error) {
	ar, err := c.ExecArray(cmd, args...)
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

// ExecBytesArray executes the provided command and returns the reponse as a
// slice of byte slices.
//
// - Array: returns the array values as byte slices.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *Conn) ExecBytesArray(cmd string, args ...interface{}) ([][]byte, error) {
	ar, err := c.ExecArray(cmd, args...)
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

// ExecIntegerArray executes the provided command and returns the response as a
// slice of integers.
//
// - Array: returns the array values as integers.
// - Bulk string, Simple string, Integer, Error: an error is returned.
func (c *Conn) ExecIntegerArray(cmd string, args ...interface{}) ([]int64, error) {
	ar, err := c.ExecArray(cmd, args...)
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

func (c *Conn) readString() (string, bool, error) {
	s, ok, err := c.readStringInner()
	if err != nil {
		c.connError(err)
		return "", false, err
	}
	return s, ok, nil
}

func (c *Conn) readStringInner() (string, bool, error) {
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
		msg, err := c.r.ReadError()
		if err != nil {
			return "", false, err
		}
		return "", false, &RedisError{msg: msg}
	case resp.ArrayType:
		return "", false, c.discardNext("string", "array")
	default:
		return "", false, c.invalidTypeError(t)
	}
}

func (c *Conn) readInteger() (int64, error) {
	i, err := c.readIntegerInner()
	if err != nil {
		c.connError(err)
		return 0, err
	}
	return i, nil
}

func (c *Conn) readIntegerInner() (int64, error) {
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
		msg, err := c.r.ReadError()
		if err != nil {
			return 0, err
		}
		return 0, &RedisError{msg: msg}
	case resp.ArrayType:
		return 0, c.discardNext("integer", "array")
	default:
		return 0, c.invalidTypeError(t)
	}
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("red: unable to parse integer: '%s'", s)
	}
	return i, nil
}

func (c *Conn) readBytes() ([]byte, error) {
	b, err := c.readBytesInner()
	if err != nil {
		c.connError(err)
		return nil, err
	}
	return b, nil
}

func (c *Conn) readBytesInner() ([]byte, error) {
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
		msg, err := c.r.ReadError()
		if err != nil {
			return nil, err
		}
		return nil, &RedisError{msg: msg}
	case resp.ArrayType:
		return nil, c.discardNext("bytes", "array")
	default:
		return nil, c.invalidTypeError(t)
	}
}

func (c *Conn) invalidTypeError(t resp.DataType) error {
	err := fmt.Errorf("red: invalid data type byte: '%s'", string(t))
	c.forceConnError(err)
	return err
}

func (c *Conn) discardNext(exp, got string) error {
	err := c.readDiscard()
	if err != nil {
		return err
	}
	return fmt.Errorf("red: unable to parse type '%s' as '%s'", got, exp)
}

func (c *Conn) connError(err error) {
	if _, ok := err.(*resp.Error); ok {
		c.forceConnError(err)
	}
}

func (c *Conn) forceConnError(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}

func (c *Conn) writeArg(arg interface{}) error {
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
	case nil:
		return c.w.WriteBulkString("")
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, v)
		return c.w.WriteBulkStringBytes(buf.Bytes())
	}
}

func (c *Conn) writeInt(i int64) error {
	b := strconv.AppendInt(c.scratch[:0], i, 10)
	return c.w.WriteBulkStringBytes(b)
}

func (c *Conn) writeUint(i uint64) error {
	b := strconv.AppendUint(c.scratch[:0], i, 10)
	return c.w.WriteBulkStringBytes(b)
}

func (c *Conn) writeFloat(f float64) error {
	b := strconv.AppendFloat(c.scratch[:0], f, 'f', -1, 64)
	return c.w.WriteBulkStringBytes(b)
}
