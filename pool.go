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
	"net"
	"sync"
	"time"
)

var (
	// ErrTooManyActive represents the error on a Pool when the
	// MaxActiveConns and MaxWaiting limits have been reached.
	ErrTooManyActive = errors.New("red: too many active connections")
	errPoolClosed    = errors.New("red: pool closed")
)

// Client represents a pool of zero or more Redis Connections.
//
// All methods on a Client are safe to use concurrently. All fields on a Client
// are optional.
type Client struct {
	// Addr represents the address of the Redis server to connect to in the
	// form: <address>:<port>. If no address is provided, "localhost:6379"
	// will be used.
	Addr string

	// IdleConnTimeout represents the approximate amount of time before an
	// idle connection will be terminated. By default, there is no idle
	// timeout.
	IdleConnTimeout time.Duration

	// IOBufferSize represents the byte size for both the read & write
	// buffers. The default size is DefaultBufferSize.
	IOBufferSize int

	// MaxActiveConns represents the maximum total number of active (open)
	// connections at a given time. A value <= 0 means no active limit.
	MaxActiveConns int

	// MaxIdleConns represents the maximum total number of idle connections
	// at a given time. The default value is 10 idle connections.
	MaxIdleConns int

	// MaxWaiting represents the maximum number of goroutines waiting for a
	// Conn because they are blocked by MaxActiveConns. The default value is
	// no limit. With a value < 0, ErrTooManyActive will be returned whenever
	// MaxActiveConns is reached.
	MaxWaiting int

	// NetDial represents an optional function that will be used to create
	// a net.Conn.
	NetDial func(string) (net.Conn, error)

	// NetTimeout sets a timeout on a Conn for when all IO operations will
	// return an error. It calls the SetDeadline method on the underlying
	// net.Conn before presenting the Conn for use.
	NetTimeout time.Duration

	// OnNew represents an optional function that will be called before
	// presenting the Conn for use. If a non-nil error is returned, the
	// error will be passed to the calling function.
	OnNew func(*Conn) error

	// OnReuse is an optional function to intercept an idle Conn before use.
	// If OnReuse returns false, the Conn will be closed and another will be
	// retrieved/created.
	OnReuse func(*Conn) bool

	mu           sync.Mutex
	cond         *sync.Cond
	numWaiting   int
	numActive    int
	closed       bool
	chMaxIdleDur chan struct{}
	idleConns    []idleConn
}

type idleConn struct {
	conn      *Conn
	idleSince time.Time
}

// Status represents the status of a Pool.
type Status struct {
	// Closed is true if the Pool has been closed.
	Closed bool
	// NumActive is the total number of active connections.
	NumActive int
	// NumIdle is the number of idle connections.
	NumIdle int
	// NumWaiting is the total number of connections waiting for an idle
	// connection.
	NumWaiting int
}

// Status returns the current status of the Pool.
func (cl *Client) Status() Status {
	cl.mu.Lock()
	s := Status{
		Closed:     cl.closed,
		NumActive:  cl.numActive,
		NumIdle:    len(cl.idleConns),
		NumWaiting: cl.numWaiting,
	}
	cl.mu.Unlock()
	return s
}

// Close closes the Pool and any of its idle connections. After being closed, a
// pool cannot be used and any attempt to use it will return an error.
func (cl *Client) Close() error {
	cl.mu.Lock()
	if cl.closed {
		cl.mu.Unlock()
		return nil
	}
	cl.closed = true

	toClose := make([]*Conn, len(cl.idleConns))
	for i, c := range cl.idleConns {
		toClose[i] = c.conn
		cl.idleConns[i] = idleConn{}
	}
	cl.idleConns = nil

	if cl.chMaxIdleDur != nil {
		select {
		case cl.chMaxIdleDur <- struct{}{}:
		default:
		}
	}
	cl.numActive -= len(toClose)
	if cl.cond != nil && cl.numWaiting > 0 {
		cl.cond.Broadcast()
	}
	cl.mu.Unlock()

	for _, c := range toClose {
		c.Close()
	}

	return nil
}

// Conn allows for direct access to a Conn using the provided function. The
// Conn is only valid during the lifetime of the function. Conn will return any
// error returned by fn.
func (cl *Client) Conn(fn func(c *Conn) error) error {
	c, err := cl.getConn()
	if err != nil {
		return err
	}
	defer cl.putConn(c)
	cl.setTimeout(c)
	return fn(c)
}

func (cl *Client) getConn() (*Conn, error) {
	cl.mu.Lock()
	for {
		// Return if pool is closed.
		if cl.closed {
			cl.mu.Unlock()
			return nil, errPoolClosed
		}

		// Attempt to get an idle conn.
		if len(cl.idleConns) > 0 {
			c := cl.popIdle()
			cl.mu.Unlock()
			if cl.OnReuse != nil && !cl.OnReuse(c) {
				c.Close()
				cl.mu.Lock()
				cl.decrActiveLocked(1)
				continue
			}
			return c, nil
		}

		// Check number of active conns.
		if cl.MaxActiveConns <= 0 || cl.numActive < cl.MaxActiveConns {
			break
		}
		if cl.cond == nil {
			cl.cond = sync.NewCond(&cl.mu)
		}

		// Check wait limits.
		if !cl.waitOK() {
			cl.mu.Unlock()
			return nil, ErrTooManyActive
		}

		cl.numWaiting++
		cl.cond.Wait()
		cl.numWaiting--
	}
	cl.numActive++
	cl.mu.Unlock()

	// Create and return a new connection.
	c, err := cl.dialConn()
	if err != nil {
		cl.mu.Lock()
		cl.decrActiveLocked(1)
		cl.mu.Unlock()
		return nil, err
	}
	return c, nil
}

func (cl *Client) setTimeout(c *Conn) {
	if cl.NetTimeout > 0 {
		c.NetConn().SetDeadline(time.Now().Add(cl.NetTimeout))
	}
}

func (cl *Client) dialConn() (*Conn, error) {
	var c net.Conn
	var err error
	if cl.NetDial != nil {
		c, err = cl.NetDial(cl.Addr)
	} else {
		addr := cl.Addr
		if addr == "" {
			addr = "localhost:6379"
		}
		c, err = net.DialTimeout("tcp", addr, 30*time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("red: dial conn: %s", err.Error())
	}

	bufSize := DefaultBufferSize
	if cl.IOBufferSize > 0 {
		bufSize = cl.IOBufferSize
	}
	rc := NewConnSize(c, bufSize)
	if cl.OnNew == nil {
		return rc, nil
	}
	err = cl.OnNew(rc)
	if err != nil {
		return nil, fmt.Errorf("red: OnNew: %s", err.Error())
	}
	return rc, nil
}

func (cl *Client) popIdle() *Conn {
	// TODO (ryanfowler): optimize?
	c := cl.idleConns[0]
	copy(cl.idleConns, cl.idleConns[1:])
	cl.idleConns[len(cl.idleConns)-1] = idleConn{}
	cl.idleConns = cl.idleConns[:len(cl.idleConns)-1]
	return c.conn
}

func (cl *Client) waitOK() bool {
	return cl.MaxWaiting == 0 || cl.numWaiting < cl.MaxWaiting
}

func (cl *Client) putConn(c *Conn) {
	if c == nil {
		return
	}

	cl.mu.Lock()

	// Return if the pool is closed or the connection has a fatal error.
	if cl.closed || c.Err() != nil {
		cl.decrActiveLocked(1)
		cl.mu.Unlock()
		c.Close()
		return
	}

	// Check number of idle conns.
	maxIdle := cl.MaxIdleConns
	if maxIdle < 1 {
		maxIdle = 10
	}
	if len(cl.idleConns) >= maxIdle {
		cl.decrActiveLocked(1)
		cl.mu.Unlock()
		c.Close()
		return
	}

	// Add to idle conns and optionally start the idle conn worker.
	cl.idleConns = append(cl.idleConns, idleConn{conn: c, idleSince: time.Now()})
	cl.startMaxIdleWorker()
	cl.decrActiveLocked(0)
	cl.mu.Unlock()
}

func (cl *Client) decrActiveLocked(n int) {
	cl.numActive -= n
	if cl.cond != nil && cl.numWaiting > 0 {
		cl.cond.Signal()
	}
}

func (cl *Client) ExecStringArray(cmd string, args ...interface{}) ([]string, error) {
	var ss []string
	err := cl.Conn(func(c *Conn) error {
		var err error
		ss, err = c.ExecStringArray(cmd, args...)
		return err
	})
	return ss, err
}

func (cl *Client) ExecNullStringArray(cmd string, args ...interface{}) ([]NullString, error) {
	var nss []NullString
	err := cl.Conn(func(c *Conn) error {
		var err error
		nss, err = c.ExecNullStringArray(cmd, args...)
		return err
	})
	return nss, err
}

func (cl *Client) ExecBytesArray(cmd string, args ...interface{}) ([][]byte, error) {
	var bs [][]byte
	err := cl.Conn(func(c *Conn) error {
		var err error
		bs, err = c.ExecBytesArray(cmd, args...)
		return err
	})
	return bs, err
}

func (cl *Client) ExecIntegerArray(cmd string, args ...interface{}) ([]int64, error) {
	var is []int64
	err := cl.Conn(func(c *Conn) error {
		var err error
		is, err = c.ExecIntegerArray(cmd, args...)
		return err
	})
	return is, err
}

func (cl *Client) ExecString(cmd string, args ...interface{}) (string, error) {
	var s string
	err := cl.Conn(func(c *Conn) error {
		var err error
		s, err = c.ExecString(cmd, args...)
		return err
	})
	return s, err
}

func (cl *Client) ExecBytes(cmd string, args ...interface{}) ([]byte, error) {
	var b []byte
	err := cl.Conn(func(c *Conn) error {
		var err error
		b, err = c.ExecBytes(cmd, args...)
		return err
	})
	return b, err
}

func (cl *Client) ExecNullString(cmd string, args ...interface{}) (NullString, error) {
	var ns NullString
	err := cl.Conn(func(c *Conn) error {
		var err error
		ns, err = c.ExecNullString(cmd, args...)
		return err
	})
	return ns, err
}

func (cl *Client) ExecInteger(cmd string, args ...interface{}) (int64, error) {
	var i int64
	err := cl.Conn(func(c *Conn) error {
		var err error
		i, err = c.ExecInteger(cmd, args...)
		return err
	})
	return i, err
}

func (cl *Client) startMaxIdleWorker() {
	if cl.IdleConnTimeout > 0 && cl.chMaxIdleDur == nil && len(cl.idleConns) > 0 {
		cl.chMaxIdleDur = make(chan struct{}, 1)
		go cl.maxIdleWorker()
	}
}

func (cl *Client) maxIdleWorker() {
	dur := maxDuration(cl.IdleConnTimeout, time.Second)
	t := time.NewTimer(dur)
	for {
		select {
		case <-t.C:
		case <-cl.chMaxIdleDur:
		}

		cl.mu.Lock()

		if cl.closed || len(cl.idleConns) == 0 {
			cl.chMaxIdleDur = nil
			cl.mu.Unlock()
			t.Stop()
			return
		}

		var toClose []*Conn
		expiry := time.Now().Add(-dur)
		for len(cl.idleConns) > 0 {
			c := cl.idleConns[0]
			if c.idleSince.After(expiry) {
				break
			}
			toClose = append(toClose, c.conn)
			copy(cl.idleConns, cl.idleConns[1:])
			cl.idleConns[len(cl.idleConns)-1] = idleConn{}
			cl.idleConns = cl.idleConns[:len(cl.idleConns)-1]

		}
		cl.numActive -= len(toClose)
		if cl.cond != nil {
			for i := 0; i < len(toClose) && i < cl.numWaiting; i++ {
				cl.cond.Signal()
			}
		}
		cl.mu.Unlock()

		for _, c := range toClose {
			c.Close()
		}

		t.Reset(dur)
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
