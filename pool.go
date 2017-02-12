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
	"sync"
	"time"
)

var (
	// ErrTooManyActive represents the error on a Pool when the
	// MaxActiveConns and MaxWaiting limits have been reached.
	ErrTooManyActive = errors.New("red: too many active connections")
	errPoolClosed    = errors.New("red: pool closed")
)

type idleConn struct {
	conn      Conn
	idleSince time.Time
}

// Pool represents a pool of zero or more Redis Conns. All methods on a Pool are
// save to use concurrently.
type Pool struct {
	// Addr represents the address of the Redis server to connect to; Required.
	Addr string
	// Dial is an optional function to create a Conn. The default dial
	// attempts to create a TCP connection to the Redis server with a 30 -
	// second connection timeout.
	Dial func(string) (Conn, error)
	// MaxActiveConns represents the maximum total active connections at a
	// given time. A value <= 0 is no active limit.
	MaxActiveConns int
	// MaxIdleConns represents the maximum number of idle connections at a
	// given time. The default value is 10.
	MaxIdleConns int
	// MaxIdleDuration represents the timeout for an idle connection. By
	// default, there is no idle timeout.
	MaxIdleDuration time.Duration
	// MaxWaiting represents the maximum number of goroutines waiting for a
	// Conn because they are blocked by MaxActiveConns. The default value is
	// no limit. With a value < 0, ErrTooManyActive will be returned whenever
	// MaxActiveConns is reached.
	MaxWaiting int
	// OnReuse is an optional function to intercept an idle Conn before use.
	// If OnReuse returns false, the Conn will be closed and another will be
	// retrieved/created.
	OnReuse func(Conn) bool

	mu           sync.Mutex
	cond         *sync.Cond
	numWaiting   int
	numActive    int
	closed       bool
	chMaxIdleDur chan struct{}
	idleConns    []*idleConn
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
func (p *Pool) Status() Status {
	p.mu.Lock()
	s := Status{
		Closed:     p.closed,
		NumActive:  p.numActive,
		NumIdle:    len(p.idleConns),
		NumWaiting: p.numWaiting,
	}
	p.mu.Unlock()
	return s
}

// Close closes the Pool and any of its idle connections. After being closed, a
// pool cannot be used and any attempt to use it will return an error.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	toClose := make([]Conn, len(p.idleConns))
	for i, c := range p.idleConns {
		toClose[i] = c.conn
		p.idleConns[i] = nil
	}
	p.idleConns = nil

	if p.chMaxIdleDur != nil {
		select {
		case p.chMaxIdleDur <- struct{}{}:
		default:
		}
	}
	p.numActive -= len(toClose)
	if p.cond != nil && p.numWaiting > 0 {
		p.cond.Broadcast()
	}
	p.mu.Unlock()

	for _, c := range toClose {
		c.Close()
	}

	return nil
}

// Conn allows for direct access to a Conn using the provided function. The
// Conn is only valid during the lifetime of the function. Conn will return any
// error returned by fn.
func (p *Pool) Conn(fn func(c Conn) error) error {
	c, err := p.getConn()
	if err != nil {
		return err
	}
	defer p.putConn(c)
	return fn(c)
}

func (p *Pool) getConn() (Conn, error) {
	p.mu.Lock()
	for {
		// Return if pool is closed.
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}

		// Attempt to get an idle conn.
		if len(p.idleConns) > 0 {
			c := p.popIdle()
			p.mu.Unlock()
			if p.OnReuse != nil && !p.OnReuse(c) {
				c.Close()
				p.mu.Lock()
				p.decrActiveLocked(1)
				continue
			}
			return c, nil
		}

		// Check number of active conns.
		if p.MaxActiveConns <= 0 || p.numActive < p.MaxActiveConns {
			break
		}
		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}

		// Check wait limits.
		if !p.waitOK() {
			p.mu.Unlock()
			return nil, ErrTooManyActive
		}

		p.numWaiting++
		p.cond.Wait()
		p.numWaiting--
	}
	p.numActive++
	p.mu.Unlock()

	// Create and return a new connection.
	c, err := p.dialConn()
	if err != nil {
		p.mu.Lock()
		p.decrActiveLocked(1)
		p.mu.Unlock()
		return nil, err
	}
	return c, nil
}

func (p *Pool) dialConn() (Conn, error) {
	if p.Dial == nil {
		return Dial(p.Addr, defaultDialTimeout)
	}
	return p.Dial(p.Addr)
}

func (p *Pool) popIdle() Conn {
	// TODO (ryanfowler): optimize?
	c := p.idleConns[0]
	copy(p.idleConns, p.idleConns[1:])
	p.idleConns[len(p.idleConns)-1] = nil
	p.idleConns = p.idleConns[:len(p.idleConns)-1]
	return c.conn
}

func (p *Pool) waitOK() bool {
	if p.MaxWaiting < 0 {
		return false
	}
	if p.MaxWaiting > 0 && p.numWaiting >= p.MaxWaiting {
		return false
	}
	return true
}

func (p *Pool) putConn(c Conn) {
	if c == nil {
		return
	}

	p.mu.Lock()

	// Return if the pool is closed or the connection has a fatal error.
	if p.closed || c.Err() != nil {
		p.decrActiveLocked(1)
		p.mu.Unlock()
		c.Close()
		return
	}

	// Check number of idle conns.
	maxIdle := p.MaxIdleConns
	if maxIdle < 1 {
		maxIdle = 10
	}
	if len(p.idleConns) >= maxIdle {
		p.decrActiveLocked(1)
		p.mu.Unlock()
		c.Close()
		return
	}

	// Add to idle conns and optionally start the idle conn worker.
	p.idleConns = append(p.idleConns, &idleConn{conn: c, idleSince: time.Now()})
	p.startMaxIdleWorker()
	p.decrActiveLocked(0)
	p.mu.Unlock()
}

func (p *Pool) decrActiveLocked(n int) {
	p.numActive -= n
	if p.cond != nil && p.numWaiting > 0 {
		p.cond.Signal()
	}
}

func (p *Pool) ExecArray(cmd string, args []interface{}, fn func(*ArrayRes) error) error {
	return p.Conn(func(c Conn) error {
		return p.execArray(c, cmd, args, fn)
	})
}

func (p *Pool) execArray(c Conn, cmd string, args []interface{}, fn func(*ArrayRes) error) error {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return err
	}
	ar, err := c.ReadArray()
	if err != nil {
		return err
	}
	defer ar.Close()
	return fn(ar)
}

func (p *Pool) ExecStringArr(cmd string, args ...interface{}) ([]string, error) {
	var ss []string
	err := p.Conn(func(c Conn) error {
		var err error
		ss, err = p.execStringArr(c, cmd, args...)
		return err
	})
	return ss, err
}

func (p *Pool) execStringArr(c Conn, cmd string, args ...interface{}) ([]string, error) {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return nil, err
	}
	return c.ReadStringArr()
}

func (p *Pool) ExecString(cmd string, args ...interface{}) (string, error) {
	var s string
	err := p.Conn(func(c Conn) error {
		var err error
		s, err = p.execString(c, cmd, args...)
		return err
	})
	return s, err
}

func (p *Pool) execString(c Conn, cmd string, args ...interface{}) (string, error) {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return "", err
	}
	return c.ReadString()
}

func (p *Pool) ExecBytes(cmd string, args ...interface{}) ([]byte, error) {
	var b []byte
	err := p.Conn(func(c Conn) error {
		var err error
		b, err = p.execBytes(c, cmd, args...)
		return err
	})
	return b, err
}

func (p *Pool) execBytes(c Conn, cmd string, args ...interface{}) ([]byte, error) {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return nil, err
	}
	return c.ReadBytes()
}

func (p *Pool) ExecNullString(cmd string, args ...interface{}) (NullString, error) {
	var ns NullString
	err := p.Conn(func(c Conn) error {
		var err error
		ns, err = p.execNullString(c, cmd, args...)
		return err
	})
	return ns, err
}

func (p *Pool) execNullString(c Conn, cmd string, args ...interface{}) (NullString, error) {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return NullString{}, err
	}
	return c.ReadNullString()
}

func (p *Pool) ExecInteger(cmd string, args ...interface{}) (int64, error) {
	var i int64
	err := p.Conn(func(c Conn) error {
		var err error
		i, err = p.execInteger(c, cmd, args...)
		return err
	})
	return i, err
}

func (p *Pool) execInteger(c Conn, cmd string, args ...interface{}) (int64, error) {
	c.Cmd(cmd, args...)
	err := c.Send()
	if err != nil {
		return 0, err
	}
	return c.ReadInteger()
}

func (p *Pool) startMaxIdleWorker() {
	if p.MaxIdleDuration > 0 && p.chMaxIdleDur == nil && len(p.idleConns) > 0 {
		p.chMaxIdleDur = make(chan struct{}, 1)
		go p.maxIdleWorker()
	}
}

func (p *Pool) maxIdleWorker() {
	dur := maxDuration(p.MaxIdleDuration, time.Second)
	t := time.NewTimer(dur)
	for {
		select {
		case <-t.C:
		case <-p.chMaxIdleDur:
		}

		p.mu.Lock()

		if p.closed || len(p.idleConns) == 0 {
			p.chMaxIdleDur = nil
			p.mu.Unlock()
			t.Stop()
			return
		}

		var toClose []Conn
		expiry := time.Now().Add(-dur)
		for len(p.idleConns) > 0 {
			c := p.idleConns[0]
			if c.idleSince.After(expiry) {
				break
			}
			toClose = append(toClose, c.conn)
			copy(p.idleConns, p.idleConns[1:])
			p.idleConns[len(p.idleConns)-1] = nil
			p.idleConns = p.idleConns[:len(p.idleConns)-1]

		}
		p.numActive -= len(toClose)
		if p.cond != nil {
			for i := 0; i < len(toClose) && i < p.numWaiting; i++ {
				p.cond.Signal()
			}
		}
		p.mu.Unlock()

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
