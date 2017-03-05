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
	"fmt"

	"github.com/ryanfowler/red/resp"
)

type Pipeline struct {
	c        *Conn
	outCnt   int
	writeCnt int
}

func (p *Pipeline) Cmd(cmd string, args ...interface{}) error {
	p.writeCnt++
	return p.c.cmd(cmd, args...)
}

func (p *Pipeline) Exec() error {
	p.outCnt += p.writeCnt
	p.writeCnt = 0
	return p.c.exec()
}

func (p *Pipeline) Close() error {
	if p.writeCnt > 0 {
		err := fmt.Errorf("red: %d pipeline commands not executed", p.writeCnt)
		p.c.forceConnError(err)
		return err
	}
	if p.outCnt != 0 {
		err := fmt.Errorf("red: %d outstanding pipeline commands", p.outCnt)
		p.c.forceConnError(err)
		return err
	}
	return nil
}

func (p *Pipeline) NextType() (resp.DataType, error) {
	return p.c.nextType()
}

func (p *Pipeline) ReadString() (string, error) {
	p.outCnt--
	s, _, err := p.c.readString()
	return s, err
}

func (p *Pipeline) ReadNullString() (NullString, error) {
	p.outCnt--
	s, ok, err := p.c.readString()
	return NullString{
		String: s,
		Valid:  ok,
	}, err
}

func (p *Pipeline) ReadBytes() ([]byte, error) {
	p.outCnt--
	return p.c.readBytes()
}

func (p *Pipeline) ReadInteger() (int64, error) {
	p.outCnt--
	return p.c.readInteger()
}

func (p *Pipeline) ReadArray() (*Array, error) {
	p.outCnt--
	return p.c.readArray()
}

func (p *Pipeline) ReadDiscard() error {
	p.outCnt--
	return p.c.readDiscard()
}
