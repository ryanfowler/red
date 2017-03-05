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
	"sync"
	"time"
)

const (
	TypeMessage      = "message"
	TypePMessage     = "pmessage"
	TypePong         = "pong"
	TypePSubscribe   = "psubscribe"
	TypePUnsubscribe = "punsubscribe"
	TypeSubscribe    = "subscribe"
	TypeUnsubscribe  = "unsubscribe"
)

type PubSub struct {
	c *Conn

	mu      sync.Mutex
	closed  bool
	chPing  chan struct{}
	pingDur time.Duration
}

func (ps *PubSub) Close() error {
	ps.mu.Lock()
	if ps.closed {
		ps.mu.Unlock()
		return nil
	}
	ps.closed = true
	ps.pingDur = 0
	if ps.chPing != nil {
		select {
		case ps.chPing <- struct{}{}:
		default:
		}
	}
	ps.mu.Unlock()
	return ps.c.Close()
}

func (ps *PubSub) Ping(msg string) error {
	ps.mu.Lock()
	err := ps.pingLocked(msg)
	ps.mu.Unlock()
	return err
}

func (ps *PubSub) pingLocked(msg string) error {
	return ps.c.cmdSend("PING", msg)
}

func (ps *PubSub) PSubscribe(channels ...interface{}) error {
	ps.mu.Lock()
	err := ps.c.cmdSend("PSUBSCRIBE", channels...)
	ps.mu.Unlock()
	return err
}

func (ps *PubSub) PUnsubscribe(channels ...interface{}) error {
	ps.mu.Lock()
	err := ps.c.cmdSend("PUNSUBSCRIBE", channels...)
	ps.mu.Unlock()
	return err
}

func (ps *PubSub) Subscribe(channels ...interface{}) error {
	ps.mu.Lock()
	err := ps.c.cmdSend("SUBSCRIBE", channels...)
	ps.mu.Unlock()
	return err
}

func (ps *PubSub) Unsubscribe(channels ...interface{}) error {
	ps.mu.Lock()
	err := ps.c.cmdSend("UNSUBSCRIBE", channels...)
	ps.mu.Unlock()
	return err
}

type Message struct {
	Type    string
	Pattern string
	Channel string
	Data    string
	Count   int
}

func (ps *PubSub) ReadMessage() (Message, error) {
	// Read array length.
	length, err := ps.c.r.ReadArrayLength()
	if err != nil {
		return Message{}, err
	}
	if length < 1 {
		err = fmt.Errorf("red: invalid pubsub message array length: %d", length)
		ps.c.forceConnError(err)
		return Message{}, err
	}

	// Read message type.
	msgType, _, err := ps.c.readString()
	if err != nil {
		return Message{}, err
	}
	switch msgType {
	case TypeMessage:
		return ps.readMessage(length)
	case TypePMessage:
		return ps.readPMessage(length)
	case TypeSubscribe:
		return ps.readSubscribe(length, TypeSubscribe)
	case TypeUnsubscribe:
		return ps.readSubscribe(length, TypeUnsubscribe)
	case TypePSubscribe:
		return ps.readSubscribe(length, TypePSubscribe)
	case TypePUnsubscribe:
		return ps.readSubscribe(length, TypePUnsubscribe)
	case TypePong:
		return ps.readPong(length)
	default:
		err = fmt.Errorf("red: invalid pubusb message type: %s", msgType)
		ps.c.forceConnError(err)
		return Message{}, err
	}
}

func (ps *PubSub) readPong(length int) (Message, error) {
	msg := Message{Type: TypePong}
	if length != 2 {
		err := pubsubLengthErr(length, TypePong)
		ps.c.forceConnError(err)
	}
	s, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Data = s
	return msg, nil
}

func (ps *PubSub) readMessage(length int) (Message, error) {
	msg := Message{Type: TypeMessage}
	if length != 3 {
		err := pubsubLengthErr(length, TypeMessage)
		ps.c.forceConnError(err)
		return msg, err
	}

	// Read channel.
	channel, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Channel = channel

	// Read data.
	data, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Data = data
	return msg, nil
}

func (ps *PubSub) readPMessage(length int) (Message, error) {
	msg := Message{Type: TypePMessage}
	if length != 4 {
		err := pubsubLengthErr(length, TypePMessage)
		ps.c.forceConnError(err)
		return msg, err
	}

	// Read pattern.
	pattern, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Pattern = pattern

	// Read channel.
	channel, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Channel = channel

	// Read data.
	data, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Data = data
	return msg, nil
}

func (ps *PubSub) readSubscribe(length int, msgType string) (Message, error) {
	msg := Message{Type: msgType}
	if length != 3 {
		err := pubsubLengthErr(length, msgType)
		ps.c.forceConnError(err)
		return msg, err
	}

	// Read channel.
	channel, _, err := ps.c.readString()
	if err != nil {
		return msg, err
	}
	msg.Channel = channel

	// Read count.
	cnt, err := ps.c.readInteger()
	if err != nil {
		return msg, err
	}
	msg.Count = int(cnt)
	return msg, nil
}

func pubsubLengthErr(length int, msgType string) error {
	return fmt.Errorf("red: invalid pubsub message array length for type '%s': %d", msgType, length)
}

func (ps *PubSub) SetPingInterval(dur time.Duration) {
	const minPingInterval = time.Second
	if dur <= 0 {
		dur = 0
	} else if dur < minPingInterval {
		dur = minPingInterval
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return
	}
	ps.pingDur = dur
	if ps.chPing != nil {
		select {
		case ps.chPing <- struct{}{}:
		default:
		}
	}
	if ps.pingDur > 0 && ps.chPing == nil {
		ps.chPing = make(chan struct{}, 1)
		go ps.pingWorker(ps.pingDur)
	}
}

func (ps *PubSub) pingWorker(dur time.Duration) {
	t := time.NewTimer(dur)
	for {
		select {
		case <-t.C:
		case <-ps.chPing:
		}

		ps.mu.Lock()
		dur = ps.pingDur
		if dur <= 0 {
			ps.chPing = nil
			ps.mu.Unlock()
			t.Stop()
			return
		}
		t.Reset(dur)
		_ = ps.pingLocked("")
		ps.mu.Unlock()
	}
}
