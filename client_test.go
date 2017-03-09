package red_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/ryanfowler/red"
)

func TestClientConn(t *testing.T) {
	var cl red.Client

	// Test failed connection.
	cl.OnNew = func(c *red.Conn) error {
		return errors.New("test error")
	}
	err := cl.Conn(func(c *red.Conn) error {
		return nil
	})
	if err == nil || !strings.HasSuffix(err.Error(), "test error") {
		t.Fatalf("Unexpected conn error: %v", err)
	}

	// Test basic connection.
	cl = red.Client{}
	err = cl.Conn(func(c *red.Conn) error {
		pong, err := c.ExecString("PING", "testing")
		if err != nil {
			return err
		}
		if pong != "testing" {
			t.Errorf("Unexpected pong value %s", pong)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected Conn error: %s", err.Error())
	}

	// Test borrowed connection.
	var reused int
	cl.OnReuse = func(c *red.Conn) bool {
		reused++
		return true
	}
	cl.OnNew = func(c *red.Conn) error {
		t.Fatal("Unexpected new connection")
		return nil
	}
	err = cl.Conn(func(c *red.Conn) error {
		pong, err := c.ExecString("PING", "test2")
		if err != nil {
			return err
		}
		if pong != "test2" {
			t.Errorf("Unexpected pong value %s", pong)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected borrowed Conn error: %s", err.Error())
	}
	if reused != 1 {
		t.Fatalf("Unexpected number of OnReuse calls: %d", reused)
	}

	// Refuse borrowed connection.
	var onNew int
	cl.OnReuse = func(c *red.Conn) bool {
		return false
	}
	cl.OnNew = func(c *red.Conn) error {
		onNew++
		return nil
	}
	err = cl.Conn(func(c *red.Conn) error {
		pong, err := c.ExecString("PING", "test3")
		if err != nil {
			return err
		}
		if pong != "test3" {
			t.Errorf("Unexpected pong value %s", pong)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Unexpected new conn error: %s", err.Error())
	}
	if onNew != 1 {
		t.Fatalf("Unexpected number of OnNew calls: %d", onNew)
	}

	// Return an error when too many active connections reached.
	cl.OnReuse = nil
	cl.OnNew = nil
	cl.MaxActiveConns = 1
	cl.MaxWaiting = -1
	cl.IOBufferSize = 100
	cl.Conn(func(c *red.Conn) error {
		err := cl.Conn(func(c *red.Conn) error { return nil })
		if err != red.ErrTooManyActive {
			t.Fatalf("Unexpected waiting error: %v", err)
		}
		return nil
	})

	// Wait for an active connection when limit reached.
	cl.MaxWaiting = 0
	chWait := make(chan struct{})
	cl.Conn(func(c *red.Conn) error {
		go func() {
			<-chWait
			err := cl.Conn(func(c *red.Conn) error {
				chWait <- struct{}{}
				return nil
			})
			if err != nil {
				t.Fatalf("Unexpected conn error: %s", err.Error())
			}
		}()
		chWait <- struct{}{}
		return nil
	})
	<-chWait

	// Receive an error from a closed Client.
	err = cl.Close()
	if err != nil {
		t.Fatalf("Unexpected error while closing: %s", err.Error())
	}
	err = cl.Conn(func(c *red.Conn) error { return nil })
	if err == nil || err.Error() != "red: client closed" {
		t.Fatalf("Unexpected conn error: %v", err)
	}
}
