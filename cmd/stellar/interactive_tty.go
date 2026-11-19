package main

import (
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Interactive mode runs one bubbletea program per prompt, and later hands the
// terminal to a child stream process. bubbletea cannot be trusted to let go of
// the terminal: when a program quits, its input goroutine can stay blocked
// reading the terminal, and it then swallows the next keypress (or, once a
// stream has started, the operator's first command line) and drops it.
//
// So interactive mode gives each prompt an input of its own instead of the
// terminal itself. One process-wide pump is the only reader of the terminal;
// each prompt reads from the pump through a promptInput, which stops taking
// input the moment the prompt ends. Before a child process runs, the pump is
// stopped and waited for, so nothing in this process reads the terminal while
// the child does.

// ttyPump reads the terminal on behalf of the current prompt.
type ttyPump struct {
	open func() (*os.File, error)

	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte // read but not yet taken by a prompt
	err  error  // a read error other than being stopped

	src  *os.File
	done chan struct{} // closed when the reading goroutine exits; nil when stopped
}

func newTTYPump(open func() (*os.File, error)) *ttyPump {
	p := &ttyPump{open: open}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// promptTTY is the pump the interactive prompts read through.
var promptTTY = newTTYPump(openControllingTTY)

// start begins reading, if it is not already.
func (p *ttyPump) start() error {
	if p.done != nil {
		return nil
	}
	if p.src == nil {
		src, err := p.open()
		if err != nil {
			return err
		}
		p.src = src
	}
	// A deadline left from stopping would end the next read at once.
	if err := p.src.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	p.mu.Lock()
	p.err = nil
	p.mu.Unlock()
	p.done = make(chan struct{})
	go p.read(p.done)
	return nil
}

func (p *ttyPump) read(done chan struct{}) {
	defer close(done)
	b := make([]byte, 256)
	for {
		n, err := p.src.Read(b)
		p.mu.Lock()
		p.buf = append(p.buf, b[:n]...)
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			p.err = err
		}
		p.cond.Broadcast()
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// stop ends reading and waits until nothing in this process reads the
// terminal. Input typed but not yet taken by a prompt is dropped.
func (p *ttyPump) stop() {
	if p.done == nil {
		return
	}
	_ = p.src.SetReadDeadline(time.Now())
	<-p.done
	p.done = nil
	p.mu.Lock()
	p.buf = nil
	p.cond.Broadcast()
	p.mu.Unlock()
}

// input returns a reader for one prompt. Close it when the prompt ends.
func (p *ttyPump) input() (*promptInput, error) {
	if err := p.start(); err != nil {
		return nil, err
	}
	return &promptInput{pump: p}, nil
}

// promptInput is one prompt's view of the terminal. It looks like a terminal
// to bubbletea (Fd is the real one, so raw mode applies) but has no Name, so
// bubbletea reads it with plain Read calls rather than polling the descriptor.
type promptInput struct {
	pump   *ttyPump
	closed bool // guarded by pump.mu
}

func (in *promptInput) Read(b []byte) (int, error) {
	p := in.pump
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		switch {
		case in.closed:
			return 0, io.EOF
		case len(p.buf) > 0:
			n := copy(b, p.buf)
			p.buf = p.buf[n:]
			return n, nil
		case p.err != nil:
			return 0, p.err
		}
		p.cond.Wait()
	}
}

// Close ends the prompt's input; a read still waiting returns io.EOF without
// taking anything.
func (in *promptInput) Close() error { //nolint:unparam // io.Closer.
	in.pump.mu.Lock()
	in.closed = true
	in.pump.cond.Broadcast()
	in.pump.mu.Unlock()
	return nil
}

func (in *promptInput) Write(b []byte) (int, error) { return os.Stdout.Write(b) }

func (in *promptInput) Fd() uintptr { return os.Stdin.Fd() }
