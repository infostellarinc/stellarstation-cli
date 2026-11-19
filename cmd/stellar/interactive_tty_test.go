package main

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pipePump(t *testing.T) (*ttyPump, *os.File) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	return newTTYPump(func() (*os.File, error) { return r, nil }), w
}

func readWithin(t *testing.T, in io.Reader) (string, error) {
	t.Helper()
	type result struct {
		s   string
		err error
	}
	got := make(chan result, 1)
	go func() {
		b := make([]byte, 64)
		n, err := in.Read(b)
		got <- result{string(b[:n]), err}
	}()
	select {
	case r := <-got:
		return r.s, r.err
	case <-time.After(2 * time.Second):
		t.Fatal("read did not return")
		return "", nil
	}
}

// A prompt that has ended takes nothing: the next prompt gets the input.
func TestPromptInputEndedPromptTakesNothing(t *testing.T) {
	pump, w := pipePump(t)
	first, err := pump.input()
	require.NoError(t, err)
	// The first prompt's reader is still waiting when the prompt ends.
	firstDone := make(chan error, 1)
	go func() { _, err := first.Read(make([]byte, 8)); firstDone <- err }()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, first.Close())
	assert.ErrorIs(t, <-firstDone, io.EOF)

	second, err := pump.input()
	require.NoError(t, err)
	_, err = w.WriteString("\r")
	require.NoError(t, err)
	s, err := readWithin(t, second)
	require.NoError(t, err)
	assert.Equal(t, "\r", s)
	pump.stop()
}

// Stopping the pump leaves nothing reading the terminal, so a child process
// gets everything typed afterwards; starting again resumes reading.
func TestTTYPumpStopLetsGo(t *testing.T) {
	pump, w := pipePump(t)
	in, err := pump.input()
	require.NoError(t, err)
	require.NoError(t, in.Close())
	pump.stop()

	_, err = w.WriteString("sat 0a0b\n")
	require.NoError(t, err)
	b := make([]byte, 64)
	require.NoError(t, pump.src.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := pump.src.Read(b)
	require.NoError(t, err)
	assert.Equal(t, "sat 0a0b\n", string(b[:n]), "the line reaches whoever reads next, not the stopped pump")

	in, err = pump.input()
	require.NoError(t, err)
	_, err = w.WriteString("x")
	require.NoError(t, err)
	s, err := readWithin(t, in)
	require.NoError(t, err)
	assert.Equal(t, "x", s)
	pump.stop()
}
