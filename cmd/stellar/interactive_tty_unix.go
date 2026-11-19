//go:build unix

package main

import "os"

// openControllingTTY opens the terminal as a file of its own, which Go can
// poll, so a read deadline interrupts a read waiting on it. Standard input
// itself is left in blocking mode for the child processes that inherit it.
func openControllingTTY() (*os.File, error) {
	return os.OpenFile("/dev/tty", os.O_RDONLY, 0)
}
