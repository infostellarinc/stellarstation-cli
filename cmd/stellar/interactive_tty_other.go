//go:build !unix

package main

import (
	"errors"
	"os"
)

// openControllingTTY is unavailable here; prompts read the terminal directly.
func openControllingTTY() (*os.File, error) {
	return nil, errors.New("no separate terminal handle on this platform")
}
