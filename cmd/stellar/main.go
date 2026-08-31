package main

import (
	"os"
)

func main() {
	if err := NewRootCommand().Execute(); err != nil {
		// Root sets SilenceErrors, so this is the single place an error is
		// shown to the user. uiErrf renders it in red on stderr; humanizeError
		// turns cobra's terse built-ins into plain guidance.
		uiErrf("%s", humanizeError(err))
		os.Exit(1)
	}
}
