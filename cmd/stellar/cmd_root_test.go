package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootCommandHelp(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--help"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("root --help returned error: %v", err)
	}
	out := buf.String()
	if out == "" {
		t.Fatal("root --help produced no output")
	}
}

func TestRootCommandSubcommandRouting(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"version"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("stellar version returned error: %v", err)
	}
	if buf.String() == "" {
		t.Fatal("version command produced no output")
	}
}

func TestSatelliteAlias(t *testing.T) {
	cmd := newSatelliteCommand()
	found := false
	for _, a := range cmd.Aliases {
		if a == "sat" {
			found = true
		}
	}
	if !found {
		t.Error("satellite command missing 'sat' alias")
	}
}

func TestRootCommandGlobalFlags(t *testing.T) {
	cmd := NewRootCommand()
	for _, name := range []string{"api-url", "credentials"} {
		if cmd.PersistentFlags().Lookup(name) == nil {
			t.Errorf("root command missing persistent flag --%s", name)
		}
	}
}

func TestGlobalFlagsInheritedByOpenStream(t *testing.T) {
	root := NewRootCommand()
	root.SetArgs([]string{"satellite", "open-stream", "--help"})
	var buf bytes.Buffer
	root.SetOut(&buf)
	_ = root.Execute()
	out := buf.String()
	for _, flag := range []string{"--api-url", "--credentials"} {
		if !strings.Contains(out, flag) {
			t.Errorf("open-stream help missing inherited flag %s", flag)
		}
	}
}
