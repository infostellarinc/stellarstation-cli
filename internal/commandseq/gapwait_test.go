// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandseq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGapWait(t *testing.T) {
	tests := []struct {
		text        string
		want        GapWait
		wantTimeout time.Duration
		wantSkips   bool
	}{
		{text: "", want: GapWait{}, wantTimeout: DefaultGapWait, wantSkips: true},
		{text: "0s", want: WaitFor(0), wantTimeout: 0, wantSkips: true},
		{text: "4s", want: WaitFor(4 * time.Second), wantTimeout: 4 * time.Second, wantSkips: true},
		{text: "1500ms", want: WaitFor(1500 * time.Millisecond), wantTimeout: 1500 * time.Millisecond, wantSkips: true},
		{text: "never", want: NeverSkip},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			got, err := ParseGapWait(tt.text)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			timeout, skips := got.timeout()
			assert.Equal(t, tt.wantTimeout, timeout)
			assert.Equal(t, tt.wantSkips, skips)
		})
	}
}

func TestParseGapWaitRejects(t *testing.T) {
	for _, text := range []string{"2", "-1s", "forever", "Never"} {
		_, err := ParseGapWait(text)
		assert.Error(t, err, text)
	}
}

// The text form survives JSON both ways, so events and execution
// configurations carry it as a string.
func TestGapWaitsJSONRoundTrip(t *testing.T) {
	in := GapWaits{Commands: WaitFor(0), Configs: NeverSkip}
	out, err := json.Marshal(in)
	require.NoError(t, err)
	assert.JSONEq(t, `{"commands": "0s", "configs": "never"}`, string(out))

	var back GapWaits
	require.NoError(t, json.Unmarshal(out, &back))
	assert.Equal(t, in, back)

	var defaults GapWaits
	require.NoError(t, json.Unmarshal([]byte(`{"commands": ""}`), &defaults))
	assert.Equal(t, GapWaits{}, defaults)
}
