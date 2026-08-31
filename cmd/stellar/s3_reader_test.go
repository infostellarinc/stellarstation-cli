package main

import (
	"testing"
)

func TestS3ExtractFramingFromS3Key(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "BITSTREAM framing",
			key:  "pass-123/1/BITSTREAM/1",
			want: "BITSTREAM",
		},
		{
			name: "IQ framing",
			key:  "pass-123/1/IQ/1",
			want: "IQ",
		},
		{
			name: "AX25 framing",
			key:  "pass-123/1/AX25/1",
			want: "AX25",
		},
		{
			name: "IMAGE_PNG framing",
			key:  "pass-123/1/IMAGE_PNG/1",
			want: "IMAGE_PNG",
		},
		{
			name: "invalid key format",
			key:  "pass-123/1",
			want: "",
		},
		{
			name: "unknown framing type",
			key:  "pass-123/1/UNKNOWN/1",
			want: "",
		},
		{
			name: "empty key",
			key:  "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFramingFromS3Key(tt.key)
			if got != tt.want {
				t.Errorf("extractFramingFromS3Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllFramingTypes(t *testing.T) {
	// Verify getAllFramingTypes() returns expected values
	expected := []string{
		"BITSTREAM",
		"IQ",
		"AX25",
		"IMAGE_PNG",
		"IMAGE_JPEG",
		"FREE_TEXT_UTF8",
		"WATERFALL",
	}

	actual := getAllFramingTypes()
	if len(actual) != len(expected) {
		t.Errorf("getAllFramingTypes() length = %v, want %v", len(actual), len(expected))
	}

	for _, exp := range expected {
		found := false
		for _, a := range actual {
			if a == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("getAllFramingTypes() missing %v", exp)
		}
	}
}

func TestChannelFramingKey(t *testing.T) {
	key1 := channelFramingKey{ChannelID: "1", Framing: "BITSTREAM"}
	key2 := channelFramingKey{ChannelID: "1", Framing: "BITSTREAM"}
	key3 := channelFramingKey{ChannelID: "1", Framing: "IQ"}
	key4 := channelFramingKey{ChannelID: "2", Framing: "BITSTREAM"}

	if key1 != key2 {
		t.Error("Same channel and framing should be equal")
	}
	if key1 == key3 {
		t.Error("Different framing should not be equal")
	}
	if key1 == key4 {
		t.Error("Different channel should not be equal")
	}
}

func TestCheckAllChannelFramingsDone(t *testing.T) {
	tests := []struct {
		name         string
		channelsDone map[channelFramingKey]bool
		activeKeys   map[channelFramingKey]bool
		want         bool
	}{
		{
			name: "all done",
			channelsDone: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			activeKeys: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			want: true,
		},
		{
			name: "not all done",
			channelsDone: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
			},
			activeKeys: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			want: false,
		},
		{
			name:         "empty",
			channelsDone: map[channelFramingKey]bool{},
			activeKeys:   map[channelFramingKey]bool{},
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkAllChannelFramingsDone(tt.channelsDone, tt.activeKeys)
			if got != tt.want {
				t.Errorf("checkAllChannelFramingsDone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckAllChannelFramingsDoneRelaxed(t *testing.T) {
	tests := []struct {
		name       string
		sawEnd     map[channelFramingKey]bool
		inFlight   map[channelFramingKey]int
		activeKeys map[channelFramingKey]bool
		want       bool
	}{
		{
			name: "all done, no in-flight",
			sawEnd: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			inFlight: map[channelFramingKey]int{
				{ChannelID: "1", Framing: "BITSTREAM"}: 0,
				{ChannelID: "1", Framing: "IQ"}:        0,
			},
			activeKeys: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			want: true,
		},
		{
			name: "not all done",
			sawEnd: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
			},
			inFlight: map[channelFramingKey]int{
				{ChannelID: "1", Framing: "BITSTREAM"}: 0,
			},
			activeKeys: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
				{ChannelID: "1", Framing: "IQ"}:        true,
			},
			want: false,
		},
		{
			name: "in-flight requests",
			sawEnd: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
			},
			inFlight: map[channelFramingKey]int{
				{ChannelID: "1", Framing: "BITSTREAM"}: 1,
			},
			activeKeys: map[channelFramingKey]bool{
				{ChannelID: "1", Framing: "BITSTREAM"}: true,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkAllChannelFramingsDoneRelaxed(tt.sawEnd, tt.inFlight, tt.activeKeys)
			if got != tt.want {
				t.Errorf("checkAllChannelFramingsDoneRelaxed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// stubAWSError is a minimal AWS-style error exposing an ErrorCode, used to test the
// transient credential-error classifier.
type stubAWSError struct{ code string }

func (e stubAWSError) Error() string     { return e.code }
func (e stubAWSError) ErrorCode() string { return e.code }

func TestIsTransientS3AuthError(t *testing.T) {
	transient := []string{"InvalidAccessKeyId", "AccessDenied", "ExpiredToken", "RequestExpired", "TokenRefreshRequired"}
	for _, code := range transient {
		if !isTransientS3AuthError(stubAWSError{code}) {
			t.Errorf("%s should be classified transient", code)
		}
	}
	if isTransientS3AuthError(stubAWSError{"NoSuchKey"}) {
		t.Error("NoSuchKey must not be transient")
	}
	if isTransientS3AuthError(nil) {
		t.Error("nil must not be transient")
	}
	if isTransientS3AuthError(plainError("plain error")) {
		t.Error("a non-AWS error must not be transient")
	}
}

type plainError string

func (e plainError) Error() string { return string(e) }
