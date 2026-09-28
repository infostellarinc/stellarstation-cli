package main

import (
	"context"
	"strings"
	"testing"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// uuidLikeID is a realistic identifier length for the stream and pass IDs the
// envelope carries, so size tests reserve the room a real send needs.
const uuidLikeID = "7f3a1c58-9d24-4b6e-8a10-5c2f7e9b43d1"

func TestValidateCommandSize(t *testing.T) {
	tests := []struct {
		name     string
		commands [][]byte
		wantErr  bool
	}{
		{"no commands", nil, false},
		{"small command", [][]byte{{0x0a, 0x1b, 0x2c, 0x3d}}, false},
		{"command at the limit", [][]byte{make([]byte, maxCommandBytes)}, false},
		{"command over the limit", [][]byte{make([]byte, maxCommandBytes+1)}, true},
		{
			"several commands within the limit",
			[][]byte{make([]byte, maxCommandBytes/2), make([]byte, maxCommandBytes/2)},
			false,
		},
		{
			"several commands over the limit together",
			[][]byte{make([]byte, maxCommandBytes), {0x00}},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommandSize(tt.commands)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateCommandSize() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !strings.Contains(err.Error(), "over the") {
				t.Errorf("error = %q, want it to state the limit", err)
			}
		})
	}
}

// A command of exactly maxCommandBytes must still publish within the payload
// limit, envelope included.
func TestMaxCommandFitsPayloadLimit(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}

	err := PublishSatCommand(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		uuidLikeID,
		uuidLikeID,
		1,
		[][]byte{make([]byte, maxCommandBytes)},
		1,
		newStatsTracker(false),
	)
	if err != nil {
		t.Fatalf("PublishSatCommand() error = %v", err)
	}
	if len(client.pubPayload) > mqttMaxPayloadBytes {
		t.Errorf(
			"published %d bytes, over the %d byte limit",
			len(client.pubPayload), mqttMaxPayloadBytes,
		)
	}
}

// The envelope must carry the command bytes once. The retired
// SendCommandsMessage held a second copy, which halved the usable payload.
func TestCommandIsNotRepeatedInEnvelope(t *testing.T) {
	const cmdLen = 4096
	client := &mockMQTTClientWriter{connected: true}

	err := PublishSatCommand(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		uuidLikeID,
		uuidLikeID,
		1,
		[][]byte{make([]byte, cmdLen)},
		1,
		newStatsTracker(false),
	)
	if err != nil {
		t.Fatalf("PublishSatCommand() error = %v", err)
	}
	if got := len(client.pubPayload); got >= 2*cmdLen {
		t.Errorf("published %d bytes for a %d byte command; the envelope repeats the payload", got, cmdLen)
	}
}

func TestPublishSatCommandRejectsOversized(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}

	err := PublishSatCommand(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		uuidLikeID,
		uuidLikeID,
		1,
		[][]byte{make([]byte, maxCommandBytes+1)},
		1,
		newStatsTracker(false),
	)
	if err == nil {
		t.Fatal("PublishSatCommand() should reject a command over the limit")
	}
	if client.published {
		t.Error("nothing may be published when the command is over the limit")
	}
}

// publishCommand guards the marshalled message as well, which covers config
// requests and any future caller that does not check its payload first.
func TestPublishCommandRejectsOversizedMessage(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}
	msg := &streaming.ToStarPassMessage{
		StreamId: uuidLikeID,
		PassId:   uuidLikeID,
		Index:    1,
		Command:  [][]byte{make([]byte, mqttMaxPayloadBytes)},
	}

	err := publishCommand(client, "dev/pass-123/uplink", 1, msg, nil, "uplink", 1)
	if err == nil {
		t.Fatal("publishCommand() should reject a message over the limit")
	}
	if client.published {
		t.Error("nothing may be published when the message is over the limit")
	}
}

// Interactive input must accept a line carrying a full-size command. bufio's
// default 64 KiB line cap would refuse one long before the size check runs.
func TestCommandScannerAcceptsLongestCommandLine(t *testing.T) {
	line := "sat " + strings.Repeat("a5", maxCommandBytes)

	sc := newCommandScanner(strings.NewReader(line + "\n"))
	if !sc.Scan() {
		t.Fatalf("scanner refused a %d byte line: %v", len(line), sc.Err())
	}
	if sc.Text() != line {
		t.Errorf("scanned %d bytes, want %d", len(sc.Text()), len(line))
	}
}

// The one-shot sender rejects an oversized command before it is published.
func TestCreateSendSatCommandFuncRejectsOversized(t *testing.T) {
	client := &mockMQTTClient{connected: true}
	sendFunc := createSendSatCommandFunc(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		"stream-1",
		"plan-1",
		Config{MQTTQoS: 1},
		newStatsTracker(false),
	)

	err := sendFunc(strings.Repeat("ab", maxCommandBytes+1), 1)
	if err == nil {
		t.Fatal("sendFunc() should reject a command over the limit")
	}
	if client.published {
		t.Error("nothing may be published when the command is over the limit")
	}
}

// Interactive mode reports the rejection and leaves the session running, so the
// operator can retry with a smaller command.
func TestInteractiveSatCommandOverSizeLimit(t *testing.T) {
	s := commandingSession([]channelTarget{
		{ChannelID: "ch-1", UplinkTopic: "topic/uplink"},
	})

	out := captureStderr(t, func() {
		s.sendSatCommand(context.Background(), []string{strings.Repeat("ab", maxCommandBytes+1)})
	})

	if !strings.Contains(out, "over the") {
		t.Errorf("output = %q, want an error stating the command size limit", out)
	}
	if s.satIndices[0] != 1 {
		t.Errorf("satellite command index advanced to %d; nothing may be sent", s.satIndices[0])
	}
}
