package main

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	"github.com/infostellarinc/stellarstation-cli/internal/commandseq"
)

// The Python example decodes the wire format by hand, and its own tests encode
// with the same constants, so only this test catches a wrong field number or
// enum name.

// The examples live at the repository root, two levels above this package.
const pythonStreamingModule = "../../examples/python/streaming.py"

func readPythonStreaming(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile(pythonStreamingModule)
	require.NoError(t, err)
	// A checkout with CRLF line endings (git's autocrlf on Windows) would
	// otherwise hide every constant from the line-anchored patterns.
	return strings.ReplaceAll(string(src), "\r\n", "\n")
}

func TestPythonExampleFieldNumbersMatchTheProto(t *testing.T) {
	// Longest prefix first, so STATUS_BATCH_ is not read as STATUS_.
	messages := []struct {
		prefix string
		desc   protoreflect.MessageDescriptor
	}{
		{"STATUS_BATCH_", (&streaming.CommandStatusBatch{}).ProtoReflect().Descriptor()},
		{"STATUS_", (&streaming.CommandStatus{}).ProtoReflect().Descriptor()},
		{"CONFIG_FIELD_", (&streaming.ConfigFieldResult{}).ProtoReflect().Descriptor()},
		{"SKIPPED_", (&streaming.SkippedRange{}).ProtoReflect().Descriptor()},
		{"ACK_", (&streaming.Ack{}).ProtoReflect().Descriptor()},
		{"TIMESTAMP_", (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()},
		{"TO_STARPASS_", (&streaming.ToStarPassMessage{}).ProtoReflect().Descriptor()},
		{"FROM_STARPASS_", (&streaming.FromStarPassMessage{}).ProtoReflect().Descriptor()},
		{"SEND_TELEMETRY_", (&streaming.SendTelemetryMessage{}).ProtoReflect().Descriptor()},
		{"TELEMETRY_", (&streaming.Telemetry{}).ProtoReflect().Descriptor()},
	}
	fieldNames := map[string]string{"FROM_STARPASS_SEND_TELEMETRY": "send_telemetry_message"}

	constant := regexp.MustCompile(`(?m)^([A-Z][A-Z_]*) = (\d+)$`)
	checked := 0
	for _, m := range constant.FindAllStringSubmatch(readPythonStreaming(t), -1) {
		name, number := m[1], m[2]
		for _, msg := range messages {
			suffix, ok := strings.CutPrefix(name, msg.prefix)
			if !ok {
				continue
			}
			field := fieldNames[name]
			if field == "" {
				field = strings.ToLower(suffix)
			}
			fd := msg.desc.Fields().ByName(protoreflect.Name(field))
			if assert.NotNil(t, fd, "%s: %s has no field %q", name, msg.desc.FullName(), field) {
				assert.Equal(t, strconv.Itoa(int(fd.Number())), number, "%s is %s.%s", name, msg.desc.FullName(), field)
			}
			checked++
			break
		}
	}
	assert.GreaterOrEqual(t, checked, 35, "constants found in %s", pythonStreamingModule)
}

func TestPythonExampleEnumNamesMatchTheProto(t *testing.T) {
	src := readPythonStreaming(t)
	enums := map[string]struct {
		desc   protoreflect.EnumDescriptor
		prefix string
	}{
		"ACK_STATUS_NAMES": {streaming.Ack_ACK.Descriptor(), ""},
		"STAGE_NAMES":      {streaming.CommandStage(0).Descriptor(), "COMMAND_STAGE_"},
		"OUTCOME_NAMES":    {streaming.CommandOutcome(0).Descriptor(), "COMMAND_OUTCOME_"},
		"FAILURE_NAMES":    {streaming.CommandFailure(0).Descriptor(), "COMMAND_FAILURE_"},
	}
	entry := regexp.MustCompile(`(\d+): "([A-Z_]*)"`)
	for name, enum := range enums {
		body := regexp.MustCompile(`(?s)\n` + name + ` = \{(.*?)\}`).FindStringSubmatch(src)
		require.NotNil(t, body, "%s not found in %s", name, pythonStreamingModule)
		got := map[int]string{}
		for _, e := range entry.FindAllStringSubmatch(body[1], -1) {
			n, _ := strconv.Atoi(e[1])
			got[n] = e[2]
		}
		want := map[int]string{}
		values := enum.desc.Values()
		for i := range values.Len() {
			v := values.Get(i)
			short := strings.TrimPrefix(string(v.Name()), enum.prefix)
			if enum.prefix != "" && v.Number() == 0 {
				// The unspecified value reads as "".
				short = ""
			}
			want[int(v.Number())] = short
		}
		assert.Equal(t, want, got, name)
	}
}

func TestPythonExampleReasonCodes(t *testing.T) {
	assert.Contains(t, readPythonStreaming(t),
		`REASON_HOLD_BUFFER_FULL = "`+commandseq.ReasonHoldBufferFull+`"`)
}
