// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"strings"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// enumName is the enum's name without its type prefix, for example RADIATED
// for COMMAND_STAGE_RADIATED. The unspecified value is "", which readers
// treat as absent.
func enumName[E interface {
	~int32
	String() string
}](v E, prefix string) string {
	if v == 0 {
		return ""
	}
	return strings.TrimPrefix(v.String(), prefix)
}

func StageName(v streaming.CommandStage) string { return enumName(v, "COMMAND_STAGE_") }

func OutcomeName(v streaming.CommandOutcome) string { return enumName(v, "COMMAND_OUTCOME_") }

func FailureName(v streaming.CommandFailure) string { return enumName(v, "COMMAND_FAILURE_") }
