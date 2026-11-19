// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"testing"

	"github.com/stretchr/testify/assert"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

func TestEnumNames(t *testing.T) {
	assert.Equal(t, "RADIATED", StageName(streaming.CommandStage_COMMAND_STAGE_RADIATED))
	assert.Empty(t, StageName(streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED))
	assert.Equal(t, "UNKNOWN", OutcomeName(streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN))
	assert.Empty(t, OutcomeName(streaming.CommandOutcome_COMMAND_OUTCOME_UNSPECIFIED))
	assert.Equal(t, "DEADLINE_EXCEEDED", FailureName(streaming.CommandFailure_COMMAND_FAILURE_DEADLINE_EXCEEDED))
	assert.Empty(t, FailureName(streaming.CommandFailure_COMMAND_FAILURE_UNSPECIFIED))
}
