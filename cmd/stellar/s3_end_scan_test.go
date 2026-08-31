package main

import "testing"

// The high-rate S3 reader must keep fetching a channel/framing after an END
// marker in ANY mode. This guards the regression where re-streaming a past pass
// stopped at the first END and missed later objects still in S3. An END must not
// change the fetch decision; only the in-flight window cap gates it.
func TestShouldSpawnRelaxedFetch_EndNeverStopsFetching(t *testing.T) {
	key := channelFramingKey{ChannelID: "ch1", Framing: "IQ"}
	ended := map[channelFramingKey]bool{key: true}
	notEnded := map[channelFramingKey]bool{}
	within := map[channelFramingKey]int{key: 1} // inFlight < window
	atCap := map[channelFramingKey]int{key: 4}  // inFlight == window

	for _, autoClose := range []bool{false, true} {
		cfg := Config{WindowSize: 4, EnableAutoClose: autoClose}

		// END must not change the outcome vs. not-ended, in either mode.
		if got, want := shouldSpawnRelaxedFetch(cfg, key, ended, within), shouldSpawnRelaxedFetch(cfg, key, notEnded, within); got != want {
			t.Errorf("autoClose=%v: END changed the fetch decision (%v vs %v); END must never gate fetching", autoClose, got, want)
		}
		// With window available, it keeps fetching even after END.
		if !shouldSpawnRelaxedFetch(cfg, key, ended, within) {
			t.Errorf("autoClose=%v: expected to keep fetching after END while window has room", autoClose)
		}
		// The window cap still applies (END or not).
		if shouldSpawnRelaxedFetch(cfg, key, ended, atCap) {
			t.Errorf("autoClose=%v: should not exceed the in-flight window cap", autoClose)
		}
	}
}
