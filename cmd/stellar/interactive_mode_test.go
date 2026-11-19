package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

// scriptedPrompter answers prompts from a script, failing the test when a
// prompt it does not expect comes up.
type scriptedPrompter struct {
	t       *testing.T
	answers []any // int for Select, []bool for MultiSelect, string for Input, bool for Confirm
	asked   []string
}

func (s *scriptedPrompter) next(title string) any {
	s.t.Helper()
	s.asked = append(s.asked, title)
	if len(s.answers) == 0 {
		s.t.Fatalf("unexpected prompt %q", title)
	}
	a := s.answers[0]
	s.answers = s.answers[1:]
	if err, ok := a.(error); ok {
		return err
	}
	return a
}

func (s *scriptedPrompter) Select(title string, options []string, _ int) (int, error) {
	a := s.next(title)
	if err, ok := a.(error); ok {
		return 0, err
	}
	i := a.(int)
	require.Less(s.t, i, len(options), "answer out of range for %q", title)
	return i, nil
}

func (s *scriptedPrompter) MultiSelect(title string, _ []string, _ []bool) ([]bool, error) {
	a := s.next(title)
	if err, ok := a.(error); ok {
		return nil, err
	}
	return a.([]bool), nil
}

func (s *scriptedPrompter) Input(title, _, _ string, validate func(string) error) (string, error) {
	a := s.next(title)
	if err, ok := a.(error); ok {
		return "", err
	}
	v := a.(string)
	if validate != nil {
		require.NoError(s.t, validate(v), "scripted answer refused for %q", title)
	}
	return v, nil
}

func (s *scriptedPrompter) Confirm(title string, _ bool) (bool, error) {
	a := s.next(title)
	if err, ok := a.(error); ok {
		return false, err
	}
	return a.(bool), nil
}

// fakeInteractiveAPI serves canned data and records bookings.
type fakeInteractiveAPI struct {
	satellites []apiclient.Satellite
	passes     []apiclient.Pass
	visibility []apiclient.Visibility
	configs    []apiclient.ExecutionConfig
	channels   []apiclient.PassChannel
	created    []apiclient.CreatePassRequest
	canceled   []string
	updated    map[string]apiclient.UpdatePassRequest

	orbitParams    *apiclient.OrbitParameters
	paramUpdates   []apiclient.UpdateOrbitParametersRequest
	validations    []apiclient.OrbitValidationResult // answered in turn; the last repeats
	validated      []apiclient.ValidateOrbitDataRequest
	createdOrbit   []apiclient.CreateOrbitDataRequest
	createOrbitErr error
}

func (f *fakeInteractiveAPI) ListSatellites(context.Context) ([]apiclient.Satellite, error) {
	return f.satellites, nil
}

func (f *fakeInteractiveAPI) ListPasses(context.Context, apiclient.ListPassesOpts) ([]apiclient.Pass, error) {
	return f.passes, nil
}

func (f *fakeInteractiveAPI) ListVisibilities(context.Context, apiclient.ListVisibilitiesOpts) ([]apiclient.Visibility, error) {
	return f.visibility, nil
}

func (f *fakeInteractiveAPI) ListConfigurations(context.Context, string, string) ([]apiclient.ExecutionConfig, error) {
	return f.configs, nil
}

func (f *fakeInteractiveAPI) CreatePass(_ context.Context, req apiclient.CreatePassRequest) (*apiclient.Pass, error) {
	f.created = append(f.created, req)
	return &apiclient.Pass{ID: "new-pass"}, nil
}

func (f *fakeInteractiveAPI) GetPassChannels(context.Context, string) ([]apiclient.PassChannel, error) {
	return f.channels, nil
}

func (f *fakeInteractiveAPI) UpdatePass(
	_ context.Context, id string, req apiclient.UpdatePassRequest,
) (*apiclient.Pass, error) {
	if f.updated == nil {
		f.updated = map[string]apiclient.UpdatePassRequest{}
	}
	f.updated[id] = req
	return &apiclient.Pass{ID: id}, nil
}

func (f *fakeInteractiveAPI) CancelPass(_ context.Context, id string) (*apiclient.Pass, error) {
	f.canceled = append(f.canceled, id)
	return &apiclient.Pass{ID: id}, nil
}

var interactiveNow = time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC)

func newTestInteractive(t *testing.T, api *fakeInteractiveAPI, answers ...any) (*interactiveMode, *scriptedPrompter, *[][]string, *bytes.Buffer) {
	t.Helper()
	p := &scriptedPrompter{t: t, answers: answers}
	var streams [][]string
	out := &bytes.Buffer{}
	g := &interactiveMode{
		p: p, api: api, out: out,
		now:        func() time.Time { return interactiveNow },
		openStream: func(args []string) error { streams = append(streams, args); return nil },
	}
	return g, p, &streams, out
}

func twoSatellites() []apiclient.Satellite {
	return []apiclient.Satellite{{ID: "sat-b", DisplayName: "Bravo"}, {ID: "sat-a", DisplayName: "Alpha"}}
}

func pass(id string, start time.Time, status string) apiclient.Pass {
	return apiclient.Pass{
		ID:            id,
		Satellite:     apiclient.SatelliteRef{ID: "sat-a", Name: "Alpha"},
		GroundStation: apiclient.GroundStationRef{ID: "gs-1", Name: "London"},
		Scheduled:     &apiclient.ScheduledDetails{Start: start, Stop: start.Add(10 * time.Minute)},
		Execution:     &apiclient.Execution{Status: status},
	}
}

// Streaming the next pass skips ended and canceled passes and opens a stream
// with the chosen options.
func TestInteractiveStreamsTheNextPass(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites: twoSatellites(),
		passes: []apiclient.Pass{
			pass("later", interactiveNow.Add(5*time.Hour), "PENDING"),
			pass("ended", interactiveNow.Add(-2*time.Hour), "SUCCEEDED"),
			pass("canceled", interactiveNow.Add(time.Hour), "CANCELED"),
			pass("next", interactiveNow.Add(2*time.Hour), "PENDING"),
		},
	}
	g, _, streams, out := newTestInteractive(t, api,
		interactiveStreamNext,
		0,                               // all satellites
		[]bool{false, true, true, true}, // no telemetry
		1,                               // satellite commands
		"/tmp/data",
		interactiveQuit,
	)
	require.NoError(t, g.run(t.Context()))
	require.Len(t, *streams, 1)
	assert.Equal(t, []string{
		"satellite", "open-stream", "--pass-id", "next",
		"--disable-downlink", "--disable-config-requests", "--interactive", "--dest", "/tmp/data",
	}, (*streams)[0])
	assert.Contains(t, out.String(), "Next time you can run this directly")
}

// Booking walks from a free window to a booked pass on it.
func TestInteractiveBooksAPass(t *testing.T) {
	winStart := interactiveNow.Add(3 * time.Hour)
	api := &fakeInteractiveAPI{
		satellites: twoSatellites(),
		visibility: []apiclient.Visibility{{
			Satellite:           apiclient.SatelliteRef{ID: "sat-a"},
			GroundStation:       apiclient.GroundStationRef{ID: "gs-1", Name: "London"},
			Availability:        []apiclient.Interval{{Start: winStart, Stop: winStart.Add(9 * time.Minute)}},
			MaxElevationDegrees: 42,
		}},
		configs: []apiclient.ExecutionConfig{{ID: "cfg-1", DisplayName: "TM_TC"}},
	}
	g, p, streams, out := newTestInteractive(t, api,
		interactiveBook,
		0,     // Alpha (sorted first)
		0,     // the only window
		0,     // the whole window
		true,  // book it
		false, // no stream now
		interactiveQuit,
	)
	require.NoError(t, g.run(t.Context()))
	require.Len(t, api.created, 1)
	assert.Equal(t, apiclient.CreatePassRequest{
		SatelliteID: "sat-a", GroundStationID: "gs-1", ExecutionConfigID: "cfg-1",
		Booking:   apiclient.Interval{Start: winStart, Stop: winStart.Add(9 * time.Minute)},
		Scheduled: apiclient.Interval{Start: winStart, Stop: winStart.Add(9 * time.Minute)},
	}, api.created[0])
	assert.Empty(t, *streams)
	assert.Contains(t, out.String(), "Booked pass new-pass")
	assert.Contains(t, out.String(), "stellar satellite reserve-pass --satellite-id sat-a")
	assert.Contains(t, strings.Join(p.asked, "\n"), "Which satellite?")
}

// Custom times must fall inside the window.
func TestInteractiveBooksChosenTimes(t *testing.T) {
	winStart := interactiveNow.Add(3 * time.Hour)
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		visibility: []apiclient.Visibility{{
			GroundStation: apiclient.GroundStationRef{ID: "gs-1", Name: "London"},
			Availability:  []apiclient.Interval{{Start: winStart, Stop: winStart.Add(20 * time.Minute)}},
		}},
		configs: []apiclient.ExecutionConfig{{ID: "cfg-1"}, {ID: "cfg-2", DisplayName: "LR"}},
	}
	start := winStart.Add(5 * time.Minute)
	g, _, _, _ := newTestInteractive(t, api,
		interactiveBook,
		0, // window
		1, // cfg-2
		1, // choose times
		start.Format(time.RFC3339),
		start.Add(6*time.Minute).Format(time.RFC3339),
		true, false,
		interactiveQuit,
	)
	require.NoError(t, g.run(t.Context()))
	require.Len(t, api.created, 1)
	assert.Equal(t, "cfg-2", api.created[0].ExecutionConfigID)
	assert.Equal(t, start, api.created[0].Scheduled.Start)
	assert.Equal(t, start.Add(6*time.Minute), api.created[0].Scheduled.Stop)

	assert.Error(t, timeInWindow(winStart, winStart.Add(20*time.Minute), winStart.Add(-time.Minute)),
		"a start before the window is refused")
	assert.NoError(t, timeInWindow(winStart, winStart.Add(20*time.Minute), winStart.Add(time.Minute)))
}

// Backing out of a step returns to the menu; backing out of the menu quits.
func TestInteractiveBackingOut(t *testing.T) {
	api := &fakeInteractiveAPI{satellites: twoSatellites()}
	g, _, _, out := newTestInteractive(t, api, interactiveUpcoming, errInteractiveCanceled, errInteractiveCanceled)
	require.NoError(t, g.run(t.Context()))
	assert.Contains(t, out.String(), "Back to the menu.")
}

// Canceling a pass asks first.
func TestInteractiveCancelsAPass(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		passes:     []apiclient.Pass{pass("p1", interactiveNow.Add(time.Hour), "PENDING")},
	}
	g, _, _, out := newTestInteractive(t, api, interactiveUpcoming, 0, 2, true, interactiveQuit)
	require.NoError(t, g.run(t.Context()))
	assert.Equal(t, []string{"p1"}, api.canceled)
	assert.Contains(t, out.String(), "stellar satellite cancel-pass p1")
}

// Changing a pass moves it inside its visibility and switches configuration,
// then prints the equivalent command.
func TestInteractiveChangesAPass(t *testing.T) {
	p1 := pass("p1", interactiveNow.Add(time.Hour), "PENDING")
	p1.ExecutionConfigID = "cfg-1"
	p1.Visibility = &apiclient.Interval{Start: interactiveNow.Add(55 * time.Minute), Stop: interactiveNow.Add(75 * time.Minute)}
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		passes:     []apiclient.Pass{p1},
		configs:    []apiclient.ExecutionConfig{{ID: "cfg-1", DisplayName: "Low rate"}, {ID: "cfg-2", DisplayName: "High rate"}},
	}
	g, _, _, out := newTestInteractive(t, api, interactiveUpcoming, 0,
		1,                      // change it
		2,                      // both
		"2026-11-12T01:58:00Z", // start
		"2026-11-12T02:10:00Z", // end
		1,                      // High rate
		true,                   // save
		interactiveQuit)
	require.NoError(t, g.run(t.Context()))
	req := api.updated["p1"]
	require.NotNil(t, req.Scheduled)
	assert.Equal(t, interactiveNow.Add(58*time.Minute), req.Scheduled.Start)
	assert.Equal(t, interactiveNow.Add(70*time.Minute), req.Scheduled.Stop)
	require.NotNil(t, req.ExecutionConfigID)
	assert.Equal(t, "cfg-2", *req.ExecutionConfigID)
	assert.Contains(t, out.String(), "stellar satellite update-pass p1 --scheduled-start 2026-11-12T01:58:00Z "+
		"--scheduled-stop 2026-11-12T02:10:00Z --execution-config-id cfg-2")
}

// Keeping the current configuration changes nothing.
func TestInteractiveChangePassKeepsConfiguration(t *testing.T) {
	p1 := pass("p1", interactiveNow.Add(time.Hour), "PENDING")
	p1.ExecutionConfigID = "cfg-1"
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		passes:     []apiclient.Pass{p1},
		configs:    []apiclient.ExecutionConfig{{ID: "cfg-1", DisplayName: "Low rate"}, {ID: "cfg-2", DisplayName: "High rate"}},
	}
	g, _, _, out := newTestInteractive(t, api, interactiveUpcoming, 0, 1, 1, 0, interactiveQuit)
	require.NoError(t, g.run(t.Context()))
	assert.Empty(t, api.updated)
	assert.Contains(t, out.String(), "Nothing changed.")
}

func TestStreamArgs(t *testing.T) {
	all := []bool{true, true, true, true}
	assert.Equal(t,
		[]string{"satellite", "open-stream", "--pass-id", "p", "--disable-uplink", "--disable-config-requests"},
		streamArgs("p", all, 0, nil, defaultStreamDest))
	assert.Equal(t,
		[]string{"satellite", "open-stream", "--pass-id", "p", "--interactive"},
		streamArgs("p", all, 3, nil, ""))
	assert.Equal(t,
		[]string{"satellite", "open-stream", "--pass-id", "p", "--disable-event", "--disable-uplink", "--interactive"},
		streamArgs("p", []bool{true, true, true, false}, 2, nil, defaultStreamDest))
}

func TestFormatWindow(t *testing.T) {
	start := interactiveNow.Add(90 * time.Minute)
	assert.Equal(t, "Thu 12 Nov 02:30–02:39 UTC (9 min, in 1h 30m)", formatWindow(start, start.Add(9*time.Minute), interactiveNow))
	assert.Contains(t, formatWindow(interactiveNow.Add(-time.Minute), interactiveNow.Add(time.Minute), interactiveNow), "happening now")
}

// parsedRoot is the root command with its flags parsed, as when it runs.
func parsedRoot(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := NewRootCommand()
	require.NoError(t, cmd.ParseFlags(nil))
	return cmd
}

// Interactive mode remembers the API address with the key, and every command then
// finds it without STELLAR_API_URL.
func TestInteractiveRemembersTheAPIAddress(t *testing.T) {
	t.Setenv("STELLAR_API_URL", "")
	t.Setenv(bearerTokenEnv, "")
	credsPath := filepath.Join(t.TempDir(), "credentials.json")
	require.NoError(t, auth.Save(credsPath, &auth.Credentials{
		ClientID: "client", ClientSecret: "secret", TokenEndpoint: "https://auth.example.com/oauth2/token",
	}))
	t.Setenv(envCredentialsPath, credsPath)

	cmd := parsedRoot(t)
	p := &scriptedPrompter{t: t, answers: []any{"https://api.example.com/", true}}
	require.NoError(t, interactiveAPIAddress(cmd, p, &bytes.Buffer{}, credsPath))

	creds, err := auth.Load(credsPath)
	require.NoError(t, err)
	assert.Equal(t, "https://api.example.com", creds.APIURL)

	addr, err := resolveAPIBaseURL(parsedRoot(t))
	require.NoError(t, err)
	assert.Equal(t, "https://api.example.com", addr)

	// The environment still wins.
	t.Setenv("STELLAR_API_URL", "https://api.other.example.com")
	addr, err = resolveAPIBaseURL(parsedRoot(t))
	require.NoError(t, err)
	assert.Equal(t, "https://api.other.example.com", addr)
}

// With no key activated, interactive mode activates the file the operator names.
func TestInteractiveActivatesAKey(t *testing.T) {
	t.Setenv(bearerTokenEnv, "")
	dir := t.TempDir()
	credsPath := filepath.Join(dir, "credentials.json")
	t.Setenv(envCredentialsPath, credsPath)
	keyFile := filepath.Join(dir, "key.json")
	require.NoError(t, os.WriteFile(keyFile, []byte(`{"apiKey":{"clientId":"c1"},"clientSecret":"s",`+
		`"tokenEndpoint":"https://auth.example.com/oauth2/token"}`), 0o600))

	p := &scriptedPrompter{t: t, answers: []any{keyFile}}
	out := &bytes.Buffer{}
	path, err := interactiveCredentials(parsedRoot(t), p, out)
	require.NoError(t, err)
	assert.Equal(t, credsPath, path)
	creds, err := auth.Load(credsPath)
	require.NoError(t, err)
	assert.Equal(t, "c1", creds.ClientID)
	assert.Contains(t, out.String(), "API key activated")
}

// --- prompt widgets ---------------------------------------------------------------

func press(m tea.Model, keys ...tea.KeyPressMsg) tea.Model {
	for _, k := range keys {
		m, _ = m.Update(k)
	}
	return m
}

func typed(s string) []tea.KeyPressMsg {
	var out []tea.KeyPressMsg
	for _, r := range s {
		out = append(out, tea.KeyPressMsg{Code: r, Text: string(r)})
	}
	return out
}

var (
	keyEnter = tea.KeyPressMsg{Code: tea.KeyEnter}
	keyDown  = tea.KeyPressMsg{Code: tea.KeyDown}
	keyEsc   = tea.KeyPressMsg{Code: tea.KeyEscape}
	keySpace = tea.KeyPressMsg{Code: tea.KeySpace, Text: " "}
)

func TestSelectModel(t *testing.T) {
	m := press(newSelectModel("t", []string{"London", "Haneda", "Los Angeles"}, 0), keyDown, keyEnter).(*selectModel)
	assert.Equal(t, 1, m.chosen)

	// Typing filters; Enter picks the first match.
	m = press(newSelectModel("t", []string{"London", "Haneda", "Los Angeles"}, 0), append(typed("los"), keyEnter)...).(*selectModel)
	assert.Equal(t, 2, m.chosen)

	m = press(newSelectModel("t", []string{"a"}, 0), keyEsc).(*selectModel)
	assert.True(t, m.canceled)
}

func TestMultiSelectModel(t *testing.T) {
	m := press(newMultiSelectModel("t", []string{"a", "b"}, []bool{true, true}), keyDown, keySpace, keyEnter).(*multiSelectModel)
	assert.Equal(t, []bool{true, false}, m.selected)
}

func TestInputModelValidates(t *testing.T) {
	m := newInputModel("t", "", "", func(s string) error {
		if s != "ok" {
			return errors.New("say ok")
		}
		return nil
	})
	press(m, append(typed("no"), keyEnter)...)
	assert.False(t, m.done)
	assert.EqualError(t, m.err, "say ok")
	press(m, tea.KeyPressMsg{Code: tea.KeyBackspace}, tea.KeyPressMsg{Code: tea.KeyBackspace})
	press(m, append(typed("ok"), keyEnter)...)
	assert.True(t, m.done)
	assert.Equal(t, "ok", string(m.value))
}

func TestConfirmModel(t *testing.T) {
	m := press(&confirmModel{yes: true}, typed("n")...).(*confirmModel)
	assert.False(t, m.yes)
	assert.True(t, m.done)
	m = press(&confirmModel{yes: false}, tea.KeyPressMsg{Code: tea.KeyRight}, keyEnter).(*confirmModel)
	assert.True(t, m.yes)
}

func TestQuoteArgs(t *testing.T) {
	assert.Equal(t, "--dest 'pass data'", strings.Join(quoteArgs([]string{"--dest", "pass data"}), " "))
}

func radioConfig(framing string, bps float64) *apiclient.RadioConfig {
	return &apiclient.RadioConfig{Framing: framing, Bitrate: &bps}
}

func threeChannels() []apiclient.PassChannel {
	return []apiclient.PassChannel{
		{ID: "11111111-up", Uplink: radioConfig("", 9600)},
		{ID: "22222222-lr", Downlink: radioConfig("BITSTREAM", 19660800)},
		{ID: "33333333-hr", Downlink: radioConfig("BITSTREAM", 251658240)},
	}
}

// Choosing channels narrows the stream; the checklist explains a choice that
// cannot work and asks again.
func TestInteractiveChoosesChannels(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		passes:     []apiclient.Pass{pass("p1", interactiveNow.Add(time.Hour), "PENDING")},
		channels:   threeChannels(),
	}
	g, p, streams, out := newTestInteractive(t, api,
		interactiveStreamNext,
		[]bool{true, true, true, true},
		1,                          // satellite commands
		[]bool{false, true, false}, // low-rate downlink only: no uplink
		false,                      // do not keep it
		[]bool{true, true, false},  // uplink + low-rate downlink
		defaultStreamDest,
		interactiveQuit,
	)
	require.NoError(t, g.run(t.Context()))
	require.Len(t, *streams, 1)
	assert.Equal(t, []string{
		"satellite", "open-stream", "--pass-id", "p1", "--channels", "11111111-up,22222222-lr",
		"--disable-config-requests", "--interactive",
	}, (*streams)[0])
	assert.Contains(t, out.String(), "Commands go out on an uplink channel, and none is selected.")
	assert.Contains(t, strings.Join(p.asked, "\n"), "Which channels?")
}

// Keeping every channel adds no --channels flag.
func TestInteractiveAllChannelsMeansNoFlag(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites: twoSatellites()[:1],
		passes:     []apiclient.Pass{pass("p1", interactiveNow.Add(time.Hour), "PENDING")},
		channels:   threeChannels(),
	}
	g, _, streams, _ := newTestInteractive(t, api,
		interactiveStreamNext, []bool{true, true, true, true}, 0, []bool{true, true, true}, defaultStreamDest, interactiveQuit)
	require.NoError(t, g.run(t.Context()))
	assert.NotContains(t, (*streams)[0], "--channels")
}

func TestChannelLabel(t *testing.T) {
	c := threeChannels()
	assert.Equal(t, "11111111  uplink 9.6 kbps", channelLabel(c[0]))
	assert.Equal(t, "22222222  downlink BITSTREAM 19.7 Mbps", channelLabel(c[1]))
	assert.Equal(t, "33333333  downlink BITSTREAM 252 Mbps", channelLabel(c[2]))
}
