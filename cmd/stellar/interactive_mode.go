package main

// `stellar interactive`: the CLI's standard workflows as a guided, menu-driven
// session for operators who would rather not assemble flags: stream the next
// pass, see upcoming passes, find and book a pass, open a stream with the right
// options, and upload orbit data in any supported format. Each step prints the equivalent command so the operator can
// script it next time.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// interactiveAPI is the part of the StellarStation API interactive mode uses.
type interactiveAPI interface {
	ListSatellites(ctx context.Context) ([]apiclient.Satellite, error)
	ListPasses(ctx context.Context, opts apiclient.ListPassesOpts) ([]apiclient.Pass, error)
	ListVisibilities(ctx context.Context, opts apiclient.ListVisibilitiesOpts) ([]apiclient.Visibility, error)
	ListConfigurations(ctx context.Context, satelliteID, groundStationID string) ([]apiclient.ExecutionConfig, error)
	CreatePass(ctx context.Context, req apiclient.CreatePassRequest) (*apiclient.Pass, error)
	CancelPass(ctx context.Context, passID string) (*apiclient.Pass, error)
	GetPassChannels(ctx context.Context, passID string) ([]apiclient.PassChannel, error)
	UpdatePass(ctx context.Context, passID string, req apiclient.UpdatePassRequest) (*apiclient.Pass, error)
	GetOrbitParameters(ctx context.Context, satelliteID string) (*apiclient.OrbitParameters, error)
	UpdateOrbitParameters(
		ctx context.Context, satelliteID string, req apiclient.UpdateOrbitParametersRequest,
	) (*apiclient.OrbitParameters, error)
	ValidateOrbitData(
		ctx context.Context, satelliteID string, req apiclient.ValidateOrbitDataRequest,
	) (*apiclient.OrbitValidationResult, error)
	CreateOrbitData(ctx context.Context, satelliteID string, req apiclient.CreateOrbitDataRequest) (*apiclient.OrbitData, error)
}

// interactiveLookahead is how far ahead interactive mode looks for passes and windows.
const interactiveLookahead = 7 * 24 * time.Hour

// interactiveMode runs the menu-driven workflows.
type interactiveMode struct {
	p   prompter
	api interactiveAPI
	out io.Writer
	now func() time.Time
	// openStream runs `stellar <args...>` (an open-stream command line).
	openStream func(args []string) error
	// extraArgs are global flags to repeat on commands interactive mode runs or
	// suggests (e.g. --credentials when the operator chose a file).
	extraArgs []string

	satellites []apiclient.Satellite
}

func (g *interactiveMode) sayf(format string, a ...any) {
	fmt.Fprintf(g.out, format+"\n", a...)
}

// Menu entries, in order.
const (
	interactiveStreamNext = iota
	interactiveUpcoming
	interactiveBook
	interactiveStreamByID
	interactiveOrbitData
	interactiveQuit
)

var interactiveMenu = []string{
	"Stream the next pass",
	"See upcoming passes",
	"Find and book a pass",
	"Open a stream for a pass ID",
	"Update a satellite's orbit data (TLE, OMM, Keplerian elements or OEM)",
	"Quit",
}

// run shows the main menu until the operator quits.
func (g *interactiveMode) run(ctx context.Context) error {
	sats, err := g.api.ListSatellites(ctx)
	if err != nil {
		return fmt.Errorf("could not list your satellites: %w", err)
	}
	if len(sats) == 0 {
		return errors.New("this API key can see no satellites; ask your organization's admin to grant access")
	}
	sort.Slice(sats, func(i, j int) bool { return satelliteLabel(sats[i]) < satelliteLabel(sats[j]) })
	g.satellites = sats
	g.sayf("You can operate %d satellite(s).", len(sats))

	for {
		g.sayf("")
		choice, err := g.p.Select("What would you like to do?", interactiveMenu, 0)
		if errors.Is(err, errInteractiveCanceled) || (err == nil && choice == interactiveQuit) {
			return nil
		}
		if err != nil {
			return err
		}
		var stepErr error
		switch choice {
		case interactiveStreamNext:
			stepErr = g.streamNextPass(ctx)
		case interactiveUpcoming:
			stepErr = g.upcomingPasses(ctx)
		case interactiveBook:
			stepErr = g.bookPass(ctx)
		case interactiveStreamByID:
			stepErr = g.streamPassByID(ctx)
		case interactiveOrbitData:
			stepErr = g.updateOrbitData(ctx)
		}
		switch {
		case errors.Is(stepErr, errInteractiveCanceled):
			g.sayf("Back to the menu.")
		case stepErr != nil:
			g.sayf("%s %v", uiErrorPrefix, stepErr)
		}
	}
}

const uiErrorPrefix = "Problem:"

// --- choosing ------------------------------------------------------------------

func satelliteLabel(s apiclient.Satellite) string {
	name := s.DisplayName
	if name == "" {
		name = s.ID
	}
	return name
}

// chooseSatellite picks the satellite to work with; with allowAll, "" means all.
func (g *interactiveMode) chooseSatellite(allowAll bool) (string, error) {
	if len(g.satellites) == 1 {
		g.sayf("Satellite: %s", satelliteLabel(g.satellites[0]))
		return g.satellites[0].ID, nil
	}
	var options []string
	if allowAll {
		options = append(options, "All satellites")
	}
	for _, s := range g.satellites {
		options = append(options, fmt.Sprintf("%s  (%s)", satelliteLabel(s), shortID(s.ID)))
	}
	i, err := g.p.Select("Which satellite?", options, 0)
	if err != nil {
		return "", err
	}
	if allowAll {
		if i == 0 {
			return "", nil
		}
		i--
	}
	return g.satellites[i].ID, nil
}

// formatWindow writes a time window as "Wed 12 Nov 02:14–02:23 UTC (9 min, in 3h 5m)".
func formatWindow(start, stop, now time.Time) string {
	start, stop = start.UTC(), stop.UTC()
	window := fmt.Sprintf("%s–%s UTC", start.Format("Mon 2 Jan 15:04"), stop.Format("15:04"))
	if start.YearDay() != stop.YearDay() {
		window = fmt.Sprintf("%s – %s UTC", start.Format("Mon 2 Jan 15:04"), stop.Format("Mon 2 Jan 15:04"))
	}
	length := stop.Sub(start).Round(time.Minute)
	return fmt.Sprintf("%s (%s, %s)", window, humanDuration(length), relativeTo(start, stop, now))
}

func relativeTo(start, stop, now time.Time) string {
	switch {
	case now.Before(start):
		return "in " + humanDuration(start.Sub(now).Round(time.Minute))
	case now.Before(stop):
		return "happening now"
	default:
		return humanDuration(now.Sub(stop).Round(time.Minute)) + " ago"
	}
}

func humanDuration(d time.Duration) string {
	if d < time.Minute {
		return "under a minute"
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	switch {
	case h >= 24:
		return fmt.Sprintf("%dd %dh", h/24, h%24)
	case h > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	default:
		return fmt.Sprintf("%d min", m)
	}
}

// --- passes ----------------------------------------------------------------------

// interactivePassWindow is when the pass's contact runs: its scheduled window, else its
// booking.
func interactivePassWindow(p apiclient.Pass) (time.Time, time.Time, bool) {
	switch {
	case p.Scheduled != nil:
		return p.Scheduled.Start, p.Scheduled.Stop, true
	case p.Booking != nil:
		return p.Booking.Start, p.Booking.Stop, true
	default:
		return time.Time{}, time.Time{}, false
	}
}

func passStatus(p apiclient.Pass) string {
	if p.Execution != nil && p.Execution.Status != "" {
		return p.Execution.Status
	}
	return "PENDING"
}

// upcoming returns the satellite's passes that have not ended, soonest first.
// An empty satelliteID means every satellite.
func (g *interactiveMode) upcoming(ctx context.Context, satelliteID string) ([]apiclient.Pass, error) {
	now := g.now()
	start, stop := now.Add(-24*time.Hour), now.Add(interactiveLookahead)
	opts := apiclient.ListPassesOpts{Start: &start, Stop: &stop}
	if satelliteID != "" {
		opts.SatelliteIDs = []string{satelliteID}
	} else {
		for _, s := range g.satellites {
			opts.SatelliteIDs = append(opts.SatelliteIDs, s.ID)
		}
	}
	passes, err := g.api.ListPasses(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("could not list passes: %w", err)
	}
	var out []apiclient.Pass
	for _, p := range passes {
		_, end, ok := interactivePassWindow(p)
		status := strings.ToUpper(passStatus(p))
		if !ok || !end.After(now) || strings.Contains(status, "CANCEL") || status == "FAILED" {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		a, _, _ := interactivePassWindow(out[i])
		b, _, _ := interactivePassWindow(out[j])
		return a.Before(b)
	})
	return out, nil
}

func (g *interactiveMode) passLabel(p apiclient.Pass) string {
	start, stop, _ := interactivePassWindow(p)
	sat := p.Satellite.Name
	if sat == "" {
		sat = shortID(p.Satellite.ID)
	}
	gs := p.GroundStation.Name
	if gs == "" {
		gs = shortID(p.GroundStation.ID)
	}
	return fmt.Sprintf("%s · %s at %s · %s", formatWindow(start, stop, g.now()), sat, gs, passStatus(p))
}

func (g *interactiveMode) streamNextPass(ctx context.Context) error {
	satID, err := g.chooseSatellite(true)
	if err != nil {
		return err
	}
	passes, err := g.upcoming(ctx, satID)
	if err != nil {
		return err
	}
	if len(passes) == 0 {
		g.sayf("No passes booked in the next 7 days.")
		book, err := g.p.Confirm("Find and book one now?", true)
		if err != nil || !book {
			return err
		}
		return g.bookPass(ctx)
	}
	next := passes[0]
	g.sayf("Next pass: %s", g.passLabel(next))
	g.sayf("  pass ID %s, configuration %s", next.ID, next.ExecutionConfigName)
	return g.streamPass(ctx, next.ID)
}

func (g *interactiveMode) upcomingPasses(ctx context.Context) error {
	satID, err := g.chooseSatellite(true)
	if err != nil {
		return err
	}
	passes, err := g.upcoming(ctx, satID)
	if err != nil {
		return err
	}
	if len(passes) == 0 {
		g.sayf("No passes booked in the next 7 days. Choose \"Find and book a pass\" to book one.")
		return nil
	}
	labels := make([]string, len(passes))
	for i, p := range passes {
		labels[i] = g.passLabel(p)
	}
	i, err := g.p.Select(fmt.Sprintf("%d upcoming pass(es). Choose one:", len(passes)), labels, 0)
	if err != nil {
		return err
	}
	pass := passes[i]
	g.sayf("Pass %s, configuration %s", pass.ID, pass.ExecutionConfigName)
	action, err := g.p.Select("What would you like to do with it?",
		[]string{"Open a stream for it", "Change its times or configuration", "Cancel this pass", "Back"}, 0)
	if err != nil {
		return err
	}
	switch action {
	case 0:
		return g.streamPass(ctx, pass.ID)
	case 1:
		return g.changePass(ctx, pass)
	case 2:
		sure, err := g.p.Confirm("Cancel this pass? This cannot be undone.", false)
		if err != nil || !sure {
			return err
		}
		if _, err := g.api.CancelPass(ctx, pass.ID); err != nil {
			return fmt.Errorf("could not cancel the pass: %w", err)
		}
		g.sayf("Pass %s canceled.", pass.ID)
		g.sayf("  Next time: stellar satellite cancel-pass %s", pass.ID)
	}
	return nil
}

// changePass moves a booked pass to new times inside its visibility, switches
// its configuration, or both.
func (g *interactiveMode) changePass(ctx context.Context, pass apiclient.Pass) error {
	what, err := g.p.Select("What should change?",
		[]string{"The start and end times", "The configuration", "Both"}, 0)
	if err != nil {
		return err
	}
	var req apiclient.UpdatePassRequest
	var summary []string
	now := g.now()
	if what == 0 || what == 2 {
		w := bookingWindow{start: now, stop: now.Add(interactiveLookahead)}
		if pass.Visibility != nil {
			w.start, w.stop = pass.Visibility.Start, pass.Visibility.Stop
			g.sayf("The satellite is visible %s.", formatWindow(w.start, w.stop, now))
		}
		if pass.Scheduled != nil {
			g.sayf("Now scheduled %s.", formatWindow(pass.Scheduled.Start, pass.Scheduled.Stop, now))
		}
		current := w
		if pass.Scheduled != nil {
			current.start, current.stop = pass.Scheduled.Start, pass.Scheduled.Stop
		}
		start, end, err := g.chooseTimesWithin(w, current)
		if err != nil {
			return err
		}
		req.Scheduled = &apiclient.Interval{Start: start, Stop: end}
		summary = append(summary, "times "+formatWindow(start, end, now))
	}
	if what == 1 || what == 2 {
		config, err := g.chooseConfiguration(ctx, pass.Satellite.ID, pass.GroundStation.ID, pass.ExecutionConfigID)
		if err != nil {
			return err
		}
		if config.ID != pass.ExecutionConfigID {
			req.ExecutionConfigID = &config.ID
			summary = append(summary, "configuration "+configName(config))
		}
	}
	if len(summary) == 0 {
		g.sayf("Nothing changed.")
		return nil
	}
	g.sayf("Changing pass %s: %s.", pass.ID, strings.Join(summary, ", "))
	ok, err := g.p.Confirm("Save the change?", true)
	if err != nil || !ok {
		return err
	}
	if _, err := g.api.UpdatePass(ctx, pass.ID, req); err != nil {
		return fmt.Errorf("the pass was not changed: %w", err)
	}
	g.sayf("Pass %s changed.", pass.ID)
	next := "stellar satellite update-pass " + pass.ID
	if req.Scheduled != nil {
		next += fmt.Sprintf(" --scheduled-start %s --scheduled-stop %s",
			req.Scheduled.Start.UTC().Format(time.RFC3339), req.Scheduled.Stop.UTC().Format(time.RFC3339))
	}
	if req.ExecutionConfigID != nil {
		next += " --execution-config-id " + *req.ExecutionConfigID
	}
	g.sayf("  Next time: %s", next)
	return nil
}

func configName(c apiclient.ExecutionConfig) string {
	if c.DisplayName != "" {
		return c.DisplayName
	}
	return c.ID
}

// chooseConfiguration asks which of the satellite's configurations for the
// ground station to use, starting on currentID. With only one it is chosen
// without asking.
func (g *interactiveMode) chooseConfiguration(
	ctx context.Context, satID, gsID, currentID string,
) (apiclient.ExecutionConfig, error) {
	configs, err := g.api.ListConfigurations(ctx, satID, gsID)
	if err != nil {
		return apiclient.ExecutionConfig{}, fmt.Errorf("could not list configurations: %w", err)
	}
	if len(configs) == 0 {
		return apiclient.ExecutionConfig{}, errors.New(
			"this satellite has no configuration for that ground station; ask your admin to add one")
	}
	if len(configs) == 1 {
		g.sayf("Configuration: %s", configName(configs[0]))
		return configs[0], nil
	}
	names := make([]string, len(configs))
	selected := 0
	for j, c := range configs {
		names[j] = configName(c)
		if c.ID == currentID {
			names[j] += " (current)"
			selected = j
		}
	}
	k, err := g.p.Select("Which configuration?", names, selected)
	if err != nil {
		return apiclient.ExecutionConfig{}, err
	}
	return configs[k], nil
}

func (g *interactiveMode) streamPassByID(ctx context.Context) error {
	id, err := g.p.Input("Pass ID", "the pass's ID, e.g. from the console", "", func(s string) error {
		if len(strings.TrimSpace(s)) < 8 {
			return errors.New("enter the full pass ID")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return g.streamPass(ctx, id)
}

// --- booking ---------------------------------------------------------------------

// bookingWindow is one free window of one visibility.
type bookingWindow struct {
	vis   apiclient.Visibility
	start time.Time
	stop  time.Time
}

func (g *interactiveMode) bookPass(ctx context.Context) error {
	satID, err := g.chooseSatellite(false)
	if err != nil {
		return err
	}
	now := g.now()
	stop := now.Add(interactiveLookahead)
	g.sayf("Looking for windows in the next 7 days...")
	vis, err := g.api.ListVisibilities(ctx, apiclient.ListVisibilitiesOpts{
		SatelliteIDs: []string{satID}, Start: &now, Stop: &stop,
	})
	if err != nil {
		return fmt.Errorf("could not list pass opportunities: %w", err)
	}
	var windows []bookingWindow
	for _, v := range vis {
		for _, a := range v.Availability {
			if a.Stop.After(now) && a.Stop.After(a.Start) {
				windows = append(windows, bookingWindow{vis: v, start: a.Start, stop: a.Stop})
			}
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].start.Before(windows[j].start) })
	if len(windows) == 0 {
		g.sayf("No free windows in the next 7 days for this satellite.")
		return nil
	}
	labels := make([]string, len(windows))
	for i, w := range windows {
		gs := w.vis.GroundStation.Name
		if gs == "" {
			gs = shortID(w.vis.GroundStation.ID)
		}
		labels[i] = fmt.Sprintf("%s · %s · max elevation %.0f°",
			formatWindow(w.start, w.stop, now), gs, w.vis.MaxElevationDegrees)
	}
	i, err := g.p.Select(fmt.Sprintf("%d free window(s). Choose one (type a station name to filter):", len(windows)), labels, 0)
	if err != nil {
		return err
	}
	w := windows[i]

	config, err := g.chooseConfiguration(ctx, satID, w.vis.GroundStation.ID, "")
	if err != nil {
		return err
	}

	start, end := w.start, w.stop
	whole := fmt.Sprintf("The whole window (%s)", formatWindow(w.start, w.stop, now))
	how, err := g.p.Select("How long?", []string{whole, "Choose the start and end"}, 0)
	if err != nil {
		return err
	}
	if how == 1 {
		if start, end, err = g.chooseTimesWithin(w, w); err != nil {
			return err
		}
	}

	g.sayf("Booking %s at %s, %s, configuration %s.",
		satelliteLabelByID(g.satellites, satID), w.vis.GroundStation.Name, formatWindow(start, end, now), configName(config))
	ok, err := g.p.Confirm("Book this pass?", true)
	if err != nil || !ok {
		return err
	}
	pass, err := g.api.CreatePass(ctx, apiclient.CreatePassRequest{
		SatelliteID:       satID,
		GroundStationID:   w.vis.GroundStation.ID,
		ExecutionConfigID: config.ID,
		Booking:           apiclient.Interval{Start: start, Stop: end},
		Scheduled:         apiclient.Interval{Start: start, Stop: end},
	})
	if err != nil {
		return fmt.Errorf("the pass was not booked: %w", err)
	}
	g.sayf("Booked pass %s.", pass.ID)
	g.sayf("  Next time: stellar satellite reserve-pass --satellite-id %s --ground-station-id %s "+
		"--execution-config-id %s --booking-start %s --booking-stop %s",
		satID, w.vis.GroundStation.ID, config.ID, start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
	stream, err := g.p.Confirm("Open a stream for it now? (it waits for the pass to start)", false)
	if err != nil || !stream {
		return err
	}
	return g.streamPass(ctx, pass.ID)
}

func satelliteLabelByID(sats []apiclient.Satellite, id string) string {
	for _, s := range sats {
		if s.ID == id {
			return satelliteLabel(s)
		}
	}
	return id
}

// timeInWindow refuses a time outside [start, stop].
func timeInWindow(start, stop, t time.Time) error {
	if t.Before(start) || t.After(stop) {
		return fmt.Errorf("must be between %s and %s",
			start.UTC().Format(time.RFC3339), stop.UTC().Format(time.RFC3339))
	}
	return nil
}

// chooseTimesWithin asks for a start and end inside the window w, offering
// the current times as the defaults.
func (g *interactiveMode) chooseTimesWithin(w, current bookingWindow) (time.Time, time.Time, error) {
	parse := func(s string) (time.Time, error) {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(s))
		if err != nil {
			return time.Time{}, errors.New("use the format 2026-01-15T10:00:00Z")
		}
		return t, timeInWindow(w.start, w.stop, t)
	}
	startText, err := g.p.Input("Start (UTC)", "2026-01-15T10:00:00Z",
		current.start.UTC().Format(time.RFC3339), func(s string) error { _, err := parse(s); return err })
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, _ := parse(startText)
	endText, err := g.p.Input("End (UTC)", "2026-01-15T10:10:00Z",
		current.stop.UTC().Format(time.RFC3339), func(s string) error {
			t, err := parse(s)
			if err == nil && !t.After(start) {
				return errors.New("must be after the start")
			}
			return err
		})
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, _ := parse(endText)
	return start, end, nil
}

// --- streaming -------------------------------------------------------------------

var interactiveReceive = []string{"Telemetry", "Monitoring", "Config state", "Events"}

var interactiveCommanding = []string{
	"Receive only",
	"Send satellite commands",
	"Send ground station config requests",
	"Send both",
}

// streamArgs builds the open-stream command line for the choices. channels
// empty means every channel of the pass.
func streamArgs(passID string, receive []bool, commanding int, channels []string, dest string) []string {
	args := []string{"satellite", "open-stream", "--pass-id", passID}
	if len(channels) > 0 {
		args = append(args, "--channels", strings.Join(channels, ","))
	}
	disable := []string{"--disable-downlink", "--disable-monitoring", "--disable-config-state", "--disable-event"}
	for i, on := range receive {
		if !on {
			args = append(args, disable[i])
		}
	}
	satCommands := commanding == 1 || commanding == 3
	configRequests := commanding == 2 || commanding == 3
	if !satCommands {
		args = append(args, "--disable-uplink")
	}
	if !configRequests {
		args = append(args, "--disable-config-requests")
	}
	if satCommands || configRequests {
		args = append(args, "--interactive")
	}
	if dest != "" && dest != defaultStreamDest {
		args = append(args, "--dest", dest)
	}
	return args
}

// defaultStreamDest is open-stream's default --dest.
const defaultStreamDest = "./downlink"

func (g *interactiveMode) streamPass(ctx context.Context, passID string) error {
	receive, err := g.p.MultiSelect("What should the stream receive?", interactiveReceive, []bool{true, true, true, true})
	if err != nil {
		return err
	}
	commanding, err := g.p.Select("Will you send commands during the pass?", interactiveCommanding, 0)
	if err != nil {
		return err
	}
	channels, err := g.chooseChannels(ctx, passID, receive[0], commanding)
	if err != nil {
		return err
	}
	dest, err := g.p.Input("Where should received data be saved?", defaultStreamDest, defaultStreamDest, nil)
	if err != nil {
		return err
	}
	args := append(streamArgs(passID, receive, commanding, channels, dest), g.extraArgs...)
	g.sayf("")
	g.sayf("Opening the stream. Next time you can run this directly:")
	g.sayf("  stellar %s", strings.Join(quoteArgs(args), " "))
	if commanding > 0 {
		g.sayf("Type commands when the pass starts: 'sat <hex>' for a satellite command, 'gs <json>' for a config request.")
	}
	g.sayf("Press Ctrl-C to stop the stream.")
	g.sayf("")
	return g.openStream(args)
}

func quoteArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if strings.ContainsAny(a, " \t'\"") {
			a = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
		}
		out[i] = a
	}
	return out
}

// channelLabel describes a channel for the checklist: its short ID, direction
// and radio settings.
func channelLabel(c apiclient.PassChannel) string {
	var parts []string
	describe := func(direction string, r *apiclient.RadioConfig) {
		desc := []string{direction}
		if r.Framing != "" {
			desc = append(desc, r.Framing)
		}
		if r.Bitrate != nil && *r.Bitrate > 0 {
			desc = append(desc, humanBitrate(*r.Bitrate))
		}
		parts = append(parts, strings.Join(desc, " "))
	}
	if c.Downlink != nil {
		describe("downlink", c.Downlink)
	}
	if c.Uplink != nil {
		describe("uplink", c.Uplink)
	}
	return fmt.Sprintf("%s  %s", shortID(c.ID), strings.Join(parts, " + "))
}

func humanBitrate(bps float64) string {
	switch {
	case bps >= 1e9:
		return fmt.Sprintf("%.3g Gbps", bps/1e9)
	case bps >= 1e6:
		return fmt.Sprintf("%.3g Mbps", bps/1e6)
	case bps >= 1e3:
		return fmt.Sprintf("%.3g kbps", bps/1e3)
	default:
		return fmt.Sprintf("%.0f bps", bps)
	}
}

// chooseChannels asks which of the pass's channels the stream covers and
// returns them, or nil for all of them. It warns when the choice cannot carry
// what was asked for: commands need an uplink channel, telemetry a downlink.
func (g *interactiveMode) chooseChannels(
	ctx context.Context, passID string, telemetry bool, commanding int,
) ([]string, error) {
	channels, err := g.api.GetPassChannels(ctx, passID)
	if err != nil || len(channels) == 0 {
		g.sayf("(Could not list the pass's channels, so the stream covers all of them.)")
		return nil, nil //nolint:nilerr // Falling back to every channel is the safe default.
	}
	if len(channels) == 1 {
		g.sayf("Channel: %s", channelLabel(channels[0]))
		return nil, nil
	}
	labels := make([]string, len(channels))
	all := make([]bool, len(channels))
	for i, c := range channels {
		labels[i] = channelLabel(c)
		all[i] = true
	}
	selected := all
	for {
		selected, err = g.p.MultiSelect("Which channels?", labels, selected)
		if err != nil {
			return nil, err
		}
		var ids []string
		uplink, downlink := false, false
		for i, on := range selected {
			if !on {
				continue
			}
			ids = append(ids, channels[i].ID)
			uplink = uplink || channels[i].Uplink != nil
			downlink = downlink || channels[i].Downlink != nil
		}
		var problem string
		switch {
		case len(ids) == 0:
			problem = "Choose at least one channel."
		case commanding > 0 && !uplink:
			problem = "Commands go out on an uplink channel, and none is selected."
		case telemetry && !downlink:
			problem = "Telemetry arrives on a downlink channel, and none is selected."
		}
		if problem == "" {
			if len(ids) == len(channels) {
				return nil, nil
			}
			return ids, nil
		}
		g.sayf("%s", problem)
		if len(ids) == 0 {
			continue
		}
		keep, err := g.p.Confirm("Use these channels anyway?", false)
		if err != nil {
			return nil, err
		}
		if keep {
			return ids, nil
		}
	}
}
