package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// channelTarget holds the MQTT topics for sending commands to a specific channel.
type channelTarget struct {
	ChannelID          string
	UplinkTopic        string
	ConfigRequestTopic string
}

// commandableChannelSet returns a predicate reporting whether a channel may be a
// command target. Commands are only valid on uplink-direction channels, so when
// the authorizer supplies per-channel direction metadata the predicate accepts
// only those. If no direction metadata is present (older authorizer), it accepts
// every channel so behavior is unchanged.
func commandableChannelSet(creds *AuthorizerCredentials) func(string) bool {
	uplink := map[string]bool{}
	haveDirection := false
	if creds != nil {
		for _, ch := range creds.Channels {
			if ch.Direction != "" {
				haveDirection = true
			}
			if strings.EqualFold(ch.Direction, directionUplink) {
				uplink[ch.ChannelID] = true
			}
		}
	}
	return func(channelID string) bool {
		if !haveDirection {
			return true
		}
		return uplink[channelID]
	}
}

// firstCommandable returns the first config on a commandable channel, or the
// zero config if there is none.
func firstCommandable(creds *AuthorizerCredentials, configs []CommandConfig) CommandConfig {
	commandable := commandableChannelSet(creds)
	for _, c := range configs {
		if commandable(ExtractChannelIDFromTopic(c.PublishTopic)) {
			return c
		}
	}
	return CommandConfig{}
}

// resolveAllCommandTargets extracts per-channel command topics from authorizer credentials.
func resolveAllCommandTargets(cfg Config) ([]channelTarget, error) {
	if cfg.AuthorizerCreds == nil {
		return nil, errors.New("no authorizer credentials available")
	}

	streams := cfg.AuthorizerCreds.Streams
	targetMap := make(map[string]*channelTarget)

	// The authorizer returns a command topic for every channel, but only
	// uplink-direction channels are valid command targets. Restrict targets to
	// them (when the direction metadata is available) so that a pass with a single
	// uplink channel exposes exactly one command target, letting the interactive
	// prompt accept "sat <hex>" / "gs <json>" without an explicit channel ID.
	commandable := commandableChannelSet(cfg.AuthorizerCreds)

	for _, ul := range streams.Uplink {
		chID := ExtractChannelIDFromTopic(ul.PublishTopic)
		if chID == "" || !commandable(chID) {
			continue
		}
		t, ok := targetMap[chID]
		if !ok {
			t = &channelTarget{ChannelID: chID}
			targetMap[chID] = t
		}
		t.UplinkTopic = ul.PublishTopic
	}

	for _, cr := range streams.ConfigRequest {
		chID := ExtractChannelIDFromTopic(cr.PublishTopic)
		if chID == "" || !commandable(chID) {
			continue
		}
		t, ok := targetMap[chID]
		if !ok {
			t = &channelTarget{ChannelID: chID}
			targetMap[chID] = t
		}
		t.ConfigRequestTopic = cr.PublishTopic
	}

	targets := make([]channelTarget, 0, len(targetMap))
	for _, t := range targetMap {
		targets = append(targets, *t)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ChannelID < targets[j].ChannelID
	})

	return targets, nil
}

func validateCommandFeatures(
	cfg Config,
	sendSatCommand, sendSatCommands, sendGsConfig string,
	interactive bool,
) error {
	if sendSatCommand != "" || sendSatCommands != "" {
		if !cfg.EnableUplink {
			return errors.New("uplink was disabled (-disable-uplink); omit it for satellite commands")
		}
	}
	if sendGsConfig != "" {
		if !cfg.EnableConfigRequests {
			return errors.New("config_request was disabled (-disable-config-requests); omit it for GS config")
		}
	}
	if interactive {
		if !cfg.EnableUplink && !cfg.EnableConfigRequests {
			return errors.New(
				"interactive mode needs uplink and/or config_request; remove -disable-uplink and/or -disable-config-requests",
			)
		}
	}
	return nil
}

func setupMQTTClient(
	ctx context.Context,
	cfg Config,
	sharedClient mqtt.Client,
) (mqtt.Client, error) {
	if sharedClient != nil {
		return sharedClient, nil
	}
	connCfg, err := buildMQTTConnectionConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	connectConfirmed := make(chan struct{}, 1)
	clientFactory := func() (mqtt.Client, string, error) {
		// Always read latest credentials from store (in case they were refreshed via cert rotation).
		var creds *AuthorizerCredentials
		switch {
		case cfg.CredStore != nil:
			creds = cfg.CredStore.load()
		case cfg.AuthorizerCreds != nil:
			creds = cfg.AuthorizerCreds
		default:
			return nil, "", errors.New("no credentials available")
		}

		// Re-parse the cert from the latest credentials on each attempt so that a
		// refreshed certificate (from a credential-store refresh) is always used.
		if creds.IoTCertificatePem != "" && creds.IoTPrivateKeyPem != "" {
			latestCert, err := tls.X509KeyPair(
				[]byte(creds.IoTCertificatePem),
				[]byte(creds.IoTPrivateKeyPem),
			)
			if err != nil {
				log.Printf(
					"WARNING: failed to re-parse IoT certificate from store: %v; using existing cert",
					err,
				)
			} else {
				connCfg.tlsCertificate = &latestCert
			}
		}
		opts := buildMQTTClientOptions(connCfg, connectConfirmed)
		return mqtt.NewClient(opts), connCfg.broker, nil
	}
	client, err := connectMQTTClientWithRetry(ctx, clientFactory, connectConfirmed, connCfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// subscribeCommandReplies listens on client for the acks and statuses of
// this stream's commands, so a sender sees its results without a separate
// reader connection. Replies a reader also delivers are counted once.
func subscribeCommandReplies(client mqtt.Client, cfg Config, stats *statsTracker) {
	if cfg.AuthorizerCreds == nil {
		return
	}
	topics := collapseChannelWildcards(commandReplyTopics(cfg.AuthorizerCreds.Streams))
	if len(topics) == 0 {
		return
	}
	handler := func(_ mqtt.Client, msg mqtt.Message) {
		routeCommandReply(stats, msg.Topic(), msg.Payload())
	}
	if err := subscribeMQTT(client, topics, cfg.MQTTQoS, handler); err != nil {
		uiWarnf("Command acknowledgements cannot be received on this connection; results may not be shown.")
		vlogf("command reply subscription failed: %v", err)
	}
}

func normalizeIDs(cfg Config, streamID, planID, passID string) (string, string) {
	if streamID == "" {
		// Use stream ID from authorizer (same as telemetry acks use)
		streamID = getStreamID(cfg)
		if streamID == "" {
			// Generate stream ID if authorizer doesn't provide one (shouldn't happen in normal operation)
			streamID = fmt.Sprintf("stream-%d", os.Getpid())
		}
	}
	if planID == "" {
		planID = passID
	}
	return streamID, planID
}

func parseHexCommand(hexStr string) ([]byte, error) {
	hexStr = strings.TrimSpace(hexStr)
	hexStr = strings.TrimPrefix(hexStr, "0x")
	hexStr = strings.TrimPrefix(hexStr, "0X")
	hexStr = strings.ReplaceAll(hexStr, " ", "")
	return hex.DecodeString(hexStr)
}

// commandSendFunc sends one command payload with the given index.
type commandSendFunc func(payload string, index uint32) (*sentCommand, error)

func createSendSatCommandFunc(pub *commandPublisher, topic, streamID, planID string) commandSendFunc {
	return func(cmdHex string, index uint32) (*sentCommand, error) {
		cmdBytes, err := parseHexCommand(cmdHex)
		if err != nil {
			return nil, fmt.Errorf("invalid hex command %q: %w", cmdHex, err)
		}
		// Checked before the echo below, so an oversized payload is never
		// reported as being sent.
		if err := validateCommandSize([][]byte{cmdBytes}); err != nil {
			return nil, err
		}
		//nolint:gosec // G706: cmdHex is a trusted hex-encoded command from user input
		uiDataf(kindUplink, "sending command %d: %s (%d bytes)", index, cmdHex, len(cmdBytes))
		return PublishSatCommand(pub, topic, streamID, planID, index, [][]byte{cmdBytes})
	}
}

func createSendGsConfigFunc(pub *commandPublisher, topic, streamID, planID string) commandSendFunc {
	return func(configJSON string, index uint32) (*sentCommand, error) {
		configRequest := &v1.GroundStationConfigurationRequest{}
		if configJSON == "" {
			uiDataf(kindUplink, "sending ground-station config request %d (empty)", index)
		} else {
			if err := protojson.Unmarshal([]byte(configJSON), configRequest); err != nil {
				return nil, fmt.Errorf("that config request is not valid JSON: %w", err)
			}
			//nolint:gosec // G706: configJSON is a trusted JSON config from user input
			uiDataf(kindUplink, "sending ground-station config request %d: %s", index, configJSON)
		}
		return PublishGsConfig(pub, topic, streamID, planID, index, configRequest)
	}
}

// waitingNote is shown once a one-shot send is published.
func waitingNote(sent string, mode waitMode) string {
	if mode == waitForFinal {
		return sent + " Waiting for acknowledgement and delivery status..."
	}
	return sent + " Waiting for acknowledgement..."
}

func handleOneShotSatCommand(sendSatCommand string, send commandSendFunc, mode waitMode) ([]*sentCommand, error) {
	if sendSatCommand == "" {
		return nil, nil
	}
	cmd, err := send(sendSatCommand, 1)
	if err != nil {
		return nil, err
	}
	uiOKf("%s", waitingNote("Command sent.", mode))
	return []*sentCommand{cmd}, nil
}

func handleOneShotSatCommands(sendSatCommands string, send commandSendFunc, mode waitMode) ([]*sentCommand, error) {
	if sendSatCommands == "" {
		return nil, nil
	}
	commands := strings.Split(sendSatCommands, ",")
	sent := make([]*sentCommand, 0, len(commands))
	for i, cmdHex := range commands {
		cmd, err := send(cmdHex, uint32(i+1))
		if err != nil {
			return sent, err
		}
		sent = append(sent, cmd)
		time.Sleep(interCommandDelay)
	}
	uiOKf("%s", waitingNote("All commands sent.", mode))
	return sent, nil
}

func handleOneShotGsConfig(sendGsConfig string, send commandSendFunc, mode waitMode) ([]*sentCommand, error) {
	if sendGsConfig == "" {
		return nil, nil
	}
	cmd, err := send(sendGsConfig, 1)
	if err != nil {
		return nil, err
	}
	uiOKf("%s", waitingNote("Ground-station config request sent.", mode))
	return []*sentCommand{cmd}, nil
}

// resolveCommandTopics returns the uplink and config_request publish topics
// on the first commandable channel, or an error when an enabled path has none.
func resolveCommandTopics(cfg Config) (satCommandTopic, gsConfigTopic string, err error) {
	if cfg.AuthorizerCreds != nil {
		creds := cfg.AuthorizerCreds
		satCommandTopic = firstCommandable(creds, creds.Streams.Uplink).PublishTopic
		gsConfigTopic = firstCommandable(creds, creds.Streams.ConfigRequest).PublishTopic
	}
	if cfg.EnableUplink && satCommandTopic == "" {
		return "", "", errors.New("uplink topic not provided by authorizer")
	}
	if cfg.EnableConfigRequests && gsConfigTopic == "" {
		return "", "", errors.New("config_request topic not provided by authorizer")
	}

	return satCommandTopic, gsConfigTopic, nil
}

// dispatchOneShotCommands sends the one-shot commands, the first non-empty
// of sendSatCommand, sendSatCommands and sendGsConfig, then waits for their
// results. It reports whether anything was sent.
func dispatchOneShotCommands(
	sendSatCommand, sendSatCommands, sendGsConfig string,
	sendSatCommandFunc, sendGsConfigFunc commandSendFunc,
	mode waitMode,
	await func([]*sentCommand) error,
) (bool, error) {
	var sent []*sentCommand
	var err error
	switch {
	case sendSatCommand != "":
		sent, err = handleOneShotSatCommand(sendSatCommand, sendSatCommandFunc, mode)
	case sendSatCommands != "":
		sent, err = handleOneShotSatCommands(sendSatCommands, sendSatCommandFunc, mode)
	case sendGsConfig != "":
		sent, err = handleOneShotGsConfig(sendGsConfig, sendGsConfigFunc, mode)
	default:
		return false, nil
	}
	if err != nil {
		if len(sent) > 0 {
			// Commands already published still get their results.
			_ = await(sent)
		}
		return len(sent) > 0, err
	}
	return true, await(sent)
}

// runCommandSender connects to MQTT and sends commands.
//
// Topics are extracted from authorizer credentials in cfg.AuthorizerCreds.
// If sharedClient is provided, it will be used instead of creating a new connection.
// If stats is provided, it will be used to track sent commands; otherwise a new stats tracker is created.
//
// Parameters:
//   - ctx: Context for cancellation
//   - cfg: The configuration containing authorizer credentials and feature flags
//   - sendSatCommand: Single satellite command to send (hex-encoded)
//   - sendSatCommands: Multiple satellite commands to send (comma-separated hex)
//   - sendGsConfig: Ground station configuration to send (JSON)
//   - interactive: Whether to run in interactive mode (read from stdin)
//   - streamID: The stream ID (will be normalized from authorizer if empty)
//   - planID: The plan ID (will use passID if empty)
//   - passID: The pass ID
//   - sharedClient: Optional shared MQTT client (nil to create new connection)
//   - stats: Optional stats tracker (nil to create new tracker)
//
// Returns:
//   - An error if command sending fails
func runCommandSender(
	ctx context.Context,
	cfg Config,
	sendSatCommand string,
	sendSatCommands string,
	sendGsConfig string,
	interactive bool,
	streamID string,
	planID string,
	passID string,
	sharedClient mqtt.Client,
	stats *statsTracker, // Optional: main stats tracker for diagnostics
) error {
	if err := validateCommandFeatures(cfg, sendSatCommand, sendSatCommands, sendGsConfig, interactive); err != nil {
		return err
	}

	// Commands are only valid between the booking start and stop, so resolve
	// that window before connecting. One-shot sends are checked once, here;
	// interactive sessions re-check per command, since the booking can close
	// while the session is open.
	windowPassID := passID
	if windowPassID == "" {
		windowPassID = cfg.PassID
	}
	window := resolveBookingWindow(ctx, cfg, windowPassID)
	if sendSatCommand != "" || sendSatCommands != "" || sendGsConfig != "" {
		if err := window.check(time.Now()); err != nil {
			return err
		}
	}

	client, err := setupMQTTClient(ctx, cfg, sharedClient)
	if err != nil {
		return err
	}
	if sharedClient == nil {
		defer client.Disconnect(mqttDisconnectQuiesce)
	}

	if stats == nil {
		stats = newStatsTracker(false)
	}
	subscribeCommandReplies(client, cfg, stats)
	mode := effectiveWaitMode(cfg)

	streamID, planID = normalizeIDs(cfg, streamID, planID, passID)

	satCommandTopic, gsConfigTopic, err := resolveCommandTopics(cfg)
	if err != nil {
		return err
	}

	pub := newCommandPublisher(client, cfg.MQTTQoS, stats, window)

	// Create command functions using the topics from authorizer.
	// Authorizer provides topic for the configured channel. Each channel has its own topic,
	// and indices are automatically per-channel (each topic has its own independent index sequence).
	sent, err := dispatchOneShotCommands(
		sendSatCommand,
		sendSatCommands,
		sendGsConfig,
		createSendSatCommandFunc(pub, satCommandTopic, streamID, planID),
		createSendGsConfigFunc(pub, gsConfigTopic, streamID, planID),
		mode,
		func(cmds []*sentCommand) error {
			return awaitCommands(stats, cmds, mode, cfg.WaitTimeout, pub.clock)
		},
	)
	if err != nil || sent || !interactive {
		return err
	}

	targets, terr := resolveAllCommandTargets(cfg)
	if terr != nil {
		return fmt.Errorf("resolving command targets: %w", terr)
	}

	return runInteractiveMode(ctx, targets, pub, cfg, streamID, planID, passID)
}

// exampleGsConfigJSON is a realistic ground-station radio-config payload shown
// in interactive-mode help, so the example is something a receiver can
// actually act on rather than an empty, un-illustrative "{}".
const exampleGsConfigJSON = `{"receiverConfigurationRequest":{"bitrate":9600,"modulation":"BPSK"}}`

// interactiveOut is where interactive mode writes its prompt, replies and
// errors: through the live stats panel's writer, so the once-a-second redraw
// of the panel does not paint over them.
var interactiveOut io.Writer = stderrPanelWriter{}

// stderrPanelWriter writes to the current os.Stderr through the stats panel.
type stderrPanelWriter struct{}

func (stderrPanelWriter) Write(p []byte) (int, error) {
	return statusClearingWriter{w: os.Stderr}.Write(p)
}

// runInteractiveMode handles interactive command input from stdin.
// The user must specify a channel ID for each command.
//
// Syntax:
//
//	sat <channel-id> <hex>   - Send satellite command to channel
//	gs  <channel-id> <json>  - Send ground station config to channel
//	exit/quit                - Exit interactive mode
func runInteractiveMode(
	ctx context.Context,
	targets []channelTarget,
	pub *commandPublisher,
	cfg Config,
	streamID, planID, passID string,
) error {
	s := newInteractiveSession(cfg, targets, pub, streamID, planID)
	s.printBanner(passID)

	scanner := newCommandScanner(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fmt.Fprint(interactiveOut, "> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}

		parts := strings.SplitN(line, " ", 3)
		cmdType := strings.ToLower(parts[0])

		switch cmdType {
		case "sat":
			s.sendSatCommand(parts[1:])
		case "gs":
			s.sendGsConfig(parts[1:])
		default:
			fmt.Fprintf(interactiveOut, "Unknown command %q; use 'sat', 'gs', or 'exit'\n", cmdType)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading stdin: %w", err)
	}
	return nil
}

// interactiveSession is the state shared by the interactive input loop and the
// per-command handlers it dispatches to. Grouping it keeps each handler to a
// single receiver rather than a ten-parameter signature, and keeps the input
// loop itself small enough to read at a glance.
type interactiveSession struct {
	cfg      Config
	targets  []channelTarget
	pub      *commandPublisher
	streamID string
	planID   string

	// Per-channel message indices. Each topic carries its own independent
	// sequence, so uplink and config requests are counted separately.
	satIndices map[int]uint32
	gsIndices  map[int]uint32

	// Defaults for the command types that have exactly one candidate channel,
	// which let the operator omit the channel ID.
	singleUplinkIdx int
	singleGsIdx     int
}

func newInteractiveSession(
	cfg Config,
	targets []channelTarget,
	pub *commandPublisher,
	streamID, planID string,
) *interactiveSession {
	s := &interactiveSession{
		cfg:             cfg,
		targets:         targets,
		pub:             pub,
		streamID:        streamID,
		planID:          planID,
		satIndices:      make(map[int]uint32, len(targets)),
		gsIndices:       make(map[int]uint32, len(targets)),
		singleUplinkIdx: singleTargetIndex(targets, func(t channelTarget) bool { return t.UplinkTopic != "" }),
		singleGsIdx: singleTargetIndex(targets, func(t channelTarget) bool {
			return t.ConfigRequestTopic != ""
		}),
	}
	for i := range targets {
		s.satIndices[i] = 1
		s.gsIndices[i] = 1
	}
	return s
}

// printBanner announces the session: whether commanding is currently open, the
// channels available, and the syntax for each command type.
func (s *interactiveSession) printBanner(passID string) {
	fmt.Fprintf(interactiveOut, "Interactive mode: Enter commands (Ctrl-C to exit)\n")
	// Say up front when the booking is not open, rather than letting the
	// operator discover it by having their first command rejected.
	if err := s.pub.window.check(time.Now()); err != nil {
		fmt.Fprintf(interactiveOut, "NOTE: %v\n", err)
	} else if s.pub.window.resolved() {
		fmt.Fprintf(interactiveOut, "Commanding is open until %s.\n", s.pub.window.stop.UTC().Format(timeFormat))
	} else {
		// Say so rather than leave the operator to assume commands are checked.
		fmt.Fprintf(interactiveOut, "NOTE: could not read the pass's times, so commands are sent whenever you type them\n")
	}
	s.printChannels()
	s.printCommandHelp()
	fmt.Fprintf(interactiveOut, "Stream ID: %s\n", s.streamID)
	fmt.Fprintf(interactiveOut, "Pass ID: %s\n", passID)
	fmt.Fprintf(interactiveOut, "\n")
}

func (s *interactiveSession) printChannels() {
	fmt.Fprintf(interactiveOut, "\nAvailable channels:\n")
	if len(s.targets) == 0 {
		fmt.Fprintf(interactiveOut, "  none: this stream has no channel that accepts commands\n")
		return
	}
	for _, t := range s.targets {
		var caps []string
		if t.UplinkTopic != "" {
			caps = append(caps, "uplink")
		}
		if t.ConfigRequestTopic != "" {
			caps = append(caps, "config_request")
		}
		fmt.Fprintf(interactiveOut, "  %s (%s)\n", t.ChannelID, strings.Join(caps, ", "))
	}
}

func (s *interactiveSession) printCommandHelp() {
	fmt.Fprintf(interactiveOut, "\nCommands:\n")
	exampleChID := "<channel-id>"
	if len(s.targets) > 0 {
		exampleChID = s.targets[0].ChannelID
	}
	// Only the kinds of command this stream may send are offered.
	switch {
	case !s.cfg.EnableUplink:
	case s.singleUplinkIdx >= 0:
		fmt.Fprintf(interactiveOut, "  sat <hex>                - Send satellite command (e.g., 'sat aaaabbbb')\n")
	default:
		fmt.Fprintf(
			os.Stderr,
			"  sat <channel-id> <hex>   - Send satellite command (e.g., 'sat %s aaaabbbb')\n",
			exampleChID,
		)
	}
	switch {
	case !s.cfg.EnableConfigRequests:
	case s.singleGsIdx >= 0:
		fmt.Fprintf(
			os.Stderr,
			"  gs  [json]               - Send ground station config (e.g., 'gs %s')\n",
			exampleGsConfigJSON,
		)
	default:
		fmt.Fprintf(
			os.Stderr,
			"  gs  <channel-id> <json>  - Send ground station config (e.g., 'gs %s %s')\n",
			exampleChID,
			exampleGsConfigJSON,
		)
	}
	fmt.Fprintf(interactiveOut, "  exit/quit                - Exit interactive mode\n")
}

// hasUplinkTarget reports whether any channel of this stream accepts satellite
// commands.
func (s *interactiveSession) hasUplinkTarget() bool {
	for _, t := range s.targets {
		if t.UplinkTopic != "" {
			return true
		}
	}
	return false
}

// hasConfigRequestTarget reports whether any channel of this stream accepts
// ground-station config requests.
func (s *interactiveSession) hasConfigRequestTarget() bool {
	for _, t := range s.targets {
		if t.ConfigRequestTopic != "" {
			return true
		}
	}
	return false
}

// sendSatCommand handles one "sat" line. Every rejection is reported to stderr
// and returns without transmitting, leaving the session running so the operator
// can correct the line and try again.
func (s *interactiveSession) sendSatCommand(args []string) {
	if !s.cfg.EnableUplink {
		fmt.Fprintf(interactiveOut, "ERROR: uplink disabled (-disable-uplink)\n")
		return
	}
	if !s.hasUplinkTarget() {
		fmt.Fprintf(interactiveOut, "ERROR: this stream has no uplink channel; satellite commands cannot be sent\n")
		return
	}
	if err := s.pub.window.check(time.Now()); err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: %v\n", err)
		return
	}
	chIdx, hexPayload, ok := resolveCommandArgs(args, s.targets, s.singleUplinkIdx, "sat")
	if !ok {
		return
	}
	target := s.targets[chIdx]
	if target.UplinkTopic == "" {
		fmt.Fprintf(interactiveOut, "ERROR: channel %s has no uplink topic\n", target.ChannelID)
		return
	}
	cmdBytes, err := parseHexCommand(hexPayload)
	if err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: invalid hex: %v\n", err)
		return
	}
	if err := validateCommandSize([][]byte{cmdBytes}); err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: %v\n", err)
		return
	}
	idx := s.satIndices[chIdx]
	uiDataf(kindUplink, "sending command %d to channel %s: %s (%d bytes)",
		idx, target.ChannelID, hexPayload, len(cmdBytes))
	cmd, err := PublishSatCommand(s.pub, target.UplinkTopic, s.streamID, s.planID,
		idx, [][]byte{cmdBytes})
	if err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: %v\n", err)
	}
	// A command the broker did not confirm may still arrive, so its index
	// stays used (see publishCommand); only one never sent frees it.
	if cmd != nil {
		s.satIndices[chIdx]++
		if err == nil {
			fmt.Fprintf(interactiveOut, "  sent; waiting for the ground station to acknowledge it\n")
		}
	}
}

// sendGsConfig handles one "gs" line, with the same reject-and-continue
// behaviour as sendSatCommand.
func (s *interactiveSession) sendGsConfig(args []string) {
	if !s.cfg.EnableConfigRequests {
		fmt.Fprintf(interactiveOut, "ERROR: config_request disabled (-disable-config-requests)\n")
		return
	}
	if !s.hasConfigRequestTarget() {
		fmt.Fprintf(
			os.Stderr,
			"ERROR: this stream has no config_request channel; ground-station config requests cannot be sent\n",
		)
		return
	}
	if err := s.pub.window.check(time.Now()); err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: %v\n", err)
		return
	}
	chIdx, payload, ok := resolveCommandArgs(args, s.targets, s.singleGsIdx, "gs")
	if !ok {
		return
	}
	target := s.targets[chIdx]
	if target.ConfigRequestTopic == "" {
		fmt.Fprintf(interactiveOut, "ERROR: channel %s has no config_request topic\n", target.ChannelID)
		return
	}
	configRequest := &v1.GroundStationConfigurationRequest{}
	if payload != "" {
		if err := protojson.Unmarshal([]byte(payload), configRequest); err != nil {
			fmt.Fprintf(interactiveOut, "ERROR: invalid JSON: %v\n", err)
			return
		}
	}
	idx := s.gsIndices[chIdx]
	uiDataf(kindUplink, "sending config request %d to channel %s: %s",
		idx, target.ChannelID, payload)
	cmd, err := PublishGsConfig(s.pub, target.ConfigRequestTopic, s.streamID, s.planID,
		idx, configRequest)
	if err != nil {
		fmt.Fprintf(interactiveOut, "ERROR: %v\n", err)
	}
	if cmd != nil {
		s.gsIndices[chIdx]++
		if err == nil {
			fmt.Fprintf(interactiveOut, "  sent; waiting for the ground station to acknowledge it\n")
		}
	}
}

// parseChannelID looks up a channel by its ID string and returns the
// 0-based index, the matching target, and true on success. On failure it
// prints an error to stderr and returns false.
func parseChannelID(s string, targets []channelTarget) (int, channelTarget, bool) {
	for i, t := range targets {
		if t.ChannelID == s {
			return i, t, true
		}
	}
	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.ChannelID
	}
	fmt.Fprintf(interactiveOut, "ERROR: unknown channel %q (available: %s)\n", s, strings.Join(ids, ", "))
	return 0, channelTarget{}, false
}

// singleTargetIndex returns the index of the single target matching pred,
// or -1 if zero or more than one match.
func singleTargetIndex(targets []channelTarget, pred func(channelTarget) bool) int {
	idx := -1
	for i, t := range targets {
		if pred(t) {
			if idx >= 0 {
				return -1
			}
			idx = i
		}
	}
	return idx
}

// resolveCommandArgs parses the arguments after the command keyword (e.g. "sat" or "gs").
// When defaultIdx >= 0 (single channel for this command type), the channel number is optional
// and the first arg is treated as the payload. Otherwise the first arg must be a channel number.
// Returns (channelIndex, payload, ok).
func resolveCommandArgs(args []string, targets []channelTarget, defaultIdx int, cmdName string) (int, string, bool) {
	if len(args) == 0 {
		if defaultIdx >= 0 {
			return defaultIdx, "", true
		}
		fmt.Fprintf(interactiveOut, "Usage: %s <channel-id> <payload>\n", cmdName)
		return 0, "", false
	}

	// If there's a default channel, try using it: treat all args as payload.
	if defaultIdx >= 0 {
		payload := strings.Join(args, " ")
		return defaultIdx, payload, true
	}

	// Multiple channels, so the first arg must be the channel ID.
	chIdx, _, ok := parseChannelID(args[0], targets)
	if !ok {
		return 0, "", false
	}
	payload := ""
	if len(args) > 1 {
		payload = strings.Join(args[1:], " ")
	}
	return chIdx, payload, true
}

// passWindow is the booking interval during which commands may be transmitted.
// A window with a zero start or stop is unresolved and permits every command.
type passWindow struct {
	start time.Time
	stop  time.Time
}

// resolved reports whether both edges of the booking window are known.
func (w passWindow) resolved() bool {
	return !w.start.IsZero() && !w.stop.IsZero()
}

// check reports whether a command may be transmitted at the given time. The
// error names the edge of the booking that the command fell outside of, so the
// operator can see how long they have to wait or how long ago the pass ended.
func (w passWindow) check(now time.Time) error {
	if !w.resolved() {
		return nil
	}
	if now.Before(w.start) {
		return fmt.Errorf("the pass has not started; commanding opens at %s, in %s",
			w.start.UTC().Format(timeFormat), w.start.Sub(now).Round(time.Second))
	}
	if now.After(w.stop) {
		return fmt.Errorf("the pass ended at %s, %s ago; commanding is closed",
			w.stop.UTC().Format(timeFormat), now.Sub(w.stop).Round(time.Second))
	}
	return nil
}

// resolveBookingWindow fetches the booking interval of the pass being commanded.
//
// It returns an unresolved window - which permits every command - when the
// timing cannot be established: no pass ID, no credentials, the pass fetch
// fails, or the pass carries no booking. Failing open is deliberate. A
// transient API error must never block commanding during a live pass, which is
// the point at which the operator can least afford to be locked out.
func resolveBookingWindow(ctx context.Context, cfg Config, passID string) passWindow {
	if passID == "" || cfg.TokenSource == nil || cfg.AuthorizerAPI == "" {
		return passWindow{}
	}
	fetchCtx, cancel := context.WithTimeout(ctx, authorizerClientTimeout)
	defer cancel()
	p, err := apiclient.New(cfg.AuthorizerAPI, cfg.TokenSource).GetPass(fetchCtx, passID)
	if err != nil || p == nil {
		vlogf("command window: could not resolve the booking times (%v); the window check is disabled", err)
		return passWindow{}
	}
	return passWindowOf(p)
}

// passWindowOf is when the pass takes commands: its booking, or its scheduled
// contact for passes that carry no booking. Unresolved when it has neither.
func passWindowOf(p *apiclient.Pass) passWindow {
	switch {
	case p.Booking != nil:
		return passWindow{start: p.Booking.Start, stop: p.Booking.Stop}
	case p.Scheduled != nil:
		return passWindow{start: p.Scheduled.Start, stop: p.Scheduled.Stop}
	default:
		vlogf("command window: the pass has no booking or scheduled times; the window check is disabled")
		return passWindow{}
	}
}
