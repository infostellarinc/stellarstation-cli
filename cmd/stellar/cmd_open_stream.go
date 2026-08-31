package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/auth"
	"github.com/infostellarinc/stellarstation-cli/internal/proxy"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

// flagSet holds all open-stream command-line flags as value types. Fields are
// populated by cobra/pflag bindings before the command's RunE is invoked.
type flagSet struct {
	sourceType             string
	passID                 string
	satelliteID            string
	destDir                string
	s3PollInterval         time.Duration
	windowSize             int
	writeInOrder           bool
	mqttQoS                int
	channelsStr            string
	sendSatCommand         string
	sendSatCommands        string
	sendGsConfig           string
	interactive            bool
	disableDownlink        bool
	disableMonitoring      bool
	disableConfigState     bool
	disableEvent           bool
	disableConfigRequests  bool
	disableUplink          bool
	disableDiagnostics     bool
	enableStdoutOutput     bool
	outputFile             string
	outputFileMode         string
	enableStats            bool
	enableVerbose          bool
	enableDebug            bool
	enableAutoClose        bool
	overrideCommandingLock bool
	acceptedFraming        string

	// Populated at runtime from inherited root flags (not bound to local flags).
	apiURL      string
	tokenSource auth.TokenSource

	// Proxy settings
	proxyMode        string
	udpListenAddr    string
	udpSendAddr      string
	tcpListenAddr    string
	proxyAllowRemote bool
}

func (f *flagSet) downlinkEnabled() bool       { return !f.disableDownlink }
func (f *flagSet) monitoringEnabled() bool     { return !f.disableMonitoring }
func (f *flagSet) configStateEnabled() bool    { return !f.disableConfigState }
func (f *flagSet) eventEnabled() bool          { return !f.disableEvent }
func (f *flagSet) configRequestsEnabled() bool { return !f.disableConfigRequests }
func (f *flagSet) uplinkEnabled() bool         { return !f.disableUplink }
func (f *flagSet) diagnosticsEnabled() bool    { return !f.disableDiagnostics }

// newOpenStreamCommand creates the "open-stream" subcommand that streams
// satellite telemetry data via S3 and/or MQTT.
func newOpenStreamCommand() *cobra.Command {
	var flags flagSet

	cmd := &cobra.Command{
		Use:   "open-stream",
		Short: "Receive a live pass: telemetry down, commands up",
		Long: `Connect to a pass and work it live: receive telemetry, monitoring, events and
configuration as they arrive, and send commands to the satellite or ground
station.

Received data is saved under the folder given by --dest (default ./downlink),
sorted by type and channel. The command keeps running until the pass ends or
you press Ctrl-C.

Open a pass you have booked:
  stellar satellite open-stream --pass-id <pass-id>

Use a satellite's next upcoming pass automatically:
  stellar satellite open-stream --satellite-id <satellite-id>

Type commands as the pass runs (interactive):
  stellar satellite open-stream --pass-id <pass-id> --interactive

Send one satellite command and exit:
  stellar satellite open-stream --pass-id <pass-id> --send-sat-command 0A1B2C3D

Tip: add --verbose for the technical detail, or --stats for live throughput.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runOpenStream(cmd, &flags)
		},
	}

	registerOpenStreamFlags(cmd, &flags)
	return cmd
}

// registerOpenStreamFlags binds all open-stream CLI flags to the flagSet struct.
func registerOpenStreamFlags(cmd *cobra.Command, flags *flagSet) {
	f := cmd.Flags()

	// Basic flags
	f.StringVar(&flags.sourceType, "source", "s3", "Advanced: data source, 's3' or 'mqtt' (selected automatically)")
	f.StringVar(&flags.passID, "pass-id", "", "The pass to open (from list-passes or reserve-pass)")
	f.StringVar(
		&flags.satelliteID,
		"satellite-id",
		"",
		"Open this satellite's next upcoming pass (use instead of --pass-id)",
	)
	f.StringVar(&flags.destDir, "dest", "./downlink", "Folder to save received data in")
	f.DurationVar(
		&flags.s3PollInterval,
		"s3-poll-interval",
		defaultS3PollInterval,
		"Polling interval for new S3 objects",
	)
	f.IntVar(&flags.windowSize, "window", defaultWindowSize, "Maximum number of in-flight downloads")
	f.BoolVar(&flags.writeInOrder, "write-in-order", true, "Write chunks strictly in index order")

	// MQTT flags
	f.IntVar(&flags.mqttQoS, "mqtt-qos", 1, "MQTT QoS (0=at most once, 1=at least once, 2=exactly once)")
	f.StringVar(&flags.channelsStr, "channels", "", "Comma-separated list of channel IDs (UUIDs)")

	// Command flags
	f.StringVar(
		&flags.sendSatCommand,
		"send-sat-command",
		"",
		"Send one satellite command as hex (for example 0A1B2C3D), then exit",
	)
	f.StringVar(
		&flags.sendSatCommands,
		"send-sat-commands",
		"",
		"Send several satellite commands in order (comma-separated hex), then exit",
	)
	f.StringVar(&flags.sendGsConfig, "send-gs-config", "", "Send one ground-station config request (JSON), then exit")
	f.BoolVar(&flags.interactive, "interactive", false, "Type commands to send while the pass runs")

	// Feature flags
	f.BoolVar(&flags.disableDownlink, "disable-downlink", false, "Do not receive telemetry")
	f.BoolVar(&flags.disableMonitoring, "disable-monitoring", false, "Do not receive monitoring data")
	f.BoolVar(&flags.disableConfigState, "disable-config-state", false, "Do not receive configuration data")
	f.BoolVar(&flags.disableEvent, "disable-event", false, "Do not receive events")
	f.BoolVar(
		&flags.disableConfigRequests,
		"disable-config-requests",
		false,
		"Do not send ground-station config requests",
	)
	f.BoolVar(&flags.disableUplink, "disable-uplink", false, "Do not send commands to the satellite")

	// Output flags
	f.BoolVar(&flags.disableDiagnostics, "disable-diagnostics", false, "Disable diagnostics file generation")
	f.StringVar(&flags.outputFile, "output-file", "", "Single output file path (use with --output-file-mode)")
	f.StringVar(
		&flags.outputFileMode,
		"output-file-mode",
		"all-combined",
		"Output file modes (comma-separated): per-channel, per-framing, per-framing-channel, all-combined",
	)
	f.BoolVar(
		&flags.enableStdoutOutput,
		"stdout",
		false,
		"Also write raw telemetry to stdout (for piping into other tools)",
	)

	// Logging flags
	f.BoolVar(&flags.enableStats, "stats", false, "Show live throughput and timing figures")
	f.BoolVar(&flags.enableVerbose, "verbose", false, "Show the technical detail of each step")
	f.BoolVar(&flags.enableDebug, "debug", false, "Show low-level debug detail (very noisy)")
	f.BoolVar(
		&flags.enableAutoClose,
		"enable-auto-close",
		false,
		"Stop automatically when the satellite signals end-of-data",
	)
	f.BoolVar(
		&flags.overrideCommandingLock,
		"override-commanding-lock",
		false,
		"Take over commanding even if another client already holds it on a channel "+
			"(bypasses the one-commanding-authority-per-channel check)",
	)
	f.StringVar(
		&flags.acceptedFraming,
		"accepted-framing",
		"",
		"Comma-separated list of accepted framing types (e.g., 'BITSTREAM,IQ')",
	)

	// Proxy flags
	f.StringVar(&flags.proxyMode, "proxy", "disabled", "Local socket proxy mode: disabled, udp, tcp")
	f.StringVar(
		&flags.udpListenAddr,
		"udp-listen-addr",
		"127.0.0.1:6000",
		"UDP proxy: address to listen for uplink data",
	)
	f.StringVar(&flags.udpSendAddr, "udp-send-addr", "127.0.0.1:6001", "UDP proxy: address to send downlink data to")
	f.StringVar(&flags.tcpListenAddr, "tcp-listen-addr", "127.0.0.1:6001", "TCP proxy: address to listen on")
	f.BoolVar(
		&flags.proxyAllowRemote,
		"proxy-allow-remote",
		false,
		"Allow the proxy to listen on a non-loopback address. The proxy has no authentication, "+
			"so any host that can reach it receives the downlink and can transmit to the satellite",
	)
}

// createOptimizedHTTPClient creates an HTTP client optimized for high-throughput
// S3 downloads with large connection pools and HTTP/2 support.
func createOptimizedHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        httpMaxIdleConns,
		MaxIdleConnsPerHost: httpMaxIdleConnsPerHost,
		MaxConnsPerHost:     httpMaxConnsPerHost,
		IdleConnTimeout:     httpIdleConnTimeout,
		DialContext: (&net.Dialer{
			Timeout:   httpDialTimeout,
			KeepAlive: httpDialKeepAlive,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion:             tls.VersionTLS12,
			InsecureSkipVerify:     false,
			SessionTicketsDisabled: false,
		},
		ForceAttemptHTTP2:  true,
		DisableCompression: true,
		DisableKeepAlives:  false,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   httpClientTimeout,
	}
}

// determineSourceType returns the effective source type based on flags. When
// downlink is enabled the source is always MQTT (which also reads S3 for
// high-rate data).
func determineSourceType(flags *flagSet) SourceType {
	srcType := SourceType(strings.ToLower(flags.sourceType))
	if flags.downlinkEnabled() {
		return SourceTypeMQTT
	}
	return srcType
}

// parseChannelIDs splits the comma-separated channels flag into validated UUIDs.
func parseChannelIDs(flags *flagSet) ([]string, error) {
	var channelIDs []string
	if flags.channelsStr != "" {
		channelParts := strings.Split(flags.channelsStr, ",")
		for _, part := range channelParts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if _, err := uuid.Parse(part); err != nil {
				return nil, fmt.Errorf("invalid channel ID: %s (must be a valid UUID)", part)
			}
			channelIDs = append(channelIDs, part)
		}
	}
	return channelIDs, nil
}

// streamTargetSpecified reports whether the request identifies a pass to open:
// an explicit --pass-id, or --satellite-id when the authorizer is available to
// resolve the next upcoming pass for it.
func streamTargetSpecified(flags *flagSet, useAuthorizer bool) bool {
	return flags.passID != "" || (useAuthorizer && flags.satelliteID != "")
}

func validateDownlinkConfig(flags *flagSet, channelIDs []string, useAuthorizer bool) {
	if !flags.downlinkEnabled() {
		return
	}
	if !streamTargetSpecified(flags, useAuthorizer) {
		streamUsageFatal(
			"Specify which pass to open: use --pass-id <id>, or --satellite-id <id> to open that satellite's next pass.",
		)
	}
	if len(channelIDs) == 0 && !useAuthorizer {
		streamUsageFatal(
			"Activate an API key first (`stellar auth activate-api-key <key-file>`) so the channels for this pass can be resolved automatically, or list them explicitly with --channels.",
		)
	}
}

// streamUsageFatal reports a user-input problem in open-stream and exits. It
// mirrors how RunE errors are shown (in red on stderr) so the experience is
// consistent, even though this runs deep in the streaming setup.
func streamUsageFatal(msg string) {
	uiErrf("%s", msg)
	os.Exit(1)
}

// needsMQTTFeatures reports whether any MQTT-backed feature is enabled.
func needsMQTTFeatures(flags *flagSet) bool {
	return flags.downlinkEnabled() || flags.monitoringEnabled() || flags.configStateEnabled() ||
		flags.eventEnabled() || flags.configRequestsEnabled() || flags.uplinkEnabled()
}

func validateMQTTConfig(flags *flagSet, channelIDs []string, useAuthorizer bool) {
	if !needsMQTTFeatures(flags) {
		return
	}
	if !streamTargetSpecified(flags, useAuthorizer) {
		streamUsageFatal(
			"Specify which pass to open: use --pass-id <id>, or --satellite-id <id> to open that satellite's next pass.",
		)
	}
	if flags.downlinkEnabled() && len(channelIDs) == 0 && !useAuthorizer {
		streamUsageFatal(
			"Activate an API key first (`stellar auth activate-api-key <key-file>`) so the channels for this pass can be resolved automatically, or list them explicitly with --channels.",
		)
	}
	if !useAuthorizer {
		streamUsageFatal(
			"An activated API key is required to receive a live pass. Run `stellar auth activate-api-key <key-file>` and try again.",
		)
	}
}

func validateBasicFlags(flags *flagSet) {
	if flags.windowSize <= 0 {
		flags.windowSize = 1
	}
	if flags.mqttQoS < 0 || flags.mqttQoS > 2 {
		streamUsageFatal(fmt.Sprintf("--mqtt-qos must be 0, 1, or 2 (you gave %d).", flags.mqttQoS))
	}
}

func validateConfig(flags *flagSet, channelIDs []string, useAuthorizer bool) {
	validateDownlinkConfig(flags, channelIDs, useAuthorizer)
	validateMQTTConfig(flags, channelIDs, useAuthorizer)
	validateBasicFlags(flags)
}

// getEffectiveEnvironment returns the environment from the ENV variable. The
// authorizer's response overrides it once credentials are fetched.
func getEffectiveEnvironment() string {
	return os.Getenv("ENV")
}

// buildPrefix returns the S3 prefix for the given pass.
func buildPrefix(flags *flagSet, effectiveEnv string, passID string) string {
	if flags.downlinkEnabled() && effectiveEnv != "" {
		return fmt.Sprintf("%s/%s/", effectiveEnv, passID)
	}
	return fmt.Sprintf("%s/", passID)
}

// resolveS3ValidationPrefix returns a prefix that the current credentials have
// access to, suitable for a lightweight ListObjectsV2 call to validate S3
// permissions.
func resolveS3ValidationPrefix(localPrefix string, authCreds *AuthorizerCredentials) string {
	if authCreds == nil {
		return localPrefix
	}
	if authCreds.Streams.HighRate != nil && authCreds.Streams.HighRate.S3Prefix != "" {
		return authCreds.Streams.HighRate.S3Prefix
	}
	if len(authCreds.Streams.LowRate) > 0 && authCreds.Streams.LowRate[0].S3Prefix != "" {
		return authCreds.Streams.LowRate[0].S3Prefix
	}
	if authCreds.Streams.Monitoring != nil && authCreds.Streams.Monitoring.S3Prefix != "" {
		return authCreds.Streams.Monitoring.S3Prefix
	}
	if authCreds.Streams.Config != nil && authCreds.Streams.Config.S3Prefix != "" {
		return authCreds.Streams.Config.S3Prefix
	}
	if authCreds.Streams.Event != nil && authCreds.Streams.Event.S3Prefix != "" {
		return authCreds.Streams.Event.S3Prefix
	}
	return localPrefix
}

// getBucket determines the S3 bucket from authorizer response or env var.
func getBucket(authCreds *AuthorizerCredentials) string {
	if authCreds != nil && authCreds.S3Bucket != "" {
		return authCreds.S3Bucket
	}
	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		return bucket
	}
	return ""
}

// loadAWSConfigWithAuthCreds builds an AWS config from the authorizer-provided
// temporary credentials. It deliberately does NOT use config.LoadDefaultConfig:
// that reads AWS_PROFILE / shared config from the environment and fails when the
// caller has an unrelated profile exported (e.g. a deploy/SSO admin profile like
// AWSAdministratorAccess-...). The authorizer's temporary credentials are
// self-contained and are all the S3 client needs.
func loadAWSConfigWithAuthCreds(
	_ context.Context,
	authCreds *AuthorizerCredentials,
	store *credentialStore,
) aws.Config {
	var credProvider aws.CredentialsProvider
	if store != nil {
		credProvider = &refreshingCredentialsProvider{store: store}
	} else {
		credProvider = credentials.NewStaticCredentialsProvider(
			authCreds.AccessKeyID, authCreds.SecretAccessKey, authCreds.SessionToken,
		)
	}
	cachedProvider := aws.NewCredentialsCache(credProvider)

	region := authCreds.S3Region
	if region == "" {
		region = os.Getenv("AWS_REGION")
		if region != "" {
			vlogf("Using AWS region from environment: %s", region)
		}
	} else {
		vlogf("Using AWS region from authorizer: %s", region)
	}

	return aws.Config{
		Region:      region,
		Credentials: cachedProvider,
	}
}

// loadAWSConfigDefault loads AWS config using the default credential chain.
func loadAWSConfigDefault(ctx context.Context) aws.Config {
	var opts []func(*config.LoadOptions) error
	if region := os.Getenv("AWS_REGION"); region != "" {
		opts = append(opts, config.WithRegion(region))
		vlogf("Using AWS region from environment: %s", region)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}
	return awsCfg
}

func initializeS3ClientWithCreds(
	ctx context.Context,
	flags *flagSet,
	bucket string,
	authCreds *AuthorizerCredentials,
	store *credentialStore,
) *s3.Client {
	needsS3 := flags.downlinkEnabled() ||
		((flags.monitoringEnabled() || flags.configStateEnabled() || flags.eventEnabled()) && bucket != "")
	if !needsS3 {
		return nil
	}

	if bucket == "" {
		if authCreds != nil {
			log.Fatal("Bucket not provided by authorizer. This should not happen.")
		} else {
			log.Fatal(
				"Bucket required. Set S3_BUCKET environment variable or use --api-url with credentials (`stellar auth activate-api-key`) for the authorizer",
			)
		}
	}

	vlogf("Configuring S3 client for bucket: %s", bucket)

	var awsCfg aws.Config
	if authCreds != nil {
		awsCfg = loadAWSConfigWithAuthCreds(ctx, authCreds, store)
	} else {
		awsCfg = loadAWSConfigDefault(ctx)
	}

	if awsCfg.Region == "" {
		log.Fatalf("AWS region not configured. Set --aws-region flag or AWS_REGION environment variable")
	}

	vlogf("S3 client initialized for bucket: %s, region: %s", bucket, awsCfg.Region)
	awsCfg.HTTPClient = createOptimizedHTTPClient()
	return s3.NewFromConfig(awsCfg)
}

// buildConfigWithCreds builds the runtime Config from parsed flags and
// authorizer credentials.
func buildConfigWithCreds(
	flags *flagSet,
	srcType SourceType,
	prefix, effectivePassID, effectiveEnv string,
	channelIDs []string,
	authCreds *AuthorizerCredentials,
	credStore *credentialStore,
	bucket string,
) Config {
	var acceptedFraming []string
	if flags.acceptedFraming != "" {
		parts := strings.Split(flags.acceptedFraming, ",")
		for _, part := range parts {
			part = strings.TrimSpace(strings.ToUpper(part))
			if part != "" {
				acceptedFraming = append(acceptedFraming, part)
			}
		}
	}

	var outputFileMode []string
	if flags.outputFileMode != "" {
		parts := strings.Split(flags.outputFileMode, ",")
		for _, part := range parts {
			part = strings.TrimSpace(strings.ToLower(part))
			if part != "" {
				outputFileMode = append(outputFileMode, part)
			}
		}
	}
	if len(outputFileMode) == 0 {
		outputFileMode = []string{"all-combined"}
	}

	return Config{
		SourceType:           srcType,
		Bucket:               bucket,
		Prefix:               prefix,
		DestDir:              flags.destDir,
		PassID:               effectivePassID,
		S3PollInterval:       flags.s3PollInterval,
		WindowSize:           flags.windowSize,
		WriteInOrder:         flags.writeInOrder,
		MQTTQoS:              byte(flags.mqttQoS),
		Environment:          effectiveEnv,
		ChannelIDs:           channelIDs,
		AuthorizerCreds:      authCreds,
		CredStore:            credStore,
		EnableDownlink:       flags.downlinkEnabled(),
		EnableMonitoring:     flags.monitoringEnabled(),
		EnableConfigState:    flags.configStateEnabled(),
		EnableEvent:          flags.eventEnabled(),
		EnableConfigRequests: flags.configRequestsEnabled(),
		EnableUplink:         flags.uplinkEnabled(),
		EnableDiagnostics:    flags.diagnosticsEnabled(),
		EnableStdoutOutput:   flags.enableStdoutOutput,
		OutputFile:           flags.outputFile,
		OutputFileMode:       outputFileMode,
		EnableStats:          flags.enableStats,
		EnableVerbose:        flags.enableVerbose,
		EnableDebug:          flags.enableDebug,
		EnableAutoClose:      flags.enableAutoClose,
		AcceptedFraming:      acceptedFraming,
	}
}

func validateFeatureFlags(flags *flagSet) {
	hasFeature := flags.downlinkEnabled() || flags.monitoringEnabled() ||
		flags.configStateEnabled() || flags.eventEnabled() ||
		flags.configRequestsEnabled() || flags.uplinkEnabled()
	if !hasFeature {
		streamUsageFatal(
			"All data types are disabled, so there is nothing to do. Remove the relevant --disable-* flag for what you want to receive or send.",
		)
	}
}

// handleOneShotCommands runs any --send-* one-shot command and then exits the
// process. closeStream releases the authorizer session and its commanding lock;
// because these exit paths use os.Exit / streamUsageFatal (which skip the
// caller's defer), they must call it explicitly before exiting so the lock does
// not leak.
func handleOneShotCommands(
	ctx context.Context,
	flags *flagSet,
	cfg Config,
	effectivePassID string,
	closeStream func(),
) {
	hasOneShotCommands := flags.sendSatCommand != "" || flags.sendSatCommands != "" || flags.sendGsConfig != ""
	if !hasOneShotCommands {
		return
	}
	if effectivePassID == "" {
		closeStream()
		streamUsageFatal("Specify which pass to command with --pass-id <id>.")
	}
	if !flags.uplinkEnabled() && (flags.sendSatCommand != "" || flags.sendSatCommands != "") {
		closeStream()
		streamUsageFatal("Uplink is disabled (--disable-uplink). Remove that flag to send satellite commands.")
	}
	if !flags.configRequestsEnabled() && flags.sendGsConfig != "" {
		closeStream()
		streamUsageFatal(
			"Ground-station config requests are disabled (--disable-config-requests). Remove that flag to send one.",
		)
	}
	stats := newStatsTracker(cfg.EnableDiagnostics)
	if err := runCommandSender(
		ctx, cfg,
		flags.sendSatCommand, flags.sendSatCommands, flags.sendGsConfig,
		false, "",
		effectivePassID, effectivePassID,
		nil, stats,
	); err != nil {
		uiErrf("Could not send the command: %v", err)
		closeStream()
		os.Exit(1)
	}
	if cfg.EnableDiagnostics {
		writeDiagnosticsFile(context.Background(), cfg, stats, nil)
	}
}

func startStatsLogger(ctx context.Context, stats *statsTracker) func() {
	// Route the standard logger through the status-clearing writer so reader
	// goroutines' log lines erase the live dashboard block before printing,
	// instead of shifting/corrupting it. Restored when the logger stops.
	prevLogOut := log.Writer()
	log.SetOutput(statusClearingWriter{w: prevLogOut})

	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(statsLogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				stats.LogStats()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stopCh)
			log.SetOutput(prevLogOut)
		})
	}
}

func startHighRateReader(
	ctx context.Context, cfg Config, s3c *s3.Client, stats *statsTracker,
	wg *sync.WaitGroup, errCh chan<- error,
) {
	if !cfg.EnableDownlink {
		return
	}
	if cfg.HighRateChannelIDs != nil {
		cfg.ChannelIDs = cfg.HighRateChannelIDs
		if len(cfg.ChannelIDs) == 0 {
			vlogf("No high-rate channels, skipping high-rate S3 reader")
			return
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		vlogf("Starting high-rate reader (S3 direct) from bucket: %s, prefix: %s, channels: %v",
			cfg.Bucket, cfg.Prefix, cfg.ChannelIDs)

		var client mqtt.Client
		var ackSdr *ackSender
		if cfg.AuthorizerCreds != nil {
			connCfg, err := buildMQTTConnectionConfig(ctx, cfg)
			if err != nil {
				log.Printf("WARNING: Failed to build MQTT connection config for high-rate acks: %v", err)
			} else {
				client, _, err = connectMQTTClientForReader(ctx, cfg, connCfg)
				if err != nil {
					log.Printf("WARNING: Failed to connect MQTT client for high-rate acks: %v", err)
					client = nil
				} else if client != nil {
					ackSdr = newAckSender(cfg, stats)
					// Deferred LIFO: flush pending acks (runs first) before the
					// client disconnects (runs last), so the final ack of the pass
					// is confirmed rather than cut off mid-publish.
					defer client.Disconnect(mqttDisconnectQuiesce)
					defer ackSdr.Flush()
					vlogf("MQTT client connected for high-rate ack publishing")
				}
			}
		}

		var err error
		if cfg.WriteInOrder {
			err = runS3InOrder(ctx, cfg, s3c, stats, client, ackSdr)
		} else {
			err = runS3Relaxed(ctx, cfg, s3c, stats, client, ackSdr)
		}
		if err != nil {
			// A Ctrl-C shutdown cancels the reader's context; that is a normal stop,
			// not a failure, so streamErrf demotes it to verbose-only instead of
			// printing a scary red ERROR line. Genuine failures still surface.
			streamErrf(err, "The high-rate data reader stopped: %v", err)
			stats.AddError(fmt.Errorf("high-rate reader failed: %w", err))
			errCh <- fmt.Errorf("high-rate reader error: %w", err)
		}
	}()
}

func startMQTTReader(
	ctx context.Context, cfg Config, s3c *s3.Client, stats *statsTracker,
	wg *sync.WaitGroup, errCh chan<- error,
) {
	hasMQTTFeatures := cfg.EnableDownlink || cfg.EnableMonitoring || cfg.EnableConfigState || cfg.EnableEvent
	if !hasMQTTFeatures {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if cfg.EnableDownlink {
			vlogf("Starting downlink reader (MQTT with S3 fallback)")
		} else {
			vlogf("Starting MQTT reader for non-telemetry messages")
		}
		if err := runMQTTReader(ctx, cfg, s3c, stats); err != nil {
			errCh <- fmt.Errorf("MQTT reader error: %w", err)
		}
	}()
}

func startInteractiveCommandSender(
	ctx context.Context, flags *flagSet, cfg Config, effectivePassID string,
	stats *statsTracker, wg *sync.WaitGroup, errCh chan<- error, statsCancel func(),
) {
	if !flags.interactive || (!flags.uplinkEnabled() && !flags.configRequestsEnabled()) {
		return
	}
	if effectivePassID == "" {
		statsCancel()
		log.Fatal("--pass-id is required for sending commands")
	}
	cmdCfg := cfg
	connCfg, err := buildMQTTConnectionConfig(ctx, cmdCfg)
	if err != nil {
		statsCancel()
		log.Fatalf("build MQTT connection config: %v", err)
	}
	stats.SetClientID(connCfg.clientID)
	connectConfirmed := make(chan struct{}, 1)
	clientFactory := func() (mqtt.Client, string, error) {
		var creds *AuthorizerCredentials
		if cfg.CredStore != nil {
			creds = cfg.CredStore.load()
		} else if cmdCfg.AuthorizerCreds != nil {
			creds = cmdCfg.AuthorizerCreds
		}
		if creds != nil && creds.IoTCertificatePem != "" && creds.IoTPrivateKeyPem != "" {
			if latestCert, certErr := tls.X509KeyPair(
				[]byte(creds.IoTCertificatePem), []byte(creds.IoTPrivateKeyPem),
			); certErr == nil {
				connCfg.tlsCertificate = &latestCert
			}
		}
		opts := buildMQTTClientOptions(connCfg, connectConfirmed)
		return mqtt.NewClient(opts), connCfg.broker, nil
	}
	cmdClient, err := connectMQTTClientWithRetry(ctx, clientFactory, connectConfirmed, connCfg)
	if err != nil {
		statsCancel()
		log.Fatalf("connect MQTT for commands: %v", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cmdClient.Disconnect(mqttDisconnectQuiesce)
		if err := runCommandSender(
			ctx, cfg, "", "", "", true, "",
			effectivePassID, effectivePassID, cmdClient, stats,
		); err != nil {
			errCh <- fmt.Errorf("command sender error: %w", err)
		}
	}()
}

// resolvePassEndTime returns the latest of the pass's scheduled, visibility, and
// booking stop times - the point after which an idle stream may auto-close. It
// returns the zero time when the timing cannot be resolved (no credentials, the
// pass fetch fails, or no stop times are set), which disables the idle monitor.
func resolvePassEndTime(ctx context.Context, flags *flagSet, passID string) time.Time {
	if passID == "" || flags.tokenSource == nil || flags.apiURL == "" {
		return time.Time{}
	}
	fetchCtx, cancel := context.WithTimeout(ctx, authorizerClientTimeout)
	defer cancel()
	p, err := apiclient.New(flags.apiURL, flags.tokenSource).GetPass(fetchCtx, passID)
	if err != nil || p == nil {
		vlogf("idle-shutdown: could not resolve pass end time (%v); auto-close disabled", err)
		return time.Time{}
	}
	var end time.Time
	consider := func(t time.Time) {
		if t.After(end) {
			end = t
		}
	}
	if p.Scheduled != nil {
		consider(p.Scheduled.Stop)
	}
	if p.Visibility != nil {
		consider(p.Visibility.Stop)
	}
	if p.Booking != nil {
		consider(p.Booking.Stop)
	}
	return end
}

// startIdleShutdownMonitor watches a live stream and gracefully closes it once
// BOTH conditions hold: the pass has ended (now is past the latest of its
// scheduled/visibility/booking stop) AND no message of any kind has been sent or
// received since the previous sample (the stream has gone quiet). After the pass
// ends it waits idleShutdownInactivityDelay of continuous inactivity before
// announcing the close, then counts down with escalating, colored warnings at
// 60/30/10 seconds and cancels the run context, which drives the normal
// graceful-shutdown path. Any activity resets it: it waits out the inactivity
// delay again the next time the stream falls idle.
//
// The pass end time is resolved inside the goroutine so stream startup is never
// blocked on the pass fetch; when it cannot be resolved the monitor simply
// exits and no auto-close happens.
func startIdleShutdownMonitor(
	ctx context.Context, cancel context.CancelFunc, flags *flagSet,
	passID string, stats *statsTracker, idleClosed *atomic.Bool,
) {
	go func() {
		endTime := resolvePassEndTime(ctx, flags, passID)
		if endTime.IsZero() {
			return
		}
		vlogf("idle-shutdown: armed; auto-close when idle after pass end %s",
			endTime.UTC().Format(time.RFC3339))

		ticker := time.NewTicker(idleShutdownPoll)
		defer ticker.Stop()

		lastCount := stats.ActivityCount()
		var idleSince time.Time // zero == active/not yet idle after pass end
		var deadline time.Time  // zero == countdown not armed
		warned30, warned10 := false, false

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			cur := stats.ActivityCount()
			active := cur != lastCount
			lastCount = cur
			now := time.Now()

			// Disarm while the pass is still running or data is still flowing.
			if now.Before(endTime) || active {
				if !deadline.IsZero() && active {
					uiCountdownf(0, "Activity resumed; auto-close canceled.")
				}
				idleSince = time.Time{}
				deadline = time.Time{}
				warned30, warned10 = false, false
				continue
			}

			// Past the pass end and quiet. Require idleShutdownInactivityDelay of
			// continuous inactivity before announcing/arming the close countdown, so
			// the operator is not warned the instant the pass ends.
			if idleSince.IsZero() {
				idleSince = now
				continue
			}
			if now.Sub(idleSince) < idleShutdownInactivityDelay {
				continue
			}

			// Inactivity threshold reached: arm the countdown on the first qualifying tick.
			if deadline.IsZero() {
				deadline = now.Add(idleShutdownGrace)
				warned30, warned10 = false, false
				uiCountdownf(
					0,
					"Pass has ended and the stream is idle - closing in %d seconds unless activity resumes.",
					int(idleShutdownGrace.Seconds()),
				)
				continue
			}

			switch remaining := time.Until(deadline); {
			case remaining <= 0:
				idleClosed.Store(true)
				uiCountdownf(2, "No activity since the pass ended - closing the stream now.")
				cancel()
				return
			case remaining <= idleShutdownWarn10 && !warned10:
				warned10 = true
				uiCountdownf(2, "Closing the stream in 10 seconds (no activity since the pass ended).")
			case remaining <= idleShutdownWarn30 && !warned30:
				warned30 = true
				uiCountdownf(1, "Closing the stream in 30 seconds (no activity since the pass ended).")
			}
		}
	}()
}

func waitForCompletion(
	ctx context.Context, cfg Config, stats *statsTracker, s3c *s3.Client,
	wg *sync.WaitGroup, errCh <-chan error, statsCancel func(), closeStream func(),
	idleClosed *atomic.Bool,
) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	onShutdown := func() {
		if cfg.EnableStats {
			stats.LogPassSummaryOnExit()
		}
		flushOutputFiles()
		if cfg.EnableDiagnostics {
			writeDiagnosticsFile(context.Background(), cfg, stats, s3c)
		}
		closeStream()
	}

	select {
	case <-ctx.Done():
		finishStatusLine()
		if idleClosed != nil && idleClosed.Load() {
			uiStepf("Closing the stream: pass ended and no activity for over a minute.")
		} else {
			uiStepf("Stopping (Ctrl-C received)...")
		}
		onShutdown()
		printStreamSummary(cfg, stats)
		return
	case <-done:
		select {
		case err := <-errCh:
			onShutdown()
			statsCancel()
			finishStatusLine()
			uiErrf("The pass stopped early: %v", err)
			printStreamSummary(cfg, stats)
			os.Exit(1)
		default:
			finishStatusLine()
			uiOKf("Pass finished.")
			onShutdown()
			printStreamSummary(cfg, stats)
		}
	}
	statsCancel()
}

// finishStatusLine erases the transient dashboard block so the closing summary
// prints cleanly in its place.
func finishStatusLine() {
	clearStatusBlock()
}

// exitAfterTeardown restores the terminal and flushes output before exiting.
// os.Exit skips defers, so hard-exit paths (e.g. end-of-pass auto-close) must
// route through here: clearStatusBlock tears down the pinned DEC scrolling
// region, otherwise xterm-family terminals keep the shrunken region and stale
// panel text after the process exits until the user runs `reset`.
func exitAfterTeardown() {
	clearStatusBlock()
	flushOutputFiles()
	os.Exit(0)
}

// closeAuthorizerStream releases this stream's authorizer session (and its
// commanding lock) via /stream/close. Safe to call more than once and on any exit
// path; a missing authorizer session or stream ID is a no-op. Server-side close
// is idempotent, so a double call is harmless.
func closeAuthorizerStream(cfg Config) {
	if cfg.AuthorizerAPI == "" || cfg.AuthorizerCreds == nil {
		vlogf("skipping stream close API (no authorizer session)")
		return
	}
	closeAuthorizerSession(cfg.AuthorizerAPI, cfg.TokenSource, cfg.PassID, getStreamID(cfg))
}

// closeAuthorizerSession releases an authorizer session (and its commanding
// lock) via /stream/close using raw parameters, so callers can release the lock
// before a full Config has been assembled (e.g. early abort paths). Safe to call
// more than once and on any exit path; a missing API URL or stream ID is a
// no-op, and server-side close is idempotent, so a double call is harmless.
func closeAuthorizerSession(api string, tokenSource auth.TokenSource, passID, streamID string) {
	if api == "" {
		vlogf("skipping stream close API (no authorizer session)")
		return
	}
	if streamID == "" {
		vlogf("skipping stream close API: no stream ID from authorizer")
		return
	}
	if err := callStreamCloseAPI(context.Background(), api, tokenSource, passID, streamID); err != nil {
		vlogf("stream close API call failed: %v", err)
	}
}

// printStreamSummary prints a short end-of-pass recap: totals per data type
// and where the files were written.
func printStreamSummary(cfg Config, stats *statsTracker) {
	mqttFiles, mqttBytes, s3Files, s3Bytes := stats.downloadTotals()
	dl := mqttFiles + s3Files
	dlBytes := mqttBytes + s3Bytes
	mon := stats.monitoringStats.mqttCount + stats.monitoringStats.s3Count
	evt := stats.eventStats.mqttCount + stats.eventStats.s3Count
	conf := stats.configStats.mqttCount + stats.configStats.s3Count

	uiHeadingf("Pass summary")
	if cfg.EnableDownlink {
		uiDataf(kindTelemetry, "%d messages, %s", dl, humanReadableBytes(dlBytes))
		printChannelBreakdown(cfg, stats)
	}
	if cfg.EnableMonitoring {
		uiDataf(kindMonitoring, "%d messages", mon)
	}
	if cfg.EnableEvent {
		uiDataf(kindEvent, "%d messages", evt)
	}
	if cfg.EnableConfigState {
		uiDataf(kindConfig, "%d messages", conf)
	}
	if cfg.EnableUplink || cfg.EnableConfigRequests {
		sat, gs := stats.SentCommandCounts()
		uiDataf(kindUplink, "%d satellite commands sent, %d ground-station config requests sent", sat, gs)
	}
	uiDataf(kindAck, "%d sent, %d received", stats.acksSent, stats.acksReceived)
	uiDimf("  Files saved under %s", cfg.DestDir)
}

// printChannelBreakdown renders a per-channel telemetry table: one row per
// channel with its rate class, message and byte counts, and a per-framing
// breakdown (a channel may carry several framings, e.g. BITSTREAM + IQ).
func printChannelBreakdown(_ Config, stats *statsTracker) {
	rows := stats.channelBreakdown()
	if len(rows) == 0 {
		return
	}
	uiDimf("  Per-channel telemetry:")
	for _, r := range rows {
		end := ""
		if r.endReceived {
			end = "   END received"
		}
		uiDimf("    %-9s  %s  %d msgs  %s   src: %s   framings: %s%s",
			r.rateClass, r.channelID, r.files, humanReadableBytes(r.bytes),
			r.formatSourceSplit(), formatFramings(r.framings), end)
	}
}

// validateS3Access verifies that the S3 client can list the target
// bucket/prefix. Retries on transient IAM propagation errors. Returns nil on
// success, or an error describing the persistent failure so the caller can
// decide whether it is fatal (high-rate channels depend on S3) or survivable
// (low-rate-only streams still receive everything over MQTT).
func validateS3Access(
	ctx context.Context, s3c *s3.Client, bucket, prefix string, authCreds *AuthorizerCredentials,
) error {
	validationPrefix := resolveS3ValidationPrefix(prefix, authCreds)
	if !strings.HasSuffix(validationPrefix, "/") {
		validationPrefix += "/"
	}
	vlogf("Validating S3 access before starting workers (prefix: %s)...", validationPrefix)

	if authCreds != nil && authCreds.AccessKeyID != "" {
		keyPreview := authCreds.AccessKeyID
		if len(keyPreview) > 8 {
			keyPreview = keyPreview[:8] + "..."
		}
		vlogf("S3 validation credentials: AccessKeyID=%s expiry=%s",
			keyPreview, authCreds.Expiration.Format(time.RFC3339))
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(validationPrefix),
		MaxKeys: aws.Int32(1),
	}

	type awsErrIface interface {
		ErrorCode() string
		ErrorMessage() string
	}

	for attempt := 0; attempt <= s3ValidationMaxRetries; attempt++ {
		_, err := s3c.ListObjectsV2(ctx, listInput)
		if err == nil {
			if attempt > 0 {
				vlogf("S3 validation succeeded on attempt %d", attempt+1)
			} else {
				vlogf("S3 validation successful")
			}
			return nil
		}

		var awsErr awsErrIface
		if !errors.As(err, &awsErr) {
			return fmt.Errorf("could not reach the data store for this pass: %w", err)
		}

		code := awsErr.ErrorCode()
		isTransient := code == "InvalidAccessKeyId" || code == "AccessDenied"
		if !isTransient || attempt == s3ValidationMaxRetries {
			return fmt.Errorf("could not access the data store for this pass (%s): %s", code, awsErr.ErrorMessage())
		}
		vlogf("S3 validation: transient %s error, retrying in %v (attempt %d/%d)...",
			code, s3ValidationRetryDelay, attempt+1, s3ValidationMaxRetries)
		time.Sleep(s3ValidationRetryDelay)
	}
	return nil
}

// fetchAndApplyAuthorizerCredentials fetches credentials from the authorizer
// if configured, and returns the updated state.
func fetchAndApplyAuthorizerCredentials(
	ctx context.Context, flags *flagSet,
	effectivePassID, effectiveEnv string, channelIDs []string,
) (authCreds *AuthorizerCredentials, credStore *credentialStore, env, prefix string) {
	env = effectiveEnv
	if flags.tokenSource == nil {
		prefix = buildPrefix(flags, env, flags.passID)
		return nil, nil, env, prefix
	}

	authorizeURL := strings.TrimRight(flags.apiURL, "/") + "/authorize"
	authReq := AuthorizerRequest{
		PassID:                 effectivePassID,
		SatelliteID:            flags.satelliteID,
		ChannelIDs:             channelIDs,
		Environment:            env,
		Region:                 os.Getenv("AWS_REGION"),
		Source:                 "streamer",
		ClientVersion:          cliVersion(),
		EnableDownlink:         flags.downlinkEnabled(),
		EnableMonitoring:       flags.monitoringEnabled(),
		EnableConfigState:      flags.configStateEnabled(),
		EnableEvent:            flags.eventEnabled(),
		EnableUplink:           flags.uplinkEnabled(),
		EnableConfigRequests:   flags.configRequestsEnabled(),
		OverrideCommandingLock: flags.overrideCommandingLock,
	}

	stopSpinner := uiSpinner("Opening stream")
	vlogf("Fetching credentials from authorizer: %s", authorizeURL)
	var err error
	authCreds, err = fetchAuthorizerCredentials(ctx, authorizeURL, flags.tokenSource, authReq)
	if err != nil {
		stopSpinner()
		uiErrf("Could not set up this pass: %v", err)
		uiInfof("Check the pass ID is correct and that your API key has access to it.")
		os.Exit(1)
	}
	stopSpinner()
	uiOKf("Stream connected.")

	// When the request supplied only --satellite-id, the authorizer resolved and
	// echoed back the next upcoming pass. Adopt it so S3 prefixes, stream close,
	// and command sending all reference the selected pass. Pin authReq to the
	// resolved pass before starting the refresher so credential refreshes stay on
	// this pass rather than re-resolving to a later one as time passes.
	if flags.passID == "" && authCreds.PassID != "" {
		flags.passID = authCreds.PassID
		authReq.PassID = authCreds.PassID
		vlogf("Defaulted to next upcoming pass for satellite %s: %s", flags.satelliteID, authCreds.PassID)
	}

	// Pin the resolved stream ID so credential refreshes REUSE this stream rather
	// than opening a new one. Without it, a commanding-enabled refresh would be
	// refused (409) by this stream's own commanding lock, and the refresh would
	// fail until the credentials expired.
	if authCreds.StreamID != "" {
		authReq.StreamID = authCreds.StreamID
	}

	credStore = newCredentialStore(authCreds)
	go runCredentialRefresher(ctx, credStore, authorizeURL, flags.tokenSource, authReq)

	if authCreds.S3Bucket == "" {
		log.Fatal("Bucket not provided by authorizer.")
	}
	vlogf("Using bucket from authorizer: %s", authCreds.S3Bucket)
	if authCreds.Environment != "" {
		env = authCreds.Environment
	}

	if authCreds.Streams.HighRate != nil {
		prefix = authCreds.Streams.HighRate.S3Prefix
	} else {
		prefix = buildPrefix(flags, env, flags.passID)
	}
	return authCreds, credStore, env, prefix
}

// runOpenStream is the main entry point for the open-stream command. It
// orchestrates credential fetching, S3/MQTT readers, command senders, and
// shutdown/diagnostics.
// configureChannelRates fills cfg.HighRateChannelIDs and cfg.ChannelFramings from
// the authorizer's per-channel metadata (accurate rate_class from the execution
// config). When the response carries no per-channel rate classes, it falls back
// to deriving the classification from the granted MQTT topics.
func configureChannelRates(cfg *Config, authCreds *AuthorizerCredentials) {
	if hr := authCreds.HighRateChannelIDs(); len(hr) > 0 {
		cfg.HighRateChannelIDs = hr
		logChannelClassification(authCreds.Channels)
	} else if authCreds != nil && len(authCreds.Streams.LowRate) > 0 {
		lowRateSet := make(map[string]bool)
		for _, lr := range authCreds.Streams.LowRate {
			if !strings.Contains(lr.MqttTopic, "/downlink/") {
				continue
			}
			if chID := ExtractChannelIDFromTopic(lr.MqttTopic); chID != "" {
				lowRateSet[chID] = true
			}
		}
		if len(lowRateSet) > 0 {
			cfg.HighRateChannelIDs = make([]string, 0, len(cfg.ChannelIDs))
			for _, chID := range cfg.ChannelIDs {
				if !lowRateSet[chID] {
					cfg.HighRateChannelIDs = append(cfg.HighRateChannelIDs, chID)
				}
			}
		}
	}

	// Per-channel framings from the authorizer metadata: the S3 reader uses these
	// to probe only the framings a channel actually emits.
	if authCreds != nil {
		for _, ch := range authCreds.Channels {
			if len(ch.Framings) > 0 {
				if cfg.ChannelFramings == nil {
					cfg.ChannelFramings = make(map[string][]string)
				}
				cfg.ChannelFramings[ch.ChannelID] = ch.Framings
			}
		}
	}
}

// proxyListenAddr returns the listen address the selected proxy mode binds.
func proxyListenAddr(mode proxy.Mode, flags *flagSet) string {
	if mode == proxy.ModeUDP {
		return flags.udpListenAddr
	}
	return flags.tcpListenAddr
}

// warnProxyExposed prints the network-exposure warning for a proxy listening
// on a non-loopback address. The proxy sockets carry no authentication, so
// this is only reached behind the explicit --proxy-allow-remote opt-in.
func warnProxyExposed(listenAddr string, uplinkWired bool) {
	uiWarnf("The proxy is listening on %q, which other machines on the network can reach.", listenAddr)
	if uplinkWired {
		uiWarnf(
			"Any host that connects can receive this pass downlink and inject data that is " +
				"transmitted to the satellite, without authentication.",
		)
	} else {
		uiWarnf("Any host that connects can receive this pass downlink, without authentication.")
	}
	uiWarnf("Only use --proxy-allow-remote on a network you fully control.")
}

// buildProxyUplinkFunc wires the proxy's uplink callback. When uplink is
// enabled and authorizer credentials are available, proxy-received bytes are
// transmitted to the satellite over the same MQTT commanding path interactive
// mode uses. Otherwise the proxy runs downlink-only and the callback reports,
// once, that received data is dropped. The returned sender is nil when no
// uplink was wired; a non-nil error aborts the stream (an uplink channel
// exists but the commanding connection could not be established, and silently
// degrading a requested uplink is worse than stopping).
func buildProxyUplinkFunc(
	ctx context.Context, cfg *Config, passID string, stats *statsTracker,
) (proxy.UplinkFunc, *proxyUplinkSender, error) {
	if cfg.EnableUplink && cfg.AuthorizerCreds != nil {
		sender, err := newProxyUplinkSender(ctx, *cfg, passID, stats)
		switch {
		case err == nil:
			return sender.send, sender, nil
		case errors.Is(err, errNoUplinkChannel):
			uiWarnf("This pass has no uplink channel. Data sent to the proxy socket will not be transmitted.")
		default:
			return nil, nil, fmt.Errorf("connect proxy uplink: %w", err)
		}
	}

	var droppedOnce sync.Once
	return func(data []byte) {
		droppedOnce.Do(func() {
			uiWarnf(
				"The proxy received %d bytes to uplink, but uplink is not available on this stream; "+
					"the data was not transmitted.", len(data),
			)
		})
		vlogf("proxy uplink: dropped %d bytes (uplink not available)", len(data))
	}, nil, nil
}

// startProxyIfConfigured starts the local socket proxy when --proxy is set,
// wiring cfg.ProxyCh so the readers publish received telemetry to it, and the
// uplink callback so bytes received from local clients are transmitted to the
// satellite (when uplink is enabled and credentials are available).
//
// Listen addresses default to loopback; a routable address is refused unless
// --proxy-allow-remote is set, and then a warning is printed, because the
// proxy sockets accept unauthenticated peers.
//
// It returns a cleanup function to run on exit; when proxying is disabled the
// cleanup is a no-op.
func startProxyIfConfigured(
	ctx context.Context, flags *flagSet, cfg *Config, passID string, stats *statsTracker,
) (cleanup func(), err error) {
	proxyMode, err := proxy.ParseMode(flags.proxyMode)
	if err != nil {
		return nil, err
	}
	if proxyMode == proxy.ModeDisabled {
		return func() {}, nil
	}

	listenAddr := proxyListenAddr(proxyMode, flags)
	remote, err := proxy.ValidateListenAddr(listenAddr, flags.proxyAllowRemote)
	if err != nil {
		return nil, err
	}

	downlinkCh := make(chan []byte, proxyDownlinkChanBuf)
	cfg.ProxyCh = downlinkCh
	uplinkFn, sender, err := buildProxyUplinkFunc(ctx, cfg, passID, stats)
	if err != nil {
		return nil, err
	}

	if remote {
		warnProxyExposed(listenAddr, sender != nil)
	}

	var p proxy.Proxy
	switch proxyMode {
	case proxy.ModeUDP:
		p, err = proxy.NewUDP(
			ctx,
			proxy.UDPConfig{ListenAddr: flags.udpListenAddr, SendAddr: flags.udpSendAddr},
			downlinkCh, uplinkFn,
		)
	case proxy.ModeTCP:
		p, err = proxy.NewTCP(
			ctx,
			proxy.TCPConfig{ListenAddr: flags.tcpListenAddr},
			downlinkCh, uplinkFn,
		)
	case proxy.ModeDisabled:
		// unreachable: handled above.
	}
	if err != nil {
		if sender != nil {
			sender.close()
		}
		return nil, fmt.Errorf("start proxy: %w", err)
	}
	go func() {
		if err := p.Start(); err != nil {
			log.Printf("proxy: %v", err)
		}
	}()
	return func() {
		_ = p.Close()
		if sender != nil {
			sender.close()
		}
	}, nil
}

func runOpenStream(cmd *cobra.Command, flags *flagSet) error {
	// Tidy up logging and decide how chatty to be before anything prints.
	configureStreamOutput(flags.enableVerbose, flags.enableDebug)

	// Read shared flags from root command.
	apiURL, err := resolveAPIBaseURL(cmd)
	if err != nil {
		return err
	}
	flags.apiURL = apiURL

	// Attempt to load credentials. If they are absent we degrade to
	// direct-S3 mode (no authorizer call, no MQTT); if they are present but
	// invalid we fail loudly so the user can fix them.
	ts, path, credErr := newTokenSource(cmd)
	switch {
	case credErr == nil:
		flags.tokenSource = ts
	case os.IsNotExist(errors.Unwrap(credErr)):
		uiWarnf("No API key found (looked in %s).", path)
		uiInfof("Running in limited mode: reading previously downloaded files only.")
		uiInfof("To receive a live pass, run `stellar auth activate-api-key <key-file>` first.")
	default:
		return credErr
	}

	// If credentials are available, the authorizer flow is active (call <api-url>/authorize).
	// Without credentials, only direct S3 mode is available.
	useAuthorizer := flags.tokenSource != nil

	srcType := determineSourceType(flags)
	channelIDs, err := parseChannelIDs(flags)
	if err != nil {
		return err
	}
	validateConfig(flags, channelIDs, useAuthorizer)
	effectivePassID := flags.passID
	effectiveEnv := getEffectiveEnvironment()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	authCreds, credStore, effectiveEnv, prefix := fetchAndApplyAuthorizerCredentials(
		ctx, flags, effectivePassID, effectiveEnv, channelIDs,
	)
	// fetchAndApplyAuthorizerCredentials may resolve flags.passID from --satellite-id.
	effectivePassID = flags.passID

	// Release the authorizer session (and its commanding lock) on every exit path.
	// Defined here, immediately after authorization, so it also covers the abort
	// paths below (S3-verify failure, missing channel IDs) that run before the full
	// Config is assembled. It captures the raw authorizer parameters rather than
	// cfg for that reason. Once-guarded so the deferred call and any explicit
	// pre-exit call together fire at most once.
	var closeStreamOnce sync.Once
	closeStream := func() {
		closeStreamOnce.Do(func() {
			if !useAuthorizer || authCreds == nil {
				return
			}
			closeAuthorizerSession(flags.apiURL, flags.tokenSource, effectivePassID, authCreds.StreamID)
		})
	}
	defer closeStream()

	bucket := getBucket(authCreds)
	if bucket == "" {
		cancel()
		return errors.New(
			"could not determine where to read this pass's data from.\n" +
				"Ensure your API key is activated (`stellar auth activate-api-key <key-file>`) so the CLI can prepare the pass",
		)
	}

	s3c := initializeS3ClientWithCreds(ctx, flags, bucket, authCreds, credStore)
	if s3c != nil {
		if verr := validateS3Access(ctx, s3c, bucket, prefix, authCreds); verr != nil {
			// High-rate channels are read only from S3, so losing S3 access is fatal
			// for them. A low-rate-only stream, by contrast, receives all telemetry
			// over MQTT and uses S3 only for catch-up, so it can continue, degraded,
			// rather than aborting the whole pass on a transient IAM propagation lag.
			hasHighRate := authCreds != nil && len(authCreds.HighRateChannelIDs()) > 0
			canDegradeToMQTT := authCreds != nil && !hasHighRate && len(authCreds.Streams.LowRate) > 0
			if canDegradeToMQTT {
				uiWarnf("Could not verify S3 access (%v).", verr)
				uiWarnf(
					"Continuing with MQTT low-rate telemetry only; S3 catch-up is unavailable until access propagates.",
				)
			} else {
				uiErrf("Error: %v", verr)
				closeStream()
				cancel()
				os.Exit(1)
			}
		}
	}

	if useAuthorizer && authCreds != nil && len(channelIDs) == 0 {
		channelIDs = collectChannelIDsFromAuthorizerStreams(authCreds.Streams)
		if len(channelIDs) > 0 {
			vlogf("using %d channel ID(s) from authorizer streams", len(channelIDs))
		}
	}
	if flags.downlinkEnabled() && len(channelIDs) == 0 {
		closeStream() // already released explicitly; the deferred call above is now a no-op
		cancel()
		uiErrf(
			"Error: downlink enabled but no channel IDs: omit --channels only when using the authorizer (--api-url and activated credentials)",
		)
		os.Exit(1) //nolint:gocritic // closeStream is sync.Once-guarded and already called above
	}

	cfg := buildConfigWithCreds(flags, srcType, prefix, effectivePassID, effectiveEnv,
		channelIDs, authCreds, credStore, bucket)

	// Classify channels by rate class and record per-channel framings from the
	// authorizer metadata (falling back to the granted MQTT topics when the
	// response has no per-channel rate classes).
	configureChannelRates(&cfg, authCreds)

	if useAuthorizer {
		cfg.AuthorizerAPI = flags.apiURL
		cfg.TokenSource = flags.tokenSource
	}
	validateFeatureFlags(flags)

	// closeStream (defined right after authorization above) releases the session
	// and its commanding lock; its defer covers one-shot commands, early errors,
	// and graceful shutdown. Note that os.Exit / log.Fatal skip defers, so those
	// hard-exit paths must call closeStream() explicitly before exiting;
	// otherwise the commanding lock leaks until an explicit close or override.
	handleOneShotCommands(ctx, flags, cfg, effectivePassID, closeStream)
	if flags.sendSatCommand != "" || flags.sendSatCommands != "" || flags.sendGsConfig != "" {
		cancel()
		return nil
	}

	if err := os.MkdirAll(cfg.DestDir, 0o750); err != nil {
		cancel()
		return fmt.Errorf("create dest dir: %w", err)
	}

	defer cancel()

	stats := newStatsTracker(cfg.EnableDiagnostics)
	stats.SetEnableStats(cfg.EnableStats)
	stats.SetPassID(effectivePassID)
	stats.SetStreamID(getStreamID(cfg))
	if authCreds != nil && authCreds.ClientID != "" {
		stats.SetClientID(authCreds.ClientID)
	}
	// Register the expected downlink channels so the live dashboard shows a row per
	// channel from the start, split by rate class.
	if authCreds != nil && len(authCreds.Channels) > 0 {
		var low, high []string
		for _, ch := range authCreds.Channels {
			switch {
			case ch.Direction == directionUplink:
				// uplink is not a telemetry channel
			case ch.RateClass == rateClassHighRate:
				high = append(high, ch.ChannelID)
			case ch.Direction == directionDownlink:
				low = append(low, ch.ChannelID)
			}
		}
		stats.SetDisplayChannels(low, high)
	}

	// Start proxy if configured.
	proxyCleanup, err := startProxyIfConfigured(ctx, flags, &cfg, effectivePassID, stats)
	if err != nil {
		return err
	}
	defer proxyCleanup()

	var wg sync.WaitGroup
	errCh := make(chan error, mainErrChBuf)

	announceStreamReady(cfg, flags, effectivePassID)

	var idleClosed atomic.Bool
	statsCancel := startStatsLogger(ctx, stats)
	startHighRateReader(ctx, cfg, s3c, stats, &wg, errCh)
	startMQTTReader(ctx, cfg, s3c, stats, &wg, errCh)
	startInteractiveCommandSender(ctx, flags, cfg, effectivePassID, stats, &wg, errCh, statsCancel)
	// Auto-close the stream once the pass has ended and it has gone idle, warning
	// at 60/30/10 seconds first (TC-ST-005). Disabled if the pass end time can't be
	// resolved (e.g. no credentials / limited mode).
	startIdleShutdownMonitor(ctx, cancel, flags, effectivePassID, stats, &idleClosed)
	waitForCompletion(ctx, cfg, stats, s3c, &wg, errCh, statsCancel, closeStream, &idleClosed)
	return nil
}

// announceStreamReady prints a friendly summary of what the stream is about to
// do (which data types it will receive and where files are saved) so the
// operator knows what to expect before data starts flowing.
func announceStreamReady(cfg Config, flags *flagSet, passID string) {
	uiStreamBanner(passID)

	var receiving []string
	if cfg.EnableDownlink {
		receiving = append(receiving, kindTelemetry.tag())
	}
	if cfg.EnableMonitoring {
		receiving = append(receiving, kindMonitoring.tag())
	}
	if cfg.EnableEvent {
		receiving = append(receiving, kindEvent.tag())
	}
	if cfg.EnableConfigState {
		receiving = append(receiving, kindConfig.tag())
	}
	if len(receiving) > 0 {
		uiStepf("Receiving: %s", joinSegs(receiving))
	}
	if cfg.EnableUplink || cfg.EnableConfigRequests {
		// Commanding is two independent command types over the commanding path (not
		// two channels): satellite uplink and ground-station config-request.
		var kinds []string
		if cfg.EnableUplink {
			kinds = append(kinds, "satellite uplink")
		}
		if cfg.EnableConfigRequests {
			kinds = append(kinds, "ground-station config-request")
		}
		detail := strings.Join(kinds, " + ")
		if flags.interactive {
			uiStepf("Commanding: interactive, %s (type commands below)", detail)
		} else {
			uiStepf("Commanding: enabled, %s", detail)
		}
	}
	uiStepf("Saving files to %s", cfg.DestDir)
	uiDimf("  Waiting for data... press Ctrl-C to stop.")
	fmt.Fprintln(uiOut)
}
