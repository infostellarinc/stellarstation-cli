package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// Message type constants
const (
	msgTypeMonitoring = "monitoring"
	msgTypeConfig     = "config"
	msgTypeEvent      = "event"
)

// runMQTTReader subscribes to MQTT topic and processes messages.

// mqttConnectionConfig is the resolved set of params used to connect + subscribe.
type mqttConnectionConfig struct {
	broker   string
	topics   []string // List of topics to subscribe to
	clientID string

	// Certificate-based mutual TLS (port 8883). The broker URL is ssl://<endpoint>:8883.
	// This is the only supported authentication method.
	tlsCertificate *tls.Certificate
}

// resolveMQTTClientID appends a random numeric suffix to the authorizer-supplied
// clientID. The authorizer authorizes client IDs of the form "<clientID>-<suffix>",
// so the bare clientID would be rejected, and the random suffix also keeps
// concurrent sessions from colliding on the same connection identity.
func resolveMQTTClientID(credsClientID string) string {
	nBig, err := rand.Int(rand.Reader, big.NewInt(1_000_000_000))
	if err != nil {
		return fmt.Sprintf("%s-%d", credsClientID, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%d", credsClientID, nBig.Int64())
}

func buildMQTTConnectionConfig(ctx context.Context, cfg Config) (*mqttConnectionConfig, error) {
	// Authorizer is required for MQTT features
	if cfg.AuthorizerCreds == nil {
		return nil, errors.New(
			"authorizer credentials required for MQTT connection: use --api-url and `stellar auth activate-api-key` (or --credentials)",
		)
	}
	creds := cfg.AuthorizerCreds

	// Certificate-based mutual TLS (port 8883) is the only supported auth method.
	// The authorizer provisions a fresh X.509 certificate for each request.
	if creds.IoTCertificatePem == "" || creds.IoTPrivateKeyPem == "" {
		return nil, errors.New(
			"authorizer did not provision an IoT certificate; MQTT not available for this stream",
		)
	}
	vlogf("Using certificate-based MQTT mutual TLS (port 8883)")
	return buildMQTTConnectionConfigFromCert(ctx, cfg, creds)
}

// normalizeConfigTopic converts /config to /config_state (but not /config_request).
// This handles the case where the authorizer returns topics with /config but
// the actual MQTT topic should be /config_state.
func normalizeConfigTopic(topic string) string {
	if strings.HasSuffix(topic, "/config") && !strings.HasSuffix(topic, "/config_request") {
		return strings.TrimSuffix(topic, "/config") + "/config_state"
	}
	return topic
}

// collectMQTTTopics returns all MQTT topics from the authorizer streams config.
func collectMQTTTopics(streams StreamsConfig) []string {
	var topics []string
	for _, stream := range streams.LowRate {
		if stream.MqttTopic != "" {
			topics = append(topics, stream.MqttTopic)
		}
	}
	if streams.Monitoring != nil && streams.Monitoring.MqttTopic != "" {
		topics = append(topics, streams.Monitoring.MqttTopic)
	}
	if streams.Config != nil && streams.Config.MqttTopic != "" {
		// Convert /config to /config_state (but not /config_request)
		configTopic := normalizeConfigTopic(streams.Config.MqttTopic)
		topics = append(topics, configTopic)
	}
	if streams.Event != nil && streams.Event.MqttTopic != "" {
		topics = append(topics, streams.Event.MqttTopic)
	}
	// Subscribe to command ack topics for all channels
	for _, uplink := range streams.Uplink {
		if uplink.AckTopic != "" {
			topics = append(topics, uplink.AckTopic)
		}
	}
	for _, configReq := range streams.ConfigRequest {
		if configReq.AckTopic != "" {
			topics = append(topics, configReq.AckTopic)
		}
	}
	return collapseChannelWildcards(topics)
}

// collapseChannelWildcards rewrites the per-channel segment of each topic to the
// MQTT single-level wildcard "+" and de-duplicates the result, preserving order.
//
// Without this, the streamer subscribes to one concrete topic per (channel,
// message-type): for a multi-channel pass with command acks that reaches ~57
// distinct subscriptions on a single MQTT connection. AWS IoT Core silently caps
// a connection at 50 subscriptions and drops the rest. Because the command-ack
// topics are collected last, the config_request/ack subscriptions beyond the cap
// were never registered and those acks were never delivered, while the earlier
// uplink/ack subscriptions stayed under the cap and worked. Collapsing
// "channel/<id>/..." to "channel/+/..." reduces the set to a handful of wildcard
// filters, well under the limit. The message handler already routes by parsing
// each message's concrete topic, so wildcard delivery needs no other change, and
// the credentials the authorizer issues cover the wildcard subscriptions.
func collapseChannelWildcards(topics []string) []string {
	seen := make(map[string]bool, len(topics))
	var out []string
	for _, t := range topics {
		w := channelWildcard(t)
		if seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, w)
	}
	return out
}

// channelWildcard replaces the channel identifier in a topic
// (".../channel/<id>/...") with the MQTT single-level wildcard "+". Topics without
// a channel segment (pass-level monitoring/config_state/event) are returned
// unchanged.
func channelWildcard(topic string) string {
	const seg = "/channel/"
	i := strings.Index(topic, seg)
	if i < 0 {
		return topic
	}
	start := i + len(seg)
	rest := topic[start:]
	j := strings.Index(rest, "/")
	if j < 0 {
		// No trailing segment after the channel id; leave unchanged.
		return topic
	}
	return topic[:start] + "+" + rest[j:]
}

// buildMQTTConnectionConfigFromCert builds an MQTT connection config using an X.509 certificate
// for mutual TLS on port 8883. The certificate is provisioned by the authorizer and
// is only valid for the duration of the pass (it is never stored).
func buildMQTTConnectionConfigFromCert(
	_ context.Context,
	_ Config,
	creds *AuthorizerCredentials,
) (*mqttConnectionConfig, error) {
	// Parse the PEM certificate and private key returned by the authorizer.
	cert, err := tls.X509KeyPair([]byte(creds.IoTCertificatePem), []byte(creds.IoTPrivateKeyPem))
	if err != nil {
		return nil, fmt.Errorf("parse IoT certificate/key from authorizer: %w", err)
	}

	endpoint := creds.IoTCertEndpoint
	if endpoint == "" {
		return nil, errors.New(
			"authorizer did not provide an IoT endpoint for certificate connection",
		)
	}

	topics := collectMQTTTopics(creds.Streams)
	if len(topics) == 0 {
		vlogf(
			"WARNING: No MQTT topics in authorizer response. Streams: LowRate=%d, Monitoring=%v, Config=%v, Event=%v",
			len(creds.Streams.LowRate),
			creds.Streams.Monitoring != nil,
			creds.Streams.Config != nil,
			creds.Streams.Event != nil,
		)
	}

	mqttClientID := resolveMQTTClientID(creds.ClientID)
	if creds.ClientID != "" {
		vlogf("Certificate TLS: clientID=%s (base: %s)", mqttClientID, creds.ClientID)
	} else {
		vlogf("Certificate TLS: generated clientID=%s", mqttClientID)
	}

	broker := "ssl://" + net.JoinHostPort(endpoint, strconv.Itoa(mqttPort))
	vlogf(
		"Starting AWS IoT MQTT reader (certificate TLS port 8883)\n  endpoint=%s\n  certId=%s\n  topics=%v\n  clientId=%s",
		endpoint,
		creds.IoTCertificateID,
		topics,
		mqttClientID,
	)

	return &mqttConnectionConfig{
		broker:         broker,
		topics:         topics,
		clientID:       mqttClientID,
		tlsCertificate: &cert,
	}, nil
}

// buildMQTTClientOptions creates paho ClientOptions for the given connection config.
// Certificate-based mutual TLS on port 8883 is the only supported authentication method.
func buildMQTTClientOptions(
	connCfg *mqttConnectionConfig,
	connectConfirmed chan struct{},
) *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(connCfg.broker)
	opts.SetClientID(connCfg.clientID)
	// AWS IoT requires MQTT 3.1.1
	opts.SetProtocolVersion(4) // 4 = MQTT 3.1.1

	// Certificate-based mutual TLS (port 8883).
	// Supply the X.509 client certificate; the server cert is validated against
	// the system root CAs (Amazon's ATS root is included in all major OS cert stores).
	if connCfg.tlsCertificate != nil {
		opts.SetTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{*connCfg.tlsCertificate},
			MinVersion:   tls.VersionTLS12,
		})
		vlogf("Configured MQTT client with X.509 certificate TLS (port 8883)")
	}

	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(mqttReconnectInterval)
	opts.SetConnectTimeout(mqttConnectTimeout)
	opts.SetPingTimeout(mqttPingTimeout)
	// Deliver inbound messages concurrently instead of on a single ordered router
	// goroutine. The telemetry handler routes each message to its channel's own
	// worker queue (see keyed_result_router.go); under a heavy downlink flood on
	// one channel that would otherwise stall a single shared router, so command
	// acks (uplink/config_request) arriving during the flood could queue behind
	// telemetry and be effectively missed, which is why early sat acks arrived
	// but later gs acks did not. Handlers guard their shared state with mu (see
	// handleTelemetryMessage / trackNonTelemetryTimestamp) and stats is
	// mutex-backed, so concurrent delivery is safe; ordering is reconstructed
	// downstream from message indices (and, per channel, within that channel's
	// own worker).
	opts.SetOrderMatters(false)
	opts.SetWriteTimeout(mqttWriteTimeout)
	opts.SetResumeSubs(true)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		vlogf("Unexpected message on topic %s", msg.Topic())
	})

	// connectCount distinguishes the initial connect (routine, already covered by
	// the "Stream connected." banner) from a later reconnect (see
	// OnConnectionLost/OnReconnecting below for why a reconnect must be visible by
	// default).
	connectCount := 0
	opts.OnConnect = func(client mqtt.Client) {
		connectCount++
		vlogf("MQTT client connected successfully")
		if connectCount > 1 {
			log.Printf("MQTT reconnected; subscriptions restored")
		}
		if connectConfirmed != nil {
			select {
			case connectConfirmed <- struct{}{}:
			default:
			}
		}
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		// Always visible, not gated behind --verbose: a broker-side disconnect
		// mid-pass can silently drop in-flight command acks (there is no S3/replay
		// backstop for acks, unlike telemetry, which the S3 fallback scanner can
		// backfill); a gap in received acks with no visible cause is worse than
		// one extra line of output. The deeper technical detail (raw error type,
		// TLS diagnosis) stays verbose-only.
		if err != nil {
			log.Printf("MQTT connection lost (%v); reconnecting, command acks or telemetry received during the outage may be missing", err)
		} else {
			log.Printf("MQTT connection lost; reconnecting, command acks or telemetry received during the outage may be missing")
		}
		if err != nil {
			errStr := err.Error()
			vlogf("Connection lost error details: %v (type: %T)", err, err)
			if strings.Contains(errStr, "tls") || strings.Contains(errStr, "certificate") ||
				strings.Contains(errStr, "x509") || strings.Contains(errStr, "handshake") {
				vlogf(
					"TLS error: the certificate may be invalid, revoked, or expired",
				)
			}
		} else {
			vlogf("Connection lost with no error (client.IsConnected()=%v)", client.IsConnected())
		}
	}
	opts.OnReconnecting = func(client mqtt.Client, opts *mqtt.ClientOptions) {
		vlogf("MQTT client reconnecting...")
		// Paho reuses the TLS config on reconnect automatically.
		// A newer certificate provisioned by the credential refresher will be picked up
		// the next time the clientFactory is called (on a hard retry cycle).
	}

	return opts
}

// isMQTTRetryableError reports whether the given connection error should trigger a retry.
func isMQTTRetryableError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "timeout") ||
		strings.Contains(s, "CONNACK not received") ||
		strings.Contains(s, "not currently connected") ||
		strings.Contains(s, "OnConnect callback did not fire") ||
		strings.Contains(s, "bad handshake")
}

// nextMQTTBackoff returns the next exponential backoff duration, capped at maxBackoff.
func nextMQTTBackoff(current, maxBackoff time.Duration) time.Duration {
	next := time.Duration(float64(current) * 2.0)
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

// connectMQTTClientWithRetry attempts to connect to MQTT with exponential backoff retries.
// Handles transient connection issues such as TLS timeouts and CONNACK delays.
// The clientFactory is called on each attempt so a refreshed IoT certificate can be used.
// Returns the successfully connected client, or an error if all retries are exhausted.
func connectMQTTClientWithRetry(
	ctx context.Context,
	clientFactory func() (mqtt.Client, string, error), // Returns (client, brokerURL, error)
	connectConfirmed chan struct{},
	connCfg *mqttConnectionConfig,
) (mqtt.Client, error) {
	const maxRetries = mqttMaxConnectRetries

	var lastErr error
	backoff := mqttRetryBackoffInit
	var client mqtt.Client

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 && client != nil {
			vlogf("Disconnecting previous client before retry...")
			client.Disconnect(mqttDisconnectQuiesce)
		}

		if attempt > 0 {
			vlogf(
				"Retrying MQTT connection (attempt %d/%d) after %v backoff...",
				attempt+1,
				maxRetries,
				backoff,
			)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(backoff):
			}
		}

		var broker string
		var err error
		client, broker, err = clientFactory()
		if err != nil {
			lastErr = fmt.Errorf("failed to create MQTT client: %w", err)
			log.Printf("Error creating client: %v", lastErr)
			// Don't retry client creation errors
			return nil, lastErr
		}

		// Reset the connectConfirmed channel for this attempt
		// (drain any leftover signals from previous attempts)
		select {
		case <-connectConfirmed:
		default:
		}

		err = connectMQTTClient(ctx, client, broker, connectConfirmed, connCfg)
		if err == nil {
			if attempt > 0 {
				vlogf("MQTT connection succeeded on attempt %d/%d", attempt+1, maxRetries)
			}
			return client, nil
		}

		lastErr = err

		if !isMQTTRetryableError(err) {
			vlogf("Non-retryable error, not retrying: %v", err)
			return nil, err
		}

		backoff = nextMQTTBackoff(backoff, mqttRetryBackoffMax)
	}

	return nil, fmt.Errorf(
		"MQTT connection failed after %d attempts (last error: %w)",
		maxRetries,
		lastErr,
	)
}

// startMQTTConnectToken initiates the MQTT Connect() call in a goroutine and returns a channel
// that receives the token result. The returned channel has a buffer of 1.
func startMQTTConnectToken(client mqtt.Client) chan error {
	connectDone := make(chan error, 1)
	connectStarted := make(chan struct{})
	go func() {
		vlogf("Initiating MQTT connection...")
		close(connectStarted)
		token := client.Connect()
		vlogf("Connect() called, waiting for token...")
		ok := token.WaitTimeout(mqttConnectTimeout)
		if !ok {
			isConnected := client.IsConnected()
			vlogf(
				"Connection token wait timeout after %v (client.IsConnected()=%v)",
				mqttConnectTimeout,
				isConnected,
			)
			if isConnected {
				connectDone <- errors.New(
					"connection token wait timeout but client reports connected - connection may have completed",
				)
			} else {
				connectDone <- fmt.Errorf(
					"connection token wait timeout after %v (client not connected)",
					mqttConnectTimeout,
				)
			}
			return
		}
		tokenErr := token.Error()
		if tokenErr != nil {
			vlogf("Connection token returned error: %v (type: %T)", tokenErr, tokenErr)
		} else {
			vlogf("Connection token completed successfully (no error)")
		}
		vlogf(
			"After token completion: token.Error()=%v, client.IsConnected()=%v",
			tokenErr,
			client.IsConnected(),
		)
		connectDone <- tokenErr
	}()
	<-connectStarted
	return connectDone
}

// logMQTTTokenError logs detailed info when a connection token returns an error,
// including TLS-specific hints.
func logMQTTTokenError(err error, connCfg *mqttConnectionConfig) {
	vlogf("Connection token returned error: %v (type: %T)", err, err)
	errStr := fmt.Sprintf("%v", err)
	if strings.Contains(errStr, "tls") || strings.Contains(errStr, "x509") ||
		strings.Contains(errStr, "certificate") {
		vlogf(
			"WARNING: TLS error: the certificate for client ID %s may be invalid or inactive",
			connCfg.clientID,
		)
	}
}

// waitForConnAckAfterTokenSuccess waits for CONNACK after the connection token completes successfully.
func waitForConnAckAfterTokenSuccess(
	connectConfirmed chan struct{},
	combinedTimeout *time.Timer,
) (bool, error) {
	select {
	case <-connectConfirmed:
		vlogf("CONNACK received after connection token success")
		return true, nil
	case <-time.After(mqttConnAckWait):
		return false, fmt.Errorf(
			"connection token succeeded but CONNACK not received within %v",
			mqttConnAckWait,
		)
	case <-combinedTimeout.C:
		return false, errors.New("CONNACK not received within timeout")
	}
}

// waitForMQTTConnAck waits for either the connection token or CONNACK to signal success.
// Returns (connAckReceived bool, err error).
func waitForMQTTConnAck(
	connectCtx context.Context,
	connectDone chan error,
	connectConfirmed chan struct{},
	combinedTimeout *time.Timer,
	connCfg *mqttConnectionConfig,
) (bool, error) {
	select {
	case <-connectCtx.Done():
		vlogf("Connection token context timed out, checking if CONNACK was received...")
		select {
		case <-connectConfirmed:
			vlogf("CONNACK received despite connection token timeout")
			return true, nil
		default:
			return false, errors.New("connection token timed out and CONNACK not received")
		}
	case err := <-connectDone:
		if err != nil {
			logMQTTTokenError(err, connCfg)
			return false, fmt.Errorf("connection token error: %w", err)
		}
		vlogf("Connection token completed successfully, waiting for CONNACK...")
		return waitForConnAckAfterTokenSuccess(connectConfirmed, combinedTimeout)
	case <-connectConfirmed:
		vlogf("CONNACK received - connection fully established")
		select {
		case err := <-connectDone:
			if err != nil {
				log.Printf("WARNING: CONNACK received but connection token reported error: %v", err)
			}
		default:
		}
		return true, nil
	case <-combinedTimeout.C:
		return false, fmt.Errorf(
			"connection timeout: CONNACK not received within %v",
			mqttCombinedTimeout,
		)
	}
}

// mqttConnectionFailedError builds a descriptive error for MQTT connection failures.
func mqttConnectionFailedError(
	connectErr error,
	broker string,
	connCfg *mqttConnectionConfig,
) error {
	clientID := "unknown"
	if connCfg != nil {
		clientID = connCfg.clientID
	}
	endpoint := broker
	if parsed, err := url.Parse(broker); err == nil {
		endpoint = parsed.Host
	}
	return fmt.Errorf(
		"MQTT connection failed: %v\n"+
			"Client ID: %s\n"+
			"Possible causes:\n"+
			"1. The stream's certificate is not yet active (usually instant, but may take a few seconds)\n"+
			"2. The stream's certificate is revoked or deactivated\n"+
			"3. Network/firewall blocking TLS on port 8883 to %s",
		connectErr,
		clientID,
		endpoint,
	)
}

func connectMQTTClient(
	ctx context.Context,
	client mqtt.Client,
	broker string,
	connectConfirmed chan struct{},
	connCfg *mqttConnectionConfig,
) error {
	if parsed, err := url.Parse(broker); err == nil {
		//nolint:gosec // G706: broker URL components are trusted values from config
		vlogf(
			"Attempting to connect to MQTT broker: scheme=%s host=%s",
			parsed.Scheme,
			parsed.Host,
		)
	} else {
		vlogf("Attempting to connect to MQTT broker (%d chars)", len(broker))
	}

	connectCtx, cancel := context.WithTimeout(ctx, mqttConnectTimeout)
	defer cancel()

	combinedTimeout := time.NewTimer(mqttCombinedTimeout)
	defer combinedTimeout.Stop()

	ticker := time.NewTicker(mqttProgressInterval)
	defer ticker.Stop()

	vlogf("Waiting for MQTT connection to complete (CONNACK required)...")
	connectDone := startMQTTConnectToken(client)
	connAckReceived, connectErr := waitForMQTTConnAck(
		connectCtx,
		connectDone,
		connectConfirmed,
		combinedTimeout,
		connCfg,
	)

	if connectErr != nil || !connAckReceived {
		return mqttConnectionFailedError(connectErr, broker, connCfg)
	}

	if !client.IsConnected() {
		return errors.New("MQTT client not connected despite CONNACK being received")
	}

	vlogf(
		"Successfully connected to MQTT broker (CONNACK received, connection fully established)",
	)
	return nil
}

// Message extraction helpers (protobuf-first)

// extractMessageIndex returns the telemetry message's dense per
// (pass, channel, framing) sequence number. message_index advances by one per
// message and matches the storage bundle key, so it is the value ordered
// writing, deduplication, and acks must use. Senders that do not set
// message_index leave it 0; first_frame_index then coincides with the message
// sequence only for one-frame bundles, which is all the fallback can offer.
func extractMessageIndex(msgProto *streaming.FromStarPassMessage) int {
	stm := msgProto.GetSendTelemetryMessage()
	if mi := stm.GetMessageIndex(); mi > 0 {
		return int(mi)
	}
	return int(stm.GetFirstFrameIndex())
}

// extractMessagePayload extracts the telemetry payload bytes from a protobuf message.
func extractMessagePayload(msgProto *streaming.FromStarPassMessage) []byte {
	var payload []byte
	if stm := msgProto.GetSendTelemetryMessage(); stm != nil {
		for _, t := range stm.GetTelemetry() {
			if d := t.GetData(); len(d) > 0 {
				payload = append(payload, d...)
			}
		}
	}
	return payload
}

// extractMessageFraming extracts the framing type from a telemetry message.
// Returns empty string if framing cannot be determined.
// framingAccepted reports whether a telemetry framing passes the --accepted-framing
// filter. An empty filter (or an unknown/empty framing) accepts everything, matching
// the live-MQTT handler's behaviour.
func framingAccepted(cfg Config, framing string) bool {
	if len(cfg.AcceptedFraming) == 0 || framing == "" {
		return true
	}
	for _, accepted := range cfg.AcceptedFraming {
		if strings.EqualFold(framing, accepted) {
			return true
		}
	}
	return false
}

// framingFromDownlinkKey extracts the framing segment from a downlink telemetry
// S3 key (".../downlink/<FRAMING>/<index>" gives "<FRAMING>"), or "" if not present.
func framingFromDownlinkKey(key string) string {
	const marker = "/downlink/"
	i := strings.Index(key, marker)
	if i < 0 {
		return ""
	}
	rest := key[i+len(marker):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return ""
}

func extractMessageFraming(msgProto *streaming.FromStarPassMessage) string {
	if stm := msgProto.GetSendTelemetryMessage(); stm != nil {
		if len(stm.GetTelemetry()) > 0 {
			return stm.GetTelemetry()[0].GetFraming().String()
		}
	}
	return ""
}

func extractMessageMetadata(
	msgProto *streaming.FromStarPassMessage,
	msg mqtt.Message,
) map[string]string {
	metadata := make(map[string]string)
	if msg != nil && msg.MessageID() > 0 {
		metadata["messageid"] = strconv.Itoa(int(msg.MessageID()))
	}

	// END detection from protobuf
	if stm := msgProto.GetSendTelemetryMessage(); stm != nil {
		if stm.GetType() == streaming.SendTelemetryMessage_END {
			metadata["messagetype"] = "END"
		}
	}

	return metadata
}

type ackSender struct {
	ackedMessages map[string]bool
	ackMu         sync.Mutex
	cfg           Config
	stats         *statsTracker
	// inFlight tracks ack publishes whose broker confirmation (PUBACK for QoS 1)
	// is still pending, so callers can Flush before disconnecting the MQTT client.
	// Otherwise the final ack of a pass can be cut off mid-publish ("connection
	// lost before Publish completed"). Add(1) happens under ackMu (see sendAck)
	// so it always happens-before Flush's Wait, which is required by sync.WaitGroup.
	inFlight sync.WaitGroup
	// closing is set (under ackMu) by Flush before it waits. Once set, sendAck
	// records no new in-flight publishes, guaranteeing no Add(1) can race the
	// Wait and take the counter from 0 to positive concurrently.
	closing bool
}

func newAckSender(cfg Config, stats *statsTracker) *ackSender {
	return &ackSender{
		ackedMessages: make(map[string]bool),
		cfg:           cfg,
		stats:         stats,
	}
}

// Flush waits (up to a short timeout) for all in-flight ack publishes to be
// confirmed by the broker, so a caller can disconnect the MQTT client cleanly
// without dropping the last ack. Safe to call on a nil sender.
func (as *ackSender) Flush() {
	if as == nil {
		return
	}
	// Stop recording new in-flight publishes before waiting. Setting closing
	// under ackMu (where sendAck also does its Add(1)) guarantees every Add
	// happens-before this Wait, so Wait never races an Add from 0.
	as.ackMu.Lock()
	as.closing = true
	as.ackMu.Unlock()
	done := make(chan struct{})
	go func() {
		as.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(ackFlushTimeout):
	}
}

func (as *ackSender) sendAck(client mqtt.Client, topic string, ackPayload []byte) {
	ackKey := topic + ":" + ackedMessageKey(ackPayload)
	as.ackMu.Lock()
	// Once Flush has begun (closing), record no new publishes: this Add(1) must
	// stay under the same lock so it always happens-before Flush's Wait.
	if as.closing || as.ackedMessages[ackKey] {
		as.ackMu.Unlock()
		return
	}
	as.ackedMessages[ackKey] = true
	as.inFlight.Add(1)
	as.ackMu.Unlock()

	ackTopic := buildAckTopic(topic)

	if !client.IsConnected() {
		log.Printf("WARNING: MQTT client not connected, cannot send ack for topic: %s", topic)
		as.ackMu.Lock()
		delete(as.ackedMessages, ackKey)
		as.ackMu.Unlock()
		as.inFlight.Done()
		return
	}

	// Receipt acks are published at QoS 0 (see ackPublishQoS): best-effort, no
	// PUBACK wait, so a high-volume end-of-pass burst neither times out nor
	// saturates the connection the command round-trip shares.
	ackToken := client.Publish(ackTopic, ackPublishQoS, false, ackPayload)
	as.stats.MarkReceivedMessageAcked(topic)

	go func() {
		defer as.inFlight.Done()
		if ackToken.WaitTimeout(ackFlushTimeout) && ackToken.Error() == nil {
			// Ack handed to the client (QoS 0: written to the network).
		} else if ackToken.Error() != nil {
			log.Printf(
				"ERROR: Failed to send ack for topic %s -> %s: %v",
				topic,
				ackTopic,
				ackToken.Error(),
			)
		}
		// No timeout branch: at QoS 0 the token completes on write, so a slow
		// broker no longer produces a spurious "publish timeout" per message.
	}()
}

func handleMonitoringMessage(
	msgProto *streaming.FromStarPassMessage,
	msg mqtt.Message,
	cfg Config,
	stats *statsTracker,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	sendAck func(string, []byte),
) bool {
	monitoringMsg := msgProto.GetMonitoringMessage()
	if monitoringMsg == nil {
		return false
	}
	if !cfg.EnableMonitoring {
		return true
	}

	// Verbose mode: Display ground station state information
	if cfg.EnableVerbose {
		if gsState := monitoringMsg.GetState(); gsState != nil {
			if antennaState := gsState.GetAntenna(); antennaState != nil {
				if antennaState.GetAzimuth() != nil && antennaState.GetElevation() != nil {
					log.Printf("VERBOSE: planId=%s azimuth=%.2f elevation=%.2f",
						monitoringMsg.GetPassId(),
						antennaState.GetAzimuth().GetMeasured(),
						antennaState.GetElevation().GetMeasured())
				}
			}
			if receiverState := gsState.GetReceiver(); receiverState != nil {
				log.Printf("VERBOSE: planId=%s central frequency=%.2f MHz",
					monitoringMsg.GetPassId(),
					float64(receiverState.GetCenterFrequencyHz())/1e6)
			}
		}
	}
	timestamp := getMessageTimestamp(monitoringMsg.GetRecordedAt())
	identity := nonTelemetryIdentity(monitoringMsg.GetIndex(), timestamp)
	duplicate := trackNonTelemetryTimestamp(mu, receivedNonTelemetryTimestamps, msgTypeMonitoring, identity)
	if duplicate {
		return true
	}
	stats.AddReceivedMessage(msg.Topic(), msgTypeMonitoring, 0, "")
	if cfg.EnableStats && monitoringMsg.GetPassId() != "" {
		stats.SetPassID(monitoringMsg.GetPassId())
	}

	msgBytes := len(msg.Payload())
	// Ack.received_at is receipt time (this side's clock at handling), not the
	// payload's capture time.
	ackPayload, err := buildNonTelemetryAck(
		cfg, "monitoring", monitoringMsg.GetIndex(), msgBytes, time.Now(), msgProto.GetMessageId(),
	)
	if err != nil {
		log.Printf("ERROR: Failed to build monitoring ack: %v", err)
		return true
	}
	sendAck(msg.Topic(), ackPayload)
	handleNonTelemetryMessage(
		monitoringMsg,
		cfg,
		stats,
		msgTypeMonitoring,
		monitoringMsg.GetPassId(),
		monitoringMsg.GetAntennaId(),
		SourceMQTT,
	)
	return true
}

// nonTelemetryMQTTParams holds the type-specific parameters for processing
// a non-telemetry MQTT message (config or event).
type nonTelemetryMQTTParams struct {
	enabled   bool
	msgType   string
	ackType   string
	timestamp int64
	// identity is the capture identity (see nonTelemetryIdentity); copies of
	// one capture share it, so only the first processes.
	identity  string
	planID    string
	antennaID string
	protoMsg  proto.Message
	// index is the payload message's capture index, echoed in the ack so the
	// ack identifies the exact message it answers. 0 when the sender does not
	// set it.
	index uint64
	// ackedMessageID is the enclosing FromStarPassMessage's message_id, echoed
	// as the ack's acked_message_id. Empty when the sender sets no ID.
	ackedMessageID string
}

func processNonTelemetryMQTTMsg(
	params nonTelemetryMQTTParams,
	msg mqtt.Message,
	cfg Config,
	stats *statsTracker,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	sendAck func(string, []byte),
) bool {
	if !params.enabled {
		return true
	}
	duplicate := trackNonTelemetryTimestamp(mu, receivedNonTelemetryTimestamps, params.msgType, params.identity)
	if duplicate {
		return true
	}
	stats.AddReceivedMessage(msg.Topic(), params.msgType, 0, "")

	msgBytes := len(msg.Payload())
	// Ack.received_at is receipt time (this side's clock at handling), not the
	// payload's capture time.
	ackPayload, err := buildNonTelemetryAck(
		cfg, params.ackType, params.index, msgBytes, time.Now(), params.ackedMessageID,
	)
	if err != nil {
		log.Printf("ERROR: Failed to build %s ack: %v", params.ackType, err)
		return true
	}
	sendAck(msg.Topic(), ackPayload)
	handleNonTelemetryMessage(
		params.protoMsg, cfg, stats, params.msgType,
		params.planID, params.antennaID, SourceMQTT,
	)
	return true
}

func handleConfigMessage(
	msgProto *streaming.FromStarPassMessage,
	msg mqtt.Message,
	cfg Config,
	stats *statsTracker,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	sendAck func(string, []byte),
) bool {
	configMsg := msgProto.GetConfigurationMessage()
	if configMsg == nil {
		return false
	}
	return processNonTelemetryMQTTMsg(nonTelemetryMQTTParams{
		enabled:        cfg.EnableConfigState,
		msgType:        msgTypeConfig,
		ackType:        "configuration",
		timestamp:      getMessageTimestamp(configMsg.GetRecordedAt()),
		identity:       nonTelemetryIdentity(configMsg.GetIndex(), getMessageTimestamp(configMsg.GetRecordedAt())),
		planID:         configMsg.GetPassId(),
		antennaID:      configMsg.GetAntennaId(),
		protoMsg:       configMsg,
		index:          configMsg.GetIndex(),
		ackedMessageID: msgProto.GetMessageId(),
	}, msg, cfg, stats, mu, receivedNonTelemetryTimestamps, sendAck)
}

func handleEventMessage(
	msgProto *streaming.FromStarPassMessage,
	msg mqtt.Message,
	cfg Config,
	stats *statsTracker,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	sendAck func(string, []byte),
) bool {
	eventMsg := msgProto.GetEventMessage()
	if eventMsg == nil {
		return false
	}
	return processNonTelemetryMQTTMsg(nonTelemetryMQTTParams{
		enabled:   cfg.EnableEvent,
		msgType:   msgTypeEvent,
		ackType:   "event",
		timestamp: getMessageTimestamp(eventMsg.GetRecordedAt()),
		// Indexed events identify by their sequence number, matching the S3
		// path; index 0 (the plan-lifecycle exporter publishes outside the
		// sequencer) falls back to recorded_at inside nonTelemetryIdentity.
		identity:       nonTelemetryIdentity(eventMsg.GetIndex(), getMessageTimestamp(eventMsg.GetRecordedAt())),
		planID:         eventMsg.GetPassId(),
		antennaID:      eventMsg.GetAntennaId(),
		protoMsg:       eventMsg,
		index:          eventMsg.GetIndex(),
		ackedMessageID: msgProto.GetMessageId(),
	}, msg, cfg, stats, mu, receivedNonTelemetryTimestamps, sendAck)
}

func getMessageTimestamp(recordedAt interface{}) int64 {
	if recordedAt == nil {
		return time.Now().UnixNano()
	}
	// Type assertion for protobuf Timestamp (has AsTime() method)
	type timestampInterface interface {
		AsTime() time.Time
	}
	if ts, ok := recordedAt.(timestampInterface); ok {
		return ts.AsTime().UnixNano()
	}
	return time.Now().UnixNano()
}

// nonTelemetryIdentity identifies one station capture across its fanout
// copies (the station publishes each monitoring/config capture to the
// pass-level topic and every per-channel topic). index is the designed
// identity; recorded_at is the fallback when a station does not set it.
// Empty identity ("", for messages carrying neither) means the message cannot
// be attributed to a capture and is never treated as a duplicate.
func nonTelemetryIdentity(captureIndex uint64, recordedAtNanos int64) string {
	if captureIndex > 0 {
		return "seq#" + strconv.FormatUint(captureIndex, 10)
	}
	if recordedAtNanos > 0 {
		return "t#" + strconv.FormatInt(recordedAtNanos, 10)
	}
	return ""
}

// trackNonTelemetryTimestamp checks whether a non-telemetry message with the given
// timestamp was already processed and, if not, records it.  Returns true when the
// message is a duplicate that should be skipped.
// The station sends the same message on both pass-level and per-channel
// topics, they share the same recordedAt timestamp, so this deduplication catches
// duplicates regardless of which topic they arrived from.
func trackNonTelemetryTimestamp(
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	msgType string,
	identity string,
) bool {
	if identity == "" {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	if receivedNonTelemetryTimestamps[msgType] == nil {
		receivedNonTelemetryTimestamps[msgType] = make(map[string]bool)
	}
	alreadyReceived := receivedNonTelemetryTimestamps[msgType][identity]
	if !alreadyReceived {
		receivedNonTelemetryTimestamps[msgType][identity] = true
	}
	return alreadyReceived
}

// telemetryDedupKey builds the key used for cross-source de-duplication in
// receivedIndices, from a channel identifier AND its framing. The live MQTT
// path holds the bare channel UUID while the S3 fallback path carries a
// trailing "/downlink" (it doubles as the stats/result key); both must map to
// the same entry or the same message arriving on both sources is counted
// twice, so the channel component is normalized to the bare UUID.
//
// Framing is part of the key because a multi-framing downlink republishes the SAME conceptual frame index once per
// declared framing, e.g. index 5 goes out as both a BITSTREAM message and an
// IQ message, each a distinct, intentional payload, not a duplicate. The
// high-rate S3 reader has always keyed everything by (channel, framing) for
// exactly this reason. Before framing was part of this key, a multi-framing
// low-rate channel's two same-indexed-but-different-framing messages collided
// in one shared dedup slot, so only whichever framing's copy of a given index
// was processed first survived and the other was silently dropped as a "the
// same message from MQTT and S3" duplicate, cutting a two-framing channel's
// received count roughly in half, with the two framings' surviving share
// determined by an MQTT/S3 timing race that varied unpredictably run to run.
func telemetryDedupKey(channelID, framing string) string {
	return strings.TrimSuffix(channelID, "/downlink") + "|" + framing
}

// isConfiguredChannel reports whether channelID (bare or with a "/downlink"
// suffix) is one of the configured channel IDs.
func isConfiguredChannel(configured []string, channelID string) bool {
	base := baseChannelID(channelID)
	for _, id := range configured {
		if strings.EqualFold(id, base) {
			return true
		}
	}
	return false
}

func handleTelemetryMessage(
	msgProto *streaming.FromStarPassMessage,
	msg mqtt.Message,
	cfg Config,
	stats *statsTracker,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	sequentialCounters map[string]*int,
	sendAck func(string, []byte),
) {
	if !cfg.EnableDownlink {
		return
	}

	// Framing filtering: Check if this framing type is accepted
	framing := extractMessageFraming(msgProto)
	if len(cfg.AcceptedFraming) > 0 && framing != "" {
		accepted := false
		for _, acceptedFraming := range cfg.AcceptedFraming {
			if strings.EqualFold(framing, acceptedFraming) {
				accepted = true
				break
			}
		}
		if !accepted {
			if cfg.EnableDebug {
				log.Printf(
					"DEBUG: Skipping telemetry with framing %s (not in accepted list: %v)",
					framing,
					cfg.AcceptedFraming,
				)
			}
			return
		}
	}
	channelID := ExtractChannelIDFromTopic(msg.Topic())
	if channelID == "" {
		log.Printf("WARNING: Could not extract channel ID from topic: %s", msg.Topic())
		return
	}

	// The MQTT subscriptions use per-channel wildcards to stay under AWS IoT's
	// subscription cap (see collapseChannelWildcards), so the broker delivers
	// telemetry for every channel of the pass. Only the configured channels are
	// processed: anything else is dropped here, before it reaches the totals,
	// the files, or the acks the server counts as deliveries.
	if len(cfg.ChannelIDs) > 0 && !isConfiguredChannel(cfg.ChannelIDs, channelID) {
		return
	}

	index := extractMessageIndex(msgProto)
	channelKey := telemetryDedupKey(channelID, framing)

	mu.Lock()
	if receivedIndices[channelKey] == nil {
		receivedIndices[channelKey] = make(map[int]bool)
	}
	if sequentialCounters[channelKey] == nil {
		counter := 1
		sequentialCounters[channelKey] = &counter
	}
	if index <= 0 {
		index = *sequentialCounters[channelKey]
		*sequentialCounters[channelKey]++
	} else if receivedIndices[channelKey][index] {
		mu.Unlock()
		return
	}
	receivedIndices[channelKey][index] = true
	mu.Unlock()

	stats.AddReceivedMessage(msg.Topic(), "telemetry", index, channelID)

	payload := extractMessagePayload(msgProto)
	payloadBytes := len(payload)

	// Framing already extracted above for filtering - use it for ack
	framingType := ""
	if framing != "" {
		framingType = framing
	}

	// Extract timestamp from telemetry message for accurate bitrate calculation
	// Build proto ack for telemetry. Ack.received_at is receipt time (this
	// side's clock at handling), not the payload's capture time.
	ackPayload, err := buildTelemetryAck(
		cfg, channelID, framingType, index, payloadBytes, time.Now(), false, msgProto.GetMessageId(),
	)
	if err != nil {
		log.Printf("ERROR: Failed to build telemetry ack: %v", err)
		return
	}
	sendAck(msg.Topic(), ackPayload)

	// Framing already extracted above for filtering
	metadata := extractMessageMetadata(msgProto, msg)

	// Debug logging
	if cfg.EnableDebug {
		log.Printf(
			"DEBUG: Received telemetry: channel=%s index=%d framing=%s size=%d bytes",
			channelID,
			index,
			framing,
			len(payload),
		)
	}

	// For telemetry, use channelID/downlink format to match S3 prefix structure
	telemetryChannelID := channelID + "/downlink"

	route(fetchResult{
		Index:     index,
		ChannelID: telemetryChannelID,
		Framing:   framing,
		Data:      payload,
		Metadata:  metadata,
		Err:       nil,
		Source:    SourceMQTT,
	})
}

func newMQTTMessageHandler(
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool, // channelID -> index -> bool
	sequentialCounters map[string]*int, // channelID -> counter
	cfg Config,
	stats *statsTracker,
	receivedNonTelemetryTimestamps map[string]map[string]bool, // msgType -> timestamp -> bool
	ackSender *ackSender,
) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		if strings.Contains(msg.Topic(), "/ack") {
			stats.AddAckReceived(msg.Topic(), msg.Payload())
			return
		}

		sendAckWithClient := func(topic string, ackPayload []byte) {
			ackSender.sendAck(client, topic, ackPayload)
		}

		msgProto := &streaming.FromStarPassMessage{}
		if err := proto.Unmarshal(msg.Payload(), msgProto); err != nil {
			route(fetchResult{
				Index:     0,
				ChannelID: "",
				Err:       fmt.Errorf("decode MQTT message as FromStarPassMessage: %w", err),
			})
			return
		}

		if handleMonitoringMessage(
			msgProto,
			msg,
			cfg,
			stats,
			mu,
			receivedNonTelemetryTimestamps,
			sendAckWithClient,
		) {
			return
		}
		if handleConfigMessage(
			msgProto,
			msg,
			cfg,
			stats,
			mu,
			receivedNonTelemetryTimestamps,
			sendAckWithClient,
		) {
			return
		}
		if handleEventMessage(
			msgProto,
			msg,
			cfg,
			stats,
			mu,
			receivedNonTelemetryTimestamps,
			sendAckWithClient,
		) {
			return
		}

		handleTelemetryMessage(
			msgProto,
			msg,
			cfg,
			stats,
			route,
			mu,
			receivedIndices,
			sequentialCounters,
			sendAckWithClient,
		)
	}
}

// handleNonTelemetryMessage processes and saves non-telemetry messages (monitoring, config, event).
// These messages are saved to files as JSON, logged, and counted in stats, but do not affect bitrate calculations.
func handleNonTelemetryMessage(
	msg proto.Message,
	cfg Config,
	stats *statsTracker,
	msgType string,
	_ string, // planID - unused but kept for API compatibility
	_ string, // antennaID - unused but kept for API compatibility
	source MessageSource,
) {
	// Marshal the message to JSON for human-readable output
	jsonData, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}.Marshal(msg)
	if err != nil {
		log.Printf("ERROR: Failed to marshal %s message to JSON: %v", msgType, err)
		return
	}

	// Save to file as JSON (low-rate for MQTT messages)
	if err := writeNonTelemetryMessage(
		jsonData,
		cfg.DestDir,
		cfg.PassID,
		msgType,
	); err != nil {
		log.Printf("ERROR: Failed to write %s message: %v", msgType, err)
		return
	}

	// Update stats
	switch msgType {
	case msgTypeMonitoring:
		stats.AddMonitoringMessage(source)
	case msgTypeConfig:
		stats.AddConfigMessage(source)
	case msgTypeEvent:
		stats.AddEventMessage(source)
	}
}

func subscribeMQTT(
	client mqtt.Client,
	topics []string,
	qos byte,
	handler mqtt.MessageHandler,
) error {
	vlogf("Subscribing to MQTT topics: %v (QoS: %d)", topics, qos)

	const maxRetries = mqttSubscribeMaxRetries
	var subscribeErr error
	var subscribedTopics []string

	// Subscribe to topics individually to better handle failures
	for _, topic := range topics {
		for i := 0; i < maxRetries; i++ {
			if !client.IsConnected() {
				vlogf(
					"Client not connected, waiting to subscribe to %s... (attempt %d/%d)",
					topic,
					i+1,
					maxRetries,
				)
				time.Sleep(mqttSubscribeConnWait)
				continue
			}

			token := client.Subscribe(topic, qos, handler)
			ok := token.WaitTimeout(mqttSubscribeTimeout)
			if !ok {
				vlogf("Subscribe to %s timed out (attempt %d/%d)", topic, i+1, maxRetries)
				if i == maxRetries-1 {
					vlogf(
						"WARNING: Failed to subscribe to %s after %d attempts, skipping",
						topic,
						maxRetries,
					)
					subscribeErr = fmt.Errorf("failed to subscribe to %s", topic)
				}
				time.Sleep(mqttSubscribeBackoff)
				continue
			}

			if token.Error() != nil {
				vlogf(
					"Subscribe to %s failed (attempt %d/%d): %v",
					topic,
					i+1,
					maxRetries,
					token.Error(),
				)
				if i == maxRetries-1 {
					vlogf(
						"WARNING: Failed to subscribe to %s after %d attempts, skipping",
						topic,
						maxRetries,
					)
					subscribeErr = fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error())
				}
				time.Sleep(mqttSubscribeBackoff)
				continue
			}

			// Check if connection is still alive after subscription
			if !client.IsConnected() {
				log.Printf("WARNING: Client disconnected after subscribing to %s", topic)
				if i == maxRetries-1 {
					return fmt.Errorf("connection lost after subscribing to %s", topic)
				}
				time.Sleep(mqttSubscribeConnWait)
				continue
			}

			// Success
			subscribedTopics = append(subscribedTopics, topic)
			break
		}
	}

	if len(subscribedTopics) > 0 {
		vlogf(
			"Successfully subscribed to %d/%d topics: %v",
			len(subscribedTopics),
			len(topics),
			subscribedTopics,
		)
	}

	if len(subscribedTopics) == 0 {
		return fmt.Errorf("failed to subscribe to any topics: %w", subscribeErr)
	}

	if len(subscribedTopics) < len(topics) {
		vlogf(
			"WARNING: Only subscribed to %d/%d topics. Some subscriptions may have failed",
			len(subscribedTopics),
			len(topics),
		)
	}

	return nil
}

func runMQTTReader(ctx context.Context, cfg Config, s3c *s3.Client, stats *statsTracker) error {
	return runMQTTReaderWithClient(ctx, cfg, s3c, stats, nil)
}

// setupMQTTClientAndConfig sets up the MQTT client and connection config.
// Returns the mqtt.Client (may be nil if using S3 fallback only) and the mqttConnectionConfig.
func setupMQTTClientAndConfig(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	stats *statsTracker,
	sharedClient mqtt.Client,
) (mqtt.Client, *mqttConnectionConfig, error) {
	if sharedClient != nil {
		connCfg, err := buildMQTTConnectionConfig(ctx, cfg)
		if err != nil {
			return nil, nil, err
		}
		stats.SetClientID(connCfg.clientID)
		return sharedClient, connCfg, nil
	}

	connCfg, err := buildMQTTConnectionConfig(ctx, cfg)
	if err != nil {
		log.Printf("WARNING: Failed to build MQTT connection config: %v", err)
		log.Printf("  This may be expected if IoT endpoint is not available. Continuing with S3 fallback only.")
		hasMQTTFeatures := cfg.EnableDownlink || cfg.EnableMonitoring || cfg.EnableConfigState || cfg.EnableEvent
		if hasMQTTFeatures && cfg.Bucket != "" && s3c != nil {
			log.Printf("Continuing with S3 fallback scanning only (MQTT unavailable)")
			return nil, &mqttConnectionConfig{broker: "", topics: []string{}, clientID: ""}, nil
		}
		return nil, nil, fmt.Errorf("MQTT connection required but not available: %w", err)
	}

	stats.SetClientID(connCfg.clientID)
	return connectMQTTClientForReader(ctx, cfg, connCfg)
}

// connectMQTTClientForReader establishes a new MQTT connection for the reader.
func connectMQTTClientForReader(
	ctx context.Context,
	cfg Config,
	connCfg *mqttConnectionConfig,
) (mqtt.Client, *mqttConnectionConfig, error) {
	connectConfirmed := make(chan struct{})
	clientFactory := func() (mqtt.Client, string, error) {
		var creds *AuthorizerCredentials
		switch {
		case cfg.CredStore != nil:
			creds = cfg.CredStore.load()
			vlogf("Using credentials from credential store (expiry: %s)", creds.Expiration.Format(time.RFC3339))
		case cfg.AuthorizerCreds != nil:
			creds = cfg.AuthorizerCreds
			vlogf("Using credentials from config (expiry: %s)", creds.Expiration.Format(time.RFC3339))
		default:
			return nil, "", errors.New("no credentials available")
		}
		if creds.IoTCertificatePem != "" && creds.IoTPrivateKeyPem != "" {
			latestCert, err := tls.X509KeyPair([]byte(creds.IoTCertificatePem), []byte(creds.IoTPrivateKeyPem))
			if err != nil {
				log.Printf("WARNING: failed to re-parse IoT certificate from store: %v; using existing cert", err)
			} else {
				connCfg.tlsCertificate = &latestCert
				// This runs on every connect attempt (including the first), not only after the
				// background credential refresher fetches new authorizer credentials.
				certID := creds.IoTCertificateID
				if certID == "" {
					certID = "(unknown)"
				}
				vlogf("Applied IoT TLS certificate from credential store for MQTT connect (certId=%s)", certID)
			}
		}
		opts := buildMQTTClientOptions(connCfg, connectConfirmed)
		return mqtt.NewClient(opts), connCfg.broker, nil
	}

	connectedClient, err := connectMQTTClientWithRetry(ctx, clientFactory, connectConfirmed, connCfg)
	if err != nil {
		log.Printf("WARNING: Failed to connect to MQTT broker after retries: %v", err)
		log.Printf("  Continuing with S3 fallback only")
		connCfg.topics = []string{}
		return nil, connCfg, nil
	}
	return connectedClient, connCfg, nil
}

func runMQTTReaderWithClient(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	stats *statsTracker,
	sharedClient mqtt.Client,
) error {
	client, connCfg, err := setupMQTTClientAndConfig(ctx, cfg, s3c, stats, sharedClient)
	if err != nil {
		return err
	}
	// Ack sender for received telemetry/monitoring/config/event messages. Flushed
	// (LIFO: runs before Disconnect) so the final ack of the pass is confirmed by
	// the broker rather than cut off when the client disconnects.
	msgAckSender := newAckSender(cfg, stats)
	if client != nil && sharedClient == nil {
		defer client.Disconnect(mqttDisconnectQuiesce)
		defer msgAckSender.Flush()
	}

	// mqttResultsChBuf sizes each CHANNEL's own inbound queue (see
	// keyedResultRouter): every channel gets its own buffer of this size, rather
	// than one buffer shared across all channels, so a slow or backed-up channel
	// can only ever fill its own queue, never anyone else's.
	mqttResultsChBuf := cfg.WindowSize * mqttResultsChWindowMultiplier
	if mqttResultsChBuf < mqttResultsChMinBuf {
		mqttResultsChBuf = mqttResultsChMinBuf
	}
	var mu sync.Mutex
	receivedIndices := make(map[string]map[int]bool) // channelID -> index -> bool
	sequentialCounters := make(map[string]*int)      // channelID -> counter
	connectionTime := time.Now()                     // Track when we connected
	receivedNonTelemetryTimestamps := make(
		map[string]map[string]bool,
	) // msgType -> timestamp -> bool

	// Each (channel, framing) telemetry sequence is processed (dedup/ordering
	// bookkeeping + disk write) by its own independent goroutine, so one
	// sequence's slow write can never delay another's; see
	// keyed_result_router.go and newMQTTInOrderWorker/newMQTTRelaxedWorker.
	// Framing is part of the key because telemetry indexes are scoped per
	// (pass, channel, framing): a multi-framing channel carries independent
	// sequences that both start at 1, and sharing one cursor/pending map
	// across them drops data.
	allDone := newKeyedDoneSet[channelFramingKey]()
	activeKeys := newKeyedSet[channelFramingKey]()
	expectedKeys := expectedTelemetryKeys(cfg)
	finishedOnce := &sync.Once{}
	var telemetryWorker func(context.Context, channelFramingKey, <-chan fetchResult) error
	if cfg.WriteInOrder {
		telemetryWorker = newMQTTInOrderWorker(cfg, stats, allDone, activeKeys, expectedKeys, finishedOnce)
	} else {
		telemetryWorker = newMQTTRelaxedWorker(cfg, stats, allDone, activeKeys, expectedKeys, finishedOnce)
	}
	router := newKeyedResultRouter(ctx, mqttResultsChBuf, telemetryWorker)
	route := func(res fetchResult) {
		if res.ChannelID == "" && res.Err != nil {
			// A decode error etc. has no channel to route by.
			router.Fail(res.Err)
			return
		}
		key := channelFramingKey{ChannelID: res.ChannelID, Framing: res.Framing}
		activeKeys.add(key)
		router.Route(key, res)
	}

	messageHandler := newMQTTMessageHandler(
		route,
		&mu,
		receivedIndices,
		sequentialCounters,
		cfg,
		stats,
		receivedNonTelemetryTimestamps,
		msgAckSender,
	)

	hasMQTTFeatures := cfg.EnableDownlink || cfg.EnableMonitoring || cfg.EnableConfigState ||
		cfg.EnableEvent

	if err := subscribeOrFallback(client, connCfg, cfg, hasMQTTFeatures, s3c, messageHandler); err != nil {
		return err
	}

	if hasMQTTFeatures && cfg.Bucket != "" && s3c != nil {
		vlogf(
			"Starting S3 fallback scanner for bucket=%s (will scan every %v for missed messages)",
			cfg.Bucket, cfg.S3PollInterval,
		)
		if cfg.EnableDebug {
			vlogf("S3 fallback enabled for: LowRate=%v, Monitoring=%v, Config=%v, Event=%v",
				cfg.EnableDownlink, cfg.EnableMonitoring, cfg.EnableConfigState, cfg.EnableEvent)
		}
		ackSender := newAckSender(cfg, stats)
		// Give the scanner a cancellable context and join it before flushing its
		// ack sender. Defers run LIFO, so the cancel+Wait below runs first (the
		// scanner goroutine exits, so no sendAck can still be in flight), then
		// Flush drains the acks it recorded, rather than racing Add against Wait.
		scanCtx, cancelScanner := context.WithCancel(ctx)
		var scannerWG sync.WaitGroup
		defer ackSender.Flush()
		defer func() {
			cancelScanner()
			scannerWG.Wait()
		}()
		scannerWG.Add(1)
		go func() {
			defer scannerWG.Done()
			runS3FallbackScanner(
				scanCtx,
				s3c,
				cfg,
				route,
				&mu,
				receivedIndices,
				sequentialCounters,
				connectionTime,
				receivedNonTelemetryTimestamps,
				stats,
				client,
				ackSender,
			)
		}()
	} else if cfg.EnableDebug {
		vlogf("S3 fallback scanner not started: hasMQTTFeatures=%v, bucket=%s, s3c=%v",
			hasMQTTFeatures, cfg.Bucket, s3c != nil)
	}

	return router.Wait()
}

// subscribeOrFallback handles subscription or S3-only fallback when no topics/client are available.
func subscribeOrFallback(
	client mqtt.Client,
	connCfg *mqttConnectionConfig,
	cfg Config,
	hasMQTTFeatures bool,
	s3c *s3.Client,
	messageHandler func(mqtt.Client, mqtt.Message),
) error {
	clientConnected := client != nil && client.IsConnected()
	vlogf("Preparing to subscribe: client=%v, topics=%d, clientIsConnected=%v",
		client != nil, len(connCfg.topics), clientConnected)

	if len(connCfg.topics) == 0 || client == nil {
		logNoMQTTConnection(cfg)
		if !hasMQTTFeatures || cfg.Bucket == "" || s3c == nil {
			return errors.New("no MQTT connection available and S3 fallback not configured")
		}
		vlogf("Continuing with S3 fallback only (MQTT unavailable)")
		return nil
	}

	vlogf(
		"Attempting to subscribe to %d MQTT topics (client connected: %v)",
		len(connCfg.topics),
		client.IsConnected(),
	)
	if err := subscribeMQTT(client, connCfg.topics, cfg.MQTTQoS, messageHandler); err != nil {
		log.Printf("WARNING: Failed to subscribe to MQTT topics: %v", err)
		log.Printf("  Continuing with S3 fallback only")
	}
	return nil
}

// logNoMQTTConnection logs a warning when no MQTT connection is available.
func logNoMQTTConnection(cfg Config) {
	if cfg.AuthorizerCreds != nil {
		vlogf(
			"WARNING: No MQTT connection available. Streams config: LowRate=%d, Monitoring=%v, Config=%v, Event=%v",
			len(cfg.AuthorizerCreds.Streams.LowRate),
			cfg.AuthorizerCreds.Streams.Monitoring != nil,
			cfg.AuthorizerCreds.Streams.Config != nil,
			cfg.AuthorizerCreds.Streams.Event != nil,
		)
	} else {
		log.Printf("WARNING: No MQTT connection available and no authorizer credentials configured")
	}
}

// expectedTelemetryKeys returns the (channel, framing) sequences the pass is
// expected to produce, taken from the framings the authorizer declared for
// each downlink channel. Empty when the authorizer supplied no channel
// metadata, in which case completion falls back to the sequences actually seen.
func expectedTelemetryKeys(cfg Config) map[channelFramingKey]bool {
	if cfg.AuthorizerCreds == nil {
		return nil
	}
	expected := make(map[channelFramingKey]bool)
	for _, ch := range cfg.AuthorizerCreds.Channels {
		if ch.Direction == directionUplink || ch.RateClass == rateClassHighRate {
			continue
		}
		for _, fr := range ch.Framings {
			if fr == "" || !framingAccepted(cfg, fr) {
				continue
			}
			expected[channelFramingKey{ChannelID: ch.ChannelID + "/downlink", Framing: fr}] = true
		}
	}
	return expected
}

// mqttAllSequencesFinished reports whether every telemetry sequence the pass
// will produce has finished.
//
// When the authorizer declared the framings per channel, that declared set is
// the bar: a sequence that has not started yet still counts as outstanding, so
// one framing reaching END cannot complete the pass while another framing of
// the same channel has yet to deliver its first message. Without that metadata
// there is nothing to enumerate against, so completion falls back to the
// sequences seen so far covering every configured channel.
func mqttAllSequencesFinished(
	allDone *keyedDoneSet[channelFramingKey],
	activeKeys *keyedSet[channelFramingKey],
	expected map[channelFramingKey]bool,
	expectedChannels int,
) bool {
	if len(expected) > 0 {
		return allDone.allDoneAmong(expected)
	}
	active := activeKeys.snapshot()
	if !allDone.allDoneAmong(active) {
		return false
	}
	channels := make(map[string]bool, len(active))
	for k := range active {
		channels[k.ChannelID] = true
	}
	return len(channels) >= expectedChannels
}

// newMQTTInOrderWorker returns a per-(channel, framing) worker function for
// keyedResultRouter that writes one low-rate MQTT/S3-fallback telemetry
// sequence's chunks to disk in index order. Each sequence gets its own call
// (and so its own goroutine and local state below, no map keyed by sequence
// is needed), which is what makes one sequence's write speed independent of
// every other's. Indexes are scoped per (pass, channel, framing), so the
// cursor, pending map, and END state below must never be shared across
// framings of one channel.
func newMQTTInOrderWorker(
	cfg Config,
	stats *statsTracker,
	allDone *keyedDoneSet[channelFramingKey],
	activeKeys *keyedSet[channelFramingKey],
	expectedKeys map[channelFramingKey]bool,
	finishedOnce *sync.Once,
) func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
	return func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
		channelID := key.ChannelID
		pending := make(map[int]fetchResult)
		nextWrite := 1
		done := false

		writeContiguous := func() error {
			for {
				pr, ok := pending[nextWrite]
				if !ok {
					break
				}

				if err := writeChunkToFile(
					pr.Data,
					cfg.DestDir,
					cfg.Prefix,
					cfg.PassID,
					pr.Index,
					pr.ChannelID,
					pr.Framing,
					"low-rate",
					stats,
					cfg.WriteInOrder,
					cfg,
				); err != nil {
					stats.AddError(
						fmt.Errorf("write index %d (channel %s): %w", pr.Index, pr.ChannelID, err),
					)
					return fmt.Errorf("write index %d (channel %s): %w", pr.Index, pr.ChannelID, err)
				}

				stats.AddChannelWrite(channelID, int64(len(pr.Data)))
				delete(pending, nextWrite)
				nextWrite++

				stats.SetChannelNextIndex(channelID, nextWrite)

				if isEndMessage(pr.Metadata) {
					stats.MarkChannelEnd(channelID)
					emitLine("")
					// Auto-close: Check for zero-size data as stream end indicator
					if cfg.EnableAutoClose && len(pr.Data) == 0 {
						log.Printf("Stream auto-close: zero-size END message detected, exiting...")
						closeAuthorizerStream(cfg)
						exitAfterTeardown()
					}
					done = true
					pending = nil // release; further sends are drained and discarded below
					allDone.markDone(key)
					if mqttAllSequencesFinished(allDone, activeKeys, expectedKeys, len(cfg.ChannelIDs)) {
						finishedOnce.Do(func() {
							if cfg.EnableAutoClose {
								uiOKf("All channels have finished. Stopping.")
								exitAfterTeardown()
							}
							uiOKf("All channels have finished. Still listening; press Ctrl-C to stop.")
						})
					}
					break
				}
			}
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case res, ok := <-in:
				if !ok {
					return nil
				}
				if res.Err != nil {
					if errors.Is(res.Err, context.Canceled) {
						emitLine("")
						vlogf("Context cancelled; exiting.")
						return nil
					}
					return fmt.Errorf(
						"MQTT message error for index %d (channel %s): %w",
						res.Index,
						res.ChannelID,
						res.Err,
					)
				}

				if done {
					continue
				}

				stats.AddChannelDownload(channelID, res.Framing, int64(len(res.Data)), res.Source, res.Index)
				pending[res.Index] = res
				stats.SetChannelNextIndex(channelID, nextWrite)

				if err := writeContiguous(); err != nil {
					return err
				}
			}
		}
	}
}

// newMQTTRelaxedWorker returns a per-(channel, framing) worker function for
// keyedResultRouter that writes one low-rate MQTT/S3-fallback telemetry
// sequence's chunks to disk as they arrive (not necessarily in order), with
// auto-close/grace-period handling. As with newMQTTInOrderWorker, each call
// owns its own local state for exactly one sequence, so sequences never
// contend with each other and one framing's END cannot complete another
// framing's stream.
//
//nolint:gocognit,cyclop,funlen // Complex state machine for relaxed-order MQTT message processing with auto-close logic
func newMQTTRelaxedWorker(
	cfg Config,
	stats *statsTracker,
	allDone *keyedDoneSet[channelFramingKey],
	activeKeys *keyedSet[channelFramingKey],
	expectedKeys map[channelFramingKey]bool,
	finishedOnce *sync.Once,
) func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
	return func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
		channelID := key.ChannelID
		sawEnd := false
		endIndex := 0
		receivedIndices := make(map[int]bool) // index -> bool (for completeness check)
		// Grace period for auto-close: wait for missing messages after END is detected
		var autoCloseTimer *time.Timer
		defer func() {
			if autoCloseTimer != nil {
				autoCloseTimer.Stop()
			}
		}()

		checkAndAutoClose := func() {
			if !cfg.EnableAutoClose || !sawEnd {
				return
			}

			// Check if all messages up to END have been received
			endIdx := endIndex
			allReceived := true
			for i := 1; i <= endIdx; i++ {
				if !receivedIndices[i] {
					allReceived = false
					break
				}
			}

			if allReceived {
				allDone.markDone(key)
				if mqttAllSequencesFinished(allDone, activeKeys, expectedKeys, len(cfg.ChannelIDs)) {
					finishedOnce.Do(func() {
						log.Printf(
							"Stream auto-close: all channels completed with all messages received, exiting...",
						)
						exitAfterTeardown()
					})
				}
			} else {
				// Not all messages received yet, start/restart grace period timer
				if autoCloseTimer != nil {
					autoCloseTimer.Stop()
				}
				autoCloseTimer = time.AfterFunc(autoCloseGracePeriod, func() {
					// Grace period expired, check again
					allReceivedNow := true
					for i := 1; i <= endIdx; i++ {
						if !receivedIndices[i] {
							allReceivedNow = false
							break
						}
					}
					if allReceivedNow {
						allDone.markDone(key)
						if mqttAllSequencesFinished(allDone, activeKeys, expectedKeys, len(cfg.ChannelIDs)) {
							finishedOnce.Do(func() {
								log.Printf("Stream auto-close: all channels completed (after grace period), exiting...")
								exitAfterTeardown()
							})
						}
					} else {
						log.Printf(
							"Stream auto-close: grace period expired but missing messages for channel %s framing %s (indices 1-%d), exiting anyway...",
							channelID,
							key.Framing,
							endIdx,
						)
						exitAfterTeardown()
					}
				})
			}
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case res, ok := <-in:
				if !ok {
					return nil
				}
				if res.Err != nil {
					if errors.Is(res.Err, context.Canceled) {
						emitLine("")
						vlogf("Context cancelled; exiting.")
						return nil
					}
					return fmt.Errorf(
						"MQTT message error for index %d (channel %s): %w",
						res.Index,
						res.ChannelID,
						res.Err,
					)
				}

				stats.AddChannelDownload(channelID, res.Framing, int64(len(res.Data)), res.Source, res.Index)

				// Track received indices for completeness checking
				receivedIndices[res.Index] = true

				if sawEnd && res.Index > endIndex {
					continue
				}

				if err := writeChunkToFile(
					res.Data,
					cfg.DestDir,
					cfg.Prefix,
					cfg.PassID,
					res.Index,
					res.ChannelID,
					res.Framing,
					"low-rate", // low-rate from MQTT
					stats,
					cfg.WriteInOrder,
					cfg,
				); err != nil {
					stats.AddError(
						fmt.Errorf("write index %d (channel %s): %w", res.Index, res.ChannelID, err),
					)
					return fmt.Errorf("write index %d (channel %s): %w", res.Index, res.ChannelID, err)
				}

				stats.AddChannelWrite(channelID, int64(len(res.Data)))

				nextIdxForDisplay := res.Index + 1
				if sawEnd {
					nextIdxForDisplay = endIndex + 1
				}
				stats.SetChannelNextIndex(channelID, nextIdxForDisplay)

				if isEndMessage(res.Metadata) {
					endIndex = res.Index
					sawEnd = true
					emitLine("")
					// Auto-close: Check for zero-size data as stream end indicator (immediate exit)
					if cfg.EnableAutoClose && len(res.Data) == 0 {
						log.Printf("Stream auto-close: zero-size END message detected, exiting...")
						closeAuthorizerStream(cfg)
						exitAfterTeardown()
					}
					// Check if we can auto-close (all messages received or after grace period)
					checkAndAutoClose()
				} else if sawEnd {
					// Received a message after END was detected, check again if we can auto-close
					checkAndAutoClose()
				}
			}
		}
	}
}

// runS3FallbackScanner periodically scans S3 for MQTT messages that were missed
// (published before connection or during disconnection).
// Uses background context so it continues running even after main reader finishes.
func runS3FallbackScanner(
	ctx context.Context,
	s3c *s3.Client,
	cfg Config,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	sequentialCounters map[string]*int,
	connectionTime time.Time,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) {
	vlogf("S3 fallback scanner started (scanning every %v)", cfg.S3PollInterval)
	ticker := time.NewTicker(cfg.S3PollInterval)
	defer ticker.Stop()

	// Delay the initial scan to allow MQTT to start receiving first.
	time.Sleep(s3FallbackInitialDelay)

	scanCtx := ctx

	// Do an initial scan immediately after the delay
	if cfg.EnableDebug {
		vlogf("S3 fallback: Performing initial scan...")
	}
	scanS3ForMissingMessages(
		scanCtx,
		s3c,
		cfg,
		route,
		mu,
		receivedIndices,
		sequentialCounters,
		connectionTime,
		receivedNonTelemetryTimestamps,
		stats,
		client,
		ackSender,
	)

	for {
		select {
		case <-ctx.Done():
			// Only exit if the main context is canceled (user Ctrl-C)
			vlogf("S3 fallback scanner stopped")
			return
		case <-ticker.C:
			if cfg.EnableDebug {
				vlogf("S3 fallback: Scanning for missing messages...")
			}
			scanS3ForMissingMessages(
				scanCtx,
				s3c,
				cfg,
				route,
				mu,
				receivedIndices,
				sequentialCounters,
				connectionTime,
				receivedNonTelemetryTimestamps,
				stats,
				client,
				ackSender,
			)
		}
	}
}

// telemetryScanTarget names one low-rate downlink stream's S3 prefix and the
// channel key its scan results are routed and counted under.
type telemetryScanTarget struct {
	prefix    string
	channelID string
}

// lowRateTelemetryScanTargets returns the streams to scan S3 for low-rate
// telemetry. The authorizer's low-rate stream list also carries each channel's
// monitoring stream; monitoring is recovered by the non-telemetry scanners, and
// a telemetry scan of a monitoring prefix counts that stream's END sentinel
// object into the telemetry totals, so only downlink streams are scanned here.
func lowRateTelemetryScanTargets(streams []StreamConfig) []telemetryScanTarget {
	targets := make([]telemetryScanTarget, 0, len(streams))
	for _, stream := range streams {
		channelID := extractChannelIDFromLowRateStream(stream)
		if channelID == "" || !strings.HasSuffix(channelID, "/downlink") {
			continue
		}
		targets = append(targets, telemetryScanTarget{prefix: stream.S3Prefix, channelID: channelID})
	}
	return targets
}

// spawnLowRateTelemetryScans spawns goroutines to scan S3 for low-rate telemetry messages.
// It uses authorizer-provided prefixes when available, falling back to constructed paths.
func spawnLowRateTelemetryScans(
	ctx context.Context,
	s3c *s3.Client,
	cfg Config,
	connectionTime time.Time,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	sequentialCounters map[string]*int,
	stats *statsTracker,
	useAuthorizerPaths bool,
	wg *sync.WaitGroup,
	client mqtt.Client,
	ackSender *ackSender,
) {
	if useAuthorizerPaths && len(cfg.AuthorizerCreds.Streams.LowRate) > 0 {
		for _, target := range lowRateTelemetryScanTargets(cfg.AuthorizerCreds.Streams.LowRate) {
			if cfg.EnableDebug {
				log.Printf(
					"S3 fallback (low-rate): bucket=%s prefix=%s channel=%s",
					cfg.Bucket, target.prefix, target.channelID,
				)
			}
			wg.Add(1)
			go func(prefix string, chID string) {
				defer wg.Done()
				if err := scanS3TelemetryMessages(
					ctx, s3c, cfg.Bucket, prefix, chID,
					connectionTime, route, mu, receivedIndices, sequentialCounters, stats,
					cfg, client, ackSender,
				); err != nil && !errors.Is(err, context.Canceled) {
					if isTransientS3AuthError(err) {
						vlogf("Transient credential error scanning telemetry for channel %s (retrying): %v", chID, err)
					} else {
						streamErrf(err, "Could not scan telemetry for channel %s: %v", chID, err)
					}
				}
			}(target.prefix, target.channelID)
		}
		return
	}
	// Construct paths manually (shouldn't happen when using authorizer)
	basePrefix := buildBasePrefix(cfg.Environment, cfg.PassID)
	for _, channelID := range cfg.ChannelIDs {
		channelPrefix := basePrefix + fmt.Sprintf("low_rate/channel/%s/", channelID)
		if cfg.EnableDebug {
			log.Printf(
				"S3 fallback (low-rate, fallback paths): bucket=%s prefix=%s channel=%s",
				cfg.Bucket, channelPrefix, channelID,
			)
		}
		wg.Add(1)
		go func(chID string) {
			defer wg.Done()
			if err := scanS3TelemetryMessages(
				ctx, s3c, cfg.Bucket, channelPrefix, chID,
				connectionTime, route, mu, receivedIndices, sequentialCounters, stats,
				cfg, client, ackSender,
			); err != nil && !errors.Is(err, context.Canceled) {
				if isTransientS3AuthError(err) {
					vlogf("Transient credential error scanning telemetry for channel %s (retrying): %v", chID, err)
				} else {
					streamErrf(err, "Could not scan telemetry for channel %s: %v", chID, err)
				}
			}
		}(channelID)
	}
}

// spawnNonTelemetryScans spawns goroutines to scan S3 for non-telemetry messages
// (monitoring, config, event) that may have been missed via MQTT.
func spawnNonTelemetryScans(
	ctx context.Context,
	s3c *s3.Client,
	cfg Config,
	connectionTime time.Time,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	stats *statsTracker,
	useAuthorizerPaths bool,
	monitoringStream, configStream, eventStream *StreamConfig,
	wg *sync.WaitGroup,
	client mqtt.Client,
	ackSender *ackSender,
) {
	msgTypes := []struct {
		name    string
		enabled bool
		stream  *StreamConfig
	}{
		{msgTypeMonitoring, cfg.EnableMonitoring, monitoringStream},
		{msgTypeConfig, cfg.EnableConfigState, configStream},
		{msgTypeEvent, cfg.EnableEvent, eventStream},
	}
	for _, msgTypeInfo := range msgTypes {
		if !msgTypeInfo.enabled {
			continue
		}
		msgType := msgTypeInfo.name
		var msgPrefix string
		if useAuthorizerPaths && msgTypeInfo.stream != nil {
			msgPrefix = msgTypeInfo.stream.S3Prefix
		} else {
			msgPrefix = buildBasePrefix(cfg.Environment, cfg.PassID) + msgType + "/"
		}
		if cfg.EnableDebug {
			vlogf("S3 fallback (%s): bucket=%s prefix=%s", msgType, cfg.Bucket, msgPrefix)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := scanS3NonTelemetryMessages(
				ctx, s3c, cfg.Bucket, msgPrefix, msgType,
				connectionTime, cfg, mu, receivedNonTelemetryTimestamps, stats,
				client, ackSender,
			); err != nil && !errors.Is(err, context.Canceled) {
				streamErrf(err, "Could not scan %s messages: %v", msgType, err)
			}
		}()
	}
}

// scanS3ForMissingMessages scans S3 for messages that should have been received via MQTT.
// All scans are done in parallel for better performance.
func scanS3ForMissingMessages(
	ctx context.Context,
	s3c *s3.Client,
	cfg Config,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	sequentialCounters map[string]*int,
	connectionTime time.Time,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) {
	var wg sync.WaitGroup
	useAuthorizerPaths := cfg.AuthorizerCreds != nil

	if cfg.EnableDownlink {
		spawnLowRateTelemetryScans(
			ctx, s3c, cfg, connectionTime, route, mu,
			receivedIndices, sequentialCounters, stats, useAuthorizerPaths, &wg,
			client, ackSender,
		)
	}

	var monitoringStream, configStream, eventStream *StreamConfig
	if useAuthorizerPaths {
		monitoringStream = cfg.AuthorizerCreds.Streams.Monitoring
		configStream = cfg.AuthorizerCreds.Streams.Config
		eventStream = cfg.AuthorizerCreds.Streams.Event
	}
	spawnNonTelemetryScans(
		ctx, s3c, cfg, connectionTime, mu,
		receivedNonTelemetryTimestamps, stats, useAuthorizerPaths,
		monitoringStream, configStream, eventStream, &wg,
		client, ackSender,
	)

	wg.Wait()
}

// buildBasePrefix constructs the base S3 prefix when authorizer is not available
func buildBasePrefix(environment, passID string) string {
	if environment != "" {
		return fmt.Sprintf("%s/%s/", environment, passID)
	}
	return fmt.Sprintf("%s/", passID)
}

// extractChannelIDFromLowRateStream extracts channel ID from a low-rate stream config
// by parsing the S3 prefix or MQTT topic (format: .../channel/<channelID>/downlink/...)
// Returns channelID with "/downlink" suffix for telemetry streams
func extractChannelIDFromLowRateStream(stream StreamConfig) string {
	// Try to extract from S3 prefix first
	// S3 prefix format: {env}/pass/{passID}/channel/{channelID}/downlink/
	if strings.Contains(stream.S3Prefix, "/channel/") {
		parts := strings.Split(stream.S3Prefix, "/channel/")
		if len(parts) > 1 {
			channelPart := strings.TrimSuffix(parts[1], "/")
			// For telemetry streams, return "{channelID}/downlink" to match topic format
			return channelPart
		}
	}
	// Try MQTT topic - extract channelID and append "/downlink" for telemetry
	channelID := ExtractChannelIDFromTopic(stream.MqttTopic)
	if channelID != "" && strings.Contains(stream.MqttTopic, "/downlink") {
		return channelID + "/downlink"
	}
	return channelID
}

func extractTimestampFromS3Key(key string) (int64, error) {
	keyParts := strings.Split(key, "/")
	if len(keyParts) == 0 {
		return 0, errors.New("invalid key format")
	}
	filename := keyParts[len(keyParts)-1]
	tsPart := filename
	if idx := strings.IndexByte(filename, '_'); idx != -1 {
		tsPart = filename[:idx]
	}
	return strconv.ParseInt(tsPart, 10, 64)
}

func downloadAndDecodeS3Object(
	ctx context.Context,
	s3c *s3.Client,
	bucket string,
	key *string,
) ([]byte, error) {
	out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    key,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	var buf bytes.Buffer
	if out.ContentLength != nil && *out.ContentLength > 0 {
		buf.Grow(int(*out.ContentLength))
	}
	copyBuf := make([]byte, s3DownloadCopyBufSize)
	if _, err := io.CopyBuffer(&buf, out.Body, copyBuf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func processS3TelemetryObject(
	ctx context.Context,
	s3c *s3.Client,
	bucket string,
	obj *s3types.Object,
	channelID string,
	connectionTime time.Time,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	cfg Config,
	client mqtt.Client,
	ackSender *ackSender,
) {
	if strings.Contains(*obj.Key, "/ack") {
		return
	}

	// Honour --accepted-framing here too: the low-rate S3 fallback lists every
	// framing under the channel prefix, so without this filter a filtered stream
	// would still write framings it was told to drop (the framing is the
	// ".../downlink/<FRAMING>/..." key segment; skip before downloading).
	if !framingAccepted(cfg, framingFromDownlinkKey(*obj.Key)) {
		return
	}

	timestamp, err := extractTimestampFromS3Key(*obj.Key)
	if err != nil {
		log.Printf("Warning: could not parse timestamp from key %s: %v", *obj.Key, err)
		return
	}

	msgTime := time.Unix(0, timestamp)
	if !msgTime.Before(connectionTime) {
		return
	}

	raw, err := downloadAndDecodeS3Object(ctx, s3c, bucket, obj.Key)
	if err != nil {
		streamErrf(err, "Could not download %s: %v", *obj.Key, err)
		return
	}

	msgProto := &streaming.FromStarPassMessage{}
	if err := proto.Unmarshal(raw, msgProto); err != nil {
		streamErrf(err, "Could not read %s: %v", *obj.Key, err)
		return
	}

	index := extractMessageIndex(msgProto)
	framing := extractMessageFraming(msgProto)
	// Dedup against the live MQTT path using the SAME key it uses: the bare channel
	// UUID plus framing. Here channelID carries a trailing "/downlink" (it doubles
	// as the stats/result channel key), but the MQTT handler keys receivedIndices
	// on the bare UUID (ExtractChannelIDFromTopic). Without stripping the suffix
	// the two paths write to different map entries and never dedup each other, so
	// every low-rate message that arrives on BOTH MQTT (live) and the S3 fallback
	// (backfill) is counted twice. Framing must also be part of the key: a
	// multi-framing channel republishes the SAME index once per framing (distinct
	// messages, not duplicates); see telemetryDedupKey's doc comment.
	channelKey := telemetryDedupKey(channelID, framing)

	mu.Lock()
	if receivedIndices[channelKey] == nil {
		receivedIndices[channelKey] = make(map[int]bool)
	}
	alreadyReceived := receivedIndices[channelKey][index]
	if !alreadyReceived {
		receivedIndices[channelKey][index] = true
	}
	mu.Unlock()

	if alreadyReceived {
		return
	}

	payload := extractMessagePayload(msgProto)
	metadata := extractMessageMetadata(msgProto, nil)

	// Send ack for S3 message via MQTT
	if client != nil && ackSender != nil && client.IsConnected() {
		// Build topic from S3 key (approximate - we need the actual MQTT topic)
		// Construct the topic from the S3 prefix
		// The topic format should match: <env>/<passID>/low_rate/channel/<channelID>
		topic := buildTelemetryTopicFromS3Key(cfg, *obj.Key, channelID)
		if topic != "" {
			framingType := ""
			if framing != "" {
				framingType = framing
			}
			// Ack.received_at is receipt time (this side's clock at handling):
			// an S3-backfilled message is acknowledged with the backfill time,
			// not the original capture time.
			ackPayload, err := buildTelemetryAck(
				cfg,
				channelID,
				framingType,
				index,
				len(payload),
				time.Now(),
				false,
				msgProto.GetMessageId(),
			)
			if err == nil {
				ackSender.sendAck(client, topic, ackPayload)
			}
		}
	}

	// channelID from extractChannelIDFromLowRateStream already includes "/downlink" for telemetry
	// Use it directly to match the format
	route(fetchResult{
		Index:     index,
		ChannelID: channelID, // Already in format "channelID/downlink" from extractChannelIDFromLowRateStream
		Framing:   framing,
		Data:      payload,
		Metadata:  metadata,
		Err:       nil,
		Source:    SourceS3,
	})
}

// scanS3TelemetryMessages scans S3 for missing telemetry messages for a specific channel.
func scanS3TelemetryMessages(
	ctx context.Context,
	s3c *s3.Client,
	bucket, prefix string,
	channelID string,
	connectionTime time.Time,
	route func(fetchResult),
	mu *sync.Mutex,
	receivedIndices map[string]map[int]bool,
	_ map[string]*int, // sequentialCounters - unused but kept for API compatibility
	_ *statsTracker, // stats - unused but kept for API compatibility
	cfg Config,
	client mqtt.Client,
	ackSender *ackSender,
) error {
	paginator := s3.NewListObjectsV2Paginator(s3c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range page.Contents {
			processS3TelemetryObject(
				ctx,
				s3c,
				bucket,
				&obj,
				channelID,
				connectionTime,
				route,
				mu,
				receivedIndices,
				cfg,
				client,
				ackSender,
			)
		}
	}

	return nil
}

func normalizeTimestamp(timestamp int64) int64 {
	if timestamp < minTimestampMicros {
		return timestamp * 1_000_000
	}
	return timestamp
}

func extractTimestampFromNonTelemetryKey(key string) (int64, error) {
	keyParts := strings.Split(key, "/")
	if len(keyParts) == 0 {
		return 0, errors.New("invalid key format")
	}
	filename := strings.TrimSuffix(keyParts[len(keyParts)-1], ".json")
	tsPart := filename
	if idx := strings.IndexByte(filename, '_'); idx != -1 {
		tsPart = filename[:idx]
	}
	timestamp, err := strconv.ParseInt(tsPart, 10, 64)
	if err != nil {
		return 0, err
	}
	return normalizeTimestamp(timestamp), nil
}

func extractMessageFromProtobuf(
	msgProto *streaming.FromStarPassMessage,
	msgType string,
) (proto.Message, string, string, int64, uint64) {
	var msg proto.Message
	var planID, antennaID string
	var recordedAtTimestamp int64
	var captureIndex uint64

	switch msgType {
	case msgTypeMonitoring:
		if m := msgProto.GetMonitoringMessage(); m != nil {
			msg = m
			planID = m.GetPassId()
			antennaID = m.GetAntennaId()
			captureIndex = m.GetIndex()
			if m.GetRecordedAt() != nil {
				recordedAtTimestamp = m.GetRecordedAt().AsTime().UnixNano()
			}
		}
	case msgTypeConfig:
		if m := msgProto.GetConfigurationMessage(); m != nil {
			msg = m
			planID = m.GetPassId()
			antennaID = m.GetAntennaId()
			captureIndex = m.GetIndex()
			if m.GetRecordedAt() != nil {
				recordedAtTimestamp = m.GetRecordedAt().AsTime().UnixNano()
			}
		}
	case msgTypeEvent:
		if m := msgProto.GetEventMessage(); m != nil {
			msg = m
			planID = m.GetPassId()
			antennaID = m.GetAntennaId()
			captureIndex = m.GetIndex()
			if m.GetRecordedAt() != nil {
				recordedAtTimestamp = m.GetRecordedAt().AsTime().UnixNano()
			}
		}
	}
	return msg, planID, antennaID, recordedAtTimestamp, captureIndex
}

func checkAndTrackNonTelemetryTimestamp(
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	msgType string,
	identity string,
) bool {
	if identity == "" {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	if receivedNonTelemetryTimestamps[msgType] == nil {
		receivedNonTelemetryTimestamps[msgType] = make(map[string]bool)
	}
	alreadyReceived := receivedNonTelemetryTimestamps[msgType][identity]
	if !alreadyReceived {
		receivedNonTelemetryTimestamps[msgType][identity] = true
	}
	return alreadyReceived
}

func shouldProcessMessageType(msgType string, cfg Config) bool {
	switch msgType {
	case msgTypeMonitoring:
		return cfg.EnableMonitoring
	case msgTypeConfig:
		return cfg.EnableConfigState
	case msgTypeEvent:
		return cfg.EnableEvent
	}
	return false
}

func processS3NonTelemetryObject(
	ctx context.Context,
	s3c *s3.Client,
	bucket string,
	obj *s3types.Object,
	msgType string,
	connectionTime time.Time,
	cfg Config,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) {
	if strings.Contains(*obj.Key, "/ack") {
		return
	}

	filenameTimestampNanos, err := extractTimestampFromNonTelemetryKey(*obj.Key)
	if err != nil {
		log.Printf("Warning: could not parse timestamp from key %s: %v", *obj.Key, err)
		return
	}

	msgTime := time.Unix(0, filenameTimestampNanos)
	if !msgTime.Before(connectionTime) {
		return
	}

	raw, err := downloadAndDecodeS3Object(ctx, s3c, bucket, obj.Key)
	if err != nil {
		streamErrf(err, "Could not download %s: %v", *obj.Key, err)
		return
	}

	msgProto := &streaming.FromStarPassMessage{}
	if err := proto.Unmarshal(raw, msgProto); err != nil {
		streamErrf(err, "Could not read %s: %v", *obj.Key, err)
		return
	}

	msg, planID, antennaID, recordedAtTimestamp, captureIndex := extractMessageFromProtobuf(msgProto, msgType)
	if msg == nil {
		vlogf(
			"Warning: %s message in %s does not contain expected message type",
			msgType,
			*obj.Key,
		)
		return
	}

	identity := nonTelemetryIdentity(captureIndex, recordedAtTimestamp)
	if identity == "" && filenameTimestampNanos > 0 {
		// Last resort for messages carrying neither identity: the S3 object's
		// own filename timestamp still dedupes re-reads of the same object,
		// though it cannot match the MQTT copy.
		identity = "s3t#" + strconv.FormatInt(filenameTimestampNanos, 10)
	}

	if checkAndTrackNonTelemetryTimestamp(
		mu,
		receivedNonTelemetryTimestamps,
		msgType,
		identity,
	) {
		// log.Printf("Skipping duplicate %s message (timestamp: %d ns, from S3: %s)", msgType, dedupTimestamp, *obj.Key)
		return
	}

	if shouldProcessMessageType(msgType, cfg) {
		handleNonTelemetryMessage(msg, cfg, stats, msgType, planID, antennaID, SourceS3)

		// Send ack for S3 message via MQTT
		if client != nil && ackSender != nil && client.IsConnected() {
			// Build topic from S3 key
			topic := buildNonTelemetryTopicFromS3Key(cfg, *obj.Key, msgType)
			if topic != "" {
				msgBytes := len(raw)
				ackMessageType := msgType
				if msgType == msgTypeConfig {
					ackMessageType = "configuration"
				}
				// Ack.received_at is receipt time (this side's clock at
				// handling): an S3-backfilled message is acknowledged with the
				// backfill time, not the original capture time.
				ackPayload, err := buildNonTelemetryAck(
					cfg, ackMessageType, captureIndex, msgBytes, time.Now(), msgProto.GetMessageId(),
				)
				if err == nil {
					ackSender.sendAck(client, topic, ackPayload)
				}
			}
		}
	}
}

// scanS3NonTelemetryMessages scans S3 for missing non-telemetry messages.
func scanS3NonTelemetryMessages(
	ctx context.Context,
	s3c *s3.Client,
	bucket, prefix, msgType string,
	connectionTime time.Time,
	cfg Config,
	mu *sync.Mutex,
	receivedNonTelemetryTimestamps map[string]map[string]bool,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) error {
	mu.Lock()
	if receivedNonTelemetryTimestamps[msgType] == nil {
		receivedNonTelemetryTimestamps[msgType] = make(map[string]bool)
	}
	mu.Unlock()

	paginator := s3.NewListObjectsV2Paginator(s3c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range page.Contents {
			processS3NonTelemetryObject(
				ctx,
				s3c,
				bucket,
				&obj,
				msgType,
				connectionTime,
				cfg,
				mu,
				receivedNonTelemetryTimestamps,
				stats,
				client,
				ackSender,
			)
		}
	}

	return nil
}
