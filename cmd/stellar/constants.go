package main

import "time"

// MQTT connection and retry timing.
const (
	// mqttConnectTimeout is the context deadline for a single connect attempt.
	mqttConnectTimeout = 30 * time.Second

	// mqttCombinedTimeout is the outer deadline that covers both the token wait
	// and receiving a CONNACK from the broker.
	mqttCombinedTimeout = 45 * time.Second

	// mqttConnAckWait is how long to wait for a CONNACK after the connect token
	// succeeds before declaring the attempt a failure.
	mqttConnAckWait = 15 * time.Second

	// mqttProgressInterval is how often a waiting-for-CONNACK log line is emitted.
	mqttProgressInterval = 10 * time.Second

	// mqttRetryBackoffInit is the starting backoff delay between connect retries.
	mqttRetryBackoffInit = 2 * time.Second

	// mqttRetryBackoffMax is the maximum backoff delay between connect retries.
	mqttRetryBackoffMax = 16 * time.Second

	// mqttMaxConnectRetries is the number of times connectMQTTClientWithRetry will attempt a connection.
	mqttMaxConnectRetries = 4

	// mqttDisconnectQuiesce is the milliseconds given to paho to flush in-flight
	// messages before forcefully closing the connection.
	mqttDisconnectQuiesce = 250

	// ackFlushTimeout bounds how long the ack sender waits for outstanding ack
	// publishes to drain before the MQTT client is disconnected, so the final ack
	// of a pass is not cut off mid-publish. A healthy connection drains in
	// milliseconds; this only caps a stuck shutdown.
	ackFlushTimeout = 5 * time.Second

	// ackPublishQoS is the MQTT QoS for telemetry/monitoring/config/event receipt
	// acks. These are best-effort receipt signals, one per received message, and a
	// high-volume pass produces thousands of them, often in an end-of-pass burst
	// as S3 catch-up completes. Publishing them at QoS 1 makes each publish wait for
	// a PUBACK; a burst exceeds AWS IoT's per-connection PUBACK throughput, so the
	// waits time out (flooding the log) and the saturated connection also starves
	// the QoS-1 command round-trip. QoS 0 sends acks fire-and-forget: no PUBACK, no
	// timeout, no inflight-window contention. Commands/config-requests stay at the
	// configured QoS because their delivery must be reliable.
	ackPublishQoS byte = 0

	// mqttSubscribeTimeout is the WaitTimeout for a single Subscribe token.
	mqttSubscribeTimeout = 30 * time.Second

	// mqttSubscribeMaxRetries is the number of per-topic subscribe attempts.
	mqttSubscribeMaxRetries = 3

	// mqttSubscribeConnWait is the sleep when the client is not yet connected
	// between subscribe retries.
	mqttSubscribeConnWait = 1 * time.Second

	// mqttSubscribeBackoff is the sleep after a subscribe timeout or error.
	mqttSubscribeBackoff = 500 * time.Millisecond

	// mqttPort is the AWS IoT port for mutual-TLS connections.
	mqttPort = 8883

	// mqttReconnectInterval is the paho SetConnectRetryInterval value.
	mqttReconnectInterval = 5 * time.Second

	// mqttPingTimeout is the paho SetPingTimeout value.
	mqttPingTimeout = 10 * time.Second

	// mqttWriteTimeout is the paho SetWriteTimeout value.
	mqttWriteTimeout = 10 * time.Second
)

// S3 and stream timing.
const (
	// s3FallbackInitialDelay is the pause before the first S3 fallback scan,
	// giving MQTT time to start receiving before we look for missed messages.
	s3FallbackInitialDelay = 2 * time.Second

	// autoCloseGracePeriod is how long the auto-close timer waits after the last
	// message is received before closing the output file for a channel.
	autoCloseGracePeriod = 5 * time.Second

	// statsLogInterval is how often the stats logger emits a progress line.
	statsLogInterval = 1 * time.Second

	// idleShutdownPoll is how often the idle-shutdown monitor samples the stream
	// for activity once the pass has ended.
	idleShutdownPoll = 1 * time.Second

	// idleShutdownInactivityDelay is how long the stream must stay quiet (past pass
	// end) before the close countdown is even announced. This avoids warning the
	// operator the instant the pass ends: only after this much continuous
	// inactivity do we surface "closing in 60 seconds...".
	idleShutdownInactivityDelay = 60 * time.Second

	// idleShutdownWarn60/30/10 are the "stream will close in N seconds" warning
	// thresholds, and idleShutdownGrace is the total countdown from the first
	// warning to the graceful close. The stream auto-closes only when the pass has
	// ended (past the latest of its scheduled/visibility/booking stop) AND no
	// message of any kind has been sent or received; any activity cancels the
	// countdown and it restarts the next time the stream falls idle.
	idleShutdownGrace  = 60 * time.Second
	idleShutdownWarn30 = 30 * time.Second
	idleShutdownWarn10 = 10 * time.Second
)

// Credential and retry timing.
const (
	// credentialRefreshMargin is how long before expiry the refresher proactively fetches new credentials.
	credentialRefreshMargin = 5 * time.Minute

	// s3ValidationRetryDelay is the pause between S3 validation retries when the error
	// is due to IAM propagation (InvalidAccessKeyId or AccessDenied immediately after
	// receiving fresh credentials from the authorizer).
	s3ValidationRetryDelay = 3 * time.Second

	// s3ValidationMaxRetries is the maximum number of additional S3 validation
	// attempts after the first failure when the error looks like a credential
	// propagation delay. Freshly issued credentials can take a while to become
	// effective, during which S3 returns AccessDenied; 30 retries at 3s covers
	// the residual lag and early-exits on the first success.
	s3ValidationMaxRetries = 30

	// credentialRefreshRetryDelay is how long the credential refresher waits before
	// retrying after a failed refresh call.
	credentialRefreshRetryDelay = 30 * time.Second

	// authorizerClientTimeout is the HTTP client timeout for the authorizer
	// call. The server provisions per-stream credentials and certificates, so
	// a generous timeout is needed.
	authorizerClientTimeout = 90 * time.Second

	// authorizerMaxRetries is the maximum number of retry attempts for 5xx errors.
	authorizerMaxRetries = 5

	// authorizerRetryInitialBackoff is the initial backoff delay between retries.
	authorizerRetryInitialBackoff = 1 * time.Second

	// authorizerRetryMaxBackoff is the maximum backoff delay between retries.
	authorizerRetryMaxBackoff = 10 * time.Second
)

// Command-sending timing.
const (
	// commandAckWait is how long a one-shot command sender waits after publishing
	// before exiting, giving the MQTT broker time to deliver the ack.
	commandAckWait = 2 * time.Second

	// interCommandDelay is the pause between commands when sending a batch.
	interCommandDelay = 100 * time.Millisecond

	// publishCommandTimeout is the time allowed for a single MQTT publish token to complete.
	publishCommandTimeout = 10 * time.Second
)

// HTTP client constants (used for S3 and authorizer calls).
const (
	// httpIdleConnTimeout is how long an idle keep-alive connection is retained in the pool.
	httpIdleConnTimeout = 120 * time.Second

	// httpDialTimeout is the timeout for establishing a new TCP connection.
	httpDialTimeout = 15 * time.Second

	// httpDialKeepAlive is the keep-alive period for TCP connections.
	httpDialKeepAlive = 60 * time.Second

	// httpClientTimeout is the overall timeout for very large S3 downloads.
	httpClientTimeout = 600 * time.Second

	// httpMaxIdleConns is the total maximum idle connections in the transport pool.
	httpMaxIdleConns = 1000

	// httpMaxIdleConnsPerHost is the per-host maximum idle connections.
	httpMaxIdleConnsPerHost = 500

	// httpMaxConnsPerHost is the hard per-host concurrency limit.
	httpMaxConnsPerHost = 1000
)

// Stats constants.
const (
	// statsInstantWindowDuration is the rolling window for instantaneous throughput stats.
	statsInstantWindowDuration = 10 * time.Second

	// statsInstantWindowMaxSamples is the maximum number of samples retained in the window.
	statsInstantWindowMaxSamples = 50_000
)

// Flag defaults.
const (
	// defaultWindowSize is the default number of in-flight S3 downloads.
	defaultWindowSize = 400

	// defaultS3PollInterval is the default polling interval for new S3 objects.
	defaultS3PollInterval = 1 * time.Second

	// mainErrChBuf is the buffer size of the top-level error channel in main.
	mainErrChBuf = 10

	// proxyDownlinkChanBuf is the buffer size of the channel feeding received
	// telemetry to the downlink proxy.
	proxyDownlinkChanBuf = 1000
)

// MQTT and S3 buffer sizing.
const (
	// mqttResultsChWindowMultiplier is how many times the window size the MQTT
	// results channel buffer is set to.
	mqttResultsChWindowMultiplier = 10

	// mqttResultsChMinBuf is the minimum MQTT results channel buffer size regardless
	// of window size (handles small window sizes in high-throughput scenarios).
	mqttResultsChMinBuf = 1000

	// s3DownloadCopyBufSize is the size of the per-request copy buffer used when
	// streaming S3 object bodies into memory.
	s3DownloadCopyBufSize = 4 << 20 // 4 MiB

	// s3SchedulerTick is how often the high-rate S3 scheduler goroutine re-checks
	// whether any channel/framing's in-flight window has room for new fetches.
	// Consumption now happens on independent per-key worker goroutines (see
	// keyed_result_router.go) rather than in the scheduler's own loop, so the
	// scheduler no longer paces itself by waiting on individual results; a short
	// tick keeps the window well-fed without meaningful CPU cost.
	s3SchedulerTick = 50 * time.Millisecond

	// minTimestampMicros is the minimum plausible value for a microsecond-epoch timestamp.
	// Values below this are treated as second-epoch and multiplied by 1_000_000.
	// Equals 2000-01-01T00:00:00Z expressed in microseconds since Unix epoch.
	minTimestampMicros = 946_684_800_000_000
)
