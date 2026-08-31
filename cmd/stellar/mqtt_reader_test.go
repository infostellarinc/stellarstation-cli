package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

func TestResolveMQTTClientID(t *testing.T) {
	tests := []struct {
		name          string
		credsClientID string
		wantPrefix    string
		wantUnique    bool
	}{
		{
			name:          "appends unique numeric suffix to authorizer clientID",
			credsClientID: "creds-client",
			wantPrefix:    "creds-client-",
			wantUnique:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveMQTTClientID(tt.credsClientID)
			if !startsWith(got, tt.wantPrefix) {
				t.Errorf("resolveMQTTClientID() = %q, want prefix %q", got, tt.wantPrefix)
			}
			if tt.wantUnique {
				got2 := resolveMQTTClientID(tt.credsClientID)
				if got == got2 {
					t.Errorf(
						"resolveMQTTClientID() should generate unique IDs on each call: both = %q",
						got,
					)
				}
			}
		})
	}
}

// extractMessageIndex must return the dense per-message sequence
// (message_index), which the ordered writer's advance-by-one cursor and the
// storage keys rely on. A multi-frame bundle makes first_frame_index sparse
// (1, 3, 5, ...), so using it would stall ordered delivery after the first
// bundle.
func TestExtractMessageIndex(t *testing.T) {
	multiFrame := &streaming.FromStarPassMessage{
		Message: &streaming.FromStarPassMessage_SendTelemetryMessage{
			SendTelemetryMessage: &streaming.SendTelemetryMessage{
				FirstFrameIndex: 3,
				MessageIndex:    2,
			},
		},
	}
	if got := extractMessageIndex(multiFrame); got != 2 {
		t.Errorf("extractMessageIndex() = %d, want the dense message_index 2", got)
	}

	noMessageIndex := &streaming.FromStarPassMessage{
		Message: &streaming.FromStarPassMessage_SendTelemetryMessage{
			SendTelemetryMessage: &streaming.SendTelemetryMessage{FirstFrameIndex: 42},
		},
	}
	if got := extractMessageIndex(noMessageIndex); got != 42 {
		t.Errorf("extractMessageIndex() = %d, want the first_frame_index fallback 42", got)
	}
}

func TestExtractMessagePayload(t *testing.T) {
	// This would require creating protobuf messages
	t.Skip("Requires protobuf message creation - integration test needed")
}

func TestExtractMessageFraming(t *testing.T) {
	// This would require creating protobuf messages
	t.Skip("Requires protobuf message creation - integration test needed")
}

func TestExtractMessageMetadata(t *testing.T) {
	// This would require creating protobuf messages and MQTT messages
	t.Skip("Requires protobuf and MQTT message creation - integration test needed")
}

func TestExtractTimestampFromS3Key(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "valid timestamp",
			key:     "pass-123/channel/1/1234567890000000",
			wantErr: false,
		},
		{
			name:    "timestamp with underscore",
			key:     "pass-123/channel/1/1234567890000000_extra",
			wantErr: false,
		},
		{
			name:    "invalid key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "non-numeric timestamp",
			key:     "pass-123/channel/1/abc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractTimestampFromS3Key(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractTimestampFromS3Key() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExtractTimestampFromNonTelemetryKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{
			name:    "valid timestamp",
			key:     "pass-123/monitoring/1234567890000000.json",
			wantErr: false,
		},
		{
			name:    "timestamp with underscore",
			key:     "pass-123/monitoring/1234567890000000_extra.json",
			wantErr: false,
		},
		{
			name:    "invalid key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "non-numeric timestamp",
			key:     "pass-123/monitoring/abc.json",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractTimestampFromNonTelemetryKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf(
					"extractTimestampFromNonTelemetryKey() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}

func TestNormalizeTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		want      int64
	}{
		{
			name:      "nanoseconds (already normalized)",
			timestamp: 1234567890000000,
			want:      1234567890000000,
		},
		{
			name:      "milliseconds (needs normalization)",
			timestamp: 1234567890,
			want:      1234567890000000,
		},
		{
			name:      "seconds (needs normalization)",
			timestamp: 1234567,
			want:      1234567000000, // Multiply by 1,000,000 to convert seconds to nanoseconds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeTimestamp(tt.timestamp)
			if got != tt.want {
				t.Errorf("normalizeTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldProcessMessageType(t *testing.T) {
	tests := []struct {
		name    string
		msgType string
		cfg     Config
		want    bool
	}{
		{
			name:    "monitoring enabled",
			msgType: msgTypeMonitoring,
			cfg:     Config{EnableMonitoring: true},
			want:    true,
		},
		{
			name:    "monitoring disabled",
			msgType: msgTypeMonitoring,
			cfg:     Config{EnableMonitoring: false},
			want:    false,
		},
		{
			name:    "config enabled",
			msgType: msgTypeConfig,
			cfg:     Config{EnableConfigState: true},
			want:    true,
		},
		{
			name:    "config disabled",
			msgType: msgTypeConfig,
			cfg:     Config{EnableConfigState: false},
			want:    false,
		},
		{
			name:    "event enabled",
			msgType: msgTypeEvent,
			cfg:     Config{EnableEvent: true},
			want:    true,
		},
		{
			name:    "event disabled",
			msgType: msgTypeEvent,
			cfg:     Config{EnableEvent: false},
			want:    false,
		},
		{
			name:    "unknown message type",
			msgType: "unknown",
			cfg:     Config{EnableMonitoring: true},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldProcessMessageType(tt.msgType, tt.cfg)
			if got != tt.want {
				t.Errorf("shouldProcessMessageType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMQTTExtractFramingFromS3Key(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "BITSTREAM framing",
			key:  "pass-123/1/BITSTREAM/1",
			want: "BITSTREAM",
		},
		{
			name: "IQ framing",
			key:  "pass-123/1/IQ/1",
			want: "IQ",
		},
		{
			name: "AX25 framing",
			key:  "pass-123/1/AX25/1",
			want: "AX25",
		},
		{
			name: "invalid key",
			key:  "pass-123/1",
			want: "",
		},
		{
			name: "unknown framing",
			key:  "pass-123/1/UNKNOWN/1",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFramingFromS3Key(tt.key)
			if got != tt.want {
				t.Errorf("extractFramingFromS3Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsNotFound(t *testing.T) {
	// This would require creating S3 error types
	// For now, we'll test that the function exists
	t.Skip("Requires S3 error types - integration test needed")
}

// mustGenerateSelfSignedCert returns a PEM-encoded self-signed cert and key pair for testing,
// calling t.Fatal on failure.
func mustGenerateSelfSignedCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	c, k, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("failed to generate test TLS cert: %v", err)
	}
	return c, k
}

// generateSelfSignedCert returns a PEM-encoded self-signed cert and key pair for testing.
func generateSelfSignedCert() (certPEM, keyPEM string, err error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}

	var certBuf, keyBuf bytes.Buffer
	if err := pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return "", "", err
	}
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	if err := pem.Encode(&keyBuf, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER}); err != nil {
		return "", "", err
	}
	return certBuf.String(), keyBuf.String(), nil
}

func TestBuildMQTTConnectionConfigFromCert(t *testing.T) {
	ctx := t.Context()
	testCertPEM, testKeyPEM := mustGenerateSelfSignedCert(t)

	tests := []struct {
		name       string
		cfg        Config
		creds      *AuthorizerCredentials
		wantTopics int
		wantErrStr string
	}{
		{
			name: "all stream types populated",
			cfg:  Config{},
			creds: &AuthorizerCredentials{
				IoTCertEndpoint:   "abc123.iot.us-east-1.amazonaws.com",
				IoTCertificatePem: testCertPEM,
				IoTPrivateKeyPem:  testKeyPEM,
				Streams: StreamsConfig{
					LowRate: []StreamConfig{
						{MqttTopic: "dev/pass-123/low_rate/channel/1"},
						{MqttTopic: "dev/pass-123/low_rate/channel/2"},
					},
					Monitoring:    &StreamConfig{MqttTopic: "dev/pass-123/monitoring"},
					Config:        &StreamConfig{MqttTopic: "dev/pass-123/config"},
					Event:         &StreamConfig{MqttTopic: "dev/pass-123/event"},
					Uplink:        []CommandConfig{{AckTopic: "dev/pass-123/uplink/ack"}},
					ConfigRequest: []CommandConfig{{AckTopic: "dev/pass-123/config_request/ack"}},
				},
			},
			wantTopics: 7,
		},
		{
			name: "only low-rate topics",
			cfg:  Config{},
			creds: &AuthorizerCredentials{
				IoTCertEndpoint:   "abc123.iot.us-east-1.amazonaws.com",
				IoTCertificatePem: testCertPEM,
				IoTPrivateKeyPem:  testKeyPEM,
				Streams: StreamsConfig{
					LowRate: []StreamConfig{
						{MqttTopic: "dev/pass-123/low_rate/channel/1"},
					},
				},
			},
			wantTopics: 1,
		},
		{
			name: "missing certificate: error",
			cfg:  Config{},
			creds: &AuthorizerCredentials{
				IoTCertEndpoint:   "abc123.iot.us-east-1.amazonaws.com",
				IoTCertificatePem: "",
				IoTPrivateKeyPem:  "",
			},
			wantErrStr: "parse IoT certificate",
		},
		{
			name: "missing endpoint: error",
			cfg:  Config{},
			creds: &AuthorizerCredentials{
				IoTCertEndpoint:   "",
				IoTCertificatePem: testCertPEM,
				IoTPrivateKeyPem:  testKeyPEM,
			},
			wantErrStr: "did not provide an IoT endpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connCfg, err := buildMQTTConnectionConfigFromCert(ctx, tt.cfg, tt.creds)
			if tt.wantErrStr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q but got nil", tt.wantErrStr)
				}
				if !strings.Contains(err.Error(), tt.wantErrStr) {
					t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErrStr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildMQTTConnectionConfigFromCert() unexpected error: %v", err)
			}
			if connCfg == nil {
				t.Fatal("returned nil config")
				return
			}
			if len(connCfg.topics) != tt.wantTopics {
				t.Errorf("topics count = %d, want %d", len(connCfg.topics), tt.wantTopics)
			}
			if !strings.HasPrefix(connCfg.broker, "ssl://") {
				t.Errorf("broker scheme should be ssl://, got %q", connCfg.broker)
			}
			if !strings.HasSuffix(connCfg.broker, ":8883") {
				t.Errorf("broker should use port 8883, got %q", connCfg.broker)
			}
			if connCfg.tlsCertificate == nil {
				t.Error("tlsCertificate should be set")
			}
		})
	}
}

// Helper function
func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
