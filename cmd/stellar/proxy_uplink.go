package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

// proxyUplinkSender transmits bytes received on the local proxy socket to the
// satellite, over the same MQTT commanding path interactive mode uses: the
// authorizer's uplink topic for the pass, guarded by the booking window. Sends
// are serialized under a mutex so the per-topic command index stays strictly
// increasing even when the proxy delivers from several connections at once.
type proxyUplinkSender struct {
	pub      *commandPublisher
	topic    string
	streamID string
	planID   string

	mu           sync.Mutex
	index        uint32
	windowWarned bool
}

// errNoUplinkChannel reports that the pass has no channel that accepts
// satellite commands, so proxy uplink cannot be wired. Callers degrade to a
// downlink-only proxy in that case rather than failing the stream.
var errNoUplinkChannel = errors.New("this pass has no uplink channel")

// newProxyUplinkSender resolves the pass's uplink topic and connects a
// dedicated MQTT client for proxy commanding. It requires authorizer
// credentials; the booking window is resolved up front and re-checked on
// every send, mirroring interactive mode.
func newProxyUplinkSender(
	ctx context.Context, cfg Config, passID string, stats *statsTracker,
) (*proxyUplinkSender, error) {
	if cfg.AuthorizerCreds == nil {
		return nil, errors.New("an activated API key is required to transmit uplink data")
	}

	topic := firstCommandable(cfg.AuthorizerCreds, cfg.AuthorizerCreds.Streams.Uplink).PublishTopic
	if topic == "" {
		return nil, errNoUplinkChannel
	}

	client, err := setupMQTTClient(ctx, cfg, nil)
	if err != nil {
		return nil, err
	}
	subscribeCommandReplies(client, cfg, stats)

	streamID, planID := normalizeIDs(cfg, "", "", passID)
	window := resolveBookingWindow(ctx, cfg, passID)

	return &proxyUplinkSender{
		pub:      newCommandPublisher(client, cfg.MQTTQoS, stats, window),
		topic:    topic,
		streamID: streamID,
		planID:   planID,
		index:    1,
	}, nil
}

// send transmits one proxy-received payload as a satellite command. Failures
// are reported and the payload is dropped; the proxy keeps running so a
// transient rejection (for example, sending before the booking opens) does not
// tear down the stream.
func (s *proxyUplinkSender) send(data []byte) {
	if len(data) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.pub.window.check(time.Now()); err != nil {
		if !s.windowWarned {
			s.windowWarned = true
			uiWarnf("Proxy uplink data was not transmitted: %v", err)
		}
		vlogf("proxy uplink: dropped %d bytes (outside the booking window)", len(data))
		return
	}
	s.windowWarned = false

	idx := s.index
	cmd, err := PublishSatCommand(s.pub, s.topic, s.streamID, s.planID, idx, [][]byte{data})
	if err != nil {
		uiErrf("Could not transmit %d bytes of proxy uplink data: %v", len(data), err)
	} else {
		vlogf("proxy uplink: transmitted command %d (%d bytes)", idx, len(data))
	}
	// An unconfirmed publish may still arrive, so its index stays used.
	if cmd != nil {
		s.index++
	}
}

// close disconnects the sender's MQTT client.
func (s *proxyUplinkSender) close() {
	if s.pub != nil && s.pub.client != nil {
		s.pub.client.Disconnect(mqttDisconnectQuiesce)
	}
}
