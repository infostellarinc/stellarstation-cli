package proxy

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestParseMode(t *testing.T) {
	tests := []struct {
		input string
		want  Mode
		err   bool
	}{
		{"", ModeDisabled, false},
		{"disabled", ModeDisabled, false},
		{"udp", ModeUDP, false},
		{"tcp", ModeTCP, false},
		{"UDP", ModeUDP, false},
		{"TCP", ModeTCP, false},
		{" udp ", ModeUDP, false},
		{"Disabled", ModeDisabled, false},
		{"websocket", "", true},
	}
	for _, tt := range tests {
		got, err := ParseMode(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("ParseMode(%q) error = %v, wantErr %v", tt.input, err, tt.err)
		}
		if got != tt.want {
			t.Errorf("ParseMode(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestUDPProxy_DownlinkSend(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	recvConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()

	p, err := NewUDP(
		context.Background(),
		UDPConfig{
			ListenAddr: "127.0.0.1:0",
			SendAddr:   recvConn.LocalAddr().String(),
		},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	downlinkCh <- []byte("hello-downlink")

	buf := make([]byte, 1024)
	recvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := recvConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read downlink: %v", err)
	}

	if string(buf[:n]) != "hello-downlink" {
		t.Errorf("received = %q, want hello-downlink", string(buf[:n]))
	}
}

func TestUDPProxy_UplinkRecv(t *testing.T) {
	downlinkCh := make(chan []byte, 10)
	uplinkData := make(chan []byte, 10)

	p, err := NewUDP(
		context.Background(),
		UDPConfig{
			ListenAddr: "127.0.0.1:0",
			SendAddr:   "127.0.0.1:19999",
		},
		downlinkCh,
		func(data []byte) { uplinkData <- data },
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	conn, err := net.Dial("udp", p.recvConn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("uplink-cmd"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case data := <-uplinkData:
		if string(data) != "uplink-cmd" {
			t.Errorf("uplink = %q, want uplink-cmd", string(data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for uplink data")
	}
}

func TestTCPProxy_DownlinkFanOut(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond) // let the accept loop register

	downlinkCh <- []byte("tcp-downlink")

	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read tcp: %v", err)
	}
	if string(buf[:n]) != "tcp-downlink" {
		t.Errorf("received = %q, want tcp-downlink", string(buf[:n]))
	}
}

func TestTCPProxy_UplinkRecv(t *testing.T) {
	downlinkCh := make(chan []byte, 10)
	uplinkData := make(chan []byte, 10)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		func(data []byte) { uplinkData <- data },
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	_, err = conn.Write([]byte("tcp-uplink"))
	if err != nil {
		t.Fatal(err)
	}

	select {
	case data := <-uplinkData:
		if string(data) != "tcp-uplink" {
			t.Errorf("uplink = %q, want tcp-uplink", string(data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for TCP uplink data")
	}
}

func TestTCPProxy_MultipleClients(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	addr := p.listener.Addr().String()
	c1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	c2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	time.Sleep(50 * time.Millisecond)

	downlinkCh <- []byte("fanout")

	for _, c := range []net.Conn{c1, c2} {
		buf := make([]byte, 1024)
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(buf[:n]) != "fanout" {
			t.Errorf("received = %q, want fanout", string(buf[:n]))
		}
	}
}

// TestNewTCP_ListenError checks that NewTCP returns an error when the address
// is invalid and cannot be bound.
func TestNewTCP_ListenError(t *testing.T) {
	_, err := NewTCP(context.Background(), TCPConfig{ListenAddr: "256.0.0.1:0"}, nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid listen address")
	}
}

// TestNewUDP_ListenError checks that NewUDP returns an error when the listen
// address is invalid.
func TestNewUDP_ListenError(t *testing.T) {
	_, err := NewUDP(
		context.Background(),
		UDPConfig{ListenAddr: "256.0.0.1:0", SendAddr: "127.0.0.1:1"},
		nil, nil,
	)
	if err == nil {
		t.Fatal("expected error for invalid listen address")
	}
}

// TestNewUDP_DialError checks that NewUDP returns an error (and closes the
// recv socket) when the send address is invalid.
func TestNewUDP_DialError(t *testing.T) {
	_, err := NewUDP(
		context.Background(),
		UDPConfig{ListenAddr: "127.0.0.1:0", SendAddr: "256.0.0.1:1"},
		nil, nil,
	)
	if err == nil {
		t.Fatal("expected error for invalid send address")
	}
}

// TestTCPProxy_ClosedDownlinkChannel verifies that fanOutLoop exits cleanly
// when the downlink channel is closed.
func TestTCPProxy_ClosedDownlinkChannel(t *testing.T) {
	downlinkCh := make(chan []byte, 1)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Start() }()

	// Close the channel; fanOutLoop should return, and since acceptLoop is
	// also running, we call Close to unblock it.
	close(downlinkCh)
	p.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop after channel close")
	}
}

// TestUDPProxy_ClosedDownlinkChannel verifies that sendLoop exits cleanly when
// the downlink channel is closed.
func TestUDPProxy_ClosedDownlinkChannel(t *testing.T) {
	downlinkCh := make(chan []byte, 1)

	p, err := NewUDP(
		context.Background(),
		UDPConfig{ListenAddr: "127.0.0.1:0", SendAddr: "127.0.0.1:19998"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Start() }()

	close(downlinkCh)
	p.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop after channel close")
	}
}

// TestTCPProxy_ClientDisconnect ensures that handleConn cleans up when the
// remote side closes the connection (EOF path).
func TestTCPProxy_ClientDisconnect(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	go p.Start()
	defer p.Close()

	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Close the connection; handleConn should get EOF and clean up.
	conn.Close()

	// Give the proxy a moment to clean up the conn map.
	time.Sleep(100 * time.Millisecond)

	p.mu.Lock()
	n := len(p.conns)
	p.mu.Unlock()
	if n != 0 {
		t.Errorf("conns map has %d entries after disconnect, want 0", n)
	}
}

// TestTCPProxy_CloseSignalsHandleConn exercises the closeCh branch inside
// handleConn by closing the proxy while a client is connected.
func TestTCPProxy_CloseSignalsHandleConn(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	p, err := NewTCP(
		context.Background(),
		TCPConfig{ListenAddr: "127.0.0.1:0"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Start() }()

	conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)
	p.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop after Close")
	}
}

// TestUDPProxy_CloseFromRecvLoop exercises the closeCh select inside recvLoop
// by closing the proxy while the recv loop is blocked.
func TestUDPProxy_CloseFromRecvLoop(t *testing.T) {
	downlinkCh := make(chan []byte, 10)

	p, err := NewUDP(
		context.Background(),
		UDPConfig{ListenAddr: "127.0.0.1:0", SendAddr: "127.0.0.1:19997"},
		downlinkCh,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- p.Start() }()

	time.Sleep(20 * time.Millisecond)
	p.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not stop after Close")
	}
}
