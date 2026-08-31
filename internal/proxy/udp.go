package proxy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	udpReadBuf     = 1024 * 1024
	udpReadTimeout = 500 * time.Millisecond
)

// UDPConfig holds the addresses for the UDP proxy.
type UDPConfig struct {
	ListenAddr string // address to listen for uplink data (e.g. ":6000")
	SendAddr   string // address to send downlink data to (e.g. "127.0.0.1:6001")
}

// UDPProxy bridges a downlink channel to a local UDP send address and listens
// on a local UDP port for uplink data.
type UDPProxy struct {
	cfg        UDPConfig
	downlinkCh <-chan []byte
	uplinkFn   UplinkFunc

	recvConn net.PacketConn
	sendConn net.Conn

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewUDP creates a UDP proxy. It immediately binds the listen and send sockets.
func NewUDP(ctx context.Context, cfg UDPConfig, downlinkCh <-chan []byte, uplinkFn UplinkFunc) (*UDPProxy, error) {
	recvConn, err := (&net.ListenConfig{}).ListenPacket(ctx, "udp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen UDP %s: %w", cfg.ListenAddr, err)
	}

	sendConn, err := (&net.Dialer{}).DialContext(ctx, "udp", cfg.SendAddr)
	if err != nil {
		_ = recvConn.Close()
		return nil, fmt.Errorf("dial UDP %s: %w", cfg.SendAddr, err)
	}

	return &UDPProxy{
		cfg:        cfg,
		downlinkCh: downlinkCh,
		uplinkFn:   uplinkFn,
		recvConn:   recvConn,
		sendConn:   sendConn,
		closeCh:    make(chan struct{}),
	}, nil
}

// Start begins forwarding. The caller should run this in a goroutine and call
// Close to stop.
func (p *UDPProxy) Start() error {
	log.Printf("UDP proxy started: uplink listen=%s, downlink send=%s",
		p.recvConn.LocalAddr(), p.sendConn.RemoteAddr())

	p.wg.Add(2)
	go p.sendLoop()
	go p.recvLoop()
	p.wg.Wait()
	return nil
}

// Close tears down the proxy. Safe to call multiple times.
func (p *UDPProxy) Close() error {
	p.closeOnce.Do(func() { close(p.closeCh) })
	_ = p.recvConn.Close()
	_ = p.sendConn.Close()
	p.wg.Wait()
	return nil
}

func (p *UDPProxy) sendLoop() {
	defer p.wg.Done()
	for {
		select {
		case payload, ok := <-p.downlinkCh:
			if !ok {
				return
			}
			if _, err := p.sendConn.Write(payload); err != nil {
				log.Printf("UDP proxy send error: %v", err)
			}
		case <-p.closeCh:
			return
		}
	}
}

func (p *UDPProxy) recvLoop() {
	defer p.wg.Done()
	buf := make([]byte, udpReadBuf)
	for {
		select {
		case <-p.closeCh:
			return
		default:
		}

		_ = p.recvConn.SetReadDeadline(time.Now().Add(udpReadTimeout))
		n, _, err := p.recvConn.ReadFrom(buf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) {
				continue
			}
			select {
			case <-p.closeCh:
				return
			default:
				log.Printf("UDP proxy recv error: %v", err)
				return
			}
		}
		if n > 0 && p.uplinkFn != nil {
			data := make([]byte, n)
			copy(data, buf[:n])
			p.uplinkFn(data)
		}
	}
}
