package proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	tcpReadBuf     = 1024 * 1024
	tcpReadTimeout = 500 * time.Millisecond
)

// TCPConfig holds the listen address for the TCP proxy.
type TCPConfig struct {
	ListenAddr string // address to listen on (e.g. ":6001")
}

// TCPProxy listens on a local TCP port. Downlink data is fanned out to all
// connected clients; uplink data from any client is forwarded via the uplink
// callback.
type TCPProxy struct {
	cfg        TCPConfig
	downlinkCh <-chan []byte
	uplinkFn   UplinkFunc

	listener net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewTCP creates a TCP proxy. It immediately binds the listen socket.
func NewTCP(ctx context.Context, cfg TCPConfig, downlinkCh <-chan []byte, uplinkFn UplinkFunc) (*TCPProxy, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, err
	}
	return &TCPProxy{
		cfg:        cfg,
		downlinkCh: downlinkCh,
		uplinkFn:   uplinkFn,
		listener:   ln,
		conns:      make(map[net.Conn]struct{}),
		closeCh:    make(chan struct{}),
	}, nil
}

// Start begins accepting connections and forwarding data.
func (p *TCPProxy) Start() error {
	log.Printf("TCP proxy started: listen=%s", p.listener.Addr())

	p.wg.Add(2)
	go p.acceptLoop()
	go p.fanOutLoop()
	p.wg.Wait()
	return nil
}

// Close shuts down the proxy. Safe to call multiple times.
func (p *TCPProxy) Close() error {
	p.closeOnce.Do(func() { close(p.closeCh) })
	_ = p.listener.Close()

	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()

	p.wg.Wait()
	return nil
}

func (p *TCPProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.closeCh:
				return
			default:
				log.Printf("TCP proxy accept error: %v", err)
				return
			}
		}
		log.Printf("TCP proxy: new client %s", conn.RemoteAddr())
		p.mu.Lock()
		p.conns[conn] = struct{}{}
		p.mu.Unlock()

		go p.handleConn(conn)
	}
}

func (p *TCPProxy) fanOutLoop() {
	defer p.wg.Done()
	for {
		select {
		case payload, ok := <-p.downlinkCh:
			if !ok {
				return
			}
			p.mu.Lock()
			for c := range p.conns {
				if _, err := c.Write(payload); err != nil {
					log.Printf("TCP proxy write error to %s: %v", c.RemoteAddr(), err)
				}
			}
			p.mu.Unlock()
		case <-p.closeCh:
			return
		}
	}
}

func (p *TCPProxy) handleConn(conn net.Conn) {
	defer func() {
		p.mu.Lock()
		delete(p.conns, conn)
		p.mu.Unlock()
		_ = conn.Close()
		log.Printf("TCP proxy: client %s disconnected", conn.RemoteAddr())
	}()

	buf := make([]byte, tcpReadBuf)
	for {
		select {
		case <-p.closeCh:
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(tcpReadTimeout))
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) {
				continue
			}
			return
		}
		if n > 0 && p.uplinkFn != nil {
			data := make([]byte, n)
			copy(data, buf[:n])
			p.uplinkFn(data)
		}
	}
}
