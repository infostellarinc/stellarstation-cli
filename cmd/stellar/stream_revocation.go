package main

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

// exitStreamRevoked: the stream was revoked, by an operator or by another
// client taking over commanding, and StellarStation disconnected it. It
// cannot be reopened; a new stream is needed.
const exitStreamRevoked = 4

// revocationCheckTimeout bounds the authorizer call made after a lost MQTT
// connection.
const revocationCheckTimeout = 15 * time.Second

// streamRevocationWatch notices that this stream was revoked and reports it
// once. The authorizer refuses to authorize a revoked stream again, so a
// refusal on a credential refresh is a sure sign; and since StellarStation
// disconnects a revoked stream at once, a lost MQTT connection is checked
// against the authorizer too, rather than leaving the client to reconnect
// until its credentials expire.
type streamRevocationWatch struct {
	apiURL      string
	tokenSource auth.TokenSource
	authReq     AuthorizerRequest
	store       *credentialStore
	// onRevoked is called once, with the authorizer's explanation.
	onRevoked func(detail string)
	revoked   atomic.Bool
	checking  atomic.Bool
}

func newStreamRevocationWatch(
	apiURL string,
	tokenSource auth.TokenSource,
	authReq AuthorizerRequest,
	store *credentialStore,
	onRevoked func(detail string),
) *streamRevocationWatch {
	return &streamRevocationWatch{
		apiURL: apiURL, tokenSource: tokenSource, authReq: authReq, store: store, onRevoked: onRevoked,
	}
}

// refresh authorizes this stream again and stores the fresh credentials. A
// stream_revoked refusal marks the stream revoked and is returned.
func (w *streamRevocationWatch) refresh(ctx context.Context) (*AuthorizerCredentials, error) {
	creds, err := fetchAuthorizerCredentials(ctx, w.apiURL, w.tokenSource, w.authReq)
	if err != nil {
		if isAuthorizerError(err, authorizerErrorStreamRevoked) {
			w.markRevoked(revocationDetail(err))
		}
		return nil, err
	}
	w.store.store(creds)
	return creds, nil
}

// connectionLost is the MQTT connection-lost hook. It reports whether the
// loss is already explained by a revocation; otherwise it asks the authorizer
// in the background, one check at a time, and reports through onRevoked. A
// nil watch (a session opened without an authorizer) is never revoked.
func (w *streamRevocationWatch) connectionLost() bool {
	if w == nil {
		return false
	}
	if w.revoked.Load() {
		return true
	}
	if !w.checking.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		defer w.checking.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), revocationCheckTimeout)
		defer cancel()
		if _, err := w.refresh(ctx); err != nil && !w.revoked.Load() {
			vlogf("could not check the stream after the lost connection: %v", err)
		}
	}()
	return false
}

func (w *streamRevocationWatch) markRevoked(detail string) {
	if w.revoked.CompareAndSwap(false, true) && w.onRevoked != nil {
		w.onRevoked(detail)
	}
}

// isRevoked reports whether the stream was revoked; a nil watch never is.
func (w *streamRevocationWatch) isRevoked() bool { return w != nil && w.revoked.Load() }

// revocationDetail is the authorizer's explanation of a revocation, such as
// "this stream was revoked by user-1: stale client; open a new stream instead".
func revocationDetail(err error) string {
	var ae *authorizerError
	if errors.As(err, &ae) && ae.Message != "" {
		return ae.Message
	}
	return "this stream was revoked"
}

// reportStreamRevoked tells the operator what happened and what to do; the
// session then stops.
func reportStreamRevoked(detail string) {
	uiErrf("Stream revoked: %s.", detail)
	uiInfof("StellarStation has disconnected this stream and it cannot be reopened. " +
		"Open a new stream to continue.")
}
