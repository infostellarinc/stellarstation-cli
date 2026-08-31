package main

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// credentialStore holds the current AuthorizerCredentials and allows atomic swaps.
// Both the S3 credentials provider and the MQTT reconnect hook read from this.
type credentialStore struct {
	ptr atomic.Pointer[AuthorizerCredentials]
}

func newCredentialStore(initial *AuthorizerCredentials) *credentialStore {
	cs := &credentialStore{}
	cs.ptr.Store(initial)
	return cs
}

// load returns the current credentials. Never returns nil after construction.
func (cs *credentialStore) load() *AuthorizerCredentials {
	return cs.ptr.Load()
}

// store atomically replaces the credentials.
func (cs *credentialStore) store(creds *AuthorizerCredentials) {
	cs.ptr.Store(creds)
}

// refreshingCredentialsProvider implements aws.CredentialsProvider.
// The AWS SDK calls Retrieve() on every S3 request, so it always uses the
// latest credentials from the store without needing to rebuild the S3 client.
type refreshingCredentialsProvider struct {
	store *credentialStore
}

// Retrieve implements aws.CredentialsProvider.
func (p *refreshingCredentialsProvider) Retrieve(_ context.Context) (aws.Credentials, error) {
	creds := p.store.load()
	return aws.Credentials{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Source:          "AuthorizerRefreshing",
		CanExpire:       true,
		Expires:         creds.Expiration,
	}, nil
}

// runCredentialRefresher runs in the background and refreshes credentials before they
// expire. It updates the credentialStore so that the S3 client automatically picks up
// new credentials on the next request (via refreshingCredentialsProvider.Retrieve) and
// the MQTT client uses a fresh IoT certificate on its next hard reconnect cycle.
// The refresher stops when ctx is cancelled (i.e. when the CLI shuts down).
func runCredentialRefresher(
	ctx context.Context,
	store *credentialStore,
	apiURL string,
	tokenSource auth.TokenSource,
	authReq AuthorizerRequest,
) {
	for {
		current := store.load()
		untilExpiry := time.Until(current.Expiration)
		refreshIn := untilExpiry - credentialRefreshMargin
		if refreshIn <= 0 {
			refreshIn = 0
		}

		vlogf("credential refresher: next refresh in %v (expiry %s)",
			refreshIn.Round(time.Second), current.Expiration.Format(time.RFC3339))

		select {
		case <-ctx.Done():
			vlogf("credential refresher: stopped")
			return
		case <-time.After(refreshIn):
		}

		vlogf("credential refresher: refreshing credentials (expiry was %s)",
			current.Expiration.Format(time.RFC3339))

		newCreds, err := fetchAuthorizerCredentials(ctx, apiURL, tokenSource, authReq)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			vlogf(
				"credential refresher: refresh failed: %v; retrying in %v",
				err,
				credentialRefreshRetryDelay,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(credentialRefreshRetryDelay):
			}
			continue
		}

		store.store(newCreds)
		vlogf("credential refresher: credentials refreshed, new expiry %s",
			newCreds.Expiration.Format(time.RFC3339))
	}
}
