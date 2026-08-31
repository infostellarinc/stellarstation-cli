package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TokenSource produces bearer tokens used on outbound requests. A TokenSource
// MUST be safe for concurrent use: the open-stream command refreshes
// authorizer credentials on a background goroutine while CRUD commands read
// tokens from the main goroutine.
type TokenSource interface {
	// Token returns a bearer token (no "Bearer " prefix). Callers SHOULD cache
	// the result for the duration of a single request but MUST call Token
	// again for subsequent requests so the implementation can rotate near
	// expiry.
	Token(ctx context.Context) (string, error)
}

// StaticTokenSource always returns the same token. Intended for tests.
type StaticTokenSource struct {
	Value string
}

// Token implements TokenSource.
func (s StaticTokenSource) Token(_ context.Context) (string, error) {
	return s.Value, nil
}

// defaultTokenTimeout bounds each OAuth2 token request. It is generous to
// accommodate Cognito cold starts while still failing fast on real errors.
const defaultTokenTimeout = 15 * time.Second

// refreshMargin controls how early we refresh before expiry. It is subtracted
// from the token lifetime so one request slightly past the boundary does not
// fail with 401.
const refreshMargin = 60 * time.Second

// OAuth2TokenSource exchanges machine-client credentials for a Cognito access
// token using the OAuth2 client_credentials grant. Tokens are cached until
// slightly before their advertised expiry.
type OAuth2TokenSource struct {
	creds      *Credentials
	httpClient *http.Client

	mu         sync.Mutex
	cached     string
	expiration time.Time
}

// NewOAuth2TokenSource builds an OAuth2TokenSource backed by the supplied
// credentials and HTTP client. A nil httpClient falls back to a private
// http.Client with a short default timeout.
//
// The token endpoint receives the client secret over Basic auth, so it must
// use HTTPS (or name a loopback address); see CheckEndpointScheme.
func NewOAuth2TokenSource(creds *Credentials, httpClient *http.Client) (*OAuth2TokenSource, error) {
	if err := creds.Validate(); err != nil {
		return nil, err
	}
	if err := CheckEndpointScheme("token endpoint", creds.TokenEndpoint); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTokenTimeout}
	}
	return &OAuth2TokenSource{
		creds:      creds,
		httpClient: httpClient,
	}, nil
}

// Token returns a cached token if it is still valid, otherwise fetches a new
// one from the configured token endpoint.
func (s *OAuth2TokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cached != "" && time.Now().Before(s.expiration) {
		return s.cached, nil
	}

	token, expiresIn, err := s.fetchToken(ctx)
	if err != nil {
		return "", err
	}
	s.cached = token
	ttl := time.Duration(expiresIn) * time.Second
	if ttl <= refreshMargin {
		// Tiny TTL: keep the token but treat it as already expired so the
		// next call re-fetches.
		s.expiration = time.Now()
	} else {
		s.expiration = time.Now().Add(ttl - refreshMargin)
	}
	return token, nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

// fetchToken performs a single client_credentials exchange.
func (s *OAuth2TokenSource) fetchToken(ctx context.Context) (string, int, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("scope", s.creds.EffectiveScope())

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		s.creds.TokenEndpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", 0, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Basic "+basicAuth(s.creds.ClientID, s.creds.ClientSecret))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("perform token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("read token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf(
			"token endpoint returned %d: %s",
			resp.StatusCode,
			strings.TrimSpace(string(body)),
		)
	}

	var parsed tokenResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", 0, fmt.Errorf("decode token response: %w", err)
	}
	if parsed.AccessToken == "" {
		return "", 0, errors.New("token endpoint did not return access_token")
	}
	if parsed.ExpiresIn <= 0 {
		// Be defensive: Cognito always returns this but some OIDC providers
		// omit it.
		parsed.ExpiresIn = 3600
	}
	return parsed.AccessToken, parsed.ExpiresIn, nil
}

func basicAuth(user, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + password))
}
