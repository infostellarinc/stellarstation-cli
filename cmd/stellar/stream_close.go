package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

const streamCloseTimeout = 5 * time.Second

// StreamCloseRequest is the request body for the stream close API
type StreamCloseRequest struct {
	PassID   string `json:"passId"`
	StreamID string `json:"streamId"`
	Source   string `json:"source"` // identifies which client the stream belongs to; the CLI sends "streamer"
}

// getStreamID returns the stream ID to use for the given pass, preferring the one returned by the authorizer.
// Returns an empty string if the authorizer is not used or did not provide a stream ID.
func getStreamID(cfg Config) string {
	if cfg.AuthorizerCreds != nil && cfg.AuthorizerCreds.StreamID != "" {
		return cfg.AuthorizerCreds.StreamID
	}
	// Stream ID should always come from authorizer
	// If not available, return empty string (caller should handle this appropriately)
	return ""
}

// callStreamCloseAPI calls the authorizer stream close API to signal that a stream has ended.
// baseURL is the common API base URL; the function appends "/stream/close" to it.
// The endpoint is authenticated with the same Cognito JWT as /authorize, minted
// from tokenSource (when nil, the request is sent without a token).
func callStreamCloseAPI(
	ctx context.Context,
	baseURL string,
	tokenSource auth.TokenSource,
	passID, streamID string,
) error {
	if baseURL == "" {
		vlogf("Skipping stream close API call: API URL not configured")
		return nil
	}
	vlogf("Calling stream close API for pass %s stream %s", passID, streamID)
	err := postStreamAction(ctx, baseURL, tokenSource, "/stream/close", StreamCloseRequest{
		PassID:   passID,
		StreamID: streamID,
		Source:   "streamer",
	})
	if err != nil {
		vlogf("Stream close API call failed: %v", err)
		return err
	}
	vlogf("Successfully called stream close API for pass %s stream %s", passID, streamID)
	return nil
}

// StreamRevokeRequest is the request body for the stream revoke API.
type StreamRevokeRequest struct {
	PassID   string `json:"passId"`
	StreamID string `json:"streamId"`
	Reason   string `json:"reason,omitempty"`
}

// callStreamRevokeAPI revokes another stream on a pass: the authorizer closes
// it and revokes its credentials.
func callStreamRevokeAPI(
	ctx context.Context,
	baseURL string,
	tokenSource auth.TokenSource,
	req StreamRevokeRequest,
) error {
	return postStreamAction(ctx, baseURL, tokenSource, "/stream/revoke", req)
}

// streamActionError is a refusal from a stream endpoint.
type streamActionError struct {
	Status  int
	Code    string
	Message string
}

func (e *streamActionError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("%s returned status %d", e.Code, e.Status)
	}
	return e.Message
}

// postStreamAction POSTs body to one of the authorizer's stream endpoints,
// authenticated like /authorize. A refusal is a *streamActionError.
func postStreamAction(
	ctx context.Context,
	baseURL string,
	tokenSource auth.TokenSource,
	path string,
	body any,
) error {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := strings.TrimRight(baseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if tokenSource != nil {
		token, tokErr := tokenSource.Token(ctx)
		if tokErr != nil {
			return fmt.Errorf("obtain access token for %s: %w", path, tokErr)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	client := &http.Client{Timeout: streamCloseTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	raw, _ := io.ReadAll(resp.Body)
	refusal := &streamActionError{Status: resp.StatusCode, Code: path}
	var decoded struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &decoded) == nil && decoded.Error != "" {
		refusal.Code, refusal.Message = decoded.Error, decoded.Message
	} else {
		refusal.Message = fmt.Sprintf("%s returned status %d: %s", path, resp.StatusCode, raw)
	}
	return refusal
}
