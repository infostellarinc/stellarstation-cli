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

	closeURL := strings.TrimRight(baseURL, "/") + "/stream/close"

	reqBody := StreamCloseRequest{
		PassID:   passID,
		StreamID: streamID,
		Source:   "streamer",
	}

	bodyJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, closeURL, bytes.NewReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if tokenSource != nil {
		token, tokErr := tokenSource.Token(ctx)
		if tokErr != nil {
			return fmt.Errorf("obtain access token for stream close: %w", tokErr)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	client := &http.Client{Timeout: streamCloseTimeout}
	resp, err := client.Do(req)
	if err != nil {
		vlogf("Error calling stream close API: %v", err)
		return fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		vlogf("Stream close API returned status %d: %s", resp.StatusCode, string(body))
		return fmt.Errorf("stream close API returned status %d: %s", resp.StatusCode, string(body))
	}

	vlogf("Successfully called stream close API for pass %s stream %s", passID, streamID)
	return nil
}
