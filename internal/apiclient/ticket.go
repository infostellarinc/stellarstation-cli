package apiclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ---- Tickets ---------------------------------------------------------------

// TicketUser is a user named on a ticket: the reporter, an assignee, or a
// comment's author.
type TicketUser struct {
	UserID           string `json:"userId"`
	UserName         string `json:"userName,omitempty"`
	OrganizationID   string `json:"organizationId,omitempty"`
	OrganizationName string `json:"organizationName,omitempty"`
}

// TicketPass is a pass linked to a ticket.
type TicketPass struct {
	ID                  string     `json:"id"`
	TicketID            string     `json:"ticketId"`
	PassID              string     `json:"passId"`
	SatelliteID         string     `json:"satelliteId,omitempty"`
	SatelliteName       string     `json:"satelliteName,omitempty"`
	GroundStationID     string     `json:"groundStationId,omitempty"`
	GroundStationName   string     `json:"groundStationName,omitempty"`
	ContactStart        *time.Time `json:"contactStart,omitempty"`
	ContactStop         *time.Time `json:"contactStop,omitempty"`
	ExecutionStatus     string     `json:"executionStatus,omitempty"`
	ExecutionConfigID   string     `json:"executionConfigId,omitempty"`
	ExecutionConfigName string     `json:"executionConfigName,omitempty"`
}

// TicketAction is one follow-up action recorded on a ticket.
type TicketAction struct {
	ID         string     `json:"id"`
	TicketID   string     `json:"ticketId"`
	ActionOn   string     `json:"actionOn"`
	ActionText string     `json:"actionText"`
	CreatedAt  *time.Time `json:"createdAt,omitempty"`
	UpdatedAt  *time.Time `json:"updatedAt,omitempty"`
}

// TicketComment is a comment on a ticket.
type TicketComment struct {
	ID               string     `json:"id"`
	TicketID         string     `json:"ticketId"`
	UserID           string     `json:"userId"`
	UserName         string     `json:"userName,omitempty"`
	OrganizationID   string     `json:"organizationId,omitempty"`
	OrganizationName string     `json:"organizationName,omitempty"`
	Content          string     `json:"content"`
	CreatedAt        *time.Time `json:"createdAt,omitempty"`
	UpdatedAt        *time.Time `json:"updatedAt,omitempty"`
}

// TicketWatcher is a user watching a ticket for notifications.
type TicketWatcher struct {
	ID       string `json:"id"`
	TicketID string `json:"ticketId"`
	UserID   string `json:"userId"`
}

// TicketAttachment is a file (logs, monitoring data) attached to a ticket.
type TicketAttachment struct {
	ID          string      `json:"id"`
	TicketID    string      `json:"ticketId"`
	FileName    string      `json:"fileName"`
	ContentType string      `json:"contentType,omitempty"`
	SizeBytes   *int64      `json:"sizeBytes,omitempty"`
	UploadedBy  *TicketUser `json:"uploadedBy,omitempty"`
	CreatedAt   *time.Time  `json:"createdAt,omitempty"`
}

// Ticket is the JSON representation of an anomaly ticket.
type Ticket struct {
	ID                  string          `json:"id"`
	Status              string          `json:"status"`
	Severity            string          `json:"severity"`
	IssueType           string          `json:"issueType"`
	Title               string          `json:"title"`
	Reporter            TicketUser      `json:"reporter"`
	InfostellarAssignee *TicketUser     `json:"infostellarAssignee,omitempty"`
	OperatorAssignee    *TicketUser     `json:"operatorAssignee,omitempty"`
	Accountable         string          `json:"accountable,omitempty"`
	AnomalyDescription  string          `json:"anomalyDescription,omitempty"`
	RootCause           string          `json:"rootCause,omitempty"`
	CorrectiveAction    string          `json:"correctiveAction,omitempty"`
	PreventiveAction    string          `json:"preventiveAction,omitempty"`
	CloseReason         string          `json:"closeReason,omitempty"`
	Passes              []TicketPass    `json:"passes,omitempty"`
	Actions             []TicketAction  `json:"actions,omitempty"`
	Comments            []TicketComment `json:"comments,omitempty"`
	Watching            *bool           `json:"watching,omitempty"`
	CreatedAt           *time.Time      `json:"createdAt,omitempty"`
	UpdatedAt           *time.Time      `json:"updatedAt,omitempty"`
}

// TicketActionInput is one action in a ticket update. An empty ID adds a new
// action; a known ID edits that action. Actions missing from the update are
// removed.
type TicketActionInput struct {
	ID         string `json:"id,omitempty"`
	ActionOn   string `json:"actionOn"`
	ActionText string `json:"actionText"`
}

// TicketCreateRequest is the body for POST /v1/tickets.
type TicketCreateRequest struct {
	Severity            string   `json:"severity"`
	IssueType           string   `json:"issueType,omitempty"`
	Title               string   `json:"title"`
	InfostellarAssignee string   `json:"infostellarAssignee,omitempty"`
	OperatorAssignee    string   `json:"operatorAssignee,omitempty"`
	Accountable         string   `json:"accountable,omitempty"`
	PassIDs             []string `json:"passIds,omitempty"`
	AnomalyDescription  string   `json:"anomalyDescription,omitempty"`
	RootCause           string   `json:"rootCause,omitempty"`
	CorrectiveAction    string   `json:"correctiveAction,omitempty"`
	PreventiveAction    string   `json:"preventiveAction,omitempty"`
	CloseReason         string   `json:"closeReason,omitempty"`
}

// TicketUpdateRequest is the body for PUT /v1/tickets/{ticketId}. Fields are
// pointers so only the ones set are sent: an omitted field keeps its stored
// value. PreventiveAction set to the empty string clears it; PassIDs and
// Actions replace the stored lists as a whole.
type TicketUpdateRequest struct {
	Status              *string              `json:"status,omitempty"`
	Severity            *string              `json:"severity,omitempty"`
	IssueType           *string              `json:"issueType,omitempty"`
	Title               *string              `json:"title,omitempty"`
	InfostellarAssignee *string              `json:"infostellarAssignee,omitempty"`
	OperatorAssignee    *string              `json:"operatorAssignee,omitempty"`
	Accountable         *string              `json:"accountable,omitempty"`
	PassIDs             *[]string            `json:"passIds,omitempty"`
	Actions             *[]TicketActionInput `json:"actions,omitempty"`
	AnomalyDescription  *string              `json:"anomalyDescription,omitempty"`
	RootCause           *string              `json:"rootCause,omitempty"`
	CorrectiveAction    *string              `json:"correctiveAction,omitempty"`
	PreventiveAction    *string              `json:"preventiveAction,omitempty"`
	CloseReason         *string              `json:"closeReason,omitempty"`
}

// ListTicketsResponse is the response from GET /v1/tickets.
type ListTicketsResponse struct {
	Tickets []Ticket `json:"tickets"`
}

// ListTicketCommentsResponse is the response from GET /v1/tickets/{id}/comments.
type ListTicketCommentsResponse struct {
	Comments []TicketComment `json:"comments"`
}

// ListTicketAttachmentsResponse is the response from
// GET /v1/tickets/{id}/attachments, with the limits the API applies.
type ListTicketAttachmentsResponse struct {
	Attachments    []TicketAttachment `json:"attachments"`
	MaxAttachments int                `json:"maxAttachments"`
	MaxSizeBytes   int64              `json:"maxSizeBytes"`
}

// TicketAttachmentCreateRequest is the body for
// POST /v1/tickets/{id}/attachments.
type TicketAttachmentCreateRequest struct {
	FileName    string `json:"fileName"`
	ContentType string `json:"contentType,omitempty"`
	SizeBytes   *int64 `json:"sizeBytes,omitempty"`
}

// PresignedTransfer is a presigned S3 request the client performs itself,
// exactly as returned: the signature covers the method, URL and headers.
type PresignedTransfer struct {
	URL       string            `json:"url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expiresAt"`
}

// TicketAttachmentCreateResponse is the attachment record plus the upload
// request for its bytes.
type TicketAttachmentCreateResponse struct {
	Attachment TicketAttachment  `json:"attachment"`
	Upload     PresignedTransfer `json:"upload"`
}

func ticketPath(ticketID string) string {
	return "/v1/tickets/" + url.PathEscape(ticketID)
}

// ListTickets calls GET /v1/tickets. pageSize of 0 leaves the API default.
func (c *Client) ListTickets(ctx context.Context, pageSize int) ([]Ticket, error) {
	query := url.Values{}
	if pageSize > 0 {
		query.Set("pageSize", strconv.Itoa(pageSize))
	}
	var resp ListTicketsResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/tickets", query, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Tickets, nil
}

// GetTicket calls GET /v1/tickets/{ticketId}.
func (c *Client) GetTicket(ctx context.Context, ticketID string) (*Ticket, error) {
	var t Ticket
	if err := c.doJSON(ctx, http.MethodGet, ticketPath(ticketID), nil, nil, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// CreateTicket calls POST /v1/tickets.
func (c *Client) CreateTicket(ctx context.Context, req TicketCreateRequest) (*Ticket, error) {
	var t Ticket
	if err := c.doJSON(ctx, http.MethodPost, "/v1/tickets", nil, req, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// UpdateTicket calls PUT /v1/tickets/{ticketId}.
func (c *Client) UpdateTicket(ctx context.Context, ticketID string, req TicketUpdateRequest) (*Ticket, error) {
	var t Ticket
	if err := c.doJSON(ctx, http.MethodPut, ticketPath(ticketID), nil, req, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// DeleteTicket calls DELETE /v1/tickets/{ticketId}.
func (c *Client) DeleteTicket(ctx context.Context, ticketID string) error {
	return c.doJSON(ctx, http.MethodDelete, ticketPath(ticketID), nil, nil, nil)
}

// WatchTicket calls POST /v1/tickets/{ticketId}/watchers, subscribing the
// caller to the ticket's notifications.
func (c *Client) WatchTicket(ctx context.Context, ticketID string) (*TicketWatcher, error) {
	var w TicketWatcher
	if err := c.doJSON(ctx, http.MethodPost, ticketPath(ticketID)+"/watchers", nil, nil, &w); err != nil {
		return nil, err
	}
	return &w, nil
}

// UnwatchTicket calls DELETE /v1/tickets/{ticketId}/watchers/self, removing
// the caller's own watch.
func (c *Client) UnwatchTicket(ctx context.Context, ticketID string) error {
	return c.doJSON(ctx, http.MethodDelete, ticketPath(ticketID)+"/watchers/self", nil, nil, nil)
}

// ListTicketComments calls GET /v1/tickets/{ticketId}/comments.
func (c *Client) ListTicketComments(ctx context.Context, ticketID string) ([]TicketComment, error) {
	var resp ListTicketCommentsResponse
	if err := c.doJSON(ctx, http.MethodGet, ticketPath(ticketID)+"/comments", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Comments, nil
}

// CreateTicketComment calls POST /v1/tickets/{ticketId}/comments.
func (c *Client) CreateTicketComment(ctx context.Context, ticketID, content string) (*TicketComment, error) {
	var comment TicketComment
	body := map[string]string{"content": content}
	if err := c.doJSON(ctx, http.MethodPost, ticketPath(ticketID)+"/comments", nil, body, &comment); err != nil {
		return nil, err
	}
	return &comment, nil
}

// UpdateTicketComment calls PUT /v1/tickets/{ticketId}/comments/{commentId}.
func (c *Client) UpdateTicketComment(ctx context.Context, ticketID, commentID, content string) (*TicketComment, error) {
	var comment TicketComment
	body := map[string]string{"content": content}
	path := ticketPath(ticketID) + "/comments/" + url.PathEscape(commentID)
	if err := c.doJSON(ctx, http.MethodPut, path, nil, body, &comment); err != nil {
		return nil, err
	}
	return &comment, nil
}

// DeleteTicketComment calls DELETE /v1/tickets/{ticketId}/comments/{commentId}.
func (c *Client) DeleteTicketComment(ctx context.Context, ticketID, commentID string) error {
	path := ticketPath(ticketID) + "/comments/" + url.PathEscape(commentID)
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, nil)
}

// ListTicketAttachments calls GET /v1/tickets/{ticketId}/attachments.
func (c *Client) ListTicketAttachments(ctx context.Context, ticketID string) (*ListTicketAttachmentsResponse, error) {
	var resp ListTicketAttachmentsResponse
	if err := c.doJSON(ctx, http.MethodGet, ticketPath(ticketID)+"/attachments", nil, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CreateTicketAttachment calls POST /v1/tickets/{ticketId}/attachments,
// registering the file and returning the presigned upload for its bytes.
func (c *Client) CreateTicketAttachment(
	ctx context.Context, ticketID string, req TicketAttachmentCreateRequest,
) (*TicketAttachmentCreateResponse, error) {
	var resp TicketAttachmentCreateResponse
	if err := c.doJSON(ctx, http.MethodPost, ticketPath(ticketID)+"/attachments", nil, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// UploadTicketAttachment registers the file and sends its bytes to S3 with
// the presigned request the API returns. A failed transfer deletes the
// registration again so the ticket is not left with a file that has no data.
// S3 refuses a chunked upload, so the bytes are sent with a Content-Length:
// req.SizeBytes when the caller knows it, otherwise the data is read first.
func (c *Client) UploadTicketAttachment(
	ctx context.Context, ticketID string, req TicketAttachmentCreateRequest, data io.Reader,
) (*TicketAttachment, error) {
	size := int64(-1)
	if req.SizeBytes != nil {
		size = *req.SizeBytes
	} else {
		buffered, err := io.ReadAll(data)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", req.FileName, err)
		}
		data = bytes.NewReader(buffered)
		size = int64(len(buffered))
		req.SizeBytes = &size
	}
	created, err := c.CreateTicketAttachment(ctx, ticketID, req)
	if err != nil {
		return nil, err
	}
	if err := c.transfer(ctx, created.Upload, data, size, io.Discard); err != nil {
		_ = c.DeleteTicketAttachment(ctx, ticketID, created.Attachment.ID)
		return nil, fmt.Errorf("upload %s: %w", req.FileName, err)
	}
	return &created.Attachment, nil
}

// DeleteTicketAttachment calls DELETE /v1/tickets/{ticketId}/attachments/{attachmentId}.
func (c *Client) DeleteTicketAttachment(ctx context.Context, ticketID, attachmentID string) error {
	path := ticketPath(ticketID) + "/attachments/" + url.PathEscape(attachmentID)
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, nil)
}

// GetTicketAttachmentDownload calls GET .../attachments/{attachmentId}/download-url.
func (c *Client) GetTicketAttachmentDownload(ctx context.Context, ticketID, attachmentID string) (*PresignedTransfer, error) {
	var resp PresignedTransfer
	path := ticketPath(ticketID) + "/attachments/" + url.PathEscape(attachmentID) + "/download-url"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DownloadTicketAttachment fetches the attachment's bytes into w through a
// presigned request and returns the number of bytes written.
func (c *Client) DownloadTicketAttachment(ctx context.Context, ticketID, attachmentID string, w io.Writer) (int64, error) {
	transfer, err := c.GetTicketAttachmentDownload(ctx, ticketID, attachmentID)
	if err != nil {
		return 0, err
	}
	counter := &countingWriter{w: w}
	if err := c.transfer(ctx, *transfer, nil, 0, counter); err != nil {
		return counter.n, fmt.Errorf("download attachment %s: %w", attachmentID, err)
	}
	return counter.n, nil
}

// transfer performs a presigned S3 request as given, without the API bearer
// token, which would invalidate the signature. body is sent on uploads with
// bodyLength as its Content-Length, since S3 rejects chunked uploads; the
// response body is copied to out.
func (c *Client) transfer(
	ctx context.Context, t PresignedTransfer, body io.Reader, bodyLength int64, out io.Writer,
) error {
	req, err := http.NewRequestWithContext(ctx, t.Method, t.URL, body)
	if err != nil {
		return fmt.Errorf("create transfer request: %w", err)
	}
	if body != nil && bodyLength >= 0 {
		req.ContentLength = bodyLength
	}
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("transfer request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("storage answered %d: %s", resp.StatusCode, string(detail))
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("read transfer response: %w", err)
	}
	return nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
