package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ticketServer is a stub of the ticket API plus a fake S3 endpoint for the
// presigned transfers. It records what it receives so tests can assert on the
// exact wire calls.
type ticketServer struct {
	t        *testing.T
	srv      *httptest.Server
	requests []string // "METHOD path" in order
	uploaded []byte
	deleted  []string
	failS3   bool
}

func newTicketServer(t *testing.T) *ticketServer {
	t.Helper()
	ts := &ticketServer{t: t}
	ts.srv = httptest.NewServer(http.HandlerFunc(ts.handle))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *ticketServer) handle(w http.ResponseWriter, r *http.Request) {
	ts.requests = append(ts.requests, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if strings.HasPrefix(r.URL.Path, "/s3/") {
		ts.handleStorage(w, r)
		return
	}
	assertBearer(ts.t, r)
	switch {
	case r.URL.Path == "/v1/tickets" && r.Method == http.MethodGet:
		if r.URL.Query().Get("pageSize") != "5" {
			ts.t.Errorf("pageSize = %q, want 5", r.URL.Query().Get("pageSize"))
		}
		_ = json.NewEncoder(w).Encode(ListTicketsResponse{Tickets: []Ticket{{ID: "t-1", Title: "Dropped"}}})
	case r.URL.Path == "/v1/tickets" && r.Method == http.MethodPost:
		var req TicketCreateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(Ticket{ID: "t-new", Title: req.Title, Severity: req.Severity, Status: "OPEN"})
	case r.URL.Path == "/v1/tickets/t-1" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(Ticket{ID: "t-1", Title: "Dropped", Status: "OPEN"})
	case r.URL.Path == "/v1/tickets/t-1" && r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		// Only the fields set are sent; an explicit empty preventive action
		// travels as "" to clear it.
		if string(body) != `{"status":"CLOSED","preventiveAction":""}` {
			ts.t.Errorf("update body = %s", body)
		}
		_ = json.NewEncoder(w).Encode(Ticket{ID: "t-1", Status: "CLOSED"})
	case r.URL.Path == "/v1/tickets/t-1" && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/tickets/t-1/watchers" && r.Method == http.MethodPost:
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(TicketWatcher{ID: "w-1", TicketID: "t-1", UserID: "u-1"})
	case r.URL.Path == "/v1/tickets/t-1/watchers/self" && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/tickets/t-1/comments" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(ListTicketCommentsResponse{Comments: []TicketComment{{ID: "c-1", Content: "hi"}}})
	case r.URL.Path == "/v1/tickets/t-1/comments" && r.Method == http.MethodPost:
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(TicketComment{ID: "c-2", Content: body["content"]})
	case r.URL.Path == "/v1/tickets/t-1/comments/c-2" && r.Method == http.MethodPut:
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.NewEncoder(w).Encode(TicketComment{ID: "c-2", Content: body["content"]})
	case r.URL.Path == "/v1/tickets/t-1/comments/c-2" && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/tickets/t-1/attachments" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(ListTicketAttachmentsResponse{
			Attachments: []TicketAttachment{{ID: "a-1", FileName: "station.log"}}, MaxAttachments: 20, MaxSizeBytes: 100,
		})
	case r.URL.Path == "/v1/tickets/t-1/attachments" && r.Method == http.MethodPost:
		var req TicketAttachmentCreateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(TicketAttachmentCreateResponse{
			Attachment: TicketAttachment{ID: "a-new", FileName: req.FileName, ContentType: req.ContentType},
			Upload: PresignedTransfer{
				URL: ts.srv.URL + "/s3/a-new", Method: http.MethodPut,
				Headers: map[string]string{"Content-Type": req.ContentType},
			},
		})
	case r.URL.Path == "/v1/tickets/t-1/attachments/a-new" && r.Method == http.MethodDelete,
		r.URL.Path == "/v1/tickets/t-1/attachments/a-1" && r.Method == http.MethodDelete:
		ts.deleted = append(ts.deleted, strings.TrimPrefix(r.URL.Path, "/v1/tickets/t-1/attachments/"))
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/v1/tickets/t-1/attachments/a-1/download-url" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(PresignedTransfer{
			URL: ts.srv.URL + "/s3/a-1", Method: http.MethodGet, Headers: map[string]string{},
		})
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"not found"}`))
	}
}

// handleStorage plays S3 for the presigned transfers.
func (ts *ticketServer) handleStorage(w http.ResponseWriter, r *http.Request) {
	// The presigned transfer must not carry the API token.
	if r.Header.Get("Authorization") != "" {
		ts.t.Errorf("presigned transfer carried Authorization %q", r.Header.Get("Authorization"))
	}
	if ts.failS3 {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<Error>expired</Error>"))
		return
	}
	if r.Method == http.MethodPut {
		if r.Header.Get("Content-Type") != "text/plain" {
			ts.t.Errorf("upload Content-Type = %q, want text/plain", r.Header.Get("Content-Type"))
		}
		// S3 answers 501 to a chunked upload, so the length must be declared.
		if r.ContentLength < 0 || len(r.TransferEncoding) != 0 {
			ts.t.Errorf("upload must carry Content-Length, got length %d encoding %v", r.ContentLength, r.TransferEncoding)
		}
		ts.uploaded, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write([]byte("log line one\nlog line two\n"))
}

func TestTicketClientCalls(t *testing.T) {
	ts := newTicketServer(t)
	c := New(ts.srv.URL, staticToken())
	ctx := context.Background()

	tickets, err := c.ListTickets(ctx, 5)
	if err != nil || len(tickets) != 1 || tickets[0].ID != "t-1" {
		t.Fatalf("ListTickets = %+v, %v", tickets, err)
	}
	got, err := c.GetTicket(ctx, "t-1")
	if err != nil || got.Title != "Dropped" {
		t.Fatalf("GetTicket = %+v, %v", got, err)
	}
	created, err := c.CreateTicket(ctx, TicketCreateRequest{Severity: "HIGH", Title: "New"})
	if err != nil || created.ID != "t-new" || created.Severity != "HIGH" {
		t.Fatalf("CreateTicket = %+v, %v", created, err)
	}
	closed := "CLOSED"
	cleared := ""
	updated, err := c.UpdateTicket(ctx, "t-1", TicketUpdateRequest{Status: &closed, PreventiveAction: &cleared})
	if err != nil || updated.Status != "CLOSED" {
		t.Fatalf("UpdateTicket = %+v, %v", updated, err)
	}
	if err := c.DeleteTicket(ctx, "t-1"); err != nil {
		t.Fatalf("DeleteTicket: %v", err)
	}
	watcher, err := c.WatchTicket(ctx, "t-1")
	if err != nil || watcher.ID != "w-1" {
		t.Fatalf("WatchTicket = %+v, %v", watcher, err)
	}
	if err := c.UnwatchTicket(ctx, "t-1"); err != nil {
		t.Fatalf("UnwatchTicket: %v", err)
	}
	comments, err := c.ListTicketComments(ctx, "t-1")
	if err != nil || len(comments) != 1 {
		t.Fatalf("ListTicketComments = %+v, %v", comments, err)
	}
	comment, err := c.CreateTicketComment(ctx, "t-1", "hello")
	if err != nil || comment.Content != "hello" {
		t.Fatalf("CreateTicketComment = %+v, %v", comment, err)
	}
	comment, err = c.UpdateTicketComment(ctx, "t-1", "c-2", "edited")
	if err != nil || comment.Content != "edited" {
		t.Fatalf("UpdateTicketComment = %+v, %v", comment, err)
	}
	if err := c.DeleteTicketComment(ctx, "t-1", "c-2"); err != nil {
		t.Fatalf("DeleteTicketComment: %v", err)
	}
	attachments, err := c.ListTicketAttachments(ctx, "t-1")
	if err != nil || len(attachments.Attachments) != 1 || attachments.MaxAttachments != 20 {
		t.Fatalf("ListTicketAttachments = %+v, %v", attachments, err)
	}
	if err := c.DeleteTicketAttachment(ctx, "t-1", "a-1"); err != nil {
		t.Fatalf("DeleteTicketAttachment: %v", err)
	}

	if _, err := c.GetTicket(ctx, "missing"); err == nil {
		t.Fatal("GetTicket(missing) should fail")
	} else {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
			t.Fatalf("GetTicket(missing) error = %v, want 404 APIError", err)
		}
	}
}

func TestUploadTicketAttachmentSendsBytesToPresignedURL(t *testing.T) {
	ts := newTicketServer(t)
	c := New(ts.srv.URL, staticToken())

	// A file reader has no length of its own; the declared size is used.
	size := int64(5)
	att, err := c.UploadTicketAttachment(context.Background(), "t-1", TicketAttachmentCreateRequest{
		FileName: "notes.txt", ContentType: "text/plain", SizeBytes: &size,
	}, io.MultiReader(strings.NewReader("hel"), strings.NewReader("lo")))
	if err != nil {
		t.Fatalf("UploadTicketAttachment: %v", err)
	}
	if att.ID != "a-new" || att.FileName != "notes.txt" {
		t.Errorf("attachment = %+v", att)
	}
	if string(ts.uploaded) != "hello" {
		t.Errorf("uploaded bytes = %q, want hello", ts.uploaded)
	}
	if len(ts.deleted) != 0 {
		t.Errorf("a successful upload must not delete the registration, deleted %v", ts.deleted)
	}

	// Without a declared size the data is read first so the length is known.
	ts.uploaded = nil
	if _, err := c.UploadTicketAttachment(context.Background(), "t-1", TicketAttachmentCreateRequest{
		FileName: "notes.txt", ContentType: "text/plain",
	}, io.MultiReader(strings.NewReader("ab"), strings.NewReader("c"))); err != nil {
		t.Fatalf("UploadTicketAttachment without size: %v", err)
	}
	if string(ts.uploaded) != "abc" {
		t.Errorf("uploaded bytes = %q, want abc", ts.uploaded)
	}
}

func TestUploadTicketAttachmentRollsBackWhenStorageRefuses(t *testing.T) {
	ts := newTicketServer(t)
	ts.failS3 = true
	c := New(ts.srv.URL, staticToken())

	_, err := c.UploadTicketAttachment(context.Background(), "t-1", TicketAttachmentCreateRequest{
		FileName: "notes.txt", ContentType: "text/plain",
	}, strings.NewReader("hello"))
	if err == nil || !strings.Contains(err.Error(), "storage answered 403") {
		t.Fatalf("expected a storage error, got %v", err)
	}
	if len(ts.deleted) != 1 || ts.deleted[0] != "a-new" {
		t.Errorf("the registration should be deleted after a failed transfer, deleted %v", ts.deleted)
	}
}

func TestDownloadTicketAttachmentWritesTheBytes(t *testing.T) {
	ts := newTicketServer(t)
	c := New(ts.srv.URL, staticToken())

	var out bytes.Buffer
	n, err := c.DownloadTicketAttachment(context.Background(), "t-1", "a-1", &out)
	if err != nil {
		t.Fatalf("DownloadTicketAttachment: %v", err)
	}
	if n != int64(out.Len()) || out.String() != "log line one\nlog line two\n" {
		t.Errorf("downloaded %d bytes: %q", n, out.String())
	}

	ts.failS3 = true
	if _, err := c.DownloadTicketAttachment(context.Background(), "t-1", "a-1", io.Discard); err == nil {
		t.Fatal("expected the refused transfer to fail")
	}
}
