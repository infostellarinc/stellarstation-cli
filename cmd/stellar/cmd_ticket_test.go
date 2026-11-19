package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// ticketCommandServer stubs the ticket API and a fake S3 endpoint for the
// command tests, recording the bodies the CLI sends.
type ticketCommandServer struct {
	srv          *httptest.Server
	t            *testing.T
	createBody   apiclient.TicketCreateRequest
	updateBody   string
	commentBody  map[string]string
	attachmentIn apiclient.TicketAttachmentCreateRequest
	uploaded     []byte
	deleted      []string
}

func newTicketCommandServer(t *testing.T) *ticketCommandServer {
	t.Helper()
	ts := &ticketCommandServer{t: t}
	now := time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC)
	ticket := apiclient.Ticket{
		ID: "t-1", Status: "OPEN", Severity: "HIGH", IssueType: "ANOMALY", Title: "Downlink dropped",
		Reporter:  apiclient.TicketUser{UserID: "u-1", UserName: "Alice"},
		Passes:    []apiclient.TicketPass{{ID: "tp-1", TicketID: "t-1", PassID: "p-1"}},
		Comments:  []apiclient.TicketComment{{ID: "c-1", Content: "hi", UserName: "Alice"}},
		UpdatedAt: &now,
	}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasPrefix(path, "/s3/"):
			if r.Method == http.MethodPut {
				ts.uploaded, _ = io.ReadAll(r.Body)
				return
			}
			_, _ = w.Write([]byte("downloaded bytes"))
		case path == "/v1/tickets" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(apiclient.ListTicketsResponse{Tickets: []apiclient.Ticket{ticket}})
		case path == "/v1/tickets" && r.Method == http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&ts.createBody)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(ticket)
		case path == "/v1/tickets/t-1" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(ticket)
		case path == "/v1/tickets/t-1" && r.Method == http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			ts.updateBody = string(body)
			_ = json.NewEncoder(w).Encode(ticket)
		case path == "/v1/tickets/t-1" && r.Method == http.MethodDelete,
			path == "/v1/tickets/t-1/watchers/self" && r.Method == http.MethodDelete,
			path == "/v1/tickets/t-1/comments/c-1" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case path == "/v1/tickets/t-1/watchers" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(apiclient.TicketWatcher{ID: "w-1", TicketID: "t-1", UserID: "u-1"})
		case path == "/v1/tickets/t-1/comments" && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(apiclient.ListTicketCommentsResponse{Comments: ticket.Comments})
		case path == "/v1/tickets/t-1/comments" && r.Method == http.MethodPost,
			path == "/v1/tickets/t-1/comments/c-1" && r.Method == http.MethodPut:
			_ = json.NewDecoder(r.Body).Decode(&ts.commentBody)
			_ = json.NewEncoder(w).Encode(apiclient.TicketComment{ID: "c-1", Content: ts.commentBody["content"], UserName: "Alice"})
		case path == "/v1/tickets/t-1/attachments" && r.Method == http.MethodGet:
			size := int64(16)
			_ = json.NewEncoder(w).Encode(apiclient.ListTicketAttachmentsResponse{
				Attachments:    []apiclient.TicketAttachment{{ID: "a-1", FileName: "station.log", SizeBytes: &size}},
				MaxAttachments: 20, MaxSizeBytes: 104857600,
			})
		case path == "/v1/tickets/t-1/attachments" && r.Method == http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&ts.attachmentIn)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(apiclient.TicketAttachmentCreateResponse{
				Attachment: apiclient.TicketAttachment{ID: "a-2", FileName: ts.attachmentIn.FileName, ContentType: ts.attachmentIn.ContentType},
				Upload:     apiclient.PresignedTransfer{URL: ts.srv.URL + "/s3/a-2", Method: http.MethodPut, Headers: map[string]string{}},
			})
		case path == "/v1/tickets/t-1/attachments/a-1/download-url":
			_ = json.NewEncoder(w).Encode(apiclient.PresignedTransfer{URL: ts.srv.URL + "/s3/a-1", Method: http.MethodGet, Headers: map[string]string{}})
		case strings.HasPrefix(path, "/v1/tickets/t-1/attachments/") && r.Method == http.MethodDelete:
			ts.deleted = append(ts.deleted, strings.TrimPrefix(path, "/v1/tickets/t-1/attachments/"))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

// runTicket executes "stellar ticket <args>" against the stub server and
// returns stdout and the error.
func runTicket(t *testing.T, srv *httptest.Server, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs(append([]string{"ticket"}, append(args, "--api-url", srv.URL)...))
	err := root.Execute()
	return out.String(), err
}

func TestTicketListAndGetCommands(t *testing.T) {
	ts := newTicketCommandServer(t)
	defer setupAuthForTest(t).Close()

	for _, format := range []string{"wide", "csv", "json"} {
		out, err := runTicket(t, ts.srv, "list", "--output", format)
		if err != nil {
			t.Fatalf("list --output %s: %v", format, err)
		}
		if !strings.Contains(out, "t-1") || !strings.Contains(out, "Downlink dropped") {
			t.Errorf("list --output %s = %q, want the ticket", format, out)
		}
	}
	out, err := runTicket(t, ts.srv, "list", "--output", "wide")
	if err != nil || !strings.Contains(out, "p-1") || !strings.Contains(out, "Alice") {
		t.Errorf("wide list should show pass IDs and the reporter: %q (%v)", out, err)
	}

	out, err = runTicket(t, ts.srv, "get", "t-1", "--output", "json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var got []apiclient.Ticket
	if err := json.Unmarshal([]byte(out), &got); err != nil || len(got) != 1 || len(got[0].Comments) != 1 {
		t.Errorf("get --output json = %q (%v)", out, err)
	}

	if _, err := runTicket(t, ts.srv, "get"); err == nil || !strings.Contains(err.Error(), "ticket ID") {
		t.Errorf("get without an ID should explain what is missing, got %v", err)
	}
	if _, err := runTicket(t, ts.srv, "get", "missing"); err == nil {
		t.Error("get of an unknown ticket should fail")
	}
}

func TestTicketCreateCommand(t *testing.T) {
	ts := newTicketCommandServer(t)
	defer setupAuthForTest(t).Close()

	out, err := runTicket(t, ts.srv, "create",
		"--title", "  Downlink dropped ", "--severity", "high", "--issue-type", "anomaly",
		"--accountable", "satellite_operator", "--pass-id", "p-1,p-2", "--anomaly-description", "Lost lock",
		"--output", "json")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !strings.Contains(out, `"id": "t-1"`) {
		t.Errorf("create output = %q", out)
	}
	want := apiclient.TicketCreateRequest{
		Severity: "HIGH", IssueType: "ANOMALY", Title: "Downlink dropped", Accountable: "SATELLITE_OPERATOR",
		PassIDs: []string{"p-1", "p-2"}, AnomalyDescription: "Lost lock",
	}
	if ts.createBody.Severity != want.Severity || ts.createBody.IssueType != want.IssueType ||
		ts.createBody.Title != want.Title || ts.createBody.Accountable != want.Accountable ||
		strings.Join(ts.createBody.PassIDs, ",") != "p-1,p-2" || ts.createBody.AnomalyDescription != want.AnomalyDescription {
		t.Errorf("create sent %+v, want %+v", ts.createBody, want)
	}

	for name, args := range map[string][]string{
		"missing title":    {"--severity", "HIGH"},
		"missing severity": {"--title", "x"},
		"bad severity":     {"--title", "x", "--severity", "URGENT"},
		"bad issue type":   {"--title", "x", "--severity", "LOW", "--issue-type", "WEATHER"},
	} {
		if _, err := runTicket(t, ts.srv, append([]string{"create"}, args...)...); err == nil {
			t.Errorf("create with %s should fail", name)
		}
	}
}

func TestTicketUpdateCommandSendsOnlyNamedFields(t *testing.T) {
	ts := newTicketCommandServer(t)
	defer setupAuthForTest(t).Close()

	if _, err := runTicket(t, ts.srv, "update", "t-1", "--status", "closed", "--preventive-action", "",
		"--action", "INFOSTELLAR=Check the ACU", "--pass-id", "p-9"); err != nil {
		t.Fatalf("update: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal([]byte(ts.updateBody), &sent); err != nil {
		t.Fatalf("update body %q: %v", ts.updateBody, err)
	}
	if sent["status"] != "CLOSED" || sent["preventiveAction"] != "" {
		t.Errorf("update body = %s", ts.updateBody)
	}
	if _, has := sent["title"]; has {
		t.Errorf("an unnamed field must not be sent: %s", ts.updateBody)
	}
	if actions, ok := sent["actions"].([]any); !ok || len(actions) != 1 {
		t.Errorf("actions = %v", sent["actions"])
	}
	if passes, ok := sent["passIds"].([]any); !ok || len(passes) != 1 || passes[0] != "p-9" {
		t.Errorf("passIds = %v", sent["passIds"])
	}

	if _, err := runTicket(t, ts.srv, "update", "t-1"); err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Errorf("update without options should say so, got %v", err)
	}
	if _, err := runTicket(t, ts.srv, "update", "t-1", "--action", "nobody"); err == nil {
		t.Error("a malformed --action should fail")
	}
	if _, err := runTicket(t, ts.srv, "update", "t-1", "--action", "MARTIANS=fix"); err == nil {
		t.Error("an unknown action party should fail")
	}
	if _, err := runTicket(t, ts.srv, "update", "t-1", "--status", "PENDING"); err == nil {
		t.Error("an unknown status should fail")
	}
}

func TestTicketWatchDeleteAndCommentCommands(t *testing.T) {
	ts := newTicketCommandServer(t)
	defer setupAuthForTest(t).Close()

	for _, args := range [][]string{
		{"watch", "t-1"}, {"unwatch", "t-1"}, {"delete", "t-1"}, {"delete-comment", "t-1", "c-1"},
	} {
		if _, err := runTicket(t, ts.srv, args...); err != nil {
			t.Errorf("%v: %v", args, err)
		}
	}

	out, err := runTicket(t, ts.srv, "list-comments", "t-1", "--output", "csv")
	if err != nil || !strings.Contains(out, "c-1") {
		t.Errorf("list-comments = %q (%v)", out, err)
	}
	if _, err := runTicket(t, ts.srv, "add-comment", "t-1", "Looking into it"); err != nil {
		t.Fatalf("add-comment: %v", err)
	}
	if ts.commentBody["content"] != "Looking into it" {
		t.Errorf("add-comment sent %v", ts.commentBody)
	}
	if _, err := runTicket(t, ts.srv, "add-comment", "t-1", "   "); err == nil {
		t.Error("an empty comment should fail")
	}
	if _, err := runTicket(t, ts.srv, "update-comment", "t-1", "c-1", "--text", "Fixed"); err != nil {
		t.Fatalf("update-comment: %v", err)
	}
	if ts.commentBody["content"] != "Fixed" {
		t.Errorf("update-comment sent %v", ts.commentBody)
	}
	if _, err := runTicket(t, ts.srv, "update-comment", "t-1", "c-1"); err == nil {
		t.Error("update-comment without --text should fail")
	}
	if _, err := runTicket(t, ts.srv, "delete-comment", "t-1"); err == nil {
		t.Error("delete-comment needs both IDs")
	}
}

func TestTicketAttachmentCommands(t *testing.T) {
	ts := newTicketCommandServer(t)
	defer setupAuthForTest(t).Close()

	out, err := runTicket(t, ts.srv, "list-attachments", "t-1", "--output", "wide")
	if err != nil || !strings.Contains(out, "station.log") || !strings.Contains(out, "16") {
		t.Errorf("list-attachments = %q (%v)", out, err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "station.log")
	if err := os.WriteFile(path, []byte("line one\nline two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runTicket(t, ts.srv, "upload-attachment", "t-1", path, "--output", "json"); err != nil {
		t.Fatalf("upload-attachment: %v", err)
	}
	if ts.attachmentIn.FileName != "station.log" || ts.attachmentIn.SizeBytes == nil || *ts.attachmentIn.SizeBytes != 18 {
		t.Errorf("upload registered %+v", ts.attachmentIn)
	}
	if ts.attachmentIn.ContentType == "" || ts.attachmentIn.ContentType == "application/octet-stream" {
		t.Errorf("content type should be guessed from the extension, got %q", ts.attachmentIn.ContentType)
	}
	if string(ts.uploaded) != "line one\nline two\n" {
		t.Errorf("uploaded %q", ts.uploaded)
	}
	if _, err := runTicket(t, ts.srv, "upload-attachment", "t-1", dir); err == nil {
		t.Error("uploading a directory should fail")
	}
	if _, err := runTicket(t, ts.srv, "upload-attachment", "t-1", filepath.Join(dir, "missing.log")); err == nil {
		t.Error("uploading a missing file should fail")
	}

	target := filepath.Join(dir, "copy.log")
	if _, err := runTicket(t, ts.srv, "download-attachment", "t-1", "a-1", "--output-file", target); err != nil {
		t.Fatalf("download-attachment: %v", err)
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "downloaded bytes" {
		t.Errorf("downloaded file = %q (%v)", data, err)
	}
	if _, err := runTicket(t, ts.srv, "download-attachment", "t-1", "a-1", "--output-file", target); err == nil {
		t.Error("download must not overwrite an existing file")
	}
	out, err = runTicket(t, ts.srv, "download-attachment", "t-1", "a-1", "--output-file", "-")
	if err != nil || out != "downloaded bytes" {
		t.Errorf("download to stdout = %q (%v)", out, err)
	}
	// Without --output-file the attachment's own name is used, in the
	// current directory.
	defaultDir := t.TempDir()
	t.Chdir(defaultDir)
	if _, err := runTicket(t, ts.srv, "download-attachment", "t-1", "a-1"); err != nil {
		t.Fatalf("download-attachment default name: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(defaultDir, "station.log")); err != nil || string(data) != "downloaded bytes" {
		t.Errorf("default-named download = %q (%v)", data, err)
	}
	if _, err := runTicket(t, ts.srv, "download-attachment", "t-1", "a-9"); err == nil ||
		!strings.Contains(err.Error(), "not on ticket") {
		t.Errorf("downloading an unknown attachment should explain, got %v", err)
	}

	if _, err := runTicket(t, ts.srv, "delete-attachment", "t-1", "a-1"); err != nil {
		t.Fatalf("delete-attachment: %v", err)
	}
	if len(ts.deleted) != 1 || ts.deleted[0] != "a-1" {
		t.Errorf("deleted %v", ts.deleted)
	}
}
