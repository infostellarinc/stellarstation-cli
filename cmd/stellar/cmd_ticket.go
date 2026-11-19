package main

import (
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

// Allowed values for the ticket enumerations, validated client-side so a typo
// is reported before any request is made.
//
//nolint:gochecknoglobals
var (
	ticketSeverities   = []string{"LOW", "MEDIUM", "HIGH"}
	ticketStatuses     = []string{"OPEN", "CLOSED"}
	ticketIssueTypes   = []string{"ANOMALY", "PERFORMANCE", "BILLING", "REQUEST", "OTHER"}
	ticketAccountables = []string{"INFOSTELLAR", "SATELLITE_OPERATOR", "GROUND_STATION_OPERATOR"}
	ticketActionOns    = ticketAccountables
)

// newTicketCommand creates the "ticket" parent command that groups the anomaly
// ticket subcommands.
func newTicketCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ticket",
		Short: "Raise and follow anomaly tickets",
		Long: `Report a problem with a pass as a ticket and follow it to resolution.

  See your tickets        list, get
  Raise and update        create, update, delete
  Follow a ticket         watch, unwatch
  Discuss                 list-comments, add-comment, update-comment, delete-comment
  Attach logs or data     list-attachments, upload-attachment, download-attachment,
                          delete-attachment

A ticket belongs to the organization that raised it; other organizations
cannot see it. Run any command with --help to see what it does and an example.`,
	}

	cmd.AddCommand(
		newListTicketsCommand(),
		newGetTicketCommand(),
		newCreateTicketCommand(),
		newUpdateTicketCommand(),
		newDeleteTicketCommand(),
		newWatchTicketCommand(),
		newUnwatchTicketCommand(),
		newListTicketCommentsCommand(),
		newAddTicketCommentCommand(),
		newUpdateTicketCommentCommand(),
		newDeleteTicketCommentCommand(),
		newListTicketAttachmentsCommand(),
		newUploadTicketAttachmentCommand(),
		newDownloadTicketAttachmentCommand(),
		newDeleteTicketAttachmentCommand(),
	)
	return cmd
}

// ---- columns ---------------------------------------------------------------

func ticketUserLabel(u *apiclient.TicketUser) string {
	if u == nil {
		return ""
	}
	return refDisplayName(u.UserName, u.UserID)
}

//nolint:gochecknoglobals
var ticketColumns = []printer.Column{
	{Header: "ID", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).ID }},                //nolint:errcheck
	{Header: "STATUS", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).Status }},        //nolint:errcheck
	{Header: "SEVERITY", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).Severity }},    //nolint:errcheck
	{Header: "ISSUE_TYPE", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).IssueType }}, //nolint:errcheck
	{Header: "TITLE", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).Title }},          //nolint:errcheck
	{Header: "REPORTER", Extract: func(r interface{}) string {
		t := r.(*apiclient.Ticket) //nolint:errcheck
		return ticketUserLabel(&t.Reporter)
	}},
	{Header: "ACCOUNTABLE", Extract: func(r interface{}) string { return r.(*apiclient.Ticket).Accountable }}, //nolint:errcheck
	{Header: "ASSIGNEE", Extract: func(r interface{}) string {
		t := r.(*apiclient.Ticket) //nolint:errcheck
		if t.OperatorAssignee != nil {
			return ticketUserLabel(t.OperatorAssignee)
		}
		return ticketUserLabel(t.InfostellarAssignee)
	}},
	{Header: "PASS_IDS", Extract: func(r interface{}) string {
		t := r.(*apiclient.Ticket) //nolint:errcheck
		ids := make([]string, 0, len(t.Passes))
		for _, p := range t.Passes {
			ids = append(ids, p.PassID)
		}
		return strings.Join(ids, " ")
	}},
	{Header: "UPDATED", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.Ticket).UpdatedAt) //nolint:errcheck
	}},
}

//nolint:gochecknoglobals
var ticketCommentColumns = []printer.Column{
	{Header: "ID", Extract: func(r interface{}) string { return r.(*apiclient.TicketComment).ID }}, //nolint:errcheck
	{Header: "AUTHOR", Extract: func(r interface{}) string {
		c := r.(*apiclient.TicketComment) //nolint:errcheck
		return refDisplayName(c.UserName, c.UserID)
	}},
	{Header: "ORGANIZATION", Extract: func(r interface{}) string {
		c := r.(*apiclient.TicketComment) //nolint:errcheck
		return refDisplayName(c.OrganizationName, c.OrganizationID)
	}},
	{Header: "CREATED", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.TicketComment).CreatedAt) //nolint:errcheck
	}},
	{Header: "CONTENT", Extract: func(r interface{}) string { return r.(*apiclient.TicketComment).Content }}, //nolint:errcheck
}

//nolint:gochecknoglobals
var ticketAttachmentColumns = []printer.Column{
	{Header: "ID", Extract: func(r interface{}) string { return r.(*apiclient.TicketAttachment).ID }},              //nolint:errcheck
	{Header: "FILE_NAME", Extract: func(r interface{}) string { return r.(*apiclient.TicketAttachment).FileName }}, //nolint:errcheck
	{Header: "SIZE_BYTES", Extract: func(r interface{}) string {
		a := r.(*apiclient.TicketAttachment) //nolint:errcheck
		if a.SizeBytes == nil {
			return ""
		}
		return fmt.Sprintf("%d", *a.SizeBytes)
	}},
	{Header: "CONTENT_TYPE", Extract: func(r interface{}) string { return r.(*apiclient.TicketAttachment).ContentType }}, //nolint:errcheck
	{Header: "UPLOADED_BY", Extract: func(r interface{}) string {
		return ticketUserLabel(r.(*apiclient.TicketAttachment).UploadedBy) //nolint:errcheck
	}},
	{Header: "UPLOADED", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.TicketAttachment).CreatedAt) //nolint:errcheck
	}},
}

// ---- helpers ---------------------------------------------------------------

// requireTwoArgs is requireOneArg for commands that name two resources.
func requireTwoArgs(what, example string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("this command needs %s.\nFor example:\n  %s", what, example)
		}
		return nil
	}
}

// enumFlag reads a string flag and checks it against the allowed values,
// case-insensitively, returning the canonical upper-case value. An unset flag
// returns "".
func enumFlag(cmd *cobra.Command, name string, allowed []string) (string, error) {
	value, _ := cmd.Flags().GetString(name)
	value = strings.ToUpper(strings.TrimSpace(value))
	if value == "" {
		return "", nil
	}
	for _, candidate := range allowed {
		if value == candidate {
			return value, nil
		}
	}
	return "", fmt.Errorf("--%s must be one of %s, not %q", name, strings.Join(allowed, ", "), value)
}

// optionalString returns a pointer to the flag's value only when the flag was
// given on the command line, so an update sends exactly the fields the user
// named. An explicitly empty value is sent as "", which clears the field.
func optionalString(cmd *cobra.Command, name string) *string {
	if !cmd.Flags().Changed(name) {
		return nil
	}
	value, _ := cmd.Flags().GetString(name)
	return &value
}

// optionalEnum is optionalString with the value validated against allowed.
func optionalEnum(cmd *cobra.Command, name string, allowed []string) (*string, error) {
	if !cmd.Flags().Changed(name) {
		return nil, nil //nolint:nilnil
	}
	value, err := enumFlag(cmd, name, allowed)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// parseActionFlags turns repeated --action "ON=text" flags into action inputs.
func parseActionFlags(values []string) ([]apiclient.TicketActionInput, error) {
	actions := make([]apiclient.TicketActionInput, 0, len(values))
	for _, value := range values {
		on, text, ok := strings.Cut(value, "=")
		on = strings.ToUpper(strings.TrimSpace(on))
		text = strings.TrimSpace(text)
		if !ok || text == "" {
			return nil, fmt.Errorf("--action must look like PARTY=text, for example SATELLITE_OPERATOR=\"Re-upload the TLE\", not %q", value)
		}
		valid := false
		for _, candidate := range ticketActionOns {
			valid = valid || on == candidate
		}
		if !valid {
			return nil, fmt.Errorf("--action party must be one of %s, not %q", strings.Join(ticketActionOns, ", "), on)
		}
		actions = append(actions, apiclient.TicketActionInput{ActionOn: on, ActionText: text})
	}
	return actions, nil
}

// addTicketFieldFlags registers the ticket fields shared by create and update.
func addTicketFieldFlags(cmd *cobra.Command) {
	cmd.Flags().String("title", "", "Short summary of the problem")
	cmd.Flags().String("severity", "", "How serious it is: LOW, MEDIUM, or HIGH")
	cmd.Flags().String("issue-type", "", "What kind of issue: ANOMALY, PERFORMANCE, BILLING, REQUEST, or OTHER")
	cmd.Flags().String("accountable", "", "Who is accountable: INFOSTELLAR, SATELLITE_OPERATOR, or GROUND_STATION_OPERATOR")
	cmd.Flags().StringSlice("pass-id", nil, "Pass the ticket is about (repeat or comma-separate for several)")
	cmd.Flags().String("anomaly-description", "", "What happened")
	cmd.Flags().String("root-cause", "", "Why it happened")
	cmd.Flags().String("corrective-action", "", "What was done about it")
	cmd.Flags().String("preventive-action", "", "What stops it happening again")
	cmd.Flags().String("close-reason", "", "Why the ticket was closed")
	cmd.Flags().String("infostellar-assignee", "", "User ID of the Infostellar staff member handling it")
	cmd.Flags().String("operator-assignee", "", "User ID of the operator handling it")
}

// ---- tickets ---------------------------------------------------------------

func newListTicketsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List your organization's tickets",
		Long: `List the tickets your organization has raised, newest first.

Examples:
  stellar ticket list
  stellar ticket list --page-size 10 --output json`,
		RunE: runListTickets,
	}
	addOutputFlag(cmd)
	cmd.Flags().Int("page-size", 0, "How many tickets to show at most (default: the API's page size)")
	return cmd
}

func runListTickets(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	pageSize, _ := cmd.Flags().GetInt("page-size")
	if pageSize < 0 {
		return errors.New("--page-size must be a positive number")
	}
	tickets, err := client.ListTickets(cmd.Context(), pageSize)
	if err != nil {
		return err
	}
	rows := make([]interface{}, len(tickets))
	for i := range tickets {
		rows[i] = &tickets[i]
	}
	return renderList(cmd, format, ticketColumns, rows, "ticket", "Raise one with: stellar ticket create --title \"...\" --severity HIGH")
}

func newGetTicketCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <ticket-id>",
		Short: "Show one ticket",
		Long: `Show a ticket with its passes, actions and comments.

The readable table shows the headline fields; use --output json for everything,
including the comments and the actions.

Example:
  stellar ticket get <ticket-id>`,
		Args: requireOneArg("a ticket ID (from \"stellar ticket list\")", "stellar ticket get <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			format, err := getOutputFormat(cmd)
			if err != nil {
				return err
			}
			ticket, err := client.GetTicket(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return renderOne(cmd, format, ticketColumns, ticket, "")
		},
	}
	addOutputFlag(cmd)
	return cmd
}

func newCreateTicketCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Raise a ticket",
		Long: `Raise a ticket about a problem, optionally naming the passes it concerns.

--title and --severity are required. Passes must belong to your organization.

Examples:
  stellar ticket create --title "Downlink dropped mid-pass" --severity HIGH --pass-id <pass-id>
  stellar ticket create --title "Invoice query" --severity LOW --issue-type BILLING`,
		RunE: runCreateTicket,
	}
	addOutputFlag(cmd)
	addTicketFieldFlags(cmd)
	return cmd
}

func runCreateTicket(cmd *cobra.Command, _ []string) error {
	title, _ := cmd.Flags().GetString("title")
	if strings.TrimSpace(title) == "" {
		return errors.New("--title is required: say in a few words what the problem is")
	}
	severity, err := enumFlag(cmd, "severity", ticketSeverities)
	if err != nil {
		return err
	}
	if severity == "" {
		return errors.New("--severity is required: LOW, MEDIUM, or HIGH")
	}
	issueType, err := enumFlag(cmd, "issue-type", ticketIssueTypes)
	if err != nil {
		return err
	}
	accountable, err := enumFlag(cmd, "accountable", ticketAccountables)
	if err != nil {
		return err
	}
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	get := func(name string) string { v, _ := cmd.Flags().GetString(name); return strings.TrimSpace(v) }
	passIDs, _ := cmd.Flags().GetStringSlice("pass-id")
	ticket, err := client.CreateTicket(cmd.Context(), apiclient.TicketCreateRequest{
		Severity:            severity,
		IssueType:           issueType,
		Title:               strings.TrimSpace(title),
		Accountable:         accountable,
		PassIDs:             passIDs,
		AnomalyDescription:  get("anomaly-description"),
		RootCause:           get("root-cause"),
		CorrectiveAction:    get("corrective-action"),
		PreventiveAction:    get("preventive-action"),
		CloseReason:         get("close-reason"),
		InfostellarAssignee: get("infostellar-assignee"),
		OperatorAssignee:    get("operator-assignee"),
	})
	if err != nil {
		return err
	}
	return renderOne(cmd, format, ticketColumns, ticket, "Ticket raised: "+ticket.ID)
}

func newUpdateTicketCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <ticket-id>",
		Short: "Change a ticket's fields",
		Long: `Change the fields of a ticket. Only the options you give are changed; the
rest keep their values.

--pass-id and --action replace the ticket's whole list when given, so repeat
them for every pass or action the ticket should end up with. Give
--preventive-action "" to clear the preventive action.

Examples:
  stellar ticket update <ticket-id> --status CLOSED --close-reason "Antenna realigned"
  stellar ticket update <ticket-id> --severity LOW --root-cause "Weather"
  stellar ticket update <ticket-id> --action SATELLITE_OPERATOR="Re-upload the TLE"`,
		Args: requireOneArg("a ticket ID (from \"stellar ticket list\")", "stellar ticket update <ticket-id> --status CLOSED"),
		RunE: runUpdateTicket,
	}
	addOutputFlag(cmd)
	addTicketFieldFlags(cmd)
	cmd.Flags().String("status", "", "OPEN or CLOSED")
	cmd.Flags().StringArray("action", nil, "A follow-up action as PARTY=text, where PARTY is INFOSTELLAR, SATELLITE_OPERATOR, or GROUND_STATION_OPERATOR (repeat for several)")
	return cmd
}

func runUpdateTicket(cmd *cobra.Command, args []string) error {
	var req apiclient.TicketUpdateRequest
	var err error
	if req.Status, err = optionalEnum(cmd, "status", ticketStatuses); err != nil {
		return err
	}
	if req.Severity, err = optionalEnum(cmd, "severity", ticketSeverities); err != nil {
		return err
	}
	if req.IssueType, err = optionalEnum(cmd, "issue-type", ticketIssueTypes); err != nil {
		return err
	}
	if req.Accountable, err = optionalEnum(cmd, "accountable", ticketAccountables); err != nil {
		return err
	}
	req.Title = optionalString(cmd, "title")
	req.AnomalyDescription = optionalString(cmd, "anomaly-description")
	req.RootCause = optionalString(cmd, "root-cause")
	req.CorrectiveAction = optionalString(cmd, "corrective-action")
	req.PreventiveAction = optionalString(cmd, "preventive-action")
	req.CloseReason = optionalString(cmd, "close-reason")
	req.InfostellarAssignee = optionalString(cmd, "infostellar-assignee")
	req.OperatorAssignee = optionalString(cmd, "operator-assignee")
	if cmd.Flags().Changed("pass-id") {
		passIDs, _ := cmd.Flags().GetStringSlice("pass-id")
		req.PassIDs = &passIDs
	}
	if cmd.Flags().Changed("action") {
		values, _ := cmd.Flags().GetStringArray("action")
		actions, err := parseActionFlags(values)
		if err != nil {
			return err
		}
		req.Actions = &actions
	}
	if req == (apiclient.TicketUpdateRequest{}) {
		return errors.New("nothing to change: give at least one option, for example --status CLOSED")
	}

	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	ticket, err := client.UpdateTicket(cmd.Context(), args[0], req)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, ticketColumns, ticket, "Ticket updated: "+ticket.ID)
}

func newDeleteTicketCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <ticket-id>",
		Short: "Delete a ticket",
		Long: `Delete a ticket with its comments, actions and attachments. This cannot be
undone; close a resolved ticket instead with "stellar ticket update --status CLOSED".

Example:
  stellar ticket delete <ticket-id>`,
		Args: requireOneArg("a ticket ID", "stellar ticket delete <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			if err := client.DeleteTicket(cmd.Context(), args[0]); err != nil {
				return err
			}
			uiOKf("Ticket deleted: %s", args[0])
			return nil
		},
	}
}

func newWatchTicketCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "watch <ticket-id>",
		Short: "Get notified about a ticket's changes",
		Long: `Follow a ticket: you are then notified when its status, severity or
assignment changes or someone comments.

Example:
  stellar ticket watch <ticket-id>`,
		Args: requireOneArg("a ticket ID", "stellar ticket watch <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			if _, err := client.WatchTicket(cmd.Context(), args[0]); err != nil {
				return err
			}
			uiOKf("Watching ticket %s", args[0])
			return nil
		},
	}
}

func newUnwatchTicketCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "unwatch <ticket-id>",
		Short: "Stop following a ticket",
		Args:  requireOneArg("a ticket ID", "stellar ticket unwatch <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			if err := client.UnwatchTicket(cmd.Context(), args[0]); err != nil {
				return err
			}
			uiOKf("No longer watching ticket %s", args[0])
			return nil
		},
	}
}

// ---- comments --------------------------------------------------------------

func newListTicketCommentsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-comments <ticket-id>",
		Short: "List a ticket's comments",
		Args:  requireOneArg("a ticket ID", "stellar ticket list-comments <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			format, err := getOutputFormat(cmd)
			if err != nil {
				return err
			}
			comments, err := client.ListTicketComments(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			rows := make([]interface{}, len(comments))
			for i := range comments {
				rows[i] = &comments[i]
			}
			return renderList(cmd, format, ticketCommentColumns, rows, "comment", "")
		},
	}
	addOutputFlag(cmd)
	return cmd
}

func newAddTicketCommentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add-comment <ticket-id> <text>",
		Short: "Comment on a ticket",
		Long: `Add a comment to a ticket. Quote the text so your shell passes it whole.

Example:
  stellar ticket add-comment <ticket-id> "Re-uploaded the TLE, next pass looks good"`,
		Args: requireTwoArgs("a ticket ID and the comment text", `stellar ticket add-comment <ticket-id> "Looking into it"`),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(args[1]) == "" {
				return errors.New("the comment text is empty")
			}
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			format, err := getOutputFormat(cmd)
			if err != nil {
				return err
			}
			comment, err := client.CreateTicketComment(cmd.Context(), args[0], args[1])
			if err != nil {
				return err
			}
			return renderOne(cmd, format, ticketCommentColumns, comment, "Comment added: "+comment.ID)
		},
	}
	addOutputFlag(cmd)
	return cmd
}

func newUpdateTicketCommentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-comment <ticket-id> <comment-id>",
		Short: "Edit one of your comments",
		Long: `Replace the text of a comment you wrote. Comment IDs come from
"stellar ticket list-comments".

Example:
  stellar ticket update-comment <ticket-id> <comment-id> --text "Corrected: the TLE was stale"`,
		Args: requireTwoArgs("a ticket ID and a comment ID", "stellar ticket update-comment <ticket-id> <comment-id> --text \"...\""),
		RunE: func(cmd *cobra.Command, args []string) error {
			text, _ := cmd.Flags().GetString("text")
			if strings.TrimSpace(text) == "" {
				return errors.New("--text is required: the new comment text")
			}
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			format, err := getOutputFormat(cmd)
			if err != nil {
				return err
			}
			comment, err := client.UpdateTicketComment(cmd.Context(), args[0], args[1], text)
			if err != nil {
				return err
			}
			return renderOne(cmd, format, ticketCommentColumns, comment, "Comment updated: "+comment.ID)
		},
	}
	addOutputFlag(cmd)
	cmd.Flags().String("text", "", "The new comment text (required)")
	return cmd
}

func newDeleteTicketCommentCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete-comment <ticket-id> <comment-id>",
		Short: "Delete one of your comments",
		Args:  requireTwoArgs("a ticket ID and a comment ID", "stellar ticket delete-comment <ticket-id> <comment-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			if err := client.DeleteTicketComment(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			uiOKf("Comment deleted: %s", args[1])
			return nil
		},
	}
}

// ---- attachments -----------------------------------------------------------

func newListTicketAttachmentsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-attachments <ticket-id>",
		Short: "List the files attached to a ticket",
		Args:  requireOneArg("a ticket ID", "stellar ticket list-attachments <ticket-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			format, err := getOutputFormat(cmd)
			if err != nil {
				return err
			}
			listed, err := client.ListTicketAttachments(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			rows := make([]interface{}, len(listed.Attachments))
			for i := range listed.Attachments {
				rows[i] = &listed.Attachments[i]
			}
			hint := fmt.Sprintf("Attach one with: stellar ticket upload-attachment %s <file>", args[0])
			if err := renderList(cmd, format, ticketAttachmentColumns, rows, "attachment", hint); err != nil {
				return err
			}
			if humanTable(format) && len(rows) > 0 {
				uiDimf("  A ticket may hold %d attachments of up to %d bytes each.", listed.MaxAttachments, listed.MaxSizeBytes)
			}
			return nil
		},
	}
	addOutputFlag(cmd)
	return cmd
}

func newUploadTicketAttachmentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload-attachment <ticket-id> <file>",
		Short: "Attach a file (logs, monitoring data) to a ticket",
		Long: `Attach a file to a ticket. The content type is guessed from the file
extension unless --content-type is given.

Example:
  stellar ticket upload-attachment <ticket-id> ./station.log`,
		Args: requireTwoArgs("a ticket ID and a file path", "stellar ticket upload-attachment <ticket-id> ./station.log"),
		RunE: runUploadTicketAttachment,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("content-type", "", "MIME type of the file, e.g. text/plain (default: from the file extension)")
	return cmd
}

func runUploadTicketAttachment(cmd *cobra.Command, args []string) error {
	path := args[1]
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s is a directory; name a file to attach", path)
	}
	contentType, _ := cmd.Flags().GetString("content-type")
	if contentType == "" {
		contentType = attachmentContentType(path)
	}

	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	size := info.Size()
	attachment, err := client.UploadTicketAttachment(cmd.Context(), args[0], apiclient.TicketAttachmentCreateRequest{
		FileName:    filepath.Base(path),
		ContentType: contentType,
		SizeBytes:   &size,
	}, file)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, ticketAttachmentColumns, attachment,
		fmt.Sprintf("Attached %s (%d bytes) as %s", filepath.Base(path), size, attachment.ID))
}

func newDownloadTicketAttachmentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download-attachment <ticket-id> <attachment-id>",
		Short: "Download a file attached to a ticket",
		Long: `Download an attachment. Attachment IDs come from "stellar ticket list-attachments".
The file is written to --output-file, or to the attachment's own file name in
the current directory. Give --output-file - to write it to standard output.

Example:
  stellar ticket download-attachment <ticket-id> <attachment-id> --output-file ./station.log`,
		Args: requireTwoArgs("a ticket ID and an attachment ID", "stellar ticket download-attachment <ticket-id> <attachment-id>"),
		RunE: runDownloadTicketAttachment,
	}
	cmd.Flags().String("output-file", "", "Where to write the file (default: the attachment's file name; - for standard output)")
	return cmd
}

func runDownloadTicketAttachment(cmd *cobra.Command, args []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	target, _ := cmd.Flags().GetString("output-file")
	if target == "" {
		listed, err := client.ListTicketAttachments(cmd.Context(), args[0])
		if err != nil {
			return err
		}
		for _, attachment := range listed.Attachments {
			if attachment.ID == args[1] {
				target = filepath.Base(attachment.FileName)
			}
		}
		if target == "" {
			return fmt.Errorf("attachment %s is not on ticket %s; see \"stellar ticket list-attachments %s\"", args[1], args[0], args[0])
		}
	}

	out := cmd.OutOrStdout()
	if target != "-" {
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("cannot write %s: %w", target, err)
		}
		defer f.Close()
		out = f
	}
	n, err := client.DownloadTicketAttachment(cmd.Context(), args[0], args[1], out)
	if err != nil {
		if target != "-" {
			_ = os.Remove(target)
		}
		return err
	}
	if target != "-" {
		uiOKf("Downloaded %d bytes to %s", n, target)
	}
	return nil
}

func newDeleteTicketAttachmentCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete-attachment <ticket-id> <attachment-id>",
		Short: "Remove a file from a ticket",
		Args:  requireTwoArgs("a ticket ID and an attachment ID", "stellar ticket delete-attachment <ticket-id> <attachment-id>"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := newAPIClient(cmd)
			if err != nil {
				return err
			}
			if err := client.DeleteTicketAttachment(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			uiOKf("Attachment deleted: %s", args[1])
			return nil
		},
	}
}

// attachmentContentTypes covers the files operators attach most. Go's own
// table knows only the web formats and otherwise asks the operating system,
// whose answer varies by platform: Windows has no entry for .log at all. These
// are fixed here so an upload is typed the same wherever the CLI runs.
var attachmentContentTypes = map[string]string{
	".log":  "text/plain",
	".txt":  "text/plain",
	".csv":  "text/csv",
	".md":   "text/markdown",
	".yaml": "application/yaml",
	".yml":  "application/yaml",
	".tle":  "text/plain",
}

// attachmentContentType guesses an attachment's content type from its file
// extension, falling back to application/octet-stream.
func attachmentContentType(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	if t, ok := attachmentContentTypes[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}
