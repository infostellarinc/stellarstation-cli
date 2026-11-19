package main

// Interactive prompts for `stellar interactive`: arrow-key menus, checklists, text
// input and yes/no questions, drawn with bubbletea. Its workflows talk
// to the prompter interface only, so tests drive them with a script.

import (
	"errors"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// errInteractiveCanceled is returned when the operator backs out of a prompt (Esc or
// Ctrl-C).
var errInteractiveCanceled = errors.New("canceled")

// prompter asks the operator questions.
type prompter interface {
	// Select asks for one of options and returns its index.
	Select(title string, options []string, defaultIdx int) (int, error)
	// MultiSelect asks which of options to turn on, starting from selected.
	MultiSelect(title string, options []string, selected []bool) ([]bool, error)
	// Input asks for text. validate, when set, explains why a value is refused.
	Input(title, placeholder, initial string, validate func(string) error) (string, error)
	// Confirm asks a yes/no question.
	Confirm(title string, def bool) (bool, error)
}

var (
	promptTitleStyle  = lipgloss.NewStyle().Bold(true)
	promptCursorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("12")).Bold(true)
	promptDimStyle    = lipgloss.NewStyle().Faint(true)
	promptErrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
)

// promptVisibleRows is how many options a menu shows at once.
const promptVisibleRows = 12

// teaPrompter asks through a terminal UI.
type teaPrompter struct{}

func runPrompt[M tea.Model](m M) (M, error) {
	var opts []tea.ProgramOption
	// Read through the shared pump (see interactive_tty.go) so the prompt
	// lets go of the terminal when it ends. Without a separate terminal
	// handle, read it directly.
	if in, err := promptTTY.input(); err == nil {
		defer in.Close() //nolint:errcheck // Close never fails.
		opts = append(opts, tea.WithInput(in))
	}
	final, err := tea.NewProgram(m, opts...).Run()
	if err != nil {
		return m, err
	}
	out, ok := final.(M)
	if !ok {
		return m, errors.New("unexpected prompt state")
	}
	return out, nil
}

func (teaPrompter) Select(title string, options []string, defaultIdx int) (int, error) {
	m, err := runPrompt(newSelectModel(title, options, defaultIdx))
	if err != nil {
		return 0, err
	}
	if m.canceled {
		return 0, errInteractiveCanceled
	}
	return m.chosen, nil
}

func (teaPrompter) MultiSelect(title string, options []string, selected []bool) ([]bool, error) {
	m, err := runPrompt(newMultiSelectModel(title, options, selected))
	if err != nil {
		return nil, err
	}
	if m.canceled {
		return nil, errInteractiveCanceled
	}
	return m.selected, nil
}

func (teaPrompter) Input(title, placeholder, initial string, validate func(string) error) (string, error) {
	m, err := runPrompt(newInputModel(title, placeholder, initial, validate))
	if err != nil {
		return "", err
	}
	if m.canceled {
		return "", errInteractiveCanceled
	}
	return strings.TrimSpace(string(m.value)), nil
}

func (teaPrompter) Confirm(title string, def bool) (bool, error) {
	m, err := runPrompt(&confirmModel{title: title, yes: def})
	if err != nil {
		return false, err
	}
	if m.canceled {
		return false, errInteractiveCanceled
	}
	return m.yes, nil
}

// --- select -------------------------------------------------------------------

// selectModel is a menu: arrows move, typing filters, Enter chooses.
type selectModel struct {
	title    string
	options  []string
	filter   string
	visible  []int // indexes of options matching the filter
	cursor   int   // position in visible
	chosen   int
	canceled bool
	done     bool
}

func newSelectModel(title string, options []string, defaultIdx int) *selectModel {
	m := &selectModel{title: title, options: options, chosen: -1}
	m.applyFilter()
	for i, idx := range m.visible {
		if idx == defaultIdx {
			m.cursor = i
		}
	}
	return m
}

func (m *selectModel) applyFilter() {
	m.visible = m.visible[:0]
	needle := strings.ToLower(m.filter)
	for i, o := range m.options {
		if needle == "" || strings.Contains(strings.ToLower(o), needle) {
			m.visible = append(m.visible, i)
		}
	}
	if m.cursor >= len(m.visible) {
		m.cursor = max(0, len(m.visible)-1)
	}
}

func (m *selectModel) Init() tea.Cmd { return nil }

func (m *selectModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "ctrl+c", "esc":
		m.canceled, m.done = true, true
		return m, tea.Quit
	case "enter":
		if len(m.visible) == 0 {
			return m, nil
		}
		m.chosen, m.done = m.visible[m.cursor], true
		return m, tea.Quit
	case "up", "ctrl+p":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "ctrl+n":
		if m.cursor < len(m.visible)-1 {
			m.cursor++
		}
	case "backspace":
		if m.filter != "" {
			m.filter = m.filter[:len(m.filter)-1]
			m.applyFilter()
		}
	default:
		if key.Text != "" {
			m.filter += key.Text
			m.applyFilter()
		}
	}
	return m, nil
}

func (m *selectModel) View() tea.View {
	var b strings.Builder
	b.WriteString(promptTitleStyle.Render(m.title) + "\n")
	if m.done {
		if !m.canceled && m.chosen >= 0 {
			b.WriteString("  " + promptCursorStyle.Render("› "+m.options[m.chosen]) + "\n")
		}
		return tea.NewView(b.String())
	}
	if m.filter != "" {
		b.WriteString(promptDimStyle.Render("  filter: "+m.filter) + "\n")
	}
	start := 0
	if m.cursor >= promptVisibleRows {
		start = m.cursor - promptVisibleRows + 1
	}
	end := min(len(m.visible), start+promptVisibleRows)
	for i := start; i < end; i++ {
		label := m.options[m.visible[i]]
		if i == m.cursor {
			b.WriteString(promptCursorStyle.Render("› "+label) + "\n")
		} else {
			b.WriteString("  " + label + "\n")
		}
	}
	if len(m.visible) == 0 {
		b.WriteString(promptDimStyle.Render("  nothing matches") + "\n")
	}
	if len(m.visible) > promptVisibleRows {
		b.WriteString(promptDimStyle.Render(fmt.Sprintf("  %d of %d", m.cursor+1, len(m.visible))) + "\n")
	}
	b.WriteString(promptDimStyle.Render("  ↑/↓ move · type to filter · Enter choose · Esc back") + "\n")
	return tea.NewView(b.String())
}

// --- multi select ---------------------------------------------------------------

// multiSelectModel is a checklist: Space toggles, Enter confirms.
type multiSelectModel struct {
	title    string
	options  []string
	selected []bool
	cursor   int
	canceled bool
	done     bool
}

func newMultiSelectModel(title string, options []string, selected []bool) *multiSelectModel {
	s := make([]bool, len(options))
	copy(s, selected)
	return &multiSelectModel{title: title, options: options, selected: s}
}

func (m *multiSelectModel) Init() tea.Cmd { return nil }

func (m *multiSelectModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "ctrl+c", "esc":
		m.canceled, m.done = true, true
		return m, tea.Quit
	case "enter":
		m.done = true
		return m, tea.Quit
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < len(m.options)-1 {
			m.cursor++
		}
	case "space", "x":
		m.selected[m.cursor] = !m.selected[m.cursor]
	}
	return m, nil
}

func (m *multiSelectModel) View() tea.View {
	var b strings.Builder
	b.WriteString(promptTitleStyle.Render(m.title) + "\n")
	for i, o := range m.options {
		box := "[ ]"
		if m.selected[i] {
			box = "[x]"
		}
		line := box + " " + o
		switch {
		case m.done && !m.selected[i]:
			continue
		case m.done:
			b.WriteString("  " + promptCursorStyle.Render(line) + "\n")
		case i == m.cursor:
			b.WriteString(promptCursorStyle.Render("› "+line) + "\n")
		default:
			b.WriteString("  " + line + "\n")
		}
	}
	if !m.done {
		b.WriteString(promptDimStyle.Render("  ↑/↓ move · Space toggle · Enter confirm · Esc back") + "\n")
	}
	return tea.NewView(b.String())
}

// --- input --------------------------------------------------------------------

// inputModel is a one-line text field: type or paste, Backspace deletes,
// Ctrl-U clears, Enter submits.
type inputModel struct {
	title       string
	placeholder string
	value       []rune
	validate    func(string) error
	err         error
	canceled    bool
	done        bool
}

func newInputModel(title, placeholder, initial string, validate func(string) error) *inputModel {
	return &inputModel{title: title, placeholder: placeholder, value: []rune(initial), validate: validate}
}

func (m *inputModel) Init() tea.Cmd { return nil }

func (m *inputModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.PasteMsg:
		m.value = append(m.value, []rune(strings.TrimRight(msg.Content, "\r\n"))...)
		m.err = nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			m.canceled, m.done = true, true
			return m, tea.Quit
		case "enter":
			value := strings.TrimSpace(string(m.value))
			if m.validate != nil {
				if err := m.validate(value); err != nil {
					m.err = err
					return m, nil
				}
			}
			m.done = true
			return m, tea.Quit
		case "backspace":
			if len(m.value) > 0 {
				m.value = m.value[:len(m.value)-1]
			}
		case "ctrl+u":
			m.value = nil
		default:
			if msg.Text != "" {
				m.value = append(m.value, []rune(msg.Text)...)
			}
		}
		m.err = nil
	}
	return m, nil
}

func (m *inputModel) View() tea.View {
	var b strings.Builder
	b.WriteString(promptTitleStyle.Render(m.title) + "\n")
	if m.done {
		if !m.canceled {
			b.WriteString("  " + promptCursorStyle.Render(string(m.value)) + "\n")
		}
		return tea.NewView(b.String())
	}
	if len(m.value) == 0 {
		b.WriteString(promptCursorStyle.Render("› ") + promptDimStyle.Render(m.placeholder) + "\n")
	} else {
		b.WriteString(promptCursorStyle.Render("› ") + string(m.value) + "█\n")
	}
	if m.err != nil {
		b.WriteString(promptErrStyle.Render("  "+m.err.Error()) + "\n")
	}
	b.WriteString(promptDimStyle.Render("  Enter confirm · Ctrl-U clear · Esc back") + "\n")
	return tea.NewView(b.String())
}

// --- confirm ------------------------------------------------------------------

type confirmModel struct {
	title    string
	yes      bool
	canceled bool
	done     bool
}

func (m *confirmModel) Init() tea.Cmd { return nil }

func (m *confirmModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyPressMsg)
	if !ok {
		return m, nil
	}
	switch strings.ToLower(key.String()) {
	case "ctrl+c", "esc":
		m.canceled, m.done = true, true
		return m, tea.Quit
	case "y":
		m.yes, m.done = true, true
		return m, tea.Quit
	case "n":
		m.yes, m.done = false, true
		return m, tea.Quit
	case "left", "right", "tab", "h", "l":
		m.yes = !m.yes
	case "enter":
		m.done = true
		return m, tea.Quit
	}
	return m, nil
}

func (m *confirmModel) View() tea.View {
	yes, no := "Yes", "No"
	if m.yes {
		yes = promptCursorStyle.Render("[Yes]")
	} else {
		no = promptCursorStyle.Render("[No]")
	}
	line := promptTitleStyle.Render(m.title) + "  " + yes + " / " + no + "\n"
	if m.done && !m.canceled {
		answer := "No"
		if m.yes {
			answer = "Yes"
		}
		return tea.NewView(promptTitleStyle.Render(m.title) + "  " + promptCursorStyle.Render(answer) + "\n")
	}
	return tea.NewView(line)
}
