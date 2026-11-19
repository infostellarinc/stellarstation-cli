package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

func newInteractiveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "interactive",
		Short: "Step-by-step help: activate your key, find and book passes, open streams",
		Long: `A menu-driven session for the common workflows:

  - activate your API key, or keep using the one already activated
  - stream your next pass
  - see your upcoming passes, and open a stream for one, change it or cancel it
  - find a free window, book a pass on it, and stream it

Each step shows the equivalent stellar command, so you can script it later.
Running "stellar" with no arguments in a terminal starts interactive mode too.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInteractive(cmd, teaPrompter{}, cmd.OutOrStdout())
		},
	}
}

// isInteractiveTerminal reports whether stdin and stdout are a terminal, so a
// guided session can run.
func isInteractiveTerminal() bool {
	return term.IsTerminal(os.Stdin.Fd()) && term.IsTerminal(os.Stdout.Fd())
}

func runInteractive(cmd *cobra.Command, p prompter, out io.Writer) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	fmt.Fprintln(out, "StellarStation interactive mode. Esc goes back, Ctrl-C quits a prompt.")
	fmt.Fprintln(out)

	credsPath, err := interactiveCredentials(cmd, p, out)
	if err != nil {
		return interactiveExit(err)
	}
	if err := interactiveAPIAddress(cmd, p, out, credsPath); err != nil {
		return interactiveExit(err)
	}
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	g := &interactiveMode{
		p:          p,
		api:        client,
		out:        out,
		now:        time.Now,
		openStream: func(args []string) error { return runStellarChild(ctx, args) },
		extraArgs:  passThroughFlags(cmd),
	}
	return interactiveExit(g.run(ctx))
}

// interactiveExit treats backing out of the first prompts as leaving interactive mode.
func interactiveExit(err error) error {
	if errors.Is(err, errInteractiveCanceled) {
		return nil
	}
	return err
}

// passThroughFlags repeats the global flags the operator set, so the commands
// interactive mode runs use the same key and address.
func passThroughFlags(cmd *cobra.Command) []string {
	var args []string
	for _, name := range []string{"credentials", "api-url"} {
		if f := cmd.Flags().Lookup(name); f != nil && f.Changed {
			args = append(args, "--"+name, f.Value.String())
		}
	}
	return args
}

// runStellarChild runs a stellar command line as a child process that has the
// terminal to itself: a stream reads typed commands from stdin, and a reader
// left behind in this process would take keystrokes from the menus
// afterwards. Ctrl-C during the stream stops the stream, not interactive mode.
func runStellarChild(ctx context.Context, args []string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("could not find the stellar program to run: %w", err)
	}
	// Nothing in this process may read the terminal while the child does.
	promptTTY.stop()
	child := exec.CommandContext(ctx, exe, args...) //nolint:gosec // Runs this same program.
	child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	defer signal.Stop(interrupts)
	err = child.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return fmt.Errorf("the stream stopped with exit code %d", exit.ExitCode())
	}
	return err
}

// interactiveCredentials makes sure an API key is active, offering to activate one,
// and returns the credentials file in use.
func interactiveCredentials(cmd *cobra.Command, p prompter, out io.Writer) (string, error) {
	if strings.TrimSpace(os.Getenv(bearerTokenEnv)) != "" {
		fmt.Fprintf(out, "Using the token in $%s.\n", bearerTokenEnv)
		return "", nil
	}
	path, err := resolveCredentialsPath(cmd)
	if err != nil {
		return "", err
	}
	creds, loadErr := auth.Load(path)
	if loadErr == nil {
		fmt.Fprintf(out, "Using your activated API key %s (%s).\n", creds.ClientID, path)
		choice, err := p.Select("Continue with this key?",
			[]string{"Yes, use this key", "No, activate a different API key file"}, 0)
		if err != nil || choice == 0 {
			return path, err
		}
	} else {
		fmt.Fprintln(out, "No API key is activated yet. Create one in the StellarStation console")
		fmt.Fprintln(out, "(Organization > API Keys), download its file, and give its location here.")
	}
	return path, activateFromPrompt(p, out, path)
}

func activateFromPrompt(p prompter, out io.Writer, path string) error {
	keyFile, err := p.Input("API key file", "~/Downloads/stellarstation-api-key.json", "", func(s string) error {
		data, err := os.ReadFile(expandHome(s)) //nolint:gosec // The operator names their own key file.
		if err != nil {
			return fmt.Errorf("cannot read it: %v", errors.Unwrap(err))
		}
		if _, err := auth.Parse(data); err != nil {
			return errors.New("this is not a StellarStation API key file")
		}
		return nil
	})
	if err != nil {
		return err
	}
	data, err := os.ReadFile(expandHome(keyFile)) //nolint:gosec // Checked above.
	if err != nil {
		return err
	}
	creds, err := auth.Parse(data)
	if err != nil {
		return err
	}
	if err := auth.Save(path, creds); err != nil {
		return err
	}
	fmt.Fprintf(out, "API key activated (saved to %s).\n", path)
	fmt.Fprintf(out, "  Next time: stellar auth activate-api-key %s\n", keyFile)
	return nil
}

func expandHome(path string) string {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}

// interactiveAPIAddress makes sure the CLI knows which StellarStation API to use,
// and offers to remember it with the activated key.
func interactiveAPIAddress(cmd *cobra.Command, p prompter, out io.Writer, credsPath string) error {
	if addr, err := resolveAPIBaseURL(cmd); err == nil {
		fmt.Fprintf(out, "API address: %s\n", addr)
		return nil
	}
	fmt.Fprintln(out, "Which StellarStation should this key talk to? Use the API address your")
	fmt.Fprintln(out, "StellarStation contact gave you (it starts with https://api.).")
	addr, err := p.Input("API address", "https://api.example.aws.stellarstation.com", "", func(s string) error {
		return auth.CheckEndpointScheme("API address", strings.TrimRight(s, "/"))
	})
	if err != nil {
		return err
	}
	addr = strings.TrimRight(addr, "/")
	if err := cmd.Flags().Set("api-url", addr); err != nil {
		return err
	}
	if credsPath == "" {
		return nil
	}
	remember, err := p.Confirm("Remember this address with your API key?", true)
	if err != nil || !remember {
		return err
	}
	creds, err := auth.Load(credsPath)
	if err != nil {
		return err
	}
	creds.APIURL = addr
	if err := auth.Save(credsPath, creds); err != nil {
		return err
	}
	fmt.Fprintln(out, "Saved: stellar commands will use it unless STELLAR_API_URL or --api-url says otherwise.")
	return nil
}

// The real client is what interactive mode talks to.
var _ interactiveAPI = (*apiclient.Client)(nil)
