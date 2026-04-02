package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clawconnect "github.com/alswl/claw-connect"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "remote":
		os.Exit(runRemote(os.Args[2:]))
	case "exec":
		os.Exit(runExec(os.Args[2:]))
	case "version":
		os.Exit(runVersion(os.Args[2:]))
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: claw-connect <command> [flags]

Commands:
  remote    Connect to a WebSocket server and listen for commands
  exec      Execute a single prompt locally
  version   Print version information

Run "claw-connect <command> --help" for command-specific flags.
`)
}

// --- remote subcommand ---

type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ", ") }
func (h *headerList) Set(val string) error {
	*h = append(*h, val)
	return nil
}

func runRemote(args []string) int {
	fs := flag.NewFlagSet("remote", flag.ExitOnError)
	server := fs.String("server", "", "WebSocket server URL (required)")
	fs.StringVar(server, "s", "", "WebSocket server URL (shorthand)")
	backend := fs.String("backend", "claude", "Agent type: claude or codex")
	fs.StringVar(backend, "b", "claude", "Agent type (shorthand)")
	executable := fs.String("executable", "", "Path to agent binary (default: auto-detect)")
	cwd := fs.String("cwd", "", "Default working directory for executions")
	fs.StringVar(cwd, "C", "", "Default working directory (shorthand)")
	maxConcurrent := fs.Int("max-concurrent", 0, "Max parallel executions (0=unlimited)")
	var headers headerList
	fs.Var(&headers, "header", "HTTP header for handshake (format: Key: Value), repeatable")
	fs.Var(&headers, "H", "HTTP header (shorthand)")

	fs.Parse(args)

	if *server == "" {
		fmt.Fprintln(os.Stderr, "error: --server is required")
		fs.Usage()
		return 1
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	execPath := *executable
	if execPath == "" {
		execPath = *backend
	}

	b, err := clawconnect.New(*backend, clawconnect.Config{
		ExecutablePath: execPath,
		Logger:         logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	headerMap := parseHeaders(headers)

	client, err := clawconnect.NewRemoteClient(clawconnect.RemoteConfig{
		ServerURL:     *server,
		Backend:       b,
		Logger:        logger,
		Headers:       headerMap,
		MaxConcurrent: *maxConcurrent,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting remote client", "server", *server, "backend", *backend)

	if err := client.Run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	logger.Info("shutdown complete")
	return 0
}

func parseHeaders(raw []string) map[string]string {
	m := make(map[string]string, len(raw))
	for _, h := range raw {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		m[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return m
}

// --- exec subcommand ---

func runExec(args []string) int {
	fs := flag.NewFlagSet("exec", flag.ExitOnError)
	backend := fs.String("backend", "claude", "Agent type: claude or codex")
	fs.StringVar(backend, "b", "claude", "Agent type (shorthand)")
	executable := fs.String("executable", "", "Path to agent binary (default: auto-detect)")
	cwd := fs.String("cwd", "", "Working directory")
	fs.StringVar(cwd, "C", "", "Working directory (shorthand)")
	model := fs.String("model", "", "Model name")
	fs.StringVar(model, "m", "", "Model name (shorthand)")
	maxTurns := fs.Int("max-turns", 0, "Max conversation turns")
	timeout := fs.Duration("timeout", 0, "Execution timeout (e.g., 5m)")

	fs.Parse(args)

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "error: prompt is required as positional argument")
		fs.Usage()
		return 1
	}
	prompt := strings.Join(fs.Args(), " ")

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	execPath := *executable
	if execPath == "" {
		execPath = *backend
	}

	b, err := clawconnect.New(*backend, clawconnect.Config{
		ExecutablePath: execPath,
		Logger:         logger,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts := clawconnect.ExecOptions{
		Cwd:      *cwd,
		Model:    *model,
		MaxTurns: *maxTurns,
		Timeout:  *timeout,
	}

	session, err := b.Execute(ctx, prompt, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	// Stream messages to stderr.
	for msg := range session.Messages {
		switch msg.Type {
		case clawconnect.MessageText:
			fmt.Fprint(os.Stderr, msg.Content)
		case clawconnect.MessageToolUse:
			fmt.Fprintf(os.Stderr, "\n[tool] %s\n", msg.Tool)
		case clawconnect.MessageToolResult:
			fmt.Fprintf(os.Stderr, "[result] %s\n", truncate(msg.Output, 200))
		case clawconnect.MessageError:
			fmt.Fprintf(os.Stderr, "[error] %s\n", msg.Content)
		}
	}

	result := <-session.Result

	// Print final output to stdout.
	if result.Output != "" {
		fmt.Println(result.Output)
	}

	if result.Status != "completed" {
		fmt.Fprintf(os.Stderr, "\nstatus: %s", result.Status)
		if result.Error != "" {
			fmt.Fprintf(os.Stderr, " (%s)", result.Error)
		}
		fmt.Fprintln(os.Stderr)
		return 1
	}

	logger.Info("done", "status", result.Status, "duration_ms", result.DurationMs)
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// --- version subcommand ---

func runVersion(args []string) int {
	fs := flag.NewFlagSet("version", flag.ExitOnError)
	executable := fs.String("executable", "", "Path to agent binary to detect version")
	fs.Parse(args)

	fmt.Printf("claw-connect %s\n", version)

	if *executable != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		v, err := clawconnect.DetectVersion(ctx, *executable)
		if err != nil {
			fmt.Fprintf(os.Stderr, "agent version: error: %v\n", err)
		} else {
			fmt.Printf("agent: %s\n", v)
		}
	}

	return 0
}
