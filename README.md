# claw-connect

A lightweight Go library for driving AI coding agents (Claude Code, Codex) via subprocess + stdio. Zero third-party dependencies.

> **Note:** This project was extracted from [github.com/multica-ai/multica](https://github.com/multica-ai/multica/) as a standalone library.

## Installation

```bash
go get github.com/alswl/claw-connect
```

Requires Go 1.21+.

## Quick Start

### Claude Code

```go
package main

import (
    "context"
    "fmt"
    "log"

    clawconnect "github.com/alswl/claw-connect"
)

func main() {
    backend, err := clawconnect.New("claude", clawconnect.Config{
        ExecutablePath: "claude", // or absolute path
    })
    if err != nil {
        log.Fatal(err)
    }

    session, err := backend.Execute(context.Background(), "Hello, what can you do?", clawconnect.ExecOptions{
        Cwd:      "/path/to/project",
        MaxTurns: 3,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Stream messages (optional)
    go func() {
        for msg := range session.Messages {
            fmt.Printf("[%s] %s\n", msg.Type, msg.Content)
        }
    }()

    // Wait for final result
    result := <-session.Result
    fmt.Printf("Status: %s\nOutput: %s\n", result.Status, result.Output)
}
```

### Codex

```go
backend, err := clawconnect.New("codex", clawconnect.Config{
    ExecutablePath: "codex",
})
```

The API is identical for both agents. Only the agent type string and executable path differ.

### Version Detection

```go
version, err := clawconnect.DetectVersion(ctx, "/usr/local/bin/claude")
// version = "1.0.12"
```

## API Reference

### Core Functions

```go
// Create a backend for "claude" or "codex"
func New(agentType string, cfg Config) (Backend, error)

// Detect CLI version
func DetectVersion(ctx context.Context, executablePath string) (string, error)
```

### Backend Interface

```go
type Backend interface {
    Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error)
}
```

### Session

```go
type Session struct {
    Messages <-chan Message  // Real-time event stream (closes before Result)
    Result   <-chan Result   // Final outcome (exactly one value)
}
```

### Message Types

| Type | Description |
|------|-------------|
| `MessageText` | Text output from the agent |
| `MessageThinking` | Agent thinking/reasoning |
| `MessageToolUse` | Tool invocation started |
| `MessageToolResult` | Tool execution result |
| `MessageStatus` | Status update |
| `MessageError` | Error message |
| `MessageLog` | Log entry |

### Configuration

```go
type Config struct {
    ExecutablePath string            // Path to CLI binary
    Env            map[string]string // Extra environment variables
    Logger         *slog.Logger      // Structured logger (defaults to slog.Default())
}

type ExecOptions struct {
    Cwd             string        // Working directory
    Model           string        // AI model to use
    SystemPrompt    string        // Additional system instructions
    MaxTurns        int           // Max conversation turns
    Timeout         time.Duration // Execution timeout (default: 20min)
    ResumeSessionID string        // Resume a previous session (Claude only)
}
```

### Result

```go
type Result struct {
    Status     string // "completed", "failed", "aborted", "timeout"
    Output     string // accumulated text output
    Error      string // error message if failed
    DurationMs int64
    SessionID  string
}
```

## Supported Agents

| Agent | Protocol | CLI Command |
|-------|----------|-------------|
| Claude Code | stream-json over stdout | `claude --output-format stream-json -p "..."` |
| Codex | JSON-RPC 2.0 over stdio | `codex app-server --listen stdio://` |

Both agents auto-approve tool execution requests (designed for autonomous/daemon use).

## Examples

### Streaming Output with Tool Events

```go
session, _ := backend.Execute(ctx, "Refactor the auth module", opts)

for msg := range session.Messages {
    switch msg.Type {
    case clawconnect.MessageText:
        fmt.Println(msg.Content)
    case clawconnect.MessageToolUse:
        fmt.Printf("Tool: %s\n", msg.Tool)
    case clawconnect.MessageToolResult:
        fmt.Printf("Result: %s\n", msg.Output)
    }
}

result := <-session.Result
fmt.Printf("Done: %s\n", result.Status)
```

### With Timeout and Custom Model

```go
session, err := backend.Execute(ctx, "Write tests for main.go", clawconnect.ExecOptions{
    Cwd:     "/my/project",
    Model:   "claude-sonnet-4-6",
    Timeout: 5 * time.Minute,
})
```

### Resume Previous Session (Claude Only)

```go
// First session
session1, _ := backend.Execute(ctx, "Create a server", opts)
result1 := <-session1.Result

// Resume later
session2, _ := backend.Execute(ctx, "Add authentication to it", clawconnect.ExecOptions{
    Cwd:             opts.Cwd,
    ResumeSessionID: result1.SessionID,
})
```

## License

Apache License 2.0. See [LICENSE](LICENSE).