package clawconnect

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BackoffConfig controls exponential backoff for reconnection.
type BackoffConfig struct {
	InitialDelay time.Duration // default 1s
	MaxDelay     time.Duration // default 60s
	Factor       float64       // default 2.0
}

func (b *BackoffConfig) withDefaults() BackoffConfig {
	out := *b
	if out.InitialDelay <= 0 {
		out.InitialDelay = time.Second
	}
	if out.MaxDelay <= 0 {
		out.MaxDelay = 60 * time.Second
	}
	if out.Factor <= 0 {
		out.Factor = 2.0
	}
	return out
}

// RemoteConfig configures a RemoteClient.
type RemoteConfig struct {
	// ServerURL is the WebSocket endpoint to connect to (e.g., "wss://example.com/ws").
	ServerURL string

	// Backend is the agent backend used to execute incoming prompts.
	Backend Backend

	// Logger for structured logging. Defaults to slog.Default() if nil.
	Logger *slog.Logger

	// Headers provides additional HTTP headers for the WebSocket handshake
	// (e.g., authentication tokens).
	Headers map[string]string

	// MaxConcurrent limits the number of simultaneous executions.
	// 0 means unlimited.
	MaxConcurrent int

	// ReconnectBackoff configures the reconnection strategy.
	// If nil, defaults are used (initial=1s, max=60s, factor=2.0).
	ReconnectBackoff *BackoffConfig

	// PingInterval is how often to send WebSocket ping frames.
	// Default: 30s.
	PingInterval time.Duration

	// PongTimeout is how long to wait for a pong after sending a ping.
	// Default: 10s.
	PongTimeout time.Duration

	// WriteTimeout is the deadline for each WebSocket write operation.
	// Default: 10s.
	WriteTimeout time.Duration
}

// RemoteClient connects to a WebSocket server and executes prompts
// received from the server using the configured Backend.
type RemoteClient struct {
	cfg    RemoteConfig
	logger *slog.Logger

	mu       sync.Mutex
	conn     *websocket.Conn
	inflight map[string]context.CancelFunc

	inflightWg sync.WaitGroup
}

// Wire protocol event types, following the multica "namespace:action" convention.
// See: github.com/multica-ai/multica server/pkg/protocol/events.go
const (
	EventTaskDispatch  = "task:dispatch"
	EventTaskMessage   = "task:message"
	EventTaskCompleted = "task:completed"
	EventTaskFailed    = "task:failed"
	EventTaskCancelled = "task:cancelled"
	EventTaskProgress  = "task:progress"
	EventDaemonRegister  = "daemon:register"
	EventDaemonHeartbeat = "daemon:heartbeat"
)

// wireMessage is the top-level envelope for all WebSocket messages.
// Compatible with multica protocol: {"type": "...", "payload": {...}}.
type wireMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// TaskDispatchPayload is sent from server to client to request prompt execution.
type TaskDispatchPayload struct {
	TaskID      string           `json:"task_id"`
	Prompt      string           `json:"prompt,omitempty"`
	Title       string           `json:"title,omitempty"`
	Description string           `json:"description,omitempty"`
	Options     *wireExecOptions `json:"options,omitempty"`
}

// TaskMessagePayload is sent from client to server for each agent event during execution.
type TaskMessagePayload struct {
	TaskID  string         `json:"task_id"`
	Seq     int            `json:"seq"`
	Type    string         `json:"type"`              // "text", "thinking", "tool_use", "tool_result", "status", "error", "log"
	Tool    string         `json:"tool,omitempty"`
	Content string         `json:"content,omitempty"`
	Input   map[string]any `json:"input,omitempty"`
	Output  string         `json:"output,omitempty"`
}

// TaskCompletedPayload is sent from client to server when execution finishes.
type TaskCompletedPayload struct {
	TaskID     string `json:"task_id"`
	Status     string `json:"status"`
	Output     string `json:"output,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
}

// TaskFailedPayload is sent from client to server when execution cannot start or fails.
type TaskFailedPayload struct {
	TaskID string `json:"task_id"`
	Error  string `json:"error"`
}

// TaskCancelledPayload is sent from server to client to cancel a running task.
type TaskCancelledPayload struct {
	TaskID string `json:"task_id"`
}

// TaskProgressPayload is sent from client to server during execution.
type TaskProgressPayload struct {
	TaskID  string `json:"task_id"`
	Summary string `json:"summary"`
	Step    int    `json:"step,omitempty"`
	Total   int    `json:"total,omitempty"`
}

// wireExecOptions is the JSON-friendly representation of ExecOptions
// used in the WebSocket protocol. Timeout is expressed as milliseconds.
type wireExecOptions struct {
	Cwd             string `json:"cwd,omitempty"`
	Model           string `json:"model,omitempty"`
	SystemPrompt    string `json:"system_prompt,omitempty"`
	MaxTurns        int    `json:"max_turns,omitempty"`
	TimeoutMs       int64  `json:"timeout_ms,omitempty"`
	ResumeSessionID string `json:"resume_session_id,omitempty"`
}

func (w *wireExecOptions) toExecOptions() ExecOptions {
	if w == nil {
		return ExecOptions{}
	}
	return ExecOptions{
		Cwd:             w.Cwd,
		Model:           w.Model,
		SystemPrompt:    w.SystemPrompt,
		MaxTurns:        w.MaxTurns,
		Timeout:         time.Duration(w.TimeoutMs) * time.Millisecond,
		ResumeSessionID: w.ResumeSessionID,
	}
}

// marshalEnvelope creates a wireMessage with the given type and payload.
func marshalEnvelope(eventType string, payload any) (wireMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return wireMessage{}, err
	}
	return wireMessage{Type: eventType, Payload: data}, nil
}

// messageTypeToWire converts a MessageType to wire format string.
func messageTypeToWire(mt MessageType) string {
	switch mt {
	case MessageToolUse:
		return "tool_use"
	case MessageToolResult:
		return "tool_result"
	default:
		return string(mt)
	}
}

// NewRemoteClient creates a new RemoteClient. Call Run() to start it.
func NewRemoteClient(cfg RemoteConfig) (*RemoteClient, error) {
	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("RemoteConfig.ServerURL is required")
	}
	if cfg.Backend == nil {
		return nil, fmt.Errorf("RemoteConfig.Backend is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.ReconnectBackoff == nil {
		cfg.ReconnectBackoff = &BackoffConfig{}
	}
	*cfg.ReconnectBackoff = cfg.ReconnectBackoff.withDefaults()

	return &RemoteClient{
		cfg:      cfg,
		logger:   cfg.Logger,
		inflight: make(map[string]context.CancelFunc),
	}, nil
}

// Run connects to the server and processes commands until the context is
// cancelled or an unrecoverable error occurs. It automatically reconnects
// on transient failures with exponential backoff.
//
// Run blocks until shutdown is complete. All in-flight executions are
// cancelled and drained before Run returns.
func (c *RemoteClient) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, err := c.connectWithBackoff(ctx)
		if err != nil {
			// Only returns error when ctx is done.
			return err
		}

		c.mu.Lock()
		c.conn = conn
		c.mu.Unlock()

		c.logger.Info("connected to remote server", "url", c.cfg.ServerURL)

		// Run the session (read loop + ping). Returns on disconnect.
		c.runSession(ctx, conn)

		// Cancel all in-flight executions from this session.
		c.cancelAllInflight()

		// Wait for in-flight goroutines to finish.
		c.inflightWg.Wait()

		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()

		_ = conn.Close()

		c.logger.Info("disconnected, will reconnect")
	}
}

// connectWithBackoff dials the WebSocket server with exponential backoff.
// Returns error only when ctx is done.
func (c *RemoteClient) connectWithBackoff(ctx context.Context) (*websocket.Conn, error) {
	backoff := c.cfg.ReconnectBackoff
	delay := backoff.InitialDelay

	header := http.Header{}
	for k, v := range c.cfg.Headers {
		header.Set(k, v)
	}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		dialer := websocket.DefaultDialer
		conn, _, err := dialer.Dial(c.cfg.ServerURL, header)
		if err == nil {
			return conn, nil
		}

		c.logger.Warn("dial failed, retrying", "error", err, "delay", delay)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		delay = time.Duration(float64(delay) * backoff.Factor)
		if delay > backoff.MaxDelay {
			delay = backoff.MaxDelay
		}
	}
}

// runSession handles a single WebSocket connection session.
// It runs the read loop and ping ticker until disconnection.
func (c *RemoteClient) runSession(ctx context.Context, conn *websocket.Conn) {
	// Create a session-scoped context so we can cancel everything when
	// this session ends (either from disconnect or parent ctx cancel).
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	// Set up pong handler to extend the read deadline.
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(c.cfg.PingInterval + c.cfg.PongTimeout))
	})
	// Set initial read deadline.
	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.PingInterval + c.cfg.PongTimeout))

	// Start ping ticker.
	go c.pingLoop(sessionCtx, conn)

	// Watch for parent context cancellation to unblock the read loop.
	go func() {
		<-sessionCtx.Done()
		conn.Close()
	}()

	// Read loop.
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
			) {
				c.logger.Warn("read error", "error", err)
			}
			return
		}

		var msg wireMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.logger.Warn("malformed message from server", "error", err)
			continue
		}

		switch msg.Type {
		case EventTaskDispatch:
			var p TaskDispatchPayload
			if err := json.Unmarshal(msg.Payload, &p); err != nil {
				c.logger.Warn("invalid task:dispatch payload", "error", err)
				continue
			}
			if p.TaskID == "" {
				c.logger.Warn("task:dispatch missing task_id")
				continue
			}
			// Build prompt: prefer explicit Prompt, fall back to Title + Description.
			prompt := p.Prompt
			if prompt == "" {
				prompt = p.Title
				if p.Description != "" {
					prompt = prompt + "\n\n" + p.Description
				}
			}
			if prompt == "" {
				c.logger.Warn("task:dispatch has no prompt/title/description", "task_id", p.TaskID)
				continue
			}
			opts := p.Options.toExecOptions()
			go c.handleExecute(sessionCtx, p.TaskID, prompt, opts)

		case EventTaskCancelled:
			var p TaskCancelledPayload
			if err := json.Unmarshal(msg.Payload, &p); err != nil {
				c.logger.Warn("invalid task:cancelled payload", "error", err)
				continue
			}
			if p.TaskID == "" {
				c.logger.Warn("task:cancelled missing task_id")
				continue
			}
			c.handleCancel(p.TaskID)

		default:
			c.logger.Warn("unknown message type from server", "type", msg.Type)
		}
	}
}

// pingLoop sends periodic ping frames until the session context is done.
func (c *RemoteClient) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			err := conn.WriteControl(
				websocket.PingMessage,
				nil,
				time.Now().Add(c.cfg.WriteTimeout),
			)
			c.mu.Unlock()
			if err != nil {
				c.logger.Warn("ping failed", "error", err)
				return
			}
		}
	}
}

// handleExecute processes a single execute command from the server.
func (c *RemoteClient) handleExecute(sessionCtx context.Context, id, prompt string, opts ExecOptions) {
	c.inflightWg.Add(1)
	defer c.inflightWg.Done()

	// Check concurrency limit.
	c.mu.Lock()
	if c.cfg.MaxConcurrent > 0 && len(c.inflight) >= c.cfg.MaxConcurrent {
		c.mu.Unlock()
		c.logger.Warn("max concurrent executions reached", "task_id", id)
		c.sendTaskFailed(id, "max concurrent executions reached")
		return
	}

	execCtx, cancel := context.WithCancel(sessionCtx)
	c.inflight[id] = cancel
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inflight, id)
		c.mu.Unlock()
		cancel()
	}()

	c.logger.Info("executing prompt", "task_id", id, "prompt_len", len(prompt))

	session, err := c.cfg.Backend.Execute(execCtx, prompt, opts)
	if err != nil {
		c.logger.Error("backend execute failed", "task_id", id, "error", err)
		c.sendTaskFailed(id, err.Error())
		return
	}

	// Stream messages as task:message events.
	seq := 0
	for msg := range session.Messages {
		seq++
		payload := TaskMessagePayload{
			TaskID:  id,
			Seq:     seq,
			Type:    messageTypeToWire(msg.Type),
			Tool:    msg.Tool,
			Content: msg.Content,
			Input:   msg.Input,
			Output:  msg.Output,
		}
		env, err := marshalEnvelope(EventTaskMessage, payload)
		if err != nil {
			c.logger.Warn("marshal task:message failed", "task_id", id, "error", err)
			continue
		}
		if err := c.writeJSON(env); err != nil {
			c.logger.Warn("write task:message failed", "task_id", id, "error", err)
		}
	}

	// Send final result as task:completed or task:failed.
	result := <-session.Result

	if result.Status == "failed" || result.Status == "timeout" {
		c.sendTaskFailed(id, result.Error)
	} else {
		payload := TaskCompletedPayload{
			TaskID:     id,
			Status:     result.Status,
			Output:     result.Output,
			DurationMs: result.DurationMs,
			SessionID:  result.SessionID,
		}
		env, err := marshalEnvelope(EventTaskCompleted, payload)
		if err != nil {
			c.logger.Warn("marshal task:completed failed", "task_id", id, "error", err)
		} else if err := c.writeJSON(env); err != nil {
			c.logger.Warn("write task:completed failed", "task_id", id, "error", err)
		}
	}

	c.logger.Info("execution completed", "task_id", id, "status", result.Status)
}

// handleCancel cancels an in-flight execution.
func (c *RemoteClient) handleCancel(id string) {
	c.mu.Lock()
	cancel, ok := c.inflight[id]
	c.mu.Unlock()

	if ok {
		c.logger.Info("cancelling execution", "id", id)
		cancel()
	} else {
		c.logger.Warn("cancel requested for unknown id", "id", id)
	}
}

// cancelAllInflight cancels all in-flight executions.
func (c *RemoteClient) cancelAllInflight() {
	c.mu.Lock()
	for id, cancel := range c.inflight {
		c.logger.Info("cancelling in-flight execution", "id", id)
		cancel()
	}
	c.mu.Unlock()
}

// writeJSON sends a JSON message over the WebSocket connection.
// It is safe for concurrent use.
func (c *RemoteClient) writeJSON(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
	return c.conn.WriteJSON(v)
}

// sendTaskFailed sends a task:failed envelope to the server.
func (c *RemoteClient) sendTaskFailed(taskID, errMsg string) {
	payload := TaskFailedPayload{
		TaskID: taskID,
		Error:  errMsg,
	}
	env, err := marshalEnvelope(EventTaskFailed, payload)
	if err != nil {
		c.logger.Warn("marshal task:failed failed", "task_id", taskID, "error", err)
		return
	}
	if err := c.writeJSON(env); err != nil {
		c.logger.Warn("write task:failed failed", "task_id", taskID, "error", err)
	}
}
