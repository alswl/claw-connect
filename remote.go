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

// Wire protocol constants.
const (
	remoteTypeExecute = "execute"
	remoteTypeCancel  = "cancel"
	remoteTypeMessage = "message"
	remoteTypeResult  = "result"
	remoteTypeError   = "error"
)

// remoteEnvelope is the top-level wire format for all WebSocket messages.
type remoteEnvelope struct {
	Type    string           `json:"type"`
	ID      string           `json:"id,omitempty"`
	Prompt  string           `json:"prompt,omitempty"`
	Options *wireExecOptions `json:"options,omitempty"`
	Message *Message         `json:"message,omitempty"`
	Result  *Result          `json:"result,omitempty"`
	Error   string           `json:"error,omitempty"`
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

		var env remoteEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			c.logger.Warn("malformed message from server", "error", err)
			continue
		}

		switch env.Type {
		case remoteTypeExecute:
			if env.ID == "" || env.Prompt == "" {
				c.logger.Warn("execute message missing id or prompt")
				continue
			}
			opts := env.Options.toExecOptions()
			go c.handleExecute(sessionCtx, env.ID, env.Prompt, opts)

		case remoteTypeCancel:
			if env.ID == "" {
				c.logger.Warn("cancel message missing id")
				continue
			}
			c.handleCancel(env.ID)

		default:
			c.logger.Warn("unknown message type from server", "type", env.Type)
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
		c.logger.Warn("max concurrent executions reached", "id", id)
		c.sendError(id, "max concurrent executions reached")
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

	c.logger.Info("executing prompt", "id", id, "prompt_len", len(prompt))

	session, err := c.cfg.Backend.Execute(execCtx, prompt, opts)
	if err != nil {
		c.logger.Error("backend execute failed", "id", id, "error", err)
		c.sendError(id, err.Error())
		return
	}

	// Stream messages.
	for msg := range session.Messages {
		env := remoteEnvelope{
			Type:    remoteTypeMessage,
			ID:      id,
			Message: &msg,
		}
		if err := c.writeJSON(env); err != nil {
			c.logger.Warn("write message failed", "id", id, "error", err)
			// Continue — don't abort the execution due to write failure.
		}
	}

	// Send final result.
	result := <-session.Result
	env := remoteEnvelope{
		Type:   remoteTypeResult,
		ID:     id,
		Result: &result,
	}
	if err := c.writeJSON(env); err != nil {
		c.logger.Warn("write result failed", "id", id, "error", err)
	}

	c.logger.Info("execution completed", "id", id, "status", result.Status)
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

// sendError sends an error envelope to the server.
func (c *RemoteClient) sendError(id, errMsg string) {
	env := remoteEnvelope{
		Type:  remoteTypeError,
		ID:    id,
		Error: errMsg,
	}
	if err := c.writeJSON(env); err != nil {
		c.logger.Warn("write error envelope failed", "id", id, "error", err)
	}
}
