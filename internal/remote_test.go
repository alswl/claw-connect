package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clawconnect "github.com/alswl/claw-connect"
	"github.com/gorilla/websocket"
)

// --- helpers ---

// fakeBackend implements Backend for testing.
type fakeBackend struct {
	executeFn func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error)
}

func (f *fakeBackend) Execute(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
	return f.executeFn(ctx, prompt, opts)
}

// fakeSession creates a Session that emits the given messages then sends result.
func fakeSession(messages []clawconnect.Message, result clawconnect.Result) *clawconnect.Session {
	msgCh := make(chan clawconnect.Message, len(messages))
	resCh := make(chan clawconnect.Result, 1)
	for _, m := range messages {
		msgCh <- m
	}
	close(msgCh)
	resCh <- result
	close(resCh)
	return &clawconnect.Session{Messages: msgCh, Result: resCh}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

// wsURL converts an httptest.Server URL from http:// to ws://.
func wsURL(s *httptest.Server) string {
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

// sendWireMessage sends a wireMessage envelope to the WebSocket connection.
func sendWireMessage(conn *websocket.Conn, eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteJSON(wireMessage{Type: eventType, Payload: data})
}

// readWireMessage reads and parses a wireMessage from the WebSocket connection.
func readWireMessage(conn *websocket.Conn) (wireMessage, error) {
	var msg wireMessage
	err := conn.ReadJSON(&msg)
	return msg, err
}

// decodePayload unmarshals a wireMessage payload into the given target.
func decodePayload(msg wireMessage, target any) error {
	return json.Unmarshal(msg.Payload, target)
}

func defaultRemoteConfig(srv *httptest.Server, backend clawconnect.Backend) RemoteConfig {
	return RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
		PongTimeout:  5 * time.Second,
	}
}

// --- Unit Tests ---

func TestNewRemoteClientDefaults(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		return fakeSession(nil, clawconnect.Result{Status: "completed"}), nil
	}}

	c, err := NewRemoteClient(RemoteConfig{
		ServerURL: "ws://localhost:9999",
		Backend:   backend,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.cfg.PingInterval != 30*time.Second {
		t.Fatalf("expected PingInterval 30s, got %v", c.cfg.PingInterval)
	}
	if c.cfg.PongTimeout != 10*time.Second {
		t.Fatalf("expected PongTimeout 10s, got %v", c.cfg.PongTimeout)
	}
	if c.cfg.WriteTimeout != 10*time.Second {
		t.Fatalf("expected WriteTimeout 10s, got %v", c.cfg.WriteTimeout)
	}
	if c.cfg.ReconnectBackoff == nil {
		t.Fatal("expected non-nil ReconnectBackoff")
	}
	if c.cfg.ReconnectBackoff.InitialDelay != time.Second {
		t.Fatalf("expected InitialDelay 1s, got %v", c.cfg.ReconnectBackoff.InitialDelay)
	}
	if c.cfg.ReconnectBackoff.MaxDelay != 60*time.Second {
		t.Fatalf("expected MaxDelay 60s, got %v", c.cfg.ReconnectBackoff.MaxDelay)
	}
	if c.cfg.ReconnectBackoff.Factor != 2.0 {
		t.Fatalf("expected Factor 2.0, got %v", c.cfg.ReconnectBackoff.Factor)
	}
	if c.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewRemoteClientValidation(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{}

	_, err := NewRemoteClient(RemoteConfig{Backend: backend})
	if err == nil || !strings.Contains(err.Error(), "ServerURL") {
		t.Fatalf("expected ServerURL error, got: %v", err)
	}

	_, err = NewRemoteClient(RemoteConfig{ServerURL: "ws://localhost"})
	if err == nil || !strings.Contains(err.Error(), "Backend") {
		t.Fatalf("expected Backend error, got: %v", err)
	}
}

func TestWireExecOptionsConversion(t *testing.T) {
	t.Parallel()

	w := &wireExecOptions{
		Cwd:             "/tmp",
		Model:           "claude-sonnet-4-6",
		SystemPrompt:    "be nice",
		MaxTurns:        5,
		TimeoutMs:       30000,
		ResumeSessionID: "sess-1",
	}

	opts := w.toExecOptions()
	if opts.Cwd != "/tmp" {
		t.Fatalf("expected Cwd /tmp, got %q", opts.Cwd)
	}
	if opts.Model != "claude-sonnet-4-6" {
		t.Fatalf("expected Model claude-sonnet-4-6, got %q", opts.Model)
	}
	if opts.Timeout != 30*time.Second {
		t.Fatalf("expected Timeout 30s, got %v", opts.Timeout)
	}
	if opts.ResumeSessionID != "sess-1" {
		t.Fatalf("expected ResumeSessionID sess-1, got %q", opts.ResumeSessionID)
	}
}

func TestWireExecOptionsNilConversion(t *testing.T) {
	t.Parallel()

	var w *wireExecOptions
	opts := w.toExecOptions()
	if opts.Cwd != "" || opts.Model != "" {
		t.Fatalf("expected zero ExecOptions, got %+v", opts)
	}
}

func TestMarshalEnvelope(t *testing.T) {
	t.Parallel()

	payload := TaskDispatchPayload{
		TaskID: "t-1",
		Prompt: "hello",
		Options: &wireExecOptions{
			Cwd:       "/project",
			TimeoutMs: 5000,
		},
	}
	env, err := marshalEnvelope(EventTaskDispatch, payload)
	if err != nil {
		t.Fatalf("marshalEnvelope: %v", err)
	}
	if env.Type != EventTaskDispatch {
		t.Fatalf("expected type %q, got %q", EventTaskDispatch, env.Type)
	}

	var decoded TaskDispatchPayload
	if err := json.Unmarshal(env.Payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.TaskID != "t-1" || decoded.Prompt != "hello" {
		t.Fatalf("unexpected decoded: %+v", decoded)
	}
	if decoded.Options == nil || decoded.Options.Cwd != "/project" {
		t.Fatalf("unexpected options: %+v", decoded.Options)
	}
}

func TestMarshalEnvelopeTaskMessage(t *testing.T) {
	t.Parallel()

	payload := TaskMessagePayload{
		TaskID:  "t-1",
		Seq:     1,
		Type:    "text",
		Content: "hi",
	}
	env, err := marshalEnvelope(EventTaskMessage, payload)
	if err != nil {
		t.Fatalf("marshalEnvelope: %v", err)
	}
	if env.Type != EventTaskMessage {
		t.Fatalf("expected type %q, got %q", EventTaskMessage, env.Type)
	}

	var decoded TaskMessagePayload
	if err := json.Unmarshal(env.Payload, &decoded); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if decoded.Content != "hi" || decoded.Seq != 1 {
		t.Fatalf("unexpected decoded: %+v", decoded)
	}
}

func TestMessageTypeToWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in  clawconnect.MessageType
		out string
	}{
		{clawconnect.MessageText, "text"},
		{clawconnect.MessageThinking, "thinking"},
		{clawconnect.MessageToolUse, "tool_use"},
		{clawconnect.MessageToolResult, "tool_result"},
		{clawconnect.MessageStatus, "status"},
		{clawconnect.MessageError, "error"},
		{clawconnect.MessageLog, "log"},
	}
	for _, tt := range tests {
		if got := messageTypeToWire(tt.in); got != tt.out {
			t.Errorf("messageTypeToWire(%q) = %q, want %q", tt.in, got, tt.out)
		}
	}
}

func TestBackoffConfigDefaults(t *testing.T) {
	t.Parallel()

	b := (&BackoffConfig{}).withDefaults()
	if b.InitialDelay != time.Second {
		t.Fatalf("expected 1s, got %v", b.InitialDelay)
	}
	if b.MaxDelay != 60*time.Second {
		t.Fatalf("expected 60s, got %v", b.MaxDelay)
	}
	if b.Factor != 2.0 {
		t.Fatalf("expected 2.0, got %v", b.Factor)
	}
}

func TestBackoffConfigPreservesCustom(t *testing.T) {
	t.Parallel()

	b := (&BackoffConfig{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Factor:       1.5,
	}).withDefaults()

	if b.InitialDelay != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", b.InitialDelay)
	}
	if b.MaxDelay != 5*time.Second {
		t.Fatalf("expected 5s, got %v", b.MaxDelay)
	}
	if b.Factor != 1.5 {
		t.Fatalf("expected 1.5, got %v", b.Factor)
	}
}

// --- Integration Tests ---

func TestRemoteClientExecuteEndToEnd(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		return fakeSession(
			[]clawconnect.Message{
				{Type: clawconnect.MessageText, Content: "working on it"},
				{Type: clawconnect.MessageToolUse, Tool: "Read", CallID: "c1"},
			},
			clawconnect.Result{Status: "completed", Output: "done"},
		), nil
	}}

	var received []wireMessage
	var mu sync.Mutex
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()

		// Send task:dispatch.
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-1",
			Prompt: "do something",
			Options: &wireExecOptions{
				Cwd: "/tmp",
			},
		})

		// Read all responses until we get task:completed.
		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			mu.Lock()
			received = append(received, msg)
			mu.Unlock()
			if msg.Type == EventTaskCompleted {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, err := NewRemoteClient(defaultRemoteConfig(srv, backend))
	if err != nil {
		t.Fatalf("NewRemoteClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for result")
	}

	cancel()

	mu.Lock()
	defer mu.Unlock()

	if len(received) < 3 {
		t.Fatalf("expected at least 3 envelopes (2 messages + 1 completed), got %d", len(received))
	}

	// First two should be task:message.
	if received[0].Type != EventTaskMessage {
		t.Fatalf("expected task:message, got %q", received[0].Type)
	}
	var msg1 TaskMessagePayload
	decodePayload(received[0], &msg1)
	if msg1.Content != "working on it" || msg1.Type != "text" || msg1.Seq != 1 {
		t.Fatalf("unexpected first message: %+v", msg1)
	}

	if received[1].Type != EventTaskMessage {
		t.Fatalf("expected task:message, got %q", received[1].Type)
	}
	var msg2 TaskMessagePayload
	decodePayload(received[1], &msg2)
	if msg2.Tool != "Read" || msg2.Type != "tool_use" || msg2.Seq != 2 {
		t.Fatalf("unexpected second message: %+v", msg2)
	}

	// Last should be task:completed.
	last := received[len(received)-1]
	if last.Type != EventTaskCompleted {
		t.Fatalf("expected task:completed, got %q", last.Type)
	}
	var result TaskCompletedPayload
	decodePayload(last, &result)
	if result.Status != "completed" || result.Output != "done" || result.TaskID != "t-1" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRemoteClientCancel(t *testing.T) {
	t.Parallel()

	executeCalled := make(chan struct{})

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		msgCh := make(chan clawconnect.Message, 256)
		resCh := make(chan clawconnect.Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)
			close(executeCalled)
			<-ctx.Done()
			resCh <- clawconnect.Result{Status: "aborted"}
		}()

		return &clawconnect.Session{Messages: msgCh, Result: resCh}, nil
	}}

	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send task:dispatch.
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-cancel",
			Prompt: "long task",
		})

		// Wait for execution to start.
		<-executeCalled

		// Send task:cancelled.
		sendWireMessage(conn, EventTaskCancelled, TaskCancelledPayload{
			TaskID: "t-cancel",
		})

		// Read the result.
		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskCompleted {
				var p TaskCompletedPayload
				decodePayload(msg, &p)
				if p.Status == "aborted" {
					close(done)
					return
				}
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for aborted result")
	}
}

func TestRemoteClientReconnect(t *testing.T) {
	t.Parallel()

	var connectCount atomic.Int32

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		return fakeSession(nil, clawconnect.Result{Status: "completed", Output: prompt}), nil
	}}

	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		n := connectCount.Add(1)

		if n == 1 {
			// First connection: close immediately to trigger reconnect.
			conn.Close()
			return
		}

		// Second connection: send a command and verify it works.
		defer conn.Close()
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-reconnect",
			Prompt: "after reconnect",
		})

		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskCompleted {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect result")
	}

	if connectCount.Load() < 2 {
		t.Fatalf("expected at least 2 connections, got %d", connectCount.Load())
	}
}

func TestRemoteClientConcurrentExecutions(t *testing.T) {
	t.Parallel()

	var execCount atomic.Int32

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		execCount.Add(1)
		time.Sleep(50 * time.Millisecond)
		return fakeSession(nil, clawconnect.Result{Status: "completed", Output: prompt}), nil
	}}

	var results sync.Map
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send 3 task:dispatch commands.
		for i := 0; i < 3; i++ {
			id := "t-" + string(rune('a'+i))
			sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
				TaskID: id,
				Prompt: "task-" + string(rune('a'+i)),
			})
		}

		// Read all results.
		resultCount := 0
		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskCompleted {
				var p TaskCompletedPayload
				decodePayload(msg, &p)
				results.Store(p.TaskID, p.Output)
				resultCount++
				if resultCount == 3 {
					close(done)
					return
				}
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for concurrent results")
	}

	if execCount.Load() != 3 {
		t.Fatalf("expected 3 executions, got %d", execCount.Load())
	}
}

func TestRemoteClientMaxConcurrent(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	block := make(chan struct{})

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		msgCh := make(chan clawconnect.Message)
		resCh := make(chan clawconnect.Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)

			started <- struct{}{}

			select {
			case <-block:
			case <-ctx.Done():
			}
			resCh <- clawconnect.Result{Status: "completed"}
		}()

		return &clawconnect.Session{Messages: msgCh, Result: resCh}, nil
	}}

	gotError := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send first task (will block).
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-1", Prompt: "task1",
		})

		// Wait for it to start executing.
		<-started

		// Send second task (should be rejected).
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-2", Prompt: "task2",
		})

		// Read the task:failed error.
		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskFailed {
				var p TaskFailedPayload
				decodePayload(msg, &p)
				if p.TaskID == "t-2" {
					gotError <- p.Error
					close(block)
					return
				}
			}
		}
	}))
	defer srv.Close()

	cfg := defaultRemoteConfig(srv, backend)
	cfg.MaxConcurrent = 1
	c, _ := NewRemoteClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case errMsg := <-gotError:
		if !strings.Contains(errMsg, "max concurrent") {
			t.Fatalf("unexpected error: %q", errMsg)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for max concurrent error")
	}
}

func TestRemoteClientGracefulShutdown(t *testing.T) {
	t.Parallel()

	executeCalled := make(chan struct{})

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		msgCh := make(chan clawconnect.Message, 256)
		resCh := make(chan clawconnect.Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)
			close(executeCalled)
			<-ctx.Done()
			resCh <- clawconnect.Result{Status: "aborted"}
		}()

		return &clawconnect.Session{Messages: msgCh, Result: resCh}, nil
	}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-shutdown", Prompt: "long task",
		})

		// Keep reading until connection closes.
		for {
			if _, err := readWireMessage(conn); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx)
	}()

	<-executeCalled
	cancel()

	select {
	case err := <-runDone:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRemoteClientMalformedMessage(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		return fakeSession(nil, clawconnect.Result{Status: "completed", Output: "ok"}), nil
	}}

	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send malformed JSON.
		conn.WriteMessage(websocket.TextMessage, []byte(`{not json}`))

		// Send unknown type.
		conn.WriteJSON(wireMessage{Type: "unknown_type", Payload: json.RawMessage(`{}`)})

		// Send valid command to verify client still works.
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID: "t-after-bad", Prompt: "hello",
		})

		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskCompleted {
				var p TaskCompletedPayload
				decodePayload(msg, &p)
				if p.TaskID == "t-after-bad" {
					close(done)
					return
				}
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out — client did not survive malformed messages")
	}
}

func TestRemoteClientHeaders(t *testing.T) {
	t.Parallel()

	gotHeader := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader <- r.Header.Get("Authorization")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.Close()
	}))
	defer srv.Close()

	backend := &fakeBackend{}

	cfg := defaultRemoteConfig(srv, backend)
	cfg.Headers = map[string]string{"Authorization": "Bearer token-123"}
	c, _ := NewRemoteClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case h := <-gotHeader:
		if h != "Bearer token-123" {
			t.Fatalf("expected 'Bearer token-123', got %q", h)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for header")
	}
}

func TestRemoteClientTitleDescriptionFallback(t *testing.T) {
	t.Parallel()

	var gotPrompt string

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts clawconnect.ExecOptions) (*clawconnect.Session, error) {
		gotPrompt = prompt
		return fakeSession(nil, clawconnect.Result{Status: "completed"}), nil
	}}

	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send task:dispatch with title+description (no prompt).
		sendWireMessage(conn, EventTaskDispatch, TaskDispatchPayload{
			TaskID:      "t-title",
			Title:       "Fix the bug",
			Description: "There is a null pointer in main.go",
		})

		for {
			msg, err := readWireMessage(conn)
			if err != nil {
				break
			}
			if msg.Type == EventTaskCompleted {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(defaultRemoteConfig(srv, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go c.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out")
	}

	expected := "Fix the bug\n\nThere is a null pointer in main.go"
	if gotPrompt != expected {
		t.Fatalf("expected prompt %q, got %q", expected, gotPrompt)
	}
}
