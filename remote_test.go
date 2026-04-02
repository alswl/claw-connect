package clawconnect

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

	"github.com/gorilla/websocket"
)

// --- helpers ---

// fakeBackend implements Backend for testing.
type fakeBackend struct {
	executeFn func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error)
}

func (f *fakeBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	return f.executeFn(ctx, prompt, opts)
}

// fakeSession creates a Session that emits the given messages then sends result.
func fakeSession(messages []Message, result Result) *Session {
	msgCh := make(chan Message, len(messages))
	resCh := make(chan Result, 1)
	for _, m := range messages {
		msgCh <- m
	}
	close(msgCh)
	resCh <- result
	close(resCh)
	return &Session{Messages: msgCh, Result: resCh}
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

// wsURL converts an httptest.Server URL from http:// to ws://.
func wsURL(s *httptest.Server) string {
	return "ws" + strings.TrimPrefix(s.URL, "http")
}

// --- Unit Tests ---

func TestNewRemoteClientDefaults(t *testing.T) {
	t.Parallel()

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		return fakeSession(nil, Result{Status: "completed"}), nil
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

func TestRemoteEnvelopeMarshalExecute(t *testing.T) {
	t.Parallel()

	env := remoteEnvelope{
		Type:   remoteTypeExecute,
		ID:     "req-1",
		Prompt: "hello",
		Options: &wireExecOptions{
			Cwd:       "/project",
			TimeoutMs: 5000,
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded remoteEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Type != remoteTypeExecute || decoded.ID != "req-1" || decoded.Prompt != "hello" {
		t.Fatalf("unexpected decoded: %+v", decoded)
	}
	if decoded.Options == nil || decoded.Options.Cwd != "/project" {
		t.Fatalf("unexpected options: %+v", decoded.Options)
	}
}

func TestRemoteEnvelopeMarshalMessage(t *testing.T) {
	t.Parallel()

	msg := Message{Type: MessageText, Content: "hi"}
	env := remoteEnvelope{
		Type:    remoteTypeMessage,
		ID:      "req-1",
		Message: &msg,
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded remoteEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Message == nil || decoded.Message.Content != "hi" {
		t.Fatalf("unexpected message: %+v", decoded.Message)
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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		return fakeSession(
			[]Message{
				{Type: MessageText, Content: "working on it"},
				{Type: MessageToolUse, Tool: "Read", CallID: "c1"},
			},
			Result{Status: "completed", Output: "done"},
		), nil
	}}

	// Start a test WebSocket server.
	var received []remoteEnvelope
	var mu sync.Mutex
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()

		// Send an execute command.
		cmd := remoteEnvelope{
			Type:   remoteTypeExecute,
			ID:     "req-1",
			Prompt: "do something",
			Options: &wireExecOptions{
				Cwd: "/tmp",
			},
		}
		if err := conn.WriteJSON(cmd); err != nil {
			t.Errorf("write: %v", err)
			return
		}

		// Read all responses until we get the result.
		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			mu.Lock()
			received = append(received, env)
			mu.Unlock()
			if env.Type == remoteTypeResult {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, err := NewRemoteClient(RemoteConfig{
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
	})
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
		t.Fatalf("expected at least 3 envelopes (2 messages + 1 result), got %d", len(received))
	}

	// First two should be message type.
	if received[0].Type != remoteTypeMessage || received[0].Message.Content != "working on it" {
		t.Fatalf("unexpected first message: %+v", received[0])
	}
	if received[1].Type != remoteTypeMessage || received[1].Message.Tool != "Read" {
		t.Fatalf("unexpected second message: %+v", received[1])
	}
	// Last should be result.
	last := received[len(received)-1]
	if last.Type != remoteTypeResult || last.Result.Status != "completed" || last.Result.Output != "done" {
		t.Fatalf("unexpected result: %+v", last)
	}
}

func TestRemoteClientCancel(t *testing.T) {
	t.Parallel()

	executeCalled := make(chan struct{})

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		msgCh := make(chan Message, 256)
		resCh := make(chan Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)
			close(executeCalled)
			// Wait for context cancellation.
			<-ctx.Done()
			resCh <- Result{Status: "aborted"}
		}()

		return &Session{Messages: msgCh, Result: resCh}, nil
	}}

	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send execute.
		conn.WriteJSON(remoteEnvelope{
			Type:   remoteTypeExecute,
			ID:     "req-cancel",
			Prompt: "long task",
		})

		// Wait for execution to start.
		<-executeCalled

		// Send cancel.
		conn.WriteJSON(remoteEnvelope{
			Type: remoteTypeCancel,
			ID:   "req-cancel",
		})

		// Read the result.
		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			if env.Type == remoteTypeResult && env.Result.Status == "aborted" {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		return fakeSession(nil, Result{Status: "completed", Output: prompt}), nil
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
		conn.WriteJSON(remoteEnvelope{
			Type:   remoteTypeExecute,
			ID:     "req-reconnect",
			Prompt: "after reconnect",
		})

		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			if env.Type == remoteTypeResult {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		execCount.Add(1)
		// Simulate some work.
		time.Sleep(50 * time.Millisecond)
		return fakeSession(nil, Result{Status: "completed", Output: prompt}), nil
	}}

	var results sync.Map
	done := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send 3 execute commands concurrently.
		for i := 0; i < 3; i++ {
			conn.WriteJSON(remoteEnvelope{
				Type:   remoteTypeExecute,
				ID:     "req-" + string(rune('a'+i)),
				Prompt: "task-" + string(rune('a'+i)),
			})
		}

		// Read all results.
		resultCount := 0
		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			if env.Type == remoteTypeResult {
				results.Store(env.ID, env.Result.Output)
				resultCount++
				if resultCount == 3 {
					close(done)
					return
				}
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		msgCh := make(chan Message)
		resCh := make(chan Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)

			started <- struct{}{}

			select {
			case <-block:
			case <-ctx.Done():
			}
			resCh <- Result{Status: "completed"}
		}()

		return &Session{Messages: msgCh, Result: resCh}, nil
	}}

	gotError := make(chan string, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Send first command (will block).
		conn.WriteJSON(remoteEnvelope{
			Type: remoteTypeExecute, ID: "req-1", Prompt: "task1",
		})

		// Wait for it to start executing.
		<-started

		// Send second command (should be rejected).
		conn.WriteJSON(remoteEnvelope{
			Type: remoteTypeExecute, ID: "req-2", Prompt: "task2",
		})

		// Read the error.
		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			if env.Type == remoteTypeError && env.ID == "req-2" {
				gotError <- env.Error
				close(block)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL:     wsURL(srv),
		Backend:       backend,
		Logger:        slog.Default(),
		MaxConcurrent: 1,
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		msgCh := make(chan Message, 256)
		resCh := make(chan Result, 1)

		go func() {
			defer close(msgCh)
			defer close(resCh)
			close(executeCalled)
			<-ctx.Done()
			resCh <- Result{Status: "aborted"}
		}()

		return &Session{Messages: msgCh, Result: resCh}, nil
	}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.WriteJSON(remoteEnvelope{
			Type: remoteTypeExecute, ID: "req-shutdown", Prompt: "long task",
		})

		// Keep reading until connection closes.
		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() {
		runDone <- c.Run(ctx)
	}()

	// Wait for execution to start.
	<-executeCalled

	// Cancel context -> triggers graceful shutdown.
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

	backend := &fakeBackend{executeFn: func(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
		return fakeSession(nil, Result{Status: "completed", Output: "ok"}), nil
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
		conn.WriteJSON(remoteEnvelope{Type: "unknown_type"})

		// Send valid command to verify client still works.
		conn.WriteJSON(remoteEnvelope{
			Type: remoteTypeExecute, ID: "req-after-bad", Prompt: "hello",
		})

		for {
			var env remoteEnvelope
			if err := conn.ReadJSON(&env); err != nil {
				break
			}
			if env.Type == remoteTypeResult && env.ID == "req-after-bad" {
				close(done)
				return
			}
		}
	}))
	defer srv.Close()

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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

	c, _ := NewRemoteClient(RemoteConfig{
		ServerURL: wsURL(srv),
		Backend:   backend,
		Logger:    slog.Default(),
		Headers:   map[string]string{"Authorization": "Bearer token-123"},
		ReconnectBackoff: &BackoffConfig{
			InitialDelay: time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Factor:       2.0,
		},
		PingInterval: 10 * time.Second,
	})

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
