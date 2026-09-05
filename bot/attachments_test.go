package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
)

// TestAttachments_TextInliningWithDynamicFence verifies text attachment inlining and dynamic code fencing.
func TestAttachments_TextInliningWithDynamicFence(t *testing.T) {
	t.Run("basic inlining without backticks", func(t *testing.T) {
		content := "server:\n  port: 8080\n"
		inlined := FormatInlinedText("config.yaml", content)

		expected := "[Attached file: config.yaml]\n```yaml\nserver:\n  port: 8080\n```"
		if inlined != expected {
			t.Errorf("got:\n%s\nwant:\n%s", inlined, expected)
		}
	})

	t.Run("dynamic fence expands when payload contains 3 backticks", func(t *testing.T) {
		content := "Example markdown:\n```go\nfmt.Println(\"hello\")\n```\nEnd of example."
		inlined := FormatInlinedText("doc.md", content)

		// Must use 4 backticks to avoid breakout from the 3 backticks in the payload
		if !strings.Contains(inlined, "````md\n") {
			t.Errorf("expected opening fence with 4 backticks ````md, got:\n%s", inlined)
		}
		if !strings.HasSuffix(inlined, "\n````") {
			t.Errorf("expected closing fence with 4 backticks ````, got:\n%s", inlined)
		}
		if !strings.Contains(inlined, "[Attached file: doc.md]") {
			t.Errorf("missing header in inlined text: %s", inlined)
		}
	})

	t.Run("dynamic fence expands when payload contains 4 backticks", func(t *testing.T) {
		content := "Nested fence:\n````sh\necho \"nested\"\n````"
		inlined := FormatInlinedText("script.sh", content)

		// Must use 5 backticks to avoid breakout
		if !strings.Contains(inlined, "`````sh\n") {
			t.Errorf("expected opening fence with 5 backticks, got:\n%s", inlined)
		}
		if !strings.HasSuffix(inlined, "\n`````") {
			t.Errorf("expected closing fence with 5 backticks, got:\n%s", inlined)
		}
	})

	t.Run("content without trailing newline gets newline before closing fence", func(t *testing.T) {
		content := "x = 42"
		inlined := FormatInlinedText("script.py", content)

		expected := "[Attached file: script.py]\n```py\nx = 42\n```"
		if inlined != expected {
			t.Errorf("got:\n%s\nwant:\n%s", inlined, expected)
		}
	})

	t.Run("yml extension maps to yaml fence", func(t *testing.T) {
		inlined := FormatInlinedText("app.yml", "env: prod\n")
		if !strings.Contains(inlined, "```yaml\n") {
			t.Errorf("expected yaml fence for .yml file, got:\n%s", inlined)
		}
	})
}

// TestAttachments_ClassifierMultiLayer verifies the 4-stage classifier logic.
func TestAttachments_ClassifierMultiLayer(t *testing.T) {
	// 1. All allowlisted extensions pass with plain text
	allowlist := []string{
		"a.txt", "b.md", "c.py", "d.go", "e.json", "f.yaml", "g.yml",
		"h.toml", "i.csv", "j.log", "k.diff", "l.patch", "m.ini",
		"n.sh", "o.sql", "p.html", "q.css", "r.xml",
	}
	for _, fname := range allowlist {
		data := []byte("plain text content for " + fname)
		if !IsInlineText(fname, data) {
			t.Errorf("expected IsInlineText(%q) to be true", fname)
		}
	}

	// 2. Disallowed extensions are rejected
	disallowed := []string{"data.bin", "archive.zip", "doc.pdf", "app.exe", "file.tar", "file.gz", "unknown"}
	for _, fname := range disallowed {
		data := []byte("plain text inside disallowed extension")
		if IsInlineText(fname, data) {
			t.Errorf("expected IsInlineText(%q) to be false for disallowed extension", fname)
		}
	}

	// 3. NUL byte veto
	nulData := []byte("Hello\x00World")
	if IsInlineText("hello.txt", nulData) {
		t.Errorf("expected IsInlineText to be false when data contains NUL byte")
	}

	// 4. Magic-byte veto (e.g. PDF disguised as .txt)
	pdfData := []byte("%PDF-1.4\n%âãÏÓ\n1 0 obj\n<<\n>>\nendobj")
	if IsInlineText("fake.txt", pdfData) {
		t.Errorf("expected IsInlineText to be false for PDF magic bytes in .txt file")
	}

	// Magic-byte veto for ZIP disguised as .json
	zipData := []byte("PK\x03\x04\x14\x00\x00\x00\x08\x00")
	if IsInlineText("fake.json", zipData) {
		t.Errorf("expected IsInlineText to be false for ZIP magic bytes in .json file")
	}

	// 5. Large text file (>100KB) routes to scratchpad (IsInlineText returns false)
	largeData := bytes.Repeat([]byte("a"), MaxInlineTextBytes+1)
	if IsInlineText("large.txt", largeData) {
		t.Errorf("expected IsInlineText to be false for file exceeding MaxInlineTextBytes (100KB)")
	}
}

// TestAttachments_DownloadBoundsAndTimeouts tests bounded download limits and errors.
func TestAttachments_DownloadBoundsAndTimeouts(t *testing.T) {
	t.Run("successful download within limits", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("hello from attachment"))
		}))
		defer srv.Close()

		data, err := downloadAttachment(context.Background(), srv.URL, 1024)
		if err != nil {
			t.Fatalf("unexpected download error: %v", err)
		}
		if string(data) != "hello from attachment" {
			t.Errorf("got %q, want %q", string(data), "hello from attachment")
		}
	})

	t.Run("exceeds limit returns ErrAttachmentTooLarge", func(t *testing.T) {
		// Server sends 500 bytes when limit is 100 bytes
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(bytes.Repeat([]byte("X"), 500))
		}))
		defer srv.Close()

		_, err := downloadAttachment(context.Background(), srv.URL, 100)
		if err == nil {
			t.Fatalf("expected ErrAttachmentTooLarge, got nil")
		}
		if err != ErrAttachmentTooLarge {
			t.Errorf("expected ErrAttachmentTooLarge, got %v", err)
		}
	})

	t.Run("Content-Length header > limit rejects early", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "25000000") // 25MB
			_, _ = w.Write([]byte("short body but large header"))
		}))
		defer srv.Close()

		_, err := downloadAttachment(context.Background(), srv.URL, MaxAttachmentBytes)
		if err != ErrAttachmentTooLarge {
			t.Errorf("expected ErrAttachmentTooLarge from Content-Length header, got %v", err)
		}
	})

	t.Run("non-200 status returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not found", http.StatusNotFound)
		}))
		defer srv.Close()

		_, err := downloadAttachment(context.Background(), srv.URL, 1024)
		if err == nil || !strings.Contains(err.Error(), "bad status") {
			t.Errorf("expected bad status error, got %v", err)
		}
	})

	t.Run("timeout terminates download", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			_, _ = w.Write([]byte("too slow"))
		}))
		defer srv.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := downloadAttachment(ctx, srv.URL, 1024)
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
	})
}

// TestAttachments_ProcessAttachments tests the end-to-end processing of attachments.
func TestAttachments_ProcessAttachments(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "WACKYPUB_ROOT"), []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("failed to create bobDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
		t.Fatalf("failed to write AGENTS.md: %v", err)
	}

	sdk := agent.NewSDK(tmpDir)
	b := &Bot{
		WsDir: tmpDir,
		SDK:   sdk,
	}

	t.Run("text attachment inlining", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("version: '3.8'\nservices:\n  app:\n    image: test"))
		}))
		defer srv.Close()

		atts := []*discordgo.MessageAttachment{
			{
				Filename:    "docker-compose.yml",
				URL:         srv.URL,
				ContentType: "text/yaml",
			},
		}

		res, err := b.ProcessAttachments(context.Background(), "bob", atts)
		if err != nil {
			t.Fatalf("ProcessAttachments failed: %v", err)
		}
		if len(res.Notices) != 0 {
			t.Errorf("unexpected notices: %v", res.Notices)
		}
		if !strings.Contains(res.PromptText, "[Attached file: docker-compose.yml]") {
			t.Errorf("missing header in PromptText: %s", res.PromptText)
		}
		if !strings.Contains(res.PromptText, "```yaml\nversion: '3.8'") {
			t.Errorf("missing formatted yaml content in PromptText: %s", res.PromptText)
		}
	})

	t.Run("binary attachment routing to scratchpad", func(t *testing.T) {
		binPayload := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write(binPayload)
		}))
		defer srv.Close()

		atts := []*discordgo.MessageAttachment{
			{
				Filename:    "archive.zip",
				URL:         srv.URL,
				ContentType: "application/zip",
			},
		}

		res, err := b.ProcessAttachments(context.Background(), "bob", atts)
		if err != nil {
			t.Fatalf("ProcessAttachments failed: %v", err)
		}

		// PromptText format: [Attached file: data.zip (saved to scratchpad entry 'x', N bytes). Use scratchpad tools to inspect.]
		expectedPrefix := "[Attached file: archive.zip (saved to scratchpad entry '"
		if !strings.HasPrefix(res.PromptText, expectedPrefix) {
			t.Fatalf("PromptText does not match expected prefix: %s", res.PromptText)
		}
		if !strings.Contains(res.PromptText, fmt.Sprintf("%d bytes). Use scratchpad tools to inspect.]", len(binPayload))) {
			t.Errorf("PromptText missing bytes size or instruction: %s", res.PromptText)
		}

		// Verify scratchpad entry was written to disk
		spDir := filepath.Join(bobDir, "scratchpad")
		entries, err := os.ReadDir(spDir)
		if err != nil || len(entries) == 0 {
			t.Fatalf("failed to read scratchpad entries: %v", err)
		}

		var foundEntry bool
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), "-0-discord_att.dat") {
				foundEntry = true
				data, err := os.ReadFile(filepath.Join(spDir, e.Name()))
				if err != nil {
					t.Fatalf("failed reading saved scratchpad file: %v", err)
				}
				if !bytes.Equal(data, binPayload) {
					t.Errorf("saved scratchpad data does not match payload")
				}
			}
		}
		if !foundEntry {
			t.Errorf("expected scratchpad file with -0-discord_att.dat suffix in %s", spDir)
		}
	})

	t.Run("large text file (>100KB) routes to scratchpad", func(t *testing.T) {
		largeText := strings.Repeat("A regular text line\n", 6000) // ~120KB
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(largeText))
		}))
		defer srv.Close()

		atts := []*discordgo.MessageAttachment{
			{
				Filename:    "big_log.txt",
				URL:         srv.URL,
				ContentType: "text/plain",
			},
		}

		res, err := b.ProcessAttachments(context.Background(), "bob", atts)
		if err != nil {
			t.Fatalf("ProcessAttachments failed: %v", err)
		}
		if !strings.Contains(res.PromptText, "[Attached file: big_log.txt (saved to scratchpad entry '") {
			t.Errorf("expected large text file to be routed to scratchpad, got: %s", res.PromptText)
		}
	})

	t.Run("size limit (>20MB) rejection with clear notice", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "25000000") // 25MB
			_, _ = w.Write([]byte("too large"))
		}))
		defer srv.Close()

		atts := []*discordgo.MessageAttachment{
			{
				Filename: "large_disk.iso",
				URL:      srv.URL,
			},
		}

		res, err := b.ProcessAttachments(context.Background(), "bob", atts)
		if err != nil {
			t.Fatalf("ProcessAttachments failed: %v", err)
		}

		// Notice must be present in Notices for user display
		if len(res.Notices) != 1 || !strings.Contains(res.Notices[0], "exceeds 20MB limit and was rejected") {
			t.Errorf("expected size limit rejection notice in Notices, got: %v", res.Notices)
		}

		// Notice must be present in PromptText so agent knows
		if !strings.Contains(res.PromptText, "[Attached file: large_disk.iso skipped: exceeds maximum allowed size of 20MB]") {
			t.Errorf("expected size limit notice in PromptText, got: %s", res.PromptText)
		}
	})

	t.Run("attachment cap (5) with remainder notice", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("content"))
		}))
		defer srv.Close()

		var atts []*discordgo.MessageAttachment
		for i := 1; i <= 7; i++ {
			atts = append(atts, &discordgo.MessageAttachment{
				Filename: fmt.Sprintf("file%d.txt", i),
				URL:      srv.URL,
			})
		}

		res, err := b.ProcessAttachments(context.Background(), "bob", atts)
		if err != nil {
			t.Fatalf("ProcessAttachments failed: %v", err)
		}

		// First 5 should be inlined
		for i := 1; i <= 5; i++ {
			if !strings.Contains(res.PromptText, fmt.Sprintf("[Attached file: file%d.txt]", i)) {
				t.Errorf("expected file%d.txt to be inlined", i)
			}
		}

		// 6 and 7 should NOT be inlined
		if strings.Contains(res.PromptText, "[Attached file: file6.txt]") || strings.Contains(res.PromptText, "[Attached file: file7.txt]") {
			t.Errorf("file6 or file7 unexpectedly inlined beyond cap")
		}

		// Remainder notice must mention the 2 skipped files
		foundCapNotice := false
		for _, n := range res.Notices {
			if strings.Contains(n, "Attachment cap (5) reached; 2 attachment(s) skipped: file6.txt, file7.txt") {
				foundCapNotice = true
			}
		}
		if !foundCapNotice {
			t.Errorf("expected remainder notice in Notices, got: %v", res.Notices)
		}
		if !strings.Contains(res.PromptText, "[Notice: Attachment cap (5) reached; 2 attachment(s) skipped: file6.txt, file7.txt]") {
			t.Errorf("expected remainder notice in PromptText, got: %s", res.PromptText)
		}
	})
}

// TestAttachments_SendAgentMessage_AllowedMentionsEmpty verifies that SendAgentMessage
// sets AllowedMentions with empty Parse on both Webhook and ChannelMessageSendComplex paths.
func TestAttachments_SendAgentMessage_AllowedMentionsEmpty(t *testing.T) {
	t.Run("webhook delivery enforces empty AllowedMentions", func(t *testing.T) {
		var mu sync.Mutex
		var capturedParams discordgo.WebhookParams

		s, err := discordgo.New("Bot fake-token")
		if err != nil {
			t.Fatalf("discordgo.New failed: %v", err)
		}
		s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			mu.Lock()
			_ = json.Unmarshal(body, &capturedParams)
			mu.Unlock()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"id":"msg_wh"}`)),
				Header:     make(http.Header),
			}, nil
		})

		wh := &discordgo.Webhook{ID: "wh_test", Token: "tok_test"}
		err = SendAgentMessage(s, "chan_1", "bob", "Hello @everyone and <@&12345>", wh)
		if err != nil {
			t.Fatalf("SendAgentMessage failed: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()

		if capturedParams.AllowedMentions == nil {
			t.Fatalf("expected AllowedMentions to be non-nil in WebhookParams")
		}
		if len(capturedParams.AllowedMentions.Parse) != 0 {
			t.Errorf("expected empty AllowedMentions.Parse, got: %v", capturedParams.AllowedMentions.Parse)
		}
	})

	t.Run("fallback message delivery enforces empty AllowedMentions", func(t *testing.T) {
		var mu sync.Mutex
		var capturedSend discordgo.MessageSend

		s, err := discordgo.New("Bot fake-token")
		if err != nil {
			t.Fatalf("discordgo.New failed: %v", err)
		}
		s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			mu.Lock()
			_ = json.Unmarshal(body, &capturedSend)
			mu.Unlock()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"id":"msg_std"}`)),
				Header:     make(http.Header),
			}, nil
		})

		// Passing nil webhook forces fallback to ChannelMessageSendComplex
		err = SendAgentMessage(s, "chan_1", "bob", "Hello @everyone", nil)
		if err != nil {
			t.Fatalf("SendAgentMessage fallback failed: %v", err)
		}

		mu.Lock()
		defer mu.Unlock()

		if capturedSend.AllowedMentions == nil {
			t.Fatalf("expected AllowedMentions to be non-nil in MessageSend")
		}
		if len(capturedSend.AllowedMentions.Parse) != 0 {
			t.Errorf("expected empty AllowedMentions.Parse in fallback, got: %v", capturedSend.AllowedMentions.Parse)
		}
	})
}

// TestAttachments_Sanitization tests filename and createdBy sanitization helpers.
func TestAttachments_Sanitization(t *testing.T) {
	t.Run("SanitizeFilename", func(t *testing.T) {
		if got := SanitizeFilename("normal_file.txt"); got != "normal_file.txt" {
			t.Errorf("expected normal_file.txt, got %q", got)
		}
		// Strips directory traversal
		if got := SanitizeFilename("../../etc/passwd"); got != "passwd" {
			t.Errorf("expected passwd, got %q", got)
		}
		// Strips control characters
		if got := SanitizeFilename("hello\x00\x1b\x07world.py"); got != "helloworld.py" {
			t.Errorf("expected helloworld.py, got %q", got)
		}
		// Empty fallback
		if got := SanitizeFilename(""); got != "attachment" {
			t.Errorf("expected attachment, got %q", got)
		}
		// Caps to 80 chars
		longName := strings.Repeat("a", 100) + ".txt"
		if got := SanitizeFilename(longName); len(got) > 80 {
			t.Errorf("expected length <= 80, got %d", len(got))
		}
	})

	t.Run("SanitizeCreatedBy", func(t *testing.T) {
		if got := SanitizeCreatedBy("discord_att"); got != "discord_att" {
			t.Errorf("expected discord_att, got %q", got)
		}
		if got := SanitizeCreatedBy("UPPER_CASE-123"); got != "upper_case-123" {
			t.Errorf("expected upper_case-123, got %q", got)
		}
		if got := SanitizeCreatedBy("../evil/path"); got != "evilpath" {
			t.Errorf("expected evilpath, got %q", got)
		}
		if got := SanitizeCreatedBy(""); got != "discord_att" {
			t.Errorf("expected discord_att, got %q", got)
		}
		long := strings.Repeat("x", 50)
		if got := SanitizeCreatedBy(long); len(got) != 32 {
			t.Errorf("expected 32 chars, got %d", len(got))
		}
	})
}

// TestHandleMessageCreate_WithAttachments tests the full HandleMessageCreate workflow with text and binary attachments.
func TestHandleMessageCreate_WithAttachments(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "WACKYPUB_ROOT"), []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("failed to create bobDir: %v", err)
	}
	_ = os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Bob system prompt"), 0644)
	_ = os.WriteFile(filepath.Join(bobDir, agent.AllowedAgentsFile), []byte("bob\n"), 0644)

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_att_test",
		AgentID:   "bob",
	})

	// Server for downloading attachments
	attServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/config.yaml":
			_, _ = w.Write([]byte("env: staging\ndebug: true\n"))
		case "/binary.bin":
			_, _ = w.Write([]byte{0x00, 0xFF, 0xAA, 0x55})
		default:
			http.NotFound(w, r)
		}
	}))
	defer attServer.Close()

	// Server for mocked LLM endpoint
	var capturedUserPrompt string
	llmServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedUserPrompt = string(body)
		// Return valid SSE response or empty stream for genai client
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"I reviewed your attachments."}],"role":"model"}}]}`))
	}))
	defer llmServer.Close()

	runtimeJSON := fmt.Sprintf(`{"model":"test-model","endpoint":%q}`, llmServer.URL)
	_ = os.WriteFile(filepath.Join(bobDir, "runtime.json"), []byte(runtimeJSON), 0644)

	origCwd, _ := os.Getwd()
	_ = os.Chdir(bobDir)
	defer os.Chdir(origCwd)

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   agent.NewSDK(tmpDir),
	}

	s, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("discordgo.New failed: %v", err)
	}
	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"id":"msg_1"}`)),
			Header:     make(http.Header),
		}, nil
	})

	msg := &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg_inbound",
			ChannelID: "chan_att_test",
			Content:   "Please check these files.",
			Author:    &discordgo.User{ID: "user_1", Bot: false},
			Attachments: []*discordgo.MessageAttachment{
				{
					Filename:    "config.yaml",
					URL:         attServer.URL + "/config.yaml",
					ContentType: "text/yaml",
				},
				{
					Filename:    "binary.bin",
					URL:         attServer.URL + "/binary.bin",
					ContentType: "application/octet-stream",
				},
			},
		},
	}

	b.HandleMessageCreate(s, msg)

	// Verify the inlined file was passed in the user prompt
	if !strings.Contains(capturedUserPrompt, "[Attached file: config.yaml]") {
		t.Errorf("expected config.yaml header in prompt, got:\n%s", capturedUserPrompt)
	}
	if !strings.Contains(capturedUserPrompt, "```yaml") || !strings.Contains(capturedUserPrompt, "env: staging") {
		t.Errorf("expected inlined config.yaml content in prompt, got:\n%s", capturedUserPrompt)
	}

	// Verify the binary file was referenced in the user prompt
	if !strings.Contains(capturedUserPrompt, "[Attached file: binary.bin (saved to scratchpad entry '") {
		t.Errorf("expected scratchpad reference for binary.bin in prompt, got:\n%s", capturedUserPrompt)
	}

	// Verify scratchpad binary entry exists on disk
	spDir := filepath.Join(bobDir, "scratchpad")
	entries, err := os.ReadDir(spDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("expected scratchpad entry for binary.bin to exist in %s", spDir)
	}
	var foundBinary bool
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "-0-discord_att.dat") {
			foundBinary = true
			data, _ := os.ReadFile(filepath.Join(spDir, e.Name()))
			if !bytes.Equal(data, []byte{0x00, 0xFF, 0xAA, 0x55}) {
				t.Errorf("unexpected binary content saved: %v", data)
			}
		}
	}
	if !foundBinary {
		t.Errorf("binary scratchpad file with suffix -0-discord_att.dat was not created")
	}
}
