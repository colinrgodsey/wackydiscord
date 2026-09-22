package bot

import (
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

type fakeEdit struct {
	id      string
	content string
}

type fakePoster struct {
	mu       sync.Mutex
	posts    []string
	edits    []fakeEdit
	nextID   string
	failPost error
	failEdit error
}

func (f *fakePoster) Post(content string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPost != nil {
		return "", f.failPost
	}
	f.posts = append(f.posts, content)
	id := f.nextID
	if id == "" {
		id = "msg-1"
	}
	f.nextID = id
	return id, nil
}

func (f *fakePoster) Edit(id string, content string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failEdit != nil {
		return f.failEdit
	}
	f.edits = append(f.edits, fakeEdit{id: id, content: content})
	return nil
}

func (f *fakePoster) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.posts), len(f.edits)
}

func announceResp(id, name, args string) *agentv1.GenerateTurnStreamResponse {
	return &agentv1.GenerateTurnStreamResponse{
		ToolCall: &agentv1.ToolCall{CallId: id, ToolName: name, ArgsSummary: args},
	}
}

func deniedAnnounce(id, name, args string) *agentv1.GenerateTurnStreamResponse {
	return &agentv1.GenerateTurnStreamResponse{
		ToolCall: &agentv1.ToolCall{CallId: id, ToolName: name, ArgsSummary: args, Denied: true},
	}
}

func updateResp(id, name, status string, bytes int64, head string) *agentv1.GenerateTurnStreamResponse {
	return &agentv1.GenerateTurnStreamResponse{
		ToolCallUpdate: &agentv1.ToolCallUpdate{
			CallId: id, ToolName: name, Status: status, ResultBytes: bytes, ResultHead: head,
		},
	}
}

func syncActivity(poster ActivityPoster) *ToolActivity {
	a := NewToolActivity(poster)
	a.SetFlushInterval(0)
	return a
}

func TestToolActivity_AnnounceThenUpdateCollapsesToOneEntry(t *testing.T) {
	p := &fakePoster{}
	a := syncActivity(p)

	a.Observe(announceResp("c1", "run_command", "command=ls -la"))
	running := a.Render()
	if !strings.Contains(running, "run_command") || !strings.Contains(running, toolIconRunning) {
		t.Fatalf("running line missing name or running icon: %q", running)
	}
	if !strings.Contains(running, "ls -la") {
		t.Fatalf("running line missing args preview: %q", running)
	}

	a.Observe(updateResp("c1", "run_command", "completed", 2048, "total 8"))
	if a.Len() != 1 {
		t.Fatalf("update opened a second entry: len=%d", a.Len())
	}
	done := a.Render()
	if !strings.Contains(done, toolIconCompleted) {
		t.Fatalf("completed icon missing after update: %q", done)
	}
	if strings.Contains(done, toolIconRunning) {
		t.Fatalf("stale running icon still rendered: %q", done)
	}
	if !strings.Contains(done, "2.0 KB") {
		t.Fatalf("result size not rendered: %q", done)
	}
}

func TestToolActivity_PostsOnceThenEditsInPlace(t *testing.T) {
	p := &fakePoster{}
	a := syncActivity(p)

	a.Observe(announceResp("c1", "read_file", "path=a.txt"))
	a.Observe(updateResp("c1", "read_file", "completed", 12, "ok"))
	a.Observe(announceResp("c2", "write_file", "path=b.txt"))
	a.Finish()

	posts, edits := p.counts()
	if posts != 1 {
		t.Errorf("expected exactly one post, got %d: %v", posts, p.posts)
	}
	if edits != 2 {
		t.Errorf("expected later states to edit in place, got %d edits", edits)
	}
	for _, e := range p.edits {
		if e.id != "msg-1" {
			t.Errorf("edit targeted %q, expected the posted message id", e.id)
		}
	}
}

func TestToolActivity_DeniedAnnounceRendersDenied(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.Observe(deniedAnnounce("c1", "run_command", "command=sudo rm -rf /"))
	line := a.Render()
	if !strings.Contains(line, toolIconDenied) || !strings.Contains(line, "denied") {
		t.Fatalf("denied rendering missing: %q", line)
	}
}

func TestToolActivity_DeniedUpdateOverridesRunningLine(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.Observe(announceResp("c1", "run_command", "command=make"))
	a.Observe(updateResp("c1", "run_command", "denied", 0, ""))
	line := a.Render()
	if !strings.Contains(line, toolIconDenied) {
		t.Fatalf("expected denied icon on update: %q", line)
	}
	if strings.Contains(line, toolIconRunning) {
		t.Fatalf("running icon survived a denied update: %q", line)
	}
}

func TestToolActivity_ErrorShowsScrubbedDetail(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.Observe(announceResp("c1", "http_get", "url=https://api.internal"))
	a.Observe(updateResp("c1", "http_get", "error", 0, "401 unauthorized for key sk-live-abcdef123456789"))
	line := a.Render()
	if !strings.Contains(line, toolIconError) || !strings.Contains(line, "401") {
		t.Fatalf("error detail missing: %q", line)
	}
	if strings.Contains(line, "sk-live-abcdef") {
		t.Fatalf("secret leaked through error detail: %q", line)
	}
}

func TestToolActivity_ScrubsSecretsFromArgsAndResult(t *testing.T) {
	secrets := []string{
		"sk-live-abcdef1234567890",
		"sk_live_zzzzzzzzzzzzzz",
		"ghp_abcdefghijklmnopqrstuvwxyz123456",
		"xoxb-1234567890-abcdef",
		"AKIAIOSFODNN7EXAMPLE",
		"-----BEGIN RSA PRIVATE KEY-----",
	}
	args := "command=curl -H 'Authorization: Bearer " + secrets[0] + "' https://api.example.com"
	head := `{"content":"OPENAI_API_KEY=` + secrets[0] + `\nSTRIPE_KEY=` + secrets[1] + `"}`

	a := syncActivity(&fakePoster{})
	a.SetShowResultHead(true)
	a.Observe(announceResp("c1", "bash", args))
	a.Observe(updateResp("c1", "bash", "completed", 9999, head))
	a.Observe(announceResp("c2", "tool", "token="+secrets[2]+" tok="+secrets[3]+" aws="+secrets[4]+" pem="+secrets[5]))
	out := a.Render()
	for _, s := range secrets {
		if strings.Contains(out, s) {
			t.Errorf("secret %q present in rendered activity: %s", s, out)
		}
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Errorf("expected redaction marker in output: %s", out)
	}
}

func TestToolActivity_ResultHeadWithheldByDefault(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.Observe(announceResp("c1", "read_file", "path=/etc/passwd"))
	a.Observe(updateResp("c1", "read_file", "completed", 1234, "root:x:0:0:root:/root:/bin/bash"))
	out := a.Render()
	if strings.Contains(out, "root:x:0:0") {
		t.Fatalf("result body rendered without opting in: %q", out)
	}
	if !strings.Contains(out, "1.2 KB") {
		t.Fatalf("expected byte size only: %q", out)
	}
}

func TestToolActivity_IgnoresTextOnlyChunks(t *testing.T) {
	p := &fakePoster{}
	a := syncActivity(p)
	a.Observe(&agentv1.GenerateTurnStreamResponse{Text: "just prose"})
	a.Observe(nil)
	if a.Len() != 0 {
		t.Fatalf("text chunk created an entry: %d", a.Len())
	}
	a.Finish()
	if posts, edits := p.counts(); posts != 0 || edits != 0 {
		t.Fatalf("nothing should have been delivered: posts=%d edits=%d", posts, edits)
	}
}

func TestToolActivity_NilReceiverIsSafe(t *testing.T) {
	var a *ToolActivity
	a.Observe(announceResp("c1", "x", "y"))
	a.Flush()
	a.Finish()
	if a.Render() != "" {
		t.Fatal("nil renderer produced content")
	}
}

func TestToolActivity_MaxLinesOverflow(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.SetMaxLines(3)
	for i := 0; i < 7; i++ {
		a.Observe(announceResp(string(rune('a'+i)), "tool", "n=x"))
	}
	out := a.Render()
	if lines := strings.Split(out, "\n"); len(lines) != 4 {
		t.Fatalf("expected 3 tool lines plus overflow tail, got %d: %q", len(lines), out)
	}
	if !strings.Contains(out, "and 4 more") {
		t.Fatalf("overflow tail missing: %q", out)
	}
}

func TestToolActivity_StaysUnderDiscordLimit(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.SetMaxLines(50)
	long := strings.Repeat("wide-argument ", 200)
	for i := 0; i < 50; i++ {
		a.Observe(announceResp("c"+string(rune('A'+i%26))+string(rune('a'+i/26)), "tool", long))
	}
	if got := len([]rune(a.Render())); got > MaxDiscordMessageLength {
		t.Fatalf("rendered %d runes, over the Discord limit of %d", got, MaxDiscordMessageLength)
	}
}

func TestToolActivity_RenderIsValidUTF8FromTruncatedInput(t *testing.T) {
	// Mirror the upstream byte truncation: cut a multibyte string mid-rune.
	raw := string([]byte(string([]rune("éééééé"))[:5]))
	if utf8.ValidString(raw) {
		t.Skip("test input is unexpectedly valid UTF-8")
	}
	a := syncActivity(&fakePoster{})
	a.Observe(announceResp("c1", "tool", raw))
	a.Observe(updateResp("c1", "tool", "completed", 9, raw))
	if out := a.Render(); !utf8.ValidString(out) {
		t.Fatalf("renderer emitted invalid UTF-8: %q", out)
	}
}

func TestToolActivity_UpdateWithoutAnnounceStillRenders(t *testing.T) {
	a := syncActivity(&fakePoster{})
	a.Observe(updateResp("c9", "mystery_tool", "completed", 42, ""))
	out := a.Render()
	if !strings.Contains(out, "mystery_tool") || !strings.Contains(out, toolIconCompleted) {
		t.Fatalf("orphan update not rendered: %q", out)
	}
}

func TestToolActivity_ThrottleEditsThenFinishDeliversFinal(t *testing.T) {
	p := &fakePoster{}
	a := NewToolActivity(p)
	interval := 250 * time.Millisecond
	a.SetFlushInterval(interval)

	a.Observe(announceResp("c1", "a_tool", "x=1"))
	if posts, _ := p.counts(); posts != 1 {
		t.Fatalf("first event should post immediately, got %d", posts)
	}
	a.Observe(updateResp("c1", "a_tool", "completed", 5, ""))
	if posts, edits := p.counts(); posts != 1 || edits != 0 {
		t.Fatalf("second event inside the window must not deliver: posts=%d edits=%d", posts, edits)
	}

	waited := make(chan struct{})
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, e := p.counts(); e >= 1 {
				close(waited)
				return
			}
			if time.Now().After(deadline) {
				t.Error("throttled delivery never fired")
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	<-waited

	posts, edits := p.counts()
	if posts+edits < 2 {
		t.Fatalf("expected at least 2 deliveries after the throttle window, got %d", posts+edits)
	}
	a.Finish()
}

func TestToolActivity_PostFailureDoesNotPanicOrMarkDelivered(t *testing.T) {
	p := &fakePoster{failPost: errFakeSend{}}
	a := syncActivity(p)
	a.Observe(announceResp("c1", "tool", "x=1"))
	if a.Len() != 1 {
		t.Fatal("event should still be tracked")
	}
	// Still undelivered, so a later flush retries the post rather than editing a
	// message that was never created.
	if posts, _ := p.counts(); posts != 0 {
		t.Fatalf("failed post should not record a delivery")
	}
	a.Finish()
}

type errFakeSend struct{}

func (errFakeSend) Error() string { return "send failed" }

func TestCollapseWhitespace(t *testing.T) {
	if got := collapseWhitespace("a\n\nb\tc   d"); got != "a b c d" {
		t.Fatalf("collapseWhitespace got %q", got)
	}
}

func TestTruncateRunesDoesNotSplitMultibyte(t *testing.T) {
	s := " hélloéé"
	got := truncateRunes(s, 3)
	if !utf8.ValidString(got) {
		t.Fatalf("truncateRunes produced invalid UTF-8: %q", got)
	}
	if len([]rune(got)) > 3 {
		t.Fatalf("truncateRunes exceeded max: %d", len([]rune(got)))
	}
	if truncateRunes("abc", 10) != "abc" {
		t.Fatal("truncateRunes altered a short string")
	}
}
