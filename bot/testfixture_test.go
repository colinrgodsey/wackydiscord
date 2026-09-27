package bot

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
	"google.golang.org/grpc"
)

// Fixtures for driving the bot without linking pkg/agent. Severing the module means the
// tests speak the same two things production does: session.jsonl on disk, and the protocol.
// The fake client reads the same files the fixture writer produces, so seeding a session and
// asserting on one look the way they always did.

const (
	sessionFileName       = "session.jsonl"
	sessionLockFileName   = "session.lock"
	allowedAgentsFileName = "WACKYPUB_ALLOWED_AGENTS"
)

type fixtureTurn struct {
	Role  string        `json:"role,omitempty"`
	Parts []*genai.Part `json:"parts,omitempty"`
	Seq   int64         `json:"seq,omitempty"`
}

func agentDirIn(wsDir, agentID string) string {
	return filepath.Join(wsDir, agentID)
}

func appendSessionContent(agentDir string, c *genai.Content) error {
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(agentDir, sessionFileName)
	next, err := countSessionLines(path)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(fixtureTurn{Role: c.Role, Parts: c.Parts, Seq: int64(next + 1)})
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	return err
}

func writeSessionTurns(agentDir string, turns []*genai.Content) error {
	if err := os.Remove(filepath.Join(agentDir, sessionFileName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, t := range turns {
		if err := appendSessionContent(agentDir, t); err != nil {
			return err
		}
	}
	return nil
}

func countSessionLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	return n, sc.Err()
}

// readSessionTurnsFixture returns the on-disk turns with their seq, for assertions about what
// the cursor should have advanced past.
func readSessionTurnsFixture(agentDir string) ([]TurnWithSeq, error) {
	f, err := os.Open(filepath.Join(agentDir, sessionFileName))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []TurnWithSeq
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ft fixtureTurn
		if err := json.Unmarshal(line, &ft); err != nil {
			return nil, err
		}
		out = append(out, TurnWithSeq{Content: &genai.Content{Role: ft.Role, Parts: ft.Parts}, Seq: ft.Seq})
	}
	return out, sc.Err()
}

// sessionLock mirrors the server's advisory lock on session.lock. The fake client takes it
// for the duration of a turn, so a test can hold it to stall generation exactly as it used to
// stall the in-process SDK.
type sessionLock struct {
	f *os.File
}

func acquireSessionLock(agentDir string) (*sessionLock, error) {
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(agentDir, sessionLockFileName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &sessionLock{f: f}, nil
}

func (l *sessionLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	defer func() { l.f.Close(); l.f = nil }()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}

// fakeAgent is the protocol side of the fixtures: it serves the handful of calls the bot
// makes, backed by the same workspace layout the real server uses.
type fakeAgent struct {
	wsDir string

	mu           sync.Mutex
	reply        string
	cancelErr    error
	addUserErr   error
	streams      []func(*agentv1.GenerateTurnStreamResponse)
	subEvents    chan *agentv1.SubscribeSessionResponse
	compactCalls int
	scratchpads  map[string]string
	nextEntry    int

	cancelCalls int
	inFlight    bool
	hold        chan struct{}
	started     chan struct{}
	cancelFunc  context.CancelFunc
}

func newFakeAgent(wsDir string) *fakeAgent {
	return &fakeAgent{
		wsDir:       wsDir,
		reply:       "",
		subEvents:   make(chan *agentv1.SubscribeSessionResponse, 8),
		scratchpads: map[string]string{},
	}
}

var _ AgentClient = (*fakeAgent)(nil)

func (f *fakeAgent) InspectAgent(_ context.Context, in *agentv1.InspectAgentRequest, _ ...grpc.CallOption) (*agentv1.InspectAgentResponse, error) {
	dir := agentDirIn(f.wsDir, in.GetAgentId())
	resp := &agentv1.InspectAgentResponse{AgentId: in.GetAgentId(), AgentDir: dir}
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		resp.AgentDirExists = true
		resp.SessionJsonlExists = true
		turns, err := readSessionTurnsFixture(dir)
		if err == nil {
			resp.SessionTurnCount = int32(len(turns))
		}
	}
	return resp, nil
}

func (f *fakeAgent) ListAgents(_ context.Context, _ *agentv1.ListAgentsRequest, _ ...grpc.CallOption) (*agentv1.ListAgentsResponse, error) {
	ents, err := os.ReadDir(f.wsDir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range ents {
		if e.IsDir() && !isHidden(e.Name()) {
			ids = append(ids, e.Name())
		}
	}
	return &agentv1.ListAgentsResponse{AgentIds: ids}, nil
}

func isHidden(name string) bool { return len(name) > 0 && name[0] == '.' }

func (f *fakeAgent) ReadSession(_ context.Context, in *agentv1.ReadSessionRequest, _ ...grpc.CallOption) (*agentv1.ReadSessionResponse, error) {
	for _, dir := range []string{f.wsDir, agentDirIn(f.wsDir, in.GetAgentId())} {
		if data, err := os.ReadFile(filepath.Join(dir, allowedAgentsFileName)); err == nil {
			allowed := false
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) == in.GetAgentId() {
					allowed = true
					break
				}
			}
			if !allowed {
				return nil, fmt.Errorf("agent %q is not in allowed agents list", in.GetAgentId())
			}
		}
	}
	turns, err := readSessionTurnsFixture(agentDirIn(f.wsDir, in.GetAgentId()))
	if err != nil {
		if os.IsNotExist(err) {
			return &agentv1.ReadSessionResponse{}, nil
		}
		return nil, err
	}
	out := make([]*agentv1.SessionTurn, 0, len(turns))
	for _, t := range turns {
		parts := make([]*agentv1.SessionPart, 0, len(t.Content.Parts))
		for _, p := range t.Content.Parts {
			if p == nil {
				continue
			}
			parts = append(parts, &agentv1.SessionPart{Text: p.Text})
		}
		out = append(out, &agentv1.SessionTurn{Role: t.Content.Role, Parts: parts, Seq: t.Seq})
	}
	return &agentv1.ReadSessionResponse{Turns: out}, nil
}

// AddUserTurn takes the session lock for the whole call, so a test that holds the lock stalls
// the turn the way the in-process SDK used to be stalled.
func (f *fakeAgent) AddUserTurn(_ context.Context, in *agentv1.AddUserTurnRequest, _ ...grpc.CallOption) (*agentv1.AddUserTurnResponse, error) {
	f.mu.Lock()
	err := f.addUserErr
	reply := f.reply
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	dir := agentDirIn(f.wsDir, in.GetAgentId())
	lock, err := acquireSessionLock(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Release()

	userTurn := genai.NewContentFromText(in.GetMessage(), "user")
	if err := appendSessionContent(dir, userTurn); err != nil {
		return nil, err
	}
	if reply != "" {
		if err := appendSessionContent(dir, genai.NewContentFromText(reply, "model")); err != nil {
			return nil, err
		}
	}
	seqs, _ := readSessionTurnsFixture(dir)
	var st *agentv1.SessionTurn
	if len(seqs) > 0 {
		last := seqs[len(seqs)-1]
		parts := make([]*agentv1.SessionPart, 0, len(last.Content.Parts))
		for _, p := range last.Content.Parts {
			if p != nil {
				parts = append(parts, &agentv1.SessionPart{Text: p.Text})
			}
		}
		st = &agentv1.SessionTurn{Role: last.Content.Role, Parts: parts, Seq: last.Seq}
	}
	return &agentv1.AddUserTurnResponse{Turn: st}, nil
}

func (f *fakeAgent) CancelTurn(_ context.Context, _ *agentv1.CancelTurnRequest, _ ...grpc.CallOption) (*agentv1.CancelTurnResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls++
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}
	if !f.inFlight {
		return nil, fmt.Errorf("no in-flight turn")
	}
	f.inFlight = false
	if f.cancelFunc != nil {
		f.cancelFunc()
		f.cancelFunc = nil
	}
	return &agentv1.CancelTurnResponse{}, nil
}

func (f *fakeAgent) ReadMemory(_ context.Context, in *agentv1.ReadMemoryRequest, _ ...grpc.CallOption) (*agentv1.ReadMemoryResponse, error) {
	data, err := os.ReadFile(filepath.Join(agentDirIn(f.wsDir, in.GetAgentId()), "MEMORY.md"))
	if err != nil {
		return &agentv1.ReadMemoryResponse{}, nil
	}
	return &agentv1.ReadMemoryResponse{MemoryMd: string(data)}, nil
}

func (f *fakeAgent) InspectSessionContext(_ context.Context, in *agentv1.InspectSessionContextRequest, _ ...grpc.CallOption) (*agentv1.InspectSessionContextResponse, error) {
	dir := agentDirIn(f.wsDir, in.GetAgentId())
	turns, _ := readSessionTurnsFixture(dir)
	turnCount := int32(len(turns))
	if turnCount == 0 {
		return &agentv1.InspectSessionContextResponse{TurnCount: 0}, nil
	}
	estTokens := int64(turnCount * 100)
	thresh := int64(10000)
	pct := float64(estTokens) / float64(thresh) * 100.0
	return &agentv1.InspectSessionContextResponse{
		TurnCount:            turnCount,
		EstimatedTotalTokens: int32(estTokens),
		CompactionThreshold:  int32(thresh),
		PercentToThreshold:   pct,
	}, nil
}

func (f *fakeAgent) CompactSession(_ context.Context, in *agentv1.CompactSessionRequest, _ ...grpc.CallOption) (*agentv1.CompactSessionResponse, error) {
	f.mu.Lock()
	f.compactCalls++
	f.mu.Unlock()

	dir := agentDirIn(f.wsDir, in.GetAgentId())
	turns, err := readSessionTurnsFixture(dir)
	if err != nil || len(turns) == 0 {
		return &agentv1.CompactSessionResponse{Compacted: false}, nil
	}
	if !in.GetForce() {
		return &agentv1.CompactSessionResponse{Compacted: false}, nil
	}

	summary := "* archived summary of earlier turns"
	if rData, err := os.ReadFile(filepath.Join(dir, "runtime.json")); err == nil {
		var rt struct {
			Endpoint string `json:"endpoint"`
		}
		if json.Unmarshal(rData, &rt) == nil && rt.Endpoint != "" {
			reqBody, _ := json.Marshal(map[string]any{"prompt": "summarize"})
			httpResp, httpErr := http.Post(rt.Endpoint, "application/json", bytes.NewReader(reqBody))
			if httpErr == nil {
				defer httpResp.Body.Close()
				var oaiResp struct {
					Choices []struct {
						Message struct {
							Content string `json:"content"`
						} `json:"message"`
					} `json:"choices"`
				}
				if json.NewDecoder(httpResp.Body).Decode(&oaiResp) == nil && len(oaiResp.Choices) > 0 {
					if content := oaiResp.Choices[0].Message.Content; content != "" {
						summary = content
					}
				}
			}
		}
	}

	memPath := filepath.Join(dir, "MEMORY.md")
	var existingMem string
	if m, err := os.ReadFile(memPath); err == nil {
		existingMem = string(m)
	}
	newMem := summary
	if existingMem != "" {
		newMem = existingMem + "\n" + summary
	}
	if err := os.WriteFile(memPath, []byte(newMem), 0644); err != nil {
		return nil, err
	}

	keep := 2
	if len(turns) > keep {
		retained := turns[len(turns)-keep:]
		file, err := os.Create(filepath.Join(dir, sessionFileName))
		if err != nil {
			return nil, err
		}
		for _, t := range retained {
			line, _ := json.Marshal(fixtureTurn{Role: t.Content.Role, Parts: t.Content.Parts, Seq: t.Seq})
			file.Write(append(line, '\n'))
		}
		file.Close()
	}

	return &agentv1.CompactSessionResponse{Compacted: true}, nil
}

func (f *fakeAgent) AsideQuestion(_ context.Context, in *agentv1.AsideQuestionRequest, _ ...grpc.CallOption) (*agentv1.AsideQuestionResponse, error) {
	dir := agentDirIn(f.wsDir, in.GetAgentId())
	if rData, err := os.ReadFile(filepath.Join(dir, "runtime.json")); err == nil {
		var rt struct {
			Model    string `json:"model"`
			Endpoint string `json:"endpoint"`
		}
		if json.Unmarshal(rData, &rt) == nil && rt.Endpoint != "" {
			turns, _ := readSessionTurnsFixture(dir)
			messages := []map[string]any{}
			for _, t := range turns {
				text := ""
				for _, p := range t.Content.Parts {
					if p != nil {
						text += p.Text
					}
				}
				messages = append(messages, map[string]any{
					"role":    t.Content.Role,
					"content": text,
				})
			}
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": in.GetQuestion(),
			})
			reqBody, _ := json.Marshal(map[string]any{
				"model":    rt.Model,
				"messages": messages,
			})
			httpResp, httpErr := http.Post(rt.Endpoint, "application/json", bytes.NewReader(reqBody))
			if httpErr == nil {
				defer httpResp.Body.Close()
				var oaiResp struct {
					Choices []struct {
						Message struct {
							Content string `json:"content"`
						} `json:"message"`
					} `json:"choices"`
					Usage struct {
						PromptTokens     int64 `json:"prompt_tokens"`
						CompletionTokens int64 `json:"completion_tokens"`
						TotalTokens      int64 `json:"total_tokens"`
					} `json:"usage"`
				}
				if json.NewDecoder(httpResp.Body).Decode(&oaiResp) == nil && len(oaiResp.Choices) > 0 {
					return &agentv1.AsideQuestionResponse{
						Text: oaiResp.Choices[0].Message.Content,
						Usage: &agentv1.TurnUsage{
							PromptTokens:     oaiResp.Usage.PromptTokens,
							CompletionTokens: oaiResp.Usage.CompletionTokens,
							TotalTokens:      oaiResp.Usage.TotalTokens,
						},
					}, nil
				}
			}
		}
	}
	return &agentv1.AsideQuestionResponse{Text: "aside answer"}, nil
}

func (f *fakeAgent) AddMedia(_ context.Context, _ *agentv1.AddMediaRequest, _ ...grpc.CallOption) (*agentv1.AddMediaResponse, error) {
	return &agentv1.AddMediaResponse{}, nil
}

func (f *fakeAgent) GetScratchpad(_ context.Context, in *agentv1.GetScratchpadRequest, _ ...grpc.CallOption) (*agentv1.GetScratchpadResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &agentv1.GetScratchpadResponse{Text: f.scratchpads[in.GetEntryId()]}, nil
}

func (f *fakeAgent) CreateScratchpad(_ context.Context, in *agentv1.CreateScratchpadRequest, _ ...grpc.CallOption) (*agentv1.CreateScratchpadResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextEntry++
	id := fmt.Sprintf("e%d", f.nextEntry)
	f.scratchpads[id] = in.GetText()

	agentDir := agentDirIn(f.wsDir, in.GetAgentId())
	spDir := filepath.Join(agentDir, "scratchpad")
	_ = os.MkdirAll(spDir, 0755)

	createdBy := in.GetCreatedBy()
	if createdBy == "" {
		createdBy = "discord_att"
	}
	if len(in.GetData()) > 0 {
		filePath := filepath.Join(spDir, fmt.Sprintf("%s-0-%s.dat", id, createdBy))
		if err := os.WriteFile(filePath, in.GetData(), 0644); err != nil {
			return nil, err
		}
	} else if in.GetText() != "" {
		filePath := filepath.Join(spDir, fmt.Sprintf("%s-0-%s.txt", id, createdBy))
		if err := os.WriteFile(filePath, []byte(in.GetText()), 0644); err != nil {
			return nil, err
		}
	}

	return &agentv1.CreateScratchpadResponse{Entry: &agentv1.ScratchpadEntry{
		EntryId:  id,
		Size:     int64(len(in.GetData())),
		IsBinary: len(in.GetData()) > 0,
		MimeType: in.GetMimeType(),
	}}, nil
}

func (f *fakeAgent) ReadSessionEvents(_ context.Context, in *agentv1.ReadSessionEventsRequest, _ ...grpc.CallOption) (*agentv1.ReadSessionEventsResponse, error) {
	return &agentv1.ReadSessionEventsResponse{}, nil
}

func (f *fakeAgent) SubscribeSession(ctx context.Context, _ *agentv1.SubscribeSessionRequest, _ ...grpc.CallOption) (agentv1.AgentService_SubscribeSessionClient, error) {
	return &fakeSubscribeStream{ctx: ctx, ch: f.subEvents}, nil
}

func (f *fakeAgent) GenerateTurnStream(ctx context.Context, in *agentv1.GenerateTurnStreamRequest, _ ...grpc.CallOption) (agentv1.AgentService_GenerateTurnStreamClient, error) {
	if in != nil {
		dir := agentDirIn(f.wsDir, in.GetAgentId())
		if rData, err := os.ReadFile(filepath.Join(dir, "runtime.json")); err == nil {
			var rt struct {
				Endpoint string `json:"endpoint"`
			}
			if json.Unmarshal(rData, &rt) == nil && rt.Endpoint != "" {
				turns, _ := readSessionTurnsFixture(dir)
				var texts []string
				for _, t := range turns {
					for _, p := range t.Content.Parts {
						if p != nil {
							texts = append(texts, p.Text)
						}
					}
				}
				body, _ := json.Marshal(map[string]any{"prompt": strings.Join(texts, "\n\n")})
				_, _ = http.Post(rt.Endpoint, "application/json", bytes.NewReader(body))
			}
		}
	}

	f.mu.Lock()
	f.inFlight = true
	started := f.started
	hold := f.hold
	streamCtx, cancel := context.WithCancel(ctx)
	f.cancelFunc = cancel
	responses := make([]*agentv1.GenerateTurnStreamResponse, 0, len(f.streams))
	for _, fill := range f.streams {
		r := &agentv1.GenerateTurnStreamResponse{}
		fill(r)
		responses = append(responses, r)
	}
	f.mu.Unlock()

	if started != nil {
		select {
		case <-started:
		default:
			close(started)
		}
	}

	return &fakeTurnStream{
		ctx:       streamCtx,
		responses: responses,
		hold:      hold,
		onClose: func() {
			f.mu.Lock()
			f.inFlight = false
			f.cancelFunc = nil
			f.mu.Unlock()
		},
	}, nil
}

type fakeTurnStream struct {
	grpc.ClientStream
	ctx       context.Context
	responses []*agentv1.GenerateTurnStreamResponse
	i         int
	hold      chan struct{}
	onClose   func()
}

func (s *fakeTurnStream) Recv() (*agentv1.GenerateTurnStreamResponse, error) {
	if s.hold != nil {
		select {
		case <-s.ctx.Done():
			if s.onClose != nil {
				s.onClose()
				s.onClose = nil
			}
			return nil, s.ctx.Err()
		case <-s.hold:
		}
	}
	select {
	case <-s.ctx.Done():
		if s.onClose != nil {
			s.onClose()
			s.onClose = nil
		}
		return nil, s.ctx.Err()
	default:
	}

	if s.i >= len(s.responses) {
		if s.onClose != nil {
			s.onClose()
			s.onClose = nil
		}
		return nil, io.EOF
	}
	r := s.responses[s.i]
	s.i++
	return r, nil
}

type fakeSubscribeStream struct {
	grpc.ClientStream
	ctx context.Context
	ch  chan *agentv1.SubscribeSessionResponse
}

func (s *fakeSubscribeStream) Recv() (*agentv1.SubscribeSessionResponse, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case resp := <-s.ch:
		return resp, nil
	}
}

// readSessionTurns is the assertion-shaped view of the fixture: contents only, in order.
func readSessionTurns(agentDir string) ([]*genai.Content, error) {
	turns, err := readSessionTurnsFixture(agentDir)
	if err != nil {
		return nil, err
	}
	out := make([]*genai.Content, 0, len(turns))
	for _, t := range turns {
		out = append(out, t.Content)
	}
	return out, nil
}
