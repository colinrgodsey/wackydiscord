package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Names the bot used to borrow from pkg/agent. They are literals on this side of the
// boundary now: keeping them local is what lets the bot build without linking the SDK.
const (
	// RootMarkerFile names the file that marks a directory as a wackypub workspace root.
	RootMarkerFile = "WACKYPUB_ROOT"
	// RuntimeConfigFileName is the per-agent runtime config file, read only for the
	// fields the protocol does not surface yet.
	RuntimeConfigFileName = "runtime.json"
	// EnvAgent2Agent and EnvCallChain are the A2A chain variables cleared at startup so
	// the bot never presents as running inside an agent-to-agent call.
	EnvAgent2Agent = "AGENT2AGENT"
	EnvCallChain   = "WACKYPUB_CALL_CHAIN"
)

// ResolveWorkspaceDir mirrors the CLI: an explicit --ws is used as given and must contain
// the marker, otherwise the marker is searched from the current directory upwards. The
// resolved value is the exact string the bot hands the spawned server as its working
// directory and as workspace_dir on every request, so nothing re-resolves per call.
func ResolveWorkspaceDir(wsFlag string, isExplicit bool) (string, error) {
	if isExplicit {
		clean := filepath.Clean(wsFlag)
		if !fileExists(filepath.Join(clean, RootMarkerFile)) {
			return "", fmt.Errorf("workspace directory %q does not contain %s marker file", wsFlag, RootMarkerFile)
		}
		return clean, nil
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}
	for dir := cwd; ; {
		if fileExists(filepath.Join(dir, RootMarkerFile)) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no %s marker file found in current directory (%s) or any parent directory", RootMarkerFile, cwd)
		}
		dir = parent
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// runtimeSummary reads the two runtime fields the bot displays that InspectAgent does not
// yet return. Deliberately narrow: one struct, two fields, no SDK config semantics, and it
// never gates anything - a failure just leaves the caller's "default" label in place.
// Follow-up: surface model and endpoint on InspectAgentResponse and delete this.
func runtimeSummary(agentDir string) (model string, endpoint string) {
	if agentDir == "" {
		return "", ""
	}
	data, err := os.ReadFile(filepath.Join(agentDir, RuntimeConfigFileName))
	if err != nil {
		return "", ""
	}
	var cfg struct {
		Model    string `json:"model"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", ""
	}
	return cfg.Model, cfg.Endpoint
}
