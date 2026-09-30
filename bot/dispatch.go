package bot

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
)

// RemoteManifestFile names the workspace-root file that routes an agent id to a bridge
// command instead of a local agent folder (D116).
const RemoteManifestFile = "REMOTE_MANIFEST"

// routedAgentRoute reports whether agentID is routed to a bridge in the workspace manifest,
// returning the route line for error messages. A missing manifest or an unreadable one is
// reported as not-routed: the native path is the normal case and a malformed manifest will
// fail loudly where it is loaded, not here.
func routedAgentRoute(wsDir, agentID string) (string, bool) {
	if wsDir == "" || agentID == "" {
		return "", false
	}
	data, err := os.ReadFile(filepath.Join(wsDir, RemoteManifestFile))
	if err != nil {
		return "", false
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id, _, found := strings.Cut(line, ":")
		if found && strings.TrimSpace(id) == agentID {
			return line, true
		}
	}
	return "", false
}

// isBridged reports whether agentID is routed to an external bridge via REMOTE_MANIFEST.
func (b *Bot) isBridged(agentID string) bool {
	if b == nil || agentID == "" {
		return false
	}
	_, routed := routedAgentRoute(b.WsDir, agentID)
	return routed
}

// routedAgentBinary returns the binary command basename from the REMOTE_MANIFEST entry for agentID.
func routedAgentBinary(wsDir, agentID string) (string, bool) {
	line, routed := routedAgentRoute(wsDir, agentID)
	if !routed {
		return "", false
	}
	_, cmdPart, found := strings.Cut(line, ":")
	if !found {
		return "", false
	}
	fields := strings.Fields(cmdPart)
	if len(fields) == 0 {
		return "", false
	}
	return filepath.Base(fields[0]), true
}

// bridgeBinary returns the binary name of the bridge handling agentID, or "remote" if unspecified.
func (b *Bot) bridgeBinary(agentID string) string {
	if b == nil || agentID == "" {
		return ""
	}
	bin, routed := routedAgentBinary(b.WsDir, agentID)
	if !routed || bin == "" {
		return "remote"
	}
	return bin
}
