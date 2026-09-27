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
