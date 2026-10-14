package bot

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The stdio transport shares a pipe with the serving child, so a child that dies while a call
// is in flight arrives as an ordinary Unavailable whose description names the read failure.
// The tail varies with where the read lands: a pipe the bot already closed reads
// "read |0: file already closed", a kill caught between reads reads "EOF". Same event.
const (
	childDeathReadFailure = "error reading from server"
	childDeathClosedPipe  = "file already closed"
)

const (
	serviceRestartNotice = "⏳ **Agent service restarted mid-turn.** Your message did not get a reply - please send it again."
	serviceCrashNotice   = "💥 **Agent service stopped unexpectedly mid-turn.** Your message did not get a reply - please send it again. If it keeps happening, check the bot logs."
)

// isChildDeathError reports whether err is the serving child dying under a live call. The
// status code must be Unavailable as well as the text, so an unrelated failure that happens
// to mention a closed pipe still surfaces verbatim.
func isChildDeathError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) != codes.Unavailable {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, childDeathReadFailure) || strings.Contains(msg, childDeathClosedPipe)
}

// describeServiceError turns a mid-turn failure into channel text. ok is false when the
// failure is not child death, so callers keep rendering the real error: hiding an
// informative provider failure behind "please retry" would be worse than the raw line.
func (b *Bot) describeServiceError(err error) (string, bool) {
	if !isChildDeathError(err) {
		return "", false
	}
	if b.server.childTerminatedByUs() != killNotOurs {
		return serviceRestartNotice, true
	}
	return serviceCrashNotice, true
}
