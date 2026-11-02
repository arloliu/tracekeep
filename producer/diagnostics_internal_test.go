package producer

import (
	"fmt"
	"slices"
	"sync"
)

// LineLog records every whole log line, its level, message and fields, as one string,
// and signals each on Seen while it has room; the package's external tests use it too.
type LineLog struct {
	mu    sync.Mutex
	lines []string
	Seen  chan string
}

// NewLineLog returns a LineLog whose Seen holds 64 lines.
func NewLineLog() *LineLog { return &LineLog{Seen: make(chan string, 64)} }

func (l *LineLog) line(level, msg string, args []any) {
	s := level + " " + msg + " " + fmt.Sprint(args...)
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
	select {
	case l.Seen <- s:
	default:
	}
}

// Info implements Logger.
func (l *LineLog) Info(msg string, args ...any) { l.line("INFO", msg, args) }

// Warn implements Logger.
func (l *LineLog) Warn(msg string, args ...any) { l.line("WARN", msg, args) }

// Error implements Logger.
func (l *LineLog) Error(msg string, args ...any) { l.line("ERROR", msg, args) }

// Lines returns the lines recorded so far.
func (l *LineLog) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.lines)
}
