package keys

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// maxFile bounds a key file's read.
const maxFile = 64 << 10

// File is the file provider (masking-keys.md §9): a file an operator or a Vault Agent places, mode 0600, holding base64 text.
// A file any other user may read is refused; it has no versions.
type File struct{}

// Material implements Provider; ref is the file's path.
func (File) Material(_ context.Context, ref, version string) ([]byte, string, error) {
	if version != "" {
		return nil, "", fmt.Errorf("file provider: no versions, asked for %q", version)
	}
	f, err := os.Open(ref)
	if err != nil {
		return nil, "", fmt.Errorf("file provider: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("file provider: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, "", fmt.Errorf("file provider: %s is mode %s, not a regular file of mode 0600", ref, info.Mode())
	}
	text, err := io.ReadAll(io.LimitReader(f, maxFile))
	if err != nil {
		return nil, "", fmt.Errorf("file provider: %w", err)
	}
	m, err := decode(string(text))
	if err != nil {
		return nil, "", fmt.Errorf("file provider: %s: %w", ref, err)
	}

	return m, "", nil
}

// Env is the env provider (masking-keys.md §9), for development only: an environment variable holding base64 text.
// It has no versions.
type Env struct{}

// Material implements Provider; ref is the variable's name.
func (Env) Material(_ context.Context, ref, version string) ([]byte, string, error) {
	if version != "" {
		return nil, "", fmt.Errorf("env provider: no versions, asked for %q", version)
	}
	text, ok := os.LookupEnv(ref)
	if !ok {
		return nil, "", fmt.Errorf("env provider: %s is not set", ref)
	}
	m, err := decode(text)
	if err != nil {
		return nil, "", fmt.Errorf("env provider: %s: %w", ref, err)
	}

	return m, "", nil
}

// decode reads material from base64 text in standard encoding, surrounding white space ignored;
// its error never quotes the text.
func decode(text string) ([]byte, error) {
	m, err := base64.StdEncoding.DecodeString(strings.TrimSpace(text))
	if err != nil {
		return nil, errors.New("not base64 text in standard encoding")
	}

	return m, nil
}
