package keys

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
)

// maxFile bounds a key file's read.
const maxFile = 64 << 10

// File is the file provider (masking-keys.md §9): a regular file an operator or a Vault Agent places, mode 0600 exactly,
// holding base64 text of at most 64 KiB; any other file is refused. It has no versions.
// An absent file is missing and any file it refuses is malformed, both refusals; another read failure is unavailable.
type File struct{}

// Material implements Provider; ref is the file's path.
func (File) Material(_ context.Context, ref, version string) ([]byte, string, error) {
	fail := func(reason, detail string) ([]byte, string, error) {
		return nil, "", &ProviderError{Provider: ProviderFile, Op: "read", Reason: reason, Detail: detail}
	}
	if version != "" {
		return fail("malformed", "no versions")
	}
	f, err := os.Open(ref)
	if errors.Is(err, fs.ErrNotExist) {
		return fail("missing", "")
	} else if err != nil {
		return fail("unreachable", "")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fail("unreachable", "")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return fail("malformed", fmt.Sprintf("mode %s, not a regular file of mode 0600", info.Mode()))
	}
	text, err := io.ReadAll(io.LimitReader(f, maxFile+1))
	if err != nil {
		return fail("unreachable", "")
	}
	if len(text) > maxFile {
		return fail("malformed", fmt.Sprintf("more than %d bytes", maxFile))
	}
	m, err := decode(string(text))
	if err != nil {
		return fail("malformed", err.Error())
	}

	return m, "", nil
}

// Env is the env provider (masking-keys.md §9), for development only: an environment variable holding base64 text.
// It has no versions; an unset variable is missing and anything else it refuses is malformed.
type Env struct{}

// Material implements Provider; ref is the variable's name.
func (Env) Material(_ context.Context, ref, version string) ([]byte, string, error) {
	fail := func(reason, detail string) ([]byte, string, error) {
		return nil, "", &ProviderError{Provider: ProviderEnv, Op: "read", Reason: reason, Detail: detail}
	}
	if version != "" {
		return fail("malformed", "no versions")
	}
	text, ok := os.LookupEnv(ref)
	if !ok {
		return fail("missing", "")
	}
	m, err := decode(text)
	if err != nil {
		return fail("malformed", err.Error())
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
