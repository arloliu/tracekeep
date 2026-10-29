// Package procconfig is a processing configuration's stored content (rollouts.md §6.4): what ingest rebuilds and executes,
// and what the admin commands stage, list and activate.
package procconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime/debug"
)

// Masks reports whether this release's ingest masks: until it does, no configuration naming a policy may be activated (rollouts.md §6.4).
const Masks = false

// MaskingFormatMinor is the format_minor a masking configuration's captures need.
const MaskingFormatMinor = 1

// NewestFormatMinor is the newest format_minor this release writes, tracepack v0.9.0's;
// a configuration needing a newer one is not executable here (rollouts.md §6.4).
const NewestFormatMinor = 2

// tracepackModule is the module whose version names the masker.
const tracepackModule = "github.com/arloliu/go-secs/tracepack"

// Masker is the masker this binary links, recorded by a masking configuration (rollouts.md §6.4):
// tracepack-redact of format major 1, at the tracepack module version the build reports, or unknown when it reports none.
// The record is informational: any conforming masker writes the same bytes.
var Masker = masker()

func masker() string {
	version := "unknown"
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == tracepackModule && d.Version != "" && d.Version != "(devel)" {
				version = d.Version
				if d.Replace != nil && d.Replace.Version != "" {
					version = d.Replace.Version
				}
			}
		}
	}

	return "tracepack-redact/1@" + version
}

// KeyRef names a key id and its material's fingerprint, as a configuration records them.
type KeyRef struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

// PolicyRef names a policy by id and version, with the SHA-256 of its canonical bytes in hex.
type PolicyRef struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	SHA256  string `json:"sha256"`
}

// Content is a processing configuration as the catalog stores it, its fields in this order and the empty ones omitted,
// so a policy-less configuration's bytes are the classifier and the frame limit alone and equal content is one configuration.
type Content struct {
	Classifier    string     `json:"classifier"`
	MaxFrameLen   uint64     `json:"max_frame_len"`
	Policy        *PolicyRef `json:"policy,omitempty"`
	DigestKey     *KeyRef    `json:"digest_key,omitempty"`
	EncryptionKey *KeyRef    `json:"encryption_key,omitempty"`
	FormatMinor   int        `json:"format_minor,omitempty"`
	// Masker records the masker a masking configuration was staged with; it does not decide which masker executes it.
	Masker string `json:"masker,omitempty"`
}

// Encode returns c's stored bytes.
func (c *Content) Encode() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("procconfig: %v", err)) // strings and integers only
	}

	return b
}

// Decode reads stored bytes back, refusing a field Content does not name.
func Decode(b []byte) (Content, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var c Content
	if err := dec.Decode(&c); err != nil {
		return Content{}, fmt.Errorf("procconfig: %w", err)
	}

	return c, nil
}
