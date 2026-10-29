// Package procconfig is a processing configuration's stored content (rollouts.md §6.4): what ingest rebuilds and executes,
// and what the admin commands stage, list and activate.
package procconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Masks reports whether this release's ingest masks: until it does, no configuration naming a policy may be activated (rollouts.md §6.4).
const Masks = false

// MaskingFormatMinor is the format_minor a masking configuration's captures need.
const MaskingFormatMinor = 1

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
