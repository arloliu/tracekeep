package procconfig

import "testing"

// A policy-less configuration's bytes are the classifier and the frame limit alone; Decode reads every field back and refuses unknown ones.
func TestContent(t *testing.T) {
	t.Parallel()
	plain := Content{Classifier: "go-secs/v2.8.0", MaxFrameLen: 4096}
	if got := string(plain.Encode()); got != `{"classifier":"go-secs/v2.8.0","max_frame_len":4096}` {
		t.Errorf("policy-less content = %s", got)
	}
	m := Content{Classifier: "c", Policy: &PolicyRef{"baseline", 1, "ab"}, DigestKey: &KeyRef{"d-1", "f"}, FormatMinor: MaskingFormatMinor}
	back, err := Decode(m.Encode())
	if err != nil || string(back.Encode()) != string(m.Encode()) {
		t.Errorf("Decode = %+v, %v", back, err)
	}
	if _, err := Decode([]byte(`{"classifier":"c","max_frame_len":0,"masker":"x"}`)); err == nil {
		t.Error("Decode accepted an unknown field")
	}
}
