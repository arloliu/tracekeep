package logfeed

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	tpbus "github.com/arloliu/go-secs/tracepack/bus"
)

// SchemaVersion is the field schema's version an entry's v names (logfeed.md §15.3).
const SchemaVersion = 1

// MaxName bounds an entry's source and file, which the catalog's keys hold (catalog.md §7.1).
const MaxName = 128

// The kinds of entry.
const (
	KindHeader       = "header"
	KindBody         = "body"
	KindEncrypted    = "encrypted"
	KindUnclassified = "unclassified"
)

// The directions a header names.
const (
	DirSent     = "sent"
	DirReceived = "received"
)

// ErrUnreadable is an entry tracekeep cannot place or time: it is counted and acknowledged, never converted (logfeed.md §15.3).
var ErrUnreadable = errors.New("logfeed: entry unreadable")

// ErrMalformed is an entry placed and timed whose kind's own fields are missing or malformed: it becomes an unrecognised-line annotation (§15.3).
var ErrMalformed = errors.New("logfeed: entry malformed for its kind")

// Entry is one entry of the field schema (logfeed.md §15.3).
type Entry struct {
	V           int
	EquipmentID string
	Source      string
	TZ          string
	File        string
	Offset      *int64
	TS          string
	Kind        string
	Dir         string
	Device      string
	Header      string
	TID         string
	SxFy        string
	Text        *string

	// Raw is the message as received, which an unrecognised-line annotation holds.
	Raw []byte
	// UTCNs is the logged time read in TZ; Uncertain marks a wall time that occurred twice or never (§15.5 *Time*).
	UTCNs     int64
	Uncertain bool
}

// At is the entry's offset, which Decode has checked is present.
func (e *Entry) At() int64 { return *e.Offset }

// Decode reads the message on the subject token token as an entry (logfeed.md §15.3).
// An entry it cannot place or time returns ErrUnreadable; one placed and timed whose kind's fields are malformed returns the entry with ErrMalformed.
// The fields that place and time an entry decode first, so a kind's field of the wrong type leaves the entry placed and malformed.
func Decode(token string, data []byte) (*Entry, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	e := &Entry{Raw: data}
	placing := map[string]any{
		"v": &e.V, "equipment_id": &e.EquipmentID, "source": &e.Source, "tz": &e.TZ, "file": &e.File, "offset": &e.Offset, "ts": &e.TS,
	}
	if err := decodeFields(fields, placing); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	if err := e.place(token); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	err := decodeFields(fields, map[string]any{
		"kind": &e.Kind, "dir": &e.Dir, "device": &e.Device, "header": &e.Header, "tid": &e.TID, "sxfy": &e.SxFy, "text": &e.Text,
	})
	if err == nil {
		err = e.kindFields()
	}
	if err != nil {
		return e, fmt.Errorf("%w: %w", ErrMalformed, err)
	}

	return e, nil
}

// decodeFields decodes each field present into its destination.
func decodeFields(fields map[string]json.RawMessage, into map[string]any) error {
	for name, dst := range into {
		raw, ok := fields[name]
		if !ok || string(raw) == "null" {
			continue
		}
		if err := json.Unmarshal(raw, dst); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}

	return nil
}

// place checks what places and times an entry: the version, its equipment against the subject, its source, file and offset, and its time.
func (e *Entry) place(token string) error {
	if e.V != SchemaVersion {
		return fmt.Errorf("schema version %d", e.V)
	}
	if e.EquipmentID == "" {
		return errors.New("no equipment_id")
	}
	if spelled, err := tpbus.EquipmentToken(e.EquipmentID); err != nil || spelled != token {
		return fmt.Errorf("equipment_id %q is not the subject's token %q", e.EquipmentID, token)
	}
	for name, v := range map[string]string{"source": e.Source, "file": e.File} {
		if v == "" || len(v) > MaxName {
			return fmt.Errorf("%s of %d bytes", name, len(v))
		}
	}
	if e.Offset == nil || *e.Offset < 0 {
		return errors.New("no offset")
	}
	var err error
	e.UTCNs, e.Uncertain, err = WallTime(e.TS, e.TZ)

	return err
}

// kindFields checks the fields an entry's kind needs.
func (e *Entry) kindFields() error {
	switch e.Kind {
	case KindHeader:
		if e.Dir != DirSent && e.Dir != DirReceived {
			return fmt.Errorf("header direction %q", e.Dir)
		}
		if err := checkSxFy(e.SxFy); err != nil {
			return err
		}
		if e.Header != "" {
			if b, err := hex.DecodeString(e.Header); err != nil || len(b) != 10 {
				return fmt.Errorf("header %q is not 10 bytes of hex", e.Header)
			}
		}
	case KindBody, KindEncrypted:
		if err := checkSxFy(e.SxFy); err != nil {
			return err
		}
		if e.Text == nil {
			return fmt.Errorf("%s without text", e.Kind)
		}
	case KindUnclassified:
		if e.Text == nil {
			return errors.New("unclassified entry without text")
		}
	default:
		return fmt.Errorf("kind %q", e.Kind)
	}

	return nil
}

// checkSxFy checks "SxFy": a stream of 0 to 127 and a function of 0 to 255.
func checkSxFy(s string) error {
	rest, ok := strings.CutPrefix(s, "S")
	st, fn, ok2 := strings.Cut(rest, "F")
	_, err1 := strconv.ParseUint(st, 10, 7)
	_, err2 := strconv.ParseUint(fn, 10, 8)
	if !ok || !ok2 || err1 != nil || err2 != nil {
		return fmt.Errorf("sxfy %q", s)
	}

	return nil
}

// tid is the item's transaction id, from either entry.
func tid(entries ...*Entry) string {
	for _, e := range entries {
		if e != nil && e.TID != "" {
			return e.TID
		}
	}

	return ""
}

// header is a logged HSMS header's bytes, false when the entry logged none.
func (e *Entry) header() ([10]byte, bool) {
	var h [10]byte
	b, err := hex.DecodeString(e.Header)
	if err != nil || len(b) != 10 {
		return h, false
	}
	copy(h[:], b)

	return h, true
}

// WallLayout is ts's layout: the logged wall time, rewritten by the pipeline without its zone, its fraction optional.
const WallLayout = "2006-01-02 15:04:05.999999999"

var zones sync.Map // name → *time.Location

func zone(name string) (*time.Location, error) {
	if l, ok := zones.Load(name); ok {
		if loc, ok := l.(*time.Location); ok {
			return loc, nil
		}
	}
	if name == "" {
		return nil, errors.New("no tz")
	}
	l, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	zones.Store(name, l)

	return l, nil
}

// WallTime reads the wall time ts in the zone tz as UTC nanoseconds (logfeed.md §15.5 *Time*):
// a wall time that occurs twice, or never, in a daylight-saving change takes the earlier reading and is uncertain.
func WallTime(ts, tz string) (utcNs int64, uncertain bool, err error) {
	loc, err := zone(tz)
	if err != nil {
		return 0, false, fmt.Errorf("tz %q: %w", tz, err)
	}
	wall, err := time.Parse(WallLayout, ts)
	if err != nil {
		return 0, false, fmt.Errorf("ts %q: %w", ts, err)
	}
	// A reading is the wall time less an offset in force around it; one whose zone shows that wall time is real.
	var readings, shown []time.Time
	for _, probe := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		_, off := wall.Add(probe).In(loc).Zone()
		t := wall.Add(-time.Duration(off) * time.Second)
		readings = append(readings, t)
		if lt := t.In(loc); sameWall(lt, wall) && !containsTime(shown, t) {
			shown = append(shown, t)
		}
	}
	if len(shown) == 1 {
		return shown[0].UnixNano(), false, nil
	}
	// Twice or never: the earlier of the two readings.
	return min(readings[0].UnixNano(), readings[1].UnixNano()), true, nil
}

func sameWall(a, b time.Time) bool {
	return a.Year() == b.Year() && a.YearDay() == b.YearDay() && a.Hour() == b.Hour() && a.Minute() == b.Minute() && a.Second() == b.Second()
}

func containsTime(ts []time.Time, t time.Time) bool {
	for _, x := range ts {
		if x.Equal(t) {
			return true
		}
	}

	return false
}
