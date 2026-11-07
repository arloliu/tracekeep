package logfeed

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/ingest"
	"github.com/arloliu/tracekeep/internal/procconfig"
	"github.com/arloliu/tracekeep/internal/store"
	"github.com/arloliu/tracekeep/producer"
)

// Dialect is a log capture's source_dialect, its suffix the field schema's version (logfeed.md §15.6).
const Dialect = "host-sml-feed/1"

// Notes is every log capture's notes: it claims no coverage (logfeed.md §15.1).
const Notes = "a live host log feed's conversion: it claims no byte-exact coverage of the log, and what the pipeline lost shows only as missing records"

// Msg is one delivery of the feed with its acknowledgement handle; a jetstream.Msg is one.
type Msg interface {
	Subject() string
	Data() []byte
	Ack() error
	Nak() error
	InProgress() error
}

// ErrGiveUp is an outcome the converter cannot resolve: its caller negatively acknowledges every held message,
// drops the converter and takes its lease again (logfeed.md §15.5 *Taking a lease*).
var ErrGiveUp = errors.New("logfeed: the converter gives up its lease")

// Converter converts one lease group's entries into log captures (logfeed.md §15.5–§15.7).
// It is not safe for concurrent use: one goroutine gives it its deliveries and ticks it.
type Converter struct {
	// Lease is the group's, which every transaction checks; Group is its number.
	Lease catalog.Lease
	Group int

	Catalog *catalog.Catalog
	Store   store.ObjectStore
	// KeyPrefix prefixes every key; WorkDir holds the segments being written; Writer names this writer in their metadata.
	KeyPrefix, WorkDir, Writer string
	// Configs are the processing configurations this replica executes, by id; Bindable admits those a log capture may bind.
	Configs  map[int64]ingest.Config
	Bindable func(configID int64) bool
	Keys     ingest.MaskKeys
	Devices  Devices
	// Flush is F; Reorder the reorder window; Pairing the pairing window; Idle the idle period;
	// AckWait bounds a flush's remote calls, as ingest.ack_wait bounds ingest's (ingest.md §6.2).
	Flush, Reorder, Idle, AckWait time.Duration
	Pairing                       int
	// MaxBatchBytes and MetaBytes make an equipment's flush due once its batches hold that many payload bytes or redaction entries' bytes;
	// zero takes ingest's defaults.
	MaxBatchBytes, MetaBytes int
	// HorizonHour returns the oldest hour retention keeps.
	HorizonHour func() int64
	Now         func() time.Time
	Log         *slog.Logger

	equipment map[string]*equipmentState
}

// handle is an entry's acknowledgement handles: its delivery's and its duplicates'.
type handle struct{ msgs []Msg }

// pend is an entry in its file's window, then, once converted, only its handle, which a redelivery joins until its flush is registered.
type pend struct {
	e         *Entry
	malformed bool
	h         *handle
}

type equipmentState struct {
	id        string
	files     map[fileKey]*fileState
	captures  map[catalog.LogKey]*captureState
	batches   map[batchKey]*batch
	parts     map[batchKey]int
	nextFlush time.Time
	// bytes and meta are the batches' payload bytes and redaction entries' bytes, which make a flush due.
	bytes, meta int
}

type fileKey struct{ source, file string }

type fileState struct {
	key     catalog.LogFile
	window  *Window[*pend]
	pairer  *Pairer
	replies Replies
	// pends are the entries released and not yet in a batch, by entry: an error before their batch leaves them here, which Abandon returns.
	pends map[*Entry]*pend
	// settled is the watermark registered, high the highest offset an item came out at; released marks the first release.
	settled, high int64
	released      bool
	lastArrival   time.Time
	// successor is the source's next dated file, once its first entry arrived; stopping marks a stop the next flush registers,
	// idle one judged idle, from the watermark idleSince; stopped refuses every later entry.
	successor *fileState
	stopping  bool
	idle      bool
	idleSince int64
	stopped   bool
}

type captureState struct {
	id       tracepack.UUID
	key      catalog.LogKey
	configID int64
	next     uint64
	started  bool
	stopped  bool
	meta     *tracepack.PackMeta
}

// batchKey is a batch's capture and hour, and its part: a batch closes before its records' redaction entries would pass half the reader's metadata budget,
// or its payload ingest's size threshold, and the next of its capture and hour opens (ingest.md §6.2); every part flushes with the equipment.
type batchKey struct {
	capture tracepack.UUID
	hour    int64
	part    int
}

type batch struct {
	c           *captureState
	hour        int64
	records     []tracepack.Record
	redaction   []tracepack.RedactionEntry
	handles     []*handle
	stop        bool
	bytes, meta int
}

// Receive takes one delivery at now (logfeed.md §15.5): an unreadable, beyond-retention or stale entry is acknowledged at once,
// a duplicate joins its held entry, and any other waits in its file's window.
func (c *Converter) Receive(ctx context.Context, m Msg, now time.Time) error {
	token := m.Subject()[strings.LastIndexByte(m.Subject(), '.')+1:]
	e, err := Decode(token, m.Data())
	if errors.Is(err, ErrUnreadable) {
		return ack(m)
	}
	malformed := errors.Is(err, ErrMalformed)
	if ingest.HourOf(e.UTCNs) < c.HorizonHour() {
		return ack(m)
	}
	f, err := c.file(ctx, e, now)
	if err != nil {
		return err
	}
	if f.stopped {
		return ack(m)
	}
	f.lastArrival = now
	p := &pend{e: e, malformed: malformed, h: &handle{msgs: []Msg{m}}}
	switch d, held := f.window.Add(e.At(), now, p); d {
	case Duplicate:
		held.h.msgs = append(held.h.msgs, m)
	case Stale:
		return ack(m)
	case Held:
	default:
		return fmt.Errorf("logfeed: disposition %d", d)
	}

	return nil
}

func ack(m Msg) error {
	if err := m.Ack(); err != nil {
		return fmt.Errorf("logfeed: acknowledge: %w", err)
	}

	return nil
}

// file is e's file, read from the catalog when first seen; a file of the source dated later becomes the successor of the source's files before it.
func (c *Converter) file(ctx context.Context, e *Entry, now time.Time) (*fileState, error) {
	if c.equipment == nil {
		c.equipment = map[string]*equipmentState{}
	}
	eq := c.equipment[e.EquipmentID]
	if eq == nil {
		eq = &equipmentState{id: e.EquipmentID, files: map[fileKey]*fileState{}, captures: map[catalog.LogKey]*captureState{},
			batches: map[batchKey]*batch{}, parts: map[batchKey]int{}, nextFlush: now.Add(c.Flush)}
		c.equipment[e.EquipmentID] = eq
	}
	k := fileKey{source: e.Source, file: e.File}
	if f := eq.files[k]; f != nil {
		return f, nil
	}
	lf := catalog.LogFile{Source: e.Source, EquipmentID: e.EquipmentID, File: e.File}
	st, err := c.Catalog.LogFileState(ctx, lf)
	if err != nil {
		return nil, err
	}
	f := &fileState{key: lf, window: NewWindow[*pend](c.Reorder, st.Watermark), pairer: NewPairer(c.Pairing), pends: map[*Entry]*pend{},
		settled: st.Watermark, high: st.Watermark, stopped: st.Stopped}
	eq.files[k] = f
	eq.linkSuccessors(e.Source)

	return f, nil
}

// linkSuccessors makes each file of source the successor of the one dated before it, whatever order they were first seen in (§15.6 *Boundaries*).
func (eq *equipmentState) linkSuccessors(source string) {
	var same []*fileState
	for k, f := range eq.files {
		if k.source == source {
			same = append(same, f)
		}
	}
	slices.SortFunc(same, func(a, b *fileState) int { return strings.Compare(a.key.File, b.key.File) })
	for i, f := range same {
		f.successor = nil
		if i+1 < len(same) {
			f.successor = same[i+1]
		}
	}
}

// Tick releases what each file's window holds past its wait, pairs and converts it, stops the files due to stop,
// and flushes each equipment whose flush is due (logfeed.md §15.5–§15.7).
func (c *Converter) Tick(ctx context.Context, now time.Time) error {
	requests, err := c.Catalog.LogStopRequests(ctx, c.Group)
	if err != nil {
		return err
	}
	for _, eq := range c.sortedEquipment() {
		files := eq.sortedFiles()
		for _, f := range files {
			if err := c.advance(ctx, eq, f, f.window.Release(now)); err != nil {
				return err
			}
		}
		// Judged once every file released, so a successor's first release this tick stops its predecessor in the same tick.
		for _, f := range files {
			if f.stopped || f.stopping {
				continue
			}
			c.judgeStop(f, now)
			if f.stopping {
				if err := c.advanceAll(ctx, eq, f); err != nil {
					return err
				}
			}
		}
		due := now.After(eq.nextFlush) || eq.stopDue() || eq.requested(requests) || c.full(eq) || eq.hourChanged()
		if due {
			if err := c.flush(ctx, eq, requests); err != nil {
				return err
			}
			eq.nextFlush = now.Add(c.Flush)
		}
	}

	return nil
}

// judgeStop marks f stopping once its successor's first entry has left its window and f's window is empty (§15.6 *Boundaries*),
// or once nothing of it arrived for the idle period.
func (c *Converter) judgeStop(f *fileState, now time.Time) {
	switch {
	case f.successor != nil && f.successor.released && f.window.Held() == 0:
		f.stopping = true
	case now.Sub(f.lastArrival) >= c.Idle && f.window.Held() == 0:
		f.stopping, f.idle, f.idleSince = true, true, f.settled
	default:
	}
}

// advanceAll drains a stopping file: its window, then its pairer, its pending headers ended without a body.
func (c *Converter) advanceAll(ctx context.Context, eq *equipmentState, f *fileState) error {
	if err := c.advance(ctx, eq, f, f.window.Drain()); err != nil {
		return err
	}

	return c.convert(ctx, eq, f, f.pairer.Flush())
}

// advance pairs released entries and converts the items they complete.
func (c *Converter) advance(ctx context.Context, eq *equipmentState, f *fileState, released []*pend) error {
	if len(released) > 0 {
		f.released = true
	}
	for _, p := range released {
		f.pends[p.e] = p
		if err := c.convert(ctx, eq, f, f.pairer.Add(p.e, p.malformed)); err != nil {
			return err
		}
	}

	return nil
}

// convert builds each item's record in its device's capture, admitting the capture at its first item, and batches it;
// an item's entries leave their file's pends only once batched or, for a stopped capture's key, acknowledged.
func (c *Converter) convert(ctx context.Context, eq *equipmentState, f *fileState, items []Item) error {
	for i := range items {
		it := &items[i]
		b, err := Build(it)
		if err != nil {
			return err
		}
		device := f.replies.Device(&c.Devices, it, &b)
		f.replies.Remember(it, &b, device)
		cs, err := c.capture(ctx, eq, f, device, it.Entries()[0])
		if err != nil {
			return err
		}
		var hs []*handle
		for _, e := range it.Entries() {
			hs = append(hs, f.pends[e].h)
		}
		if cs.stopped {
			// A stopped capture's key takes nothing more: its entries are stale (§15.11), and a redelivery of one is stale too.
			for _, e := range it.Entries() {
				if err := ackAll(f.pends[e].h); err != nil {
					return err
				}
				f.window.Forget(e.At())
			}
		} else {
			if !cs.started {
				if err := c.add(ctx, eq, cs, boundary(tracepack.BoundaryKindStart, b.Record.TSUTCNs), nil); err != nil {
					return err
				}
				cs.started = true
			}
			if err := c.add(ctx, eq, cs, c.classify(cs, b.Record), hs); err != nil {
				return err
			}
		}
		for _, e := range it.Entries() {
			f.high = max(f.high, e.At())
			f.pends[e].e = nil // the payload goes; the handle stays for a redelivery to join
			delete(f.pends, e)
		}
	}

	return nil
}

// classify judges a data record's frame with the capture's classifier: a frame it finds oversized is so recorded,
// one it cannot decode was built from a parse the log writer rejects (§15.5), and any other keeps reconstructed-ok.
func (c *Converter) classify(cs *captureState, r tracepack.Record) tracepack.Record {
	cls := c.Configs[cs.configID].Classifier
	if r.Kind != tracepack.KindData || cls == nil {
		return r
	}
	st, _ := cls.Frame(r.Payload)
	if st == tracepack.DecodeStatusOversized {
		r.DecodeStatus = st
	} else if st != tracepack.DecodeStatusOK && st != tracepack.DecodeStatusOKWithTrailing {
		r.DecodeStatus = tracepack.DecodeStatusBuildRejected
	}

	return r
}

func ackAll(h *handle) error {
	for _, m := range h.msgs {
		if err := ack(m); err != nil {
			return err
		}
	}

	return nil
}

// boundary is a capture-boundary event record of kind at ts.
func boundary(kind tracepack.BoundaryKind, ts int64) tracepack.Record {
	ev := tracepack.TransportEvent{Event: tracepack.EventCaptureBoundary, BoundaryKind: &kind}
	payload, _ := ev.MarshalBinary() // a boundary event always encodes

	return tracepack.Record{TSUTCNs: ts, Kind: tracepack.KindTransportEvent, Dir: tracepack.DirLocal,
		Fidelity: tracepack.FidelityNotApplicable, DecodeStatus: tracepack.DecodeStatusNotApplicable, Payload: payload}
}

// capture is the capture of f's device, admitted on its first item (logfeed.md §15.6): found by its key after a restart,
// with its next seq after its highest; a capture the catalog finds stopped takes nothing more.
func (c *Converter) capture(ctx context.Context, eq *equipmentState, f *fileState, device string, first *Entry) (*captureState, error) {
	key := catalog.LogKey{LogFile: f.key, Device: device}
	if cs := eq.captures[key]; cs != nil {
		return cs, nil
	}
	found, ok, err := c.Catalog.LogCaptureOf(ctx, key)
	if err != nil {
		return nil, err
	}
	meta := c.descriptor(key, first)
	cs := &captureState{key: key, meta: meta}
	if ok {
		// Started once a record of it is registered: one admitted before a crash and never flushed writes its start at seq 0.
		cs.id, cs.configID, cs.started, meta.PreviousCaptureID = found.ID, found.ConfigID, found.HasHighestSeq, found.Previous
		cs.stopped = found.State != catalog.StateOpen
		if found.HasHighestSeq {
			cs.next = uint64(found.HighestSeq) + 1 //nolint:gosec // a stored seq is below 2^63
		}
		if err := c.executable(cs.configID); err != nil {
			return nil, err
		}
		eq.captures[key] = cs

		return cs, nil
	}
	if meta.PreviousCaptureID, err = c.Catalog.LogPredecessor(ctx, key.EquipmentID, meta.RecorderInstanceID, first.UTCNs); err != nil {
		return nil, err
	}
	id := newID(c.Now())
	// No descriptor bytes: a bus descriptor serves bus captures only, and each segment carries the capture's metadata (logfeed.md §15.6).
	d := &catalog.Descriptor{EquipmentID: key.EquipmentID, RecorderInstanceID: meta.RecorderInstanceID, Device: device,
		Source: key.Source, OriginNS: first.UTCNs, PreviousCaptureID: meta.PreviousCaptureID,
		CaptureMethod: tracepack.CaptureMethodLog.String(), Vantage: tracepack.VantageHost.String(), SourceDialect: Dialect}
	res, err := c.Catalog.AdmitLog(ctx, c.Lease, key, id, d, c.Bindable)
	if errors.Is(err, catalog.ErrNoBindableConfig) || errors.Is(err, catalog.ErrLeaseLost) || errors.Is(err, catalog.ErrFileStopped) {
		// No masking configuration yet, the lease lost, or the file stopped by another holder: the entries wait for a new start.
		return nil, fmt.Errorf("%w: %w", ErrGiveUp, err)
	}
	if err != nil {
		return nil, err
	}
	if res.Found {
		// Another holder admitted the key since the lookup: read it back as found.
		return c.capture(ctx, eq, f, device, first)
	}
	cs.id, cs.configID = res.Capture, res.ConfigID
	if err := c.executable(cs.configID); err != nil {
		return nil, err
	}
	eq.captures[key] = cs

	return cs, nil
}

// executable refuses a capture bound to a configuration this replica does not execute: its records would go unmasked.
func (c *Converter) executable(configID int64) error {
	if _, ok := c.Configs[configID]; !ok {
		return fmt.Errorf("%w: capture bound to configuration %d, which this replica does not execute", ErrGiveUp, configID)
	}

	return nil
}

// newID is a UUIDv7 at now: the time first, so ids sort by creation.
func newID(now time.Time) tracepack.UUID {
	var id tracepack.UUID
	_, _ = rand.Read(id[:])
	ms := uint64(now.UnixMilli()) //nolint:gosec // a time after 1970
	for i := range 6 {
		id[i] = byte(ms >> (40 - 8*i))
	}
	id[6], id[8] = id[6]&0x0f|0x70, id[8]&0x3f|0x80

	return id
}

// descriptor is the capture-level metadata of key's capture, which every segment of it carries (logfeed.md §15.6):
// no HSMS session fields and no capture origin, which a log does not give; its zone first's.
func (c *Converter) descriptor(key catalog.LogKey, first *Entry) *tracepack.PackMeta {
	tz, dialect, notes, name, device := first.TZ, Dialect, Notes, key.Source, key.Device

	return &tracepack.PackMeta{
		EquipmentID: key.EquipmentID, Transport: tracepack.TransportHSMSSS, CaptureMethod: tracepack.CaptureMethodLog, Vantage: tracepack.VantageHost,
		Recorder: "tracekeep", TimeSource: tracepack.TimeSourceSourceLog, SourceTZ: &tz, SourceDialect: &dialect,
		SourceRefs: []string{key.Source + "/" + key.File}, LifecycleCoverage: tracepack.LifecycleCoverageNone, Notes: &notes,
		RecorderInstanceID: producer.RecorderInstanceID(key.EquipmentID, key.Device, key.Source), RecorderInstanceName: &name, Device: &device,
	}
}

// add assigns r its capture's next seq, masks it with the capture's configuration and puts it in its (capture, hour) batch.
func (c *Converter) add(ctx context.Context, eq *equipmentState, cs *captureState, r tracepack.Record, hs []*handle) error {
	if err := c.executable(cs.configID); err != nil {
		return err
	}
	r.Seq = cs.next
	cs.next++
	var entries []tracepack.RedactionEntry
	if cfg := c.Configs[cs.configID]; cfg.Masking != nil {
		masked, red, err := cfg.Masking.Mask(ctx, cs.id, &r, false, c.Keys)
		if err != nil {
			// A record the configuration cannot mask, or keys unavailable: nothing of the group is acknowledged, and its entries return.
			return fmt.Errorf("%w: mask: %w", ErrGiveUp, err)
		}
		r, entries = masked, red
	}
	ch := batchKey{capture: cs.id, hour: ingest.HourOf(r.TSUTCNs)}
	k := ch
	k.part = eq.parts[ch]
	maxBytes, maxMeta := c.limits()
	meta := ingest.RedactionBytes(entries)
	if b := eq.batches[k]; b != nil && len(b.records) > 0 && (b.meta+meta > maxMeta || b.bytes+len(r.Payload) > maxBytes) {
		eq.parts[ch]++
		k.part = eq.parts[ch]
	}
	b := eq.batches[k]
	if b == nil {
		b = &batch{c: cs, hour: k.hour}
		eq.batches[k] = b
	}
	b.records = append(b.records, r)
	b.redaction = append(b.redaction, entries...)
	b.handles = append(b.handles, hs...)
	b.bytes += len(r.Payload)
	b.meta += meta
	eq.bytes += len(r.Payload)
	eq.meta += meta
	if r.Kind == tracepack.KindTransportEvent && isStop(&r) {
		b.stop = true
	}

	return nil
}

func isStop(r *tracepack.Record) bool {
	ev, err := tracepack.UnmarshalTransportEvent(r.Payload)

	return err == nil && ev.Event == tracepack.EventCaptureBoundary && ev.BoundaryKind != nil && *ev.BoundaryKind == tracepack.BoundaryKindStop
}

// limits are a batch's payload bytes and redaction entries' bytes: ingest's size threshold and half the reader's metadata budget (ingest.md §6.2).
func (c *Converter) limits() (maxBytes, maxMeta int) {
	maxBytes, maxMeta = c.MaxBatchBytes, c.MetaBytes
	if maxBytes == 0 {
		maxBytes = 64 << 20
	}
	if maxMeta == 0 {
		maxMeta = ingest.DefaultBatchMetaBytes
	}

	return maxBytes, maxMeta
}

// full reports eq's batches holding a limit's worth, which makes its flush due.
func (c *Converter) full(eq *equipmentState) bool {
	maxBytes, maxMeta := c.limits()

	return eq.bytes >= maxBytes || eq.meta >= maxMeta
}

// hourChanged reports batches of more than one hour, whose earlier ones are flushed at the hour change (ingest.md §6.2).
func (eq *equipmentState) hourChanged() bool {
	hours := map[int64]bool{}
	for k := range eq.batches {
		hours[k.hour] = true
	}

	return len(hours) > 1
}

// stopDue reports a file of eq that is stopping, which its next flush registers.
func (eq *equipmentState) stopDue() bool {
	for _, f := range eq.files {
		if f.stopping && !f.stopped {
			return true
		}
	}

	return false
}

// requested reports an open capture of eq an operator asked to stop.
func (eq *equipmentState) requested(ids []tracepack.UUID) bool {
	for _, cs := range eq.captures {
		if cs != nil && !cs.stopped && slices.Contains(ids, cs.id) {
			return true
		}
	}

	return false
}

func (c *Converter) sortedEquipment() []*equipmentState {
	return slices.SortedFunc(maps.Values(c.equipment), func(a, b *equipmentState) int { return strings.Compare(a.id, b.id) })
}

func (eq *equipmentState) sortedFiles() []*fileState {
	return slices.SortedFunc(maps.Values(eq.files), func(a, b *fileState) int {
		return strings.Compare(a.key.Source+"/"+a.key.File, b.key.Source+"/"+b.key.File)
	})
}

// flush writes every batch of eq as a segment, with the stops due, puts them and registers them in one transaction,
// then acknowledges every entry they hold (logfeed.md §15.7).
func (c *Converter) flush(ctx context.Context, eq *equipmentState, requests []tracepack.UUID) error {
	if err := c.stops(ctx, eq, requests); err != nil {
		return err
	}
	if c.AckWait > 0 {
		// A flush not registered within ack_wait is given up, as ingest's is (ingest.md §6.2); the local file work is not interrupted.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.AckWait)
		defer cancel()
	}
	if len(eq.batches) == 0 {
		return nil
	}
	keys := slices.SortedFunc(maps.Keys(eq.batches), func(a, b batchKey) int {
		if x := slices.Compare(a.capture[:], b.capture[:]); x != 0 {
			return x
		}
		if a.hour != b.hour {
			return int(a.hour - b.hour)
		}

		return a.part - b.part
	})
	reg := &catalog.LogFlush{HorizonHour: c.HorizonHour(), Files: eq.marks()}
	var written []string
	defer func() {
		for _, p := range written {
			_ = os.Remove(p)
		}
	}()
	for _, k := range keys {
		p, path, err := c.segment(ctx, eq.batches[k])
		if path != "" {
			written = append(written, path)
		}
		if err != nil {
			return err
		}
		reg.Packs = append(reg.Packs, p)
	}
	out, err := c.Catalog.RegisterLogFlush(ctx, c.Lease, reg)
	if err != nil {
		return fmt.Errorf("%w: register: %w", ErrGiveUp, err)
	}
	if out != catalog.Registered && out != catalog.AlreadyRegistered {
		return fmt.Errorf("%w: register: outcome %d", ErrGiveUp, out)
	}
	for _, k := range keys {
		for _, h := range eq.batches[k].handles {
			if err := ackAll(h); err != nil {
				return err
			}
		}
	}
	eq.batches, eq.parts, eq.bytes, eq.meta = map[batchKey]*batch{}, map[batchKey]int{}, 0, 0
	c.settle(eq)

	return nil
}

// marks are what a flush of eq settles of each file: its new watermark, and its stop.
func (eq *equipmentState) marks() []catalog.LogFileMark {
	var out []catalog.LogFileMark
	for _, f := range eq.sortedFiles() {
		if f.high > f.settled || f.stopping && !f.stopped {
			m := catalog.LogFileMark{LogFile: f.key, Watermark: f.high, Stop: f.stopping}
			if f.idle {
				m.IdleSince = &f.idleSince
			}
			out = append(out, m)
		}
	}

	return out
}

// settle records a registered flush in eq's files: their watermarks, their stops, and a stopped, settled file forgotten.
func (c *Converter) settle(eq *equipmentState) {
	for k, f := range eq.files {
		f.settled = max(f.settled, f.high)
		f.window.Settle(f.settled)
		if f.stopping {
			f.stopping, f.stopped = false, true
			f.window.Stop()
		}
		if f.stopped && f.window.Held() == 0 && f.pairer.Waiting() == 0 {
			// A stopped, settled file is forgotten with its captures: a later entry of it reads the file's stop from the catalog.
			delete(eq.files, k)
			for key := range eq.captures {
				if key.Source == k.source && key.File == k.file {
					delete(eq.captures, key)
				}
			}
		}
	}
	if len(eq.files) == 0 {
		delete(c.equipment, eq.id)
	}
}

// stops appends a stop boundary to every open capture of a stopping file and to every capture an operator asked to stop.
func (c *Converter) stops(ctx context.Context, eq *equipmentState, requests []tracepack.UUID) error {
	nowNs := c.Now().UnixNano()
	for _, cs := range eq.captures {
		if cs == nil || cs.stopped || !cs.started {
			continue
		}
		f := eq.files[fileKey{source: cs.key.Source, file: cs.key.File}]
		if (f != nil && f.stopping) || slices.Contains(requests, cs.id) {
			if err := c.add(ctx, eq, cs, boundary(tracepack.BoundaryKindStop, nowNs), nil); err != nil {
				return err
			}
			cs.stopped = true
		}
	}

	return nil
}

// segment writes b as a generation-0 segment and puts it under its staging key; path is its working file, removed by the caller.
func (c *Converter) segment(ctx context.Context, b *batch) (*catalog.Pack, string, error) {
	cfg := c.Configs[b.c.configID]
	meta := *b.c.meta
	interval := uint64(c.Flush.Nanoseconds()) //nolint:gosec // a positive duration
	meta.Writer, meta.PackRole, meta.SeqStart = c.Writer, tracepack.PackRoleSegment, b.records[0].Seq
	meta.PeriodStart, meta.PeriodEnd = b.hour*int64(time.Hour), (b.hour+1)*int64(time.Hour)
	meta.FlushIntervalNs, meta.ScopeGeneration = &interval, new(uint64)
	meta.Classifiers, meta.MaxFrameLens = []string{cfg.Name}, []uint64{cfg.MaxFrameLen}
	if cfg.Policy != nil {
		meta.RedactionPolicy = procconfig.RedactionPolicy(cfg.Policy.ID, cfg.Policy.Version, cfg.DigestKey.ID, cfg.EncryptionKey.ID)
		meta.Redaction = b.redaction
	}
	id := newID(c.Now())
	w, err := os.CreateTemp(c.WorkDir, "logfeed-*.tpk")
	if err != nil {
		return nil, "", err
	}
	defer w.Close()
	facts := tracepack.PackFacts{DurableRecorder: true, AnyClassified: true}
	for i := range b.records {
		if b.records[i].Quality.HasAny(tracepack.QualityRedacted) {
			facts.AnyRedacted = true
		}
		if b.records[i].DecodeStatus == tracepack.DecodeStatusOversized {
			facts.AnyOversized = true
		}
	}
	tw, err := tracepack.NewWriter(w, tracepack.WriterOptions{Meta: &meta, Facts: facts, PackID: id, CaptureID: b.c.id, Sync: w, Now: c.Now})
	if err != nil {
		return nil, w.Name(), fmt.Errorf("%w: writer: %w", ErrGiveUp, err)
	}
	for i := range b.records {
		if err := tw.Append(&b.records[i]); err != nil {
			return nil, w.Name(), fmt.Errorf("%w: append seq %d: %w", ErrGiveUp, b.records[i].Seq, err)
		}
	}
	if _, err := tw.Close(); err != nil {
		return nil, w.Name(), fmt.Errorf("%w: close: %w", ErrGiveUp, err)
	}
	size, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, w.Name(), err
	}
	// The working file reopened with every reader's budget gives the format minor the writer chose.
	r, err := tracepack.Open(ctx, w, size, tracepack.ReaderOptions{})
	if err != nil {
		return nil, w.Name(), fmt.Errorf("%w: reopen: %w", ErrGiveUp, err)
	}
	minor := int(r.Header().FormatMinor)
	first := b.records[0]
	key, err := tracepack.SegmentKey(c.KeyPrefix, b.c.key.EquipmentID, b.c.id, first.Seq, id)
	if err != nil {
		return nil, w.Name(), err
	}
	if _, err := w.Seek(0, io.SeekStart); err != nil {
		return nil, w.Name(), err
	}
	if err := c.Store.Put(ctx, key, w, size); err != nil {
		// An unknown outcome: an object the put may have made is the reconciler's, which deletes a log capture's orphan (§15.8).
		return nil, w.Name(), fmt.Errorf("%w: put: %w", ErrGiveUp, err)
	}

	return &catalog.Pack{
		ID: id, Capture: b.c.id, EquipmentID: b.c.key.EquipmentID, Hour: b.hour, Role: catalog.RoleSegment, Key: key, Size: size,
		SeqStart: first.Seq, FormatMinor: minor, Stop: b.stop, Ranges: runs(b.records),
	}, w.Name(), nil
}

// runs are the exact runs of consecutive seqs records hold, sorted by seq, with each run's endpoint times (catalog.md §7.1 pack_ranges).
func runs(records []tracepack.Record) []catalog.Range {
	var out []catalog.Range
	for _, r := range records {
		if n := len(out); n > 0 && out[n-1].SeqTo+1 == r.Seq {
			out[n-1].SeqTo, out[n-1].TSTo = r.Seq, r.TSUTCNs
			continue
		}
		out = append(out, catalog.Range{SeqFrom: r.Seq, SeqTo: r.Seq, TSFrom: r.TSUTCNs, TSTo: r.TSUTCNs})
	}

	return out
}

// Heartbeat keeps every held delivery in progress, as ingest's heartbeat does (architecture.md §3.1).
func (c *Converter) Heartbeat() {
	for _, eq := range c.equipment {
		for _, f := range eq.files {
			for _, p := range f.window.All() {
				for _, m := range p.h.msgs {
					_ = m.InProgress()
				}
			}
		}
		for _, b := range eq.batches {
			for _, h := range b.handles {
				for _, m := range h.msgs {
					_ = m.InProgress()
				}
			}
		}
	}
}

// Abandon negatively acknowledges every held delivery, which the stream redelivers, and drops every state: the converter gives up its lease.
func (c *Converter) Abandon() {
	seen := map[*handle]bool{}
	nak := func(h *handle) {
		if h == nil || seen[h] {
			return
		}
		seen[h] = true
		for _, m := range h.msgs {
			_ = m.Nak()
		}
	}
	for _, eq := range c.equipment {
		for _, f := range eq.files {
			for _, p := range f.window.All() {
				nak(p.h)
			}
		}
		for _, b := range eq.batches {
			for _, h := range b.handles {
				nak(h)
			}
		}
	}
	c.equipment = nil
}
