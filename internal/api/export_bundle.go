package api

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/records"
	"github.com/arloliu/tracekeep/internal/store"
)

// ExportBounds bound an export job (api.md §10.1 *Exports*); zero fields take the defaults.
type ExportBounds struct {
	// MaxHours bounds a job's window (default 744); MaxPacks and MaxBytes the packs and object bytes it exports (10,000, 10 GiB);
	// MaxBundleBytes its work directory (32 GiB).
	MaxHours       int
	MaxPacks       int
	MaxBytes       int64
	MaxBundleBytes int64
}

func (b ExportBounds) withDefaults() ExportBounds {
	return ExportBounds{
		MaxHours: cmp.Or(b.MaxHours, 744), MaxPacks: cmp.Or(b.MaxPacks, 10000),
		MaxBytes: cmp.Or(b.MaxBytes, int64(10)<<30), MaxBundleBytes: cmp.Or(b.MaxBundleBytes, int64(32)<<30),
	}
}

// errNoHour answers an hour that holds nothing to name.
var errNoHour = errors.New("an hour without a scope")

// errBounds fails a job whose bundle passes its bounds.
var errBounds = errors.New("the export passes its bounds")

// exportRequest is a job's request as its row keeps it.
type exportRequest struct {
	Selector exportSelector `json:"selector"`
	Kind     string         `json:"kind"`
}

type exportSelector struct {
	ToolID  string   `json:"tool_id"`
	Devices []string `json:"devices,omitempty"`
	Capture string   `json:"capture_id,omitempty"`
	From    int64    `json:"from"`
	To      int64    `json:"to"`
}

func (r *exportRequest) selector() (catalog.Selector, error) {
	sel := catalog.Selector{ToolID: r.Selector.ToolID, Devices: r.Selector.Devices, From: r.Selector.From, To: r.Selector.To}
	if r.Selector.Capture != "" {
		id, err := tracepack.ParseUUID(r.Selector.Capture)
		if err != nil {
			return catalog.Selector{}, err
		}
		sel.Capture = &id
	}

	return sel, nil
}

// manifest is a bundle's manifest.json.
type manifest struct {
	Kind          string            `json:"kind"`
	Job           string            `json:"job"`
	Selector      exportSelector    `json:"selector"`
	BundleBuiltAt string            `json:"bundle_built_at"`
	Note          string            `json:"note"`
	Captures      []manifestCapture `json:"captures"`
}

type manifestCapture struct {
	Capture  json.RawMessage `json:"capture"`
	Withheld bool            `json:"withheld,omitempty"`
	// Completeness is the capture's in the window, read when the bundle reached the capture.
	Completeness json.RawMessage `json:"completeness,omitempty"`
	Hours        []manifestHour  `json:"hours,omitempty"`
}

type manifestHour struct {
	Hour    string         `json:"hour"`
	Evicted bool           `json:"evicted,omitempty"`
	Packs   []manifestPack `json:"packs,omitempty"`
	LeftOut []leftOut      `json:"left_out,omitempty"`
}

type manifestPack struct {
	PackID      string   `json:"pack_id"`
	Role        string   `json:"role"`
	State       string   `json:"state"`
	FormatMinor int      `json:"format_minor"`
	Schema      string   `json:"schema"`
	File        string   `json:"file"`
	Incomplete  []string `json:"incomplete,omitempty"`
	FooterError string   `json:"footer_error,omitempty"`
}

type leftOut struct {
	PackID string `json:"pack_id"`
	Reason string `json:"reason"`
}

// bundle builds one attempt's bundle under dir.
type bundle struct {
	s      *Server
	bounds ExportBounds
	dir    string
	tar    *tar.Writer
	file   *os.File
	// exported, bytes and written count what the attempt exported and wrote, retries included.
	exported int
	bytes    int64
	written  int64
	manifest manifest
}

// buildBundle writes job's bundle for req into dir/bundle.tar and returns its path and size.
func (s *Server) buildBundle(ctx context.Context, job tracepack.UUID, req *exportRequest, dir string) (string, int64, error) {
	sel, err := req.selector()
	if err != nil {
		return "", 0, err
	}
	path := filepath.Join(dir, "bundle.tar")
	f, err := os.Create(path)
	if err != nil {
		return "", 0, fmt.Errorf("api: export work file: %w", err)
	}
	defer f.Close()
	b := &bundle{
		s: s, bounds: s.Exports.Bounds.withDefaults(), dir: dir, file: f,
		manifest: manifest{
			Kind: "canonical", Job: job.String(), Selector: req.Selector,
			Note: "each pack is exported whole, so a pack's file holds the records of its hour outside the window too",
		},
	}
	// The tar's own bytes, headers, padding and trailer, count against the work directory's bound as the JSONL's do.
	b.tar = tar.NewWriter(&limited{w: f, b: b})
	caps, err := s.Catalog.ListCaptures(ctx, sel, s.QuietPeriod)
	if err != nil {
		return "", 0, err
	}
	slices.SortFunc(caps, func(a, b catalog.CaptureInfo) int {
		return cmp.Or(cmp.Compare(a.OriginNS, b.OriginNS), bytes.Compare(a.ID[:], b.ID[:]))
	})
	for i := range caps {
		if err := b.capture(ctx, &caps[i], sel.From, sel.To); err != nil {
			return "", 0, err
		}
	}
	b.manifest.BundleBuiltAt = time.Now().UTC().Format(time.RFC3339Nano)
	m, err := json.MarshalIndent(&b.manifest, "", "  ")
	if err != nil {
		return "", 0, err
	}
	if err := b.add("manifest.json", bytes.NewReader(m), int64(len(m))); err != nil {
		return "", 0, err
	}
	if err := b.tar.Close(); err != nil {
		return "", 0, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return "", 0, err
	}

	return path, size, f.Sync()
}

// capture adds c's hours of [from, to).
func (b *bundle) capture(ctx context.Context, c *catalog.CaptureInfo, from, to int64) error {
	mc := manifestCapture{}
	var err error
	if mc.Capture, err = protojson.Marshal(captureOf(c)); err != nil {
		return err
	}
	// Its completeness first: a withheld capture keeps it, without its packs.
	comp, err := b.s.Catalog.Completeness(ctx, c.ID)
	if err != nil {
		return err
	}
	if mc.Completeness, err = protojson.Marshal(completenessOf(inWindow(&comp, meeting{from: from, to: to}))); err != nil {
		return err
	}
	withheld, err := b.s.withholds(ctx, c.ConfigID)
	if err != nil {
		return err
	}
	if withheld {
		mc.Withheld = true
		b.manifest.Captures = append(b.manifest.Captures, mc)

		return nil
	}
	for h := floorHour(from); h <= floorHour(to-1); h++ {
		mh, err := b.hour(ctx, c.ID, h)
		if errors.Is(err, errNoHour) {
			continue
		} else if err != nil {
			return err
		}
		mc.Hours = append(mc.Hours, *mh)
	}
	b.manifest.Captures = append(b.manifest.Captures, mc)

	return nil
}

// hour exports one hour, the unit of retry: its packs' files join the tar only once all are exported,
// and an object found gone discards them and exports the hour again from its read set read again, once.
// It answers errNoHour for an hour without a scope that retention did not evict, which holds nothing to name.
func (b *bundle) hour(ctx context.Context, capture tracepack.UUID, h int64) (*manifestHour, error) {
	for try := 0; ; try++ {
		rs, err := b.readSet(ctx, capture, h)
		if err != nil {
			return nil, err
		}
		mh := &manifestHour{Hour: time.Unix(0, h*hourNS).UTC().Format(time.RFC3339)}
		if !rs.Scoped {
			if !rs.Evicted {
				return nil, errNoHour
			}
			mh.Evicted = true

			return mh, nil
		}
		hourDir := filepath.Join(b.dir, "hour")
		if err := os.RemoveAll(hourDir); err != nil {
			return nil, err
		}
		if err := os.Mkdir(hourDir, 0o700); err != nil {
			return nil, err
		}
		gone, err := b.packs(ctx, &rs, hourDir, mh)
		switch {
		case err != nil:
			return nil, err
		case gone && try == 0:
			continue // the hour again, from its read set read again
		case gone:
			mh.Packs = nil
			mh.LeftOut = mh.LeftOut[:0]
			for _, p := range rs.Packs {
				mh.LeftOut = append(mh.LeftOut, leftOut{PackID: p.ID.String(), Reason: "its object is gone, again after the hour's read set was read again"})
			}

			return mh, os.RemoveAll(hourDir)
		default:
		}
		for _, p := range mh.Packs {
			if err := b.addFile(p.File, filepath.Join(hourDir, filepath.Base(p.File))); err != nil {
				return nil, err
			}
		}

		return mh, os.RemoveAll(hourDir)
	}
}

// readSet reads an hour's read set, refusing a withheld capture's as every read does.
func (b *bundle) readSet(ctx context.Context, capture tracepack.UUID, h int64) (catalog.ReadSet, error) {
	rs, err := b.s.Catalog.ReadSet(ctx, capture, h)
	if err != nil {
		return rs, err
	}
	if withheld, err := b.s.withholds(ctx, rs.ConfigID); err != nil {
		return rs, err
	} else if withheld {
		return rs, records.ErrWithheld
	}

	return rs, nil
}

// packs exports rs's packs one at a time into hourDir, reporting whether an object was found gone.
func (b *bundle) packs(ctx context.Context, rs *catalog.ReadSet, hourDir string, mh *manifestHour) (bool, error) {
	for _, p := range rs.Packs {
		b.exported++
		b.bytes += p.Size
		if b.exported > b.bounds.MaxPacks || b.bytes > b.bounds.MaxBytes {
			return false, fmt.Errorf("%w: more than %d packs or %d bytes of pack objects", errBounds, b.bounds.MaxPacks, b.bounds.MaxBytes)
		}
		name := filepath.Join("packs", rs.Capture.String(), mh.Hour, p.ID.String()+".jsonl")
		mp, gone, err := b.pack(ctx, rs, &p, filepath.Join(hourDir, p.ID.String()+".jsonl"))
		switch {
		case gone:
			return true, nil
		case errors.Is(err, errBounds) || ctx.Err() != nil:
			return false, cmp.Or(ctx.Err(), err)
		case err != nil:
			mh.LeftOut = append(mh.LeftOut, leftOut{PackID: p.ID.String(), Reason: err.Error()})
			continue
		default:
		}
		mp.File = name
		mh.Packs = append(mh.Packs, mp)
	}

	return false, nil
}

// pack exports p whole into path; an error leaves no file, its pack left out with the error's text, which names no key.
func (b *bundle) pack(ctx context.Context, rs *catalog.ReadSet, p *catalog.ReadPack, path string) (manifestPack, bool, error) {
	mp := manifestPack{PackID: p.ID.String(), Role: p.Role, State: p.State, FormatMinor: p.FormatMinor, Schema: jsonlSchema(p.FormatMinor)}
	obj, err := records.Open(ctx, b.s.Store, p.Key)
	if errors.Is(err, store.ErrNotFound) {
		return mp, true, nil
	} else if err != nil {
		return mp, false, err
	}
	defer obj.Close()
	r, err := tracepack.Open(ctx, obj, obj.Size(), tracepack.ReaderOptions{})
	if err == nil && r.Header().CaptureID != rs.Capture {
		err = errors.New("the pack is of another capture")
	}
	if errors.Is(err, store.ErrNotFound) {
		return mp, true, nil
	} else if err != nil {
		return mp, false, err
	}
	f, err := os.Create(path)
	if err != nil {
		return mp, false, err
	}
	res, err := tracepack.ExportJSONL(ctx, r, &limited{w: f, b: b})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		if errors.Is(err, store.ErrNotFound) {
			return mp, true, nil
		}

		return mp, false, err
	}
	for _, d := range res.Incomplete {
		mp.Incomplete = append(mp.Incomplete, d.Reason.String())
	}
	if len(res.FooterErrs) > 0 {
		mp.FooterError = res.FooterErrs[0].Err.Error()
	}

	return mp, false, nil
}

// limited counts what the attempt writes against its work directory's bound.
type limited struct {
	w io.Writer
	b *bundle
}

func (l *limited) Write(p []byte) (int, error) {
	if l.b.written+int64(len(p)) > l.b.bounds.MaxBundleBytes {
		return 0, fmt.Errorf("%w: a work directory past %d bytes", errBounds, l.b.bounds.MaxBundleBytes)
	}
	n, err := l.w.Write(p)
	l.b.written += int64(n)

	return n, err
}

// addFile adds the file at path to the tar under name.
func (b *bundle) addFile(name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}

	return b.add(name, f, st.Size())
}

func (b *bundle) add(name string, r io.Reader, size int64) error {
	if err := b.tar.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: size, Format: tar.FormatPAX}); err != nil {
		return err
	}
	_, err := io.Copy(b.tar, r)

	return err
}

// jsonlSchema is the JSONL schema of a pack of format_minor m.
func jsonlSchema(m int) string { return fmt.Sprintf("tracepack-jsonl/%d", m+1) }

// ExportConfig configures the export jobs and their workers (api.md §10.1 *Exports*); zero fields take the defaults.
type ExportConfig struct {
	Bounds ExportBounds
	// WorkDir holds each attempt's directory.
	WorkDir string
	// Lease, PutTimeout and Retention are the attempt's lease, its put's bound and a bundle's retention (1 min, 10 min, 24 h);
	// MaxAttempts bounds a job's attempts (3) and MaxRunning a replica's attempts at once (1).
	Lease, PutTimeout, Retention time.Duration
	MaxAttempts, MaxRunning      int
}
