package api

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"connectrpc.com/connect"
	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/store"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
)

// Downloads is how the API hands out download URLs (api.md §10.1 *Downloads*).
type Downloads struct {
	// Presigner presigns URLs when the store presigns and presigning is on; nil serves every download through the API.
	Presigner store.Presigner
	// Tokens signs the API's download tokens, and PublicURL is the base of its download URLs.
	Tokens    *auth.Downloads
	PublicURL string
	// Lifetime is a URL's longest life; Margin is kept before the object's deadline, and a URL left with less than Floor is refused.
	Lifetime, Margin, Floor time.Duration
	// DeleteDelay is D, the deletion delay of a merge that recorded none (compactor.md §8.3).
	DeleteDelay time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// Kinds of download URL, the label of download_urls_total.
const (
	kindPresigned = "presigned"
	kindAPI       = "api"
)

// PackURL implements tracekeepv1connect.MachineServiceHandler (api.md §10.1 *Downloads*):
// a URL expiring at the earlier of now plus the lifetime and the pack's deadline less the margin,
// refused as pack-superseded when that leaves less than the floor or the pack is deleted; each issued URL is audited.
// Now is read before the deadline: a retention shortened after the read holds the old period for D from its record,
// later than now, so no URL of a lifetime below D outlives its object however long the request then takes;
// a request that takes so long its URL would live less than the floor is refused.
func (s *Server) PackURL(ctx context.Context, req *connect.Request[tkv1.PackURLRequest]) (*connect.Response[tkv1.PackURLResponse], error) {
	id, err := tracepack.ParseUUID(req.Msg.GetPackId())
	if err != nil {
		return nil, invalid("pack_id %q: %v", req.Msg.GetPackId(), err)
	}
	dl := &s.Downloads
	now := dl.now()
	d, err := s.Catalog.PackDownload(ctx, id, dl.DeleteDelay)
	switch {
	case errors.Is(err, catalog.ErrUnknownPack):
		return nil, connect.NewError(connect.CodeNotFound, err)
	case err != nil:
		return nil, failed(err)
	case !d.Known:
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("no retention recorded yet: a pack has no deadline before the compactor's first retention pass"))
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	// Whole seconds, as a token carries its expiry.
	until := time.Unix(min(now.Add(dl.Lifetime).UnixNano(), d.Deadline-int64(dl.Margin))/int64(time.Second), 0)
	if d.State == "deleted" || until.Sub(now) < dl.Floor {
		s.Metrics.DownloadRefused()
		return nil, superseded(id)
	}
	u, err := dl.url(ctx, d.Key, until)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if u.expires.Sub(dl.now()) < dl.Floor {
		s.Metrics.DownloadRefused()
		return nil, superseded(id)
	}
	p, _ := PrincipalFrom(ctx)
	entry := catalog.AuditEntry{
		Actor: p.Subject, Action: "pack-url", Capture: &d.Capture,
		Detail: fmt.Sprintf("pack %s, %s URL expiring %s", id, u.kind, u.expires.UTC().Format(time.RFC3339)),
	}
	if err := s.Catalog.Audit(ctx, entry); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("audit: %w", err))
	}
	s.Metrics.DownloadURL(u.kind)

	return connect.NewResponse(&tkv1.PackURLResponse{Url: u.url, Expires: ts(u.expires.UnixNano()), Presigned: u.kind == kindPresigned}), nil
}

func (dl *Downloads) now() time.Time {
	if dl.Now != nil {
		return dl.Now()
	}

	return time.Now()
}

// issued is a download URL, the expiry it is reported with and its kind.
type issued struct {
	url     string
	expires time.Time
	kind    string
}

// url is key's download URL, never outliving until:
// presigned, expiring when the store says it surely still reads, or the API's with a download token expiring at until.
func (dl *Downloads) url(ctx context.Context, key string, until time.Time) (issued, error) {
	if dl.Presigner != nil {
		u, expires, err := dl.Presigner.PresignGet(ctx, key, until)
		return issued{u, expires, kindPresigned}, err
	}
	tok, err := dl.Tokens.Sign(ctx, key, until)
	if err != nil {
		return issued{}, err
	}
	u, err := url.JoinPath(dl.PublicURL, "download", tok)

	return issued{u, until, kindAPI}, err
}

// superseded is the refusal of a URL for pack: failed precondition with the PackSuperseded detail (api.md §10.1 *Wire*).
func superseded(pack tracepack.UUID) error {
	err := connect.NewError(connect.CodeFailedPrecondition, errors.New("pack-superseded"))
	if detail, derr := connect.NewErrorDetail(&tkv1.PackSuperseded{PackId: pack.String()}); derr == nil {
		err.AddDetail(detail)
	}

	return err
}
