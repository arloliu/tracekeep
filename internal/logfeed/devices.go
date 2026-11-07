package logfeed

import (
	"slices"

	"github.com/arloliu/go-secs/tracepack"
)

// DeviceRule maps a header's device-number token, or else its logged session ID, to a device name (logfeed.md §15.5 *Devices*).
type DeviceRule struct {
	Token   string
	Session *uint16
	Device  string
}

// Devices is the global rule table with its per-equipment overrides and the main device of an entry no rule matches.
type Devices struct {
	Main      string
	Rules     []DeviceRule
	Overrides map[string][]DeviceRule
}

// Of is the device of an entry of equipment logging token, or else session (nil when its header logs none):
// the equipment's override table when it has one, else the global table, a token's rule before a session's, else the main device.
func (d *Devices) Of(equipment, token string, session *uint16) string {
	rules, ok := d.Overrides[equipment]
	if !ok {
		rules = d.Rules
	}
	if token != "" {
		for _, r := range rules {
			if r.Token == token {
				return r.Device
			}
		}
	}
	if session != nil {
		for _, r := range rules {
			if r.Session != nil && *r.Session == *session {
				return r.Device
			}
		}
	}

	return d.Main
}

// maxReplies bounds the primaries a file remembers for their replies; past it the oldest are forgotten, and such a reply follows the rules.
const maxReplies = 1024

// Replies remembers each primary's device, which its reply takes (§15.5 *Devices*): by its trusted session and system bytes,
// and by its TID, each with the primary's direction, so a reply, which travels the other way, finds only its own transaction.
type Replies struct {
	devices map[replyKey]string
	order   []replyKey
}

type replyKey struct {
	dir     string
	session int32
	sb      [4]byte
	tid     string
}

// replyKeys are the keys an item's transaction is remembered or looked up by, for the primary's direction dir.
func replyKeys(it *Item, b *Built, dir string) []replyKey {
	var ks []replyKey
	if b.Trusted {
		ks = append(ks, replyKey{dir: dir, session: b.Session, sb: b.SystemBytes})
	}
	if t := tid(it.Header, it.Body); t != "" {
		ks = append(ks, replyKey{dir: dir, session: -1, tid: t})
	}

	return ks
}

func opposite(dir string) string {
	if dir == DirSent {
		return DirReceived
	}

	return DirSent
}

// Device is the device of an item: a reply's is its primary's when remembered, every other's the rules'.
func (r *Replies) Device(d *Devices, it *Item, b *Built) string {
	h := it.Header
	if h == nil {
		return d.Main
	}
	if it.Kind == Pair && b.Record.Kind == tracepack.KindData && !b.Primary {
		for _, k := range replyKeys(it, b, opposite(h.Dir)) {
			if dev, ok := r.devices[k]; ok {
				return dev
			}
		}
	}
	var session *uint16
	if hb, ok := h.header(); ok {
		s := uint16(hb[0])<<8 | uint16(hb[1])
		session = &s
	}

	return d.Of(h.EquipmentID, h.Device, session)
}

// Remember keeps a primary data record's device for its reply; the oldest of more than maxReplies is forgotten,
// unless remembered again since, which keeps its newer device.
func (r *Replies) Remember(it *Item, b *Built, device string) {
	if !b.Primary || it.Header == nil || b.Record.Kind != tracepack.KindData {
		return
	}
	if r.devices == nil {
		r.devices = map[replyKey]string{}
	}
	for _, k := range replyKeys(it, b, it.Header.Dir) {
		r.devices[k] = device
		r.order = append(r.order, k)
	}
	for len(r.order) > maxReplies {
		old := r.order[0]
		r.order = r.order[1:]
		// The key's newest remembering is the last in order; an older occurrence leaves it.
		if !slices.Contains(r.order, old) {
			delete(r.devices, old)
		}
	}
}
