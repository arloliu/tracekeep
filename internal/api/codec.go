package api

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxRequestBytes bounds a request's message before it is decoded:
// a Digest's 8 MiB of originals as base64 in JSON, with every input's fields, fits well inside it.
const maxRequestBytes = 16 << 20

// errUndecodable is every decode failure's message: protobuf's own quote the token they fail on,
// which in a Digest is an original (api.md §10.1 *Wire*).
var errUndecodable = errors.New("the request does not decode")

// codecs decode as connect's own codecs do, binary and JSON, with errors that never hold the request's bytes.
func codecs() []connect.HandlerOption {
	return []connect.HandlerOption{
		connect.WithCodec(quietCodec{name: "proto"}),
		connect.WithCodec(quietCodec{name: "json", json: true}),
		connect.WithCodec(quietCodec{name: "json; charset=utf-8", json: true}),
		connect.WithReadMaxBytes(maxRequestBytes),
	}
}

// quietCodec is a protobuf codec, binary or JSON, named name.
type quietCodec struct {
	name string
	json bool
}

func (c quietCodec) Name() string { return c.name }

func (c quietCodec) Marshal(m any) ([]byte, error) {
	pm, ok := m.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T is not a protobuf message", m)
	}
	if c.json {
		return protojson.Marshal(pm)
	}

	return proto.Marshal(pm)
}

func (c quietCodec) Unmarshal(b []byte, m any) error {
	pm, ok := m.(proto.Message)
	if !ok {
		return fmt.Errorf("%T is not a protobuf message", m)
	}
	var err error
	switch {
	case !c.json:
		err = proto.Unmarshal(b, pm)
	case len(b) == 0:
		err = errUndecodable
	default:
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(b, pm)
	}
	if err != nil {
		return errUndecodable
	}

	return nil
}
