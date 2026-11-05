package api

import (
	"errors"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/safetext"
)

// safeAnswer is err, a decided answer, with its message and its cause made safe by safetext.Text; its code, details and marks are kept.
func safeAnswer(err error) error {
	var a *answer
	if err == nil || !errors.As(err, &a) {
		return err
	}
	ce := connect.NewError(a.err.Code(), errors.New(safetext.Text(a.err.Message())))
	for _, d := range a.err.Details() {
		ce.AddDetail(d)
	}
	out := &answer{err: ce, decided: a.decided, gone: a.gone}
	if a.cause != nil {
		out.cause = errors.New(safetext.Text(a.cause.Error()))
	}

	return out
}
