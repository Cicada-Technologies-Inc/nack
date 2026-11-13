package controller

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A delete discards a server-side stream and every message in it, and the
// log line written at that moment is the only record of what was there. The
// helpers below read what the stream holds immediately before the delete and
// return it as log key/values. They carry metadata only, never message
// contents, and never block the delete: when the read fails, the reason is
// logged in place of the state.

// kvBackingStreamPrefix is the prefix nats-server gives the stream that backs
// a KeyValue bucket.
const kvBackingStreamPrefix = "KV_"

// streamStateBeforeDelete describes the named stream as it is right now.
func streamStateBeforeDelete(js *jsm.Manager, name string) []any {
	s, err := js.LoadStream(name)
	if err != nil {
		return []any{"stateBeforeDelete", "unavailable", "stateErr", err.Error()}
	}
	state, err := s.LatestState()
	if err != nil {
		return []any{"stateBeforeDelete", "unavailable", "stateErr", err.Error()}
	}
	return []any{
		"messages", state.Msgs,
		"bytes", state.Bytes,
		"firstSeq", state.FirstSeq,
		"lastSeq", state.LastSeq,
		"consumers", state.Consumers,
	}
}

// keyValueStateBeforeDelete describes the stream backing the named bucket as
// it is right now.
func keyValueStateBeforeDelete(ctx context.Context, js jetstream.JetStream, bucket string) []any {
	s, err := js.Stream(ctx, kvBackingStreamPrefix+bucket)
	if err != nil {
		return []any{"stateBeforeDelete", "unavailable", "stateErr", err.Error()}
	}
	state := s.CachedInfo().State
	return []any{
		"messages", state.Msgs,
		"bytes", state.Bytes,
		"firstSeq", state.FirstSeq,
		"lastSeq", state.LastSeq,
		"consumers", state.Consumers,
	}
}
