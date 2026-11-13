package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// keyValues turns a log key/value slice into a map for assertions.
func keyValues(t *testing.T, kv []any) map[string]string {
	t.Helper()
	require.Zero(t, len(kv)%2, "log key/values must come in pairs: %v", kv)
	out := make(map[string]string, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		out[fmt.Sprint(kv[i])] = fmt.Sprint(kv[i+1])
	}
	return out
}

func Test_streamStateBeforeDelete(t *testing.T) {
	_, mgr, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	_, err := mgr.NewStream("ORDERS", jsm.Subjects("orders.>"), jsm.FileStorage())
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		_, err := js.Publish("orders.new", nil)
		require.NoError(t, err)
	}

	got := keyValues(t, streamStateBeforeDelete(mgr, "ORDERS"))
	require.Equal(t, "4", got["messages"])
	require.Equal(t, "1", got["firstSeq"])
	require.Equal(t, "4", got["lastSeq"])
	require.Equal(t, "0", got["consumers"])

	// A stream that is not there must be reported as such, not as an empty
	// one: the two mean different things when reading a delete back.
	missing := keyValues(t, streamStateBeforeDelete(mgr, "ABSENT"))
	require.Equal(t, "unavailable", missing["stateBeforeDelete"])
	require.NotEmpty(t, missing["stateErr"])
	require.NotContains(t, missing, "messages")
}

func Test_keyValueStateBeforeDelete(t *testing.T) {
	srv, _, _, cleanup := newJSTestServerWithURL(t)
	defer cleanup()

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	ctx := context.Background()
	bucket, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "settings"})
	require.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		_, err := bucket.Put(ctx, k, nil)
		require.NoError(t, err)
	}

	got := keyValues(t, keyValueStateBeforeDelete(ctx, js, "settings"))
	require.Equal(t, "3", got["messages"])
	require.Equal(t, "3", got["lastSeq"])

	missing := keyValues(t, keyValueStateBeforeDelete(ctx, js, "absent"))
	require.Equal(t, "unavailable", missing["stateBeforeDelete"])
	require.NotEmpty(t, missing["stateErr"])
}
