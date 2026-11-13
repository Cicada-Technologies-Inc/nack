package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

const testConfirmKey = defaultBackupConfirmedAnnotation

func Test_parseBackupRecord(t *testing.T) {
	r, ok := parseBackupRecord("7")
	require.True(t, ok)
	assert.Equal(t, backupRecord{Generation: 7}, r)

	r, ok = parseBackupRecord("msgs=3,gen=2,last=12,first=10")
	require.True(t, ok)
	assert.Equal(t, backupRecord{Generation: 2, Fingerprint: streamFingerprint{First: 10, Last: 12, Msgs: 3}, HasFingerprint: true}, r)

	assert.Equal(t, "gen=2,first=10,last=12,msgs=3", formatBackupRecord(2, streamFingerprint{10, 12, 3}))

	for _, bad := range []string{"", "x", "gen=1", "gen=1,first=1,last=2", "gen=1,first=1,last=2,msgs=1,extra=1",
		"gen=1,first=1,first=2,msgs=1", "gen=a,first=1,last=2,msgs=1", "gen=1,first=-1,last=2,msgs=1", "gen=1;first=1"} {
		_, ok := parseBackupRecord(bad)
		assert.False(t, ok, "must reject %q", bad)
	}
}

func Test_streamFingerprint_coveredBy(t *testing.T) {
	local := streamFingerprint{First: 101, Last: 200, Msgs: 100}
	assert.True(t, local.coveredBy(local))
	assert.True(t, local.coveredBy(streamFingerprint{First: 1, Last: 250, Msgs: 250}))
	assert.False(t, local.coveredBy(streamFingerprint{First: 1, Last: 100, Msgs: 100}), "same count, older range")
	assert.False(t, local.coveredBy(streamFingerprint{First: 102, Last: 300, Msgs: 199}), "missing the local head")
	assert.False(t, local.coveredBy(streamFingerprint{First: 101, Last: 199, Msgs: 99}), "missing the local tail")
	assert.False(t, local.coveredBy(streamFingerprint{First: 101, Last: 200, Msgs: 90}), "fewer messages over the same range")
}

func Test_decideBackupGate(t *testing.T) {
	local := streamFingerprint{First: 1, Last: 10, Msgs: 10}
	demand := formatBackupRecord(3, local)
	noProbe := (func() (peerCoverage, error))(nil)
	probeOK := func(fp streamFingerprint) func() (peerCoverage, error) {
		return func() (peerCoverage, error) { return peerCoverage{Exists: true, Fingerprint: fp}, nil }
	}

	t.Run("empty stream proceeds and consumes leftovers", func(t *testing.T) {
		res := decideBackupGate(map[string]string{testConfirmKey: "3", "keep": "x"}, testConfirmKey, 3, streamFingerprint{}, "s", noProbe)
		assert.False(t, res.Fires)
		assert.Equal(t, map[string]string{"keep": "x"}, res.Annotations)
	})

	t.Run("stale generation-only confirmation without a demand does not authorize", func(t *testing.T) {
		res := decideBackupGate(map[string]string{testConfirmKey: "3", "keep": "x"}, testConfirmKey, 3, local, "s", noProbe)
		assert.True(t, res.Fires)
		assert.Equal(t, "NoProbeConfigured", res.Reason)
		assert.Equal(t, map[string]string{"keep": "x", backupDemandAnnotation: demand}, res.Annotations,
			"demand armed and the pre-existing confirmation removed in the same write")
	})

	t.Run("generation-only confirmation answering an unchanged demand authorizes once", func(t *testing.T) {
		res := decideBackupGate(map[string]string{testConfirmKey: "3", backupDemandAnnotation: demand}, testConfirmKey, 3, local, "s", noProbe)
		assert.False(t, res.Fires)
		assert.Equal(t, map[string]string{}, res.Annotations, "confirmation and demand consumed")
	})

	t.Run("generation-only confirmation is rejected when the stream moved after the demand", func(t *testing.T) {
		grown := streamFingerprint{First: 1, Last: 12, Msgs: 12}
		res := decideBackupGate(map[string]string{testConfirmKey: "3", backupDemandAnnotation: demand}, testConfirmKey, 3, grown, "s", noProbe)
		assert.True(t, res.Fires)
		assert.Equal(t, map[string]string{backupDemandAnnotation: formatBackupRecord(3, grown)}, res.Annotations)
	})

	t.Run("generation-only confirmation for another generation is rejected", func(t *testing.T) {
		res := decideBackupGate(map[string]string{testConfirmKey: "2", backupDemandAnnotation: formatBackupRecord(2, local)}, testConfirmKey, 3, local, "s", noProbe)
		assert.True(t, res.Fires)
		assert.Equal(t, map[string]string{backupDemandAnnotation: demand}, res.Annotations)
	})

	t.Run("fingerprinted confirmation covering the local range authorizes without a demand", func(t *testing.T) {
		conf := formatBackupRecord(3, streamFingerprint{First: 1, Last: 10, Msgs: 10})
		res := decideBackupGate(map[string]string{testConfirmKey: conf}, testConfirmKey, 3, local, "s", noProbe)
		assert.False(t, res.Fires)
		assert.Equal(t, map[string]string{}, res.Annotations)
	})

	t.Run("fingerprinted confirmation with the same count over a different range is rejected", func(t *testing.T) {
		conf := formatBackupRecord(3, streamFingerprint{First: 11, Last: 20, Msgs: 10})
		res := decideBackupGate(map[string]string{testConfirmKey: conf}, testConfirmKey, 3, local, "s", noProbe)
		assert.True(t, res.Fires)
		assert.Equal(t, map[string]string{backupDemandAnnotation: demand}, res.Annotations)
	})

	t.Run("waiting on an armed demand does not rewrite annotations", func(t *testing.T) {
		res := decideBackupGate(map[string]string{backupDemandAnnotation: demand}, testConfirmKey, 3, local, "s", noProbe)
		assert.True(t, res.Fires)
		assert.Nil(t, res.Annotations)
	})

	t.Run("peer with the same count over an older range is stale", func(t *testing.T) {
		res := decideBackupGate(nil, testConfirmKey, 3, streamFingerprint{First: 11, Last: 20, Msgs: 10}, "s",
			probeOK(streamFingerprint{First: 1, Last: 10, Msgs: 10}))
		assert.True(t, res.Fires)
		assert.Equal(t, "PeerStale", res.Reason)
	})

	t.Run("peer covering the local range proceeds and consumes leftovers", func(t *testing.T) {
		res := decideBackupGate(map[string]string{testConfirmKey: "3"}, testConfirmKey, 3, local, "s",
			probeOK(streamFingerprint{First: 1, Last: 15, Msgs: 15}))
		assert.False(t, res.Fires)
		assert.Equal(t, map[string]string{}, res.Annotations)
	})

	t.Run("peer missing or unreachable fires", func(t *testing.T) {
		res := decideBackupGate(nil, testConfirmKey, 3, local, "s", func() (peerCoverage, error) { return peerCoverage{}, nil })
		assert.Equal(t, "PeerMissing", res.Reason)
		res = decideBackupGate(nil, testConfirmKey, 3, local, "s", func() (peerCoverage, error) { return peerCoverage{}, errors.New("boom") })
		assert.Equal(t, "PeerUnreachable", res.Reason)
	})
}

// backupGateHarness drives the production StreamReconciler against an
// embedded nats-server for a primary→mirror recreate.
type backupGateHarness struct {
	t      *testing.T
	js     nats.JetStreamContext
	k8s    client.Client
	r      *StreamReconciler
	key    types.NamespacedName
	stream string
}

func newBackupGateHarness(t *testing.T, peerURL string, annotations map[string]string, nMsgs int) *backupGateHarness {
	t.Helper()
	srv, _, js, cleanup := newJSTestServerWithURL(t)
	t.Cleanup(cleanup)

	const streamName = "GATED"
	_, err := js.AddStream(&nats.StreamConfig{Name: streamName, Subjects: []string{"gated.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < nMsgs; i++ {
		_, err := js.Publish("gated.evt", []byte("payload"))
		require.NoError(t, err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats"}}
	cr := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{Name: "gated", Namespace: "nats", Generation: 1, Annotations: annotations},
		Spec: api.StreamSpec{
			Name:      streamName,
			Storage:   "file",
			Retention: "limits",
			Mirror:    &api.StreamSource{Name: "UPSTREAM"},
		},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().WithScheme(sch).WithObjects(ns, cr).WithStatusSubresource(&api.Stream{}).Build()
	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace:                 "nats",
		MirrorRecreateOnConflict:  true,
		RequireBackupConfirmation: true,
		CrossRegionNATSURL:        peerURL,
	})
	require.NoError(t, err)
	return &backupGateHarness{
		t: t, js: js, k8s: k8s, stream: streamName,
		r:   &StreamReconciler{Scheme: sch, JetStreamController: base},
		key: types.NamespacedName{Namespace: "nats", Name: "gated"},
	}
}

func (h *backupGateHarness) get() *api.Stream {
	h.t.Helper()
	var s api.Stream
	require.NoError(h.t, h.k8s.Get(context.Background(), h.key, &s))
	return &s
}

func (h *backupGateHarness) reconcile() *api.Stream {
	h.t.Helper()
	require.NoError(h.t, h.r.createOrUpdate(context.Background(), logr.Discard(), h.get()))
	return h.get()
}

func (h *backupGateHarness) annotate(v string) {
	h.t.Helper()
	s := h.get()
	if s.Annotations == nil {
		s.Annotations = map[string]string{}
	}
	s.Annotations[testConfirmKey] = v
	require.NoError(h.t, h.k8s.Update(context.Background(), s))
}

func (h *backupGateHarness) serverIsMirror() bool {
	h.t.Helper()
	info, err := h.js.StreamInfo(h.stream)
	require.NoError(h.t, err)
	return info.Config.Mirror != nil
}

func backupRequired(s *api.Stream) *api.Condition {
	for i := range s.Status.Conditions {
		if s.Status.Conditions[i].Type == conditionBackupRequired {
			return &s.Status.Conditions[i]
		}
	}
	return nil
}

// A generation-only confirmation left over from an earlier cycle must not
// authorize the delete; the gate arms a demand and removes it, and only a
// confirmation written after the demand (what drp-operator's
// BackupRequired reconciler does today) lets the recreate run, once.
func Test_streamBackupGate_staleLegacyConfirmationIsNotReused(t *testing.T) {
	h := newBackupGateHarness(t, "", map[string]string{testConfirmKey: "1"}, 5)

	s := h.reconcile()
	assert.False(t, h.serverIsMirror(), "stale confirmation must not authorize the destructive recreate")
	require.NotNil(t, backupRequired(s))
	assert.Equal(t, corev1.ConditionTrue, backupRequired(s).Status)
	assert.NotContains(t, s.Annotations, testConfirmKey, "stale confirmation removed")
	assert.Equal(t, "gen=1,first=1,last=5,msgs=5", s.Annotations[backupDemandAnnotation])

	// Still waiting: a second pass must not change anything.
	s = h.reconcile()
	assert.False(t, h.serverIsMirror())
	assert.Equal(t, "gen=1,first=1,last=5,msgs=5", s.Annotations[backupDemandAnnotation])

	// The backup operator answers the demand with the legacy format.
	h.annotate("1")
	s = h.reconcile()
	assert.True(t, h.serverIsMirror(), "confirmation answering the demand authorizes the recreate")
	assert.NotContains(t, s.Annotations, testConfirmKey, "confirmation consumed")
	assert.NotContains(t, s.Annotations, backupDemandAnnotation, "demand consumed")
	assert.Nil(t, backupRequired(s))
}

// The demand binds the confirmation to the sequence range: if the stream
// receives messages after the demand, a confirmation answering the old
// demand is discarded and a new demand is armed.
func Test_streamBackupGate_legacyConfirmationRejectedAfterStreamMoves(t *testing.T) {
	h := newBackupGateHarness(t, "", nil, 5)

	s := h.reconcile()
	assert.Equal(t, "gen=1,first=1,last=5,msgs=5", s.Annotations[backupDemandAnnotation])

	h.annotate("1")
	_, err := h.js.Publish("gated.evt", []byte("late"))
	require.NoError(t, err)

	s = h.reconcile()
	assert.False(t, h.serverIsMirror(), "backup taken for 5 messages must not authorize destroying 6")
	assert.NotContains(t, s.Annotations, testConfirmKey)
	assert.Equal(t, "gen=1,first=1,last=6,msgs=6", s.Annotations[backupDemandAnnotation])
}

// A fingerprinted confirmation is self-verifying: accepted without a prior
// demand when its range covers the local stream, rejected otherwise.
func Test_streamBackupGate_fingerprintedConfirmation(t *testing.T) {
	h := newBackupGateHarness(t, "", map[string]string{testConfirmKey: "gen=1,first=6,last=10,msgs=5"}, 5)
	s := h.reconcile()
	assert.False(t, h.serverIsMirror(), "same count over a different range must not authorize")
	assert.NotContains(t, s.Annotations, testConfirmKey)

	h.annotate("gen=1,first=1,last=5,msgs=5")
	s = h.reconcile()
	assert.True(t, h.serverIsMirror())
	assert.NotContains(t, s.Annotations, testConfirmKey)
	assert.NotContains(t, s.Annotations, backupDemandAnnotation)
}

// The cross-region probe compares sequence ranges: a peer holding the same
// number of messages over a different range does not stand in for a backup.
func Test_streamBackupGate_peerWithSameCountDifferentRangeIsStale(t *testing.T) {
	peerSrv, _, peerJS, peerCleanup := newJSTestServerWithURL(t)
	t.Cleanup(peerCleanup)
	_, err := peerJS.AddStream(&nats.StreamConfig{Name: "GATED", Subjects: []string{"gated.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := peerJS.Publish("gated.evt", []byte("payload"))
		require.NoError(t, err)
	}
	// Peer holds 5 messages, sequences 6..10; local holds 5, sequences 1..5.
	require.NoError(t, peerJS.PurgeStream("GATED", &nats.StreamPurgeRequest{Sequence: 6}))

	h := newBackupGateHarness(t, peerSrv.ClientURL(), nil, 5)
	s := h.reconcile()
	assert.False(t, h.serverIsMirror())
	require.NotNil(t, backupRequired(s))
	assert.Equal(t, "PeerStale", backupRequired(s).Reason)

	// Once the peer covers the local range the recreate proceeds.
	require.NoError(t, peerJS.DeleteStream("GATED"))
	_, err = peerJS.AddStream(&nats.StreamConfig{Name: "GATED", Subjects: []string{"gated.>"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	for i := 0; i < 7; i++ {
		_, err := peerJS.Publish("gated.evt", []byte("payload"))
		require.NoError(t, err)
	}
	s = h.reconcile()
	assert.True(t, h.serverIsMirror())
	assert.NotContains(t, s.Annotations, backupDemandAnnotation, "demand consumed on the peer-covered recreate")
}

// KeyValue shares the gate: a stale generation-only confirmation does not
// authorize the destructive recreate of the backing stream.
func Test_keyValueBackupGate_staleLegacyConfirmationIsNotReused(t *testing.T) {
	srv, _, js, cleanup := newJSTestServerWithURL(t)
	t.Cleanup(cleanup)
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "gatedkv", Storage: nats.FileStorage})
	require.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		_, err := kv.PutString(k, "v")
		require.NoError(t, err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats"}}
	cr := &api.KeyValue{
		ObjectMeta: metav1.ObjectMeta{Name: "gatedkv", Namespace: "nats", Generation: 1,
			Annotations: map[string]string{testConfirmKey: "1"}},
		Spec: api.KeyValueSpec{Bucket: "gatedkv", Storage: "file", Mirror: &api.StreamSource{Name: "KV_UPSTREAM"}},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().WithScheme(sch).WithObjects(ns, cr).WithStatusSubresource(&api.KeyValue{}).Build()
	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace: "nats", MirrorRecreateOnConflict: true, RequireBackupConfirmation: true,
	})
	require.NoError(t, err)
	r := &KeyValueReconciler{Scheme: sch, JetStreamController: base}
	key := types.NamespacedName{Namespace: "nats", Name: "gatedkv"}
	ctx := context.Background()

	reconcile := func() *api.KeyValue {
		var cur api.KeyValue
		require.NoError(t, k8s.Get(ctx, key, &cur))
		require.NoError(t, r.createOrUpdate(ctx, logr.Discard(), &cur))
		require.NoError(t, k8s.Get(ctx, key, &cur))
		return &cur
	}
	isMirror := func() bool {
		info, err := js.StreamInfo("KV_gatedkv")
		require.NoError(t, err)
		return info.Config.Mirror != nil
	}

	got := reconcile()
	assert.False(t, isMirror(), "stale confirmation must not authorize the destructive recreate")
	assert.NotContains(t, got.Annotations, testConfirmKey)
	assert.Equal(t, "gen=1,first=1,last=3,msgs=3", got.Annotations[backupDemandAnnotation])

	got.Annotations[testConfirmKey] = "1"
	require.NoError(t, k8s.Update(ctx, got))
	got = reconcile()
	assert.True(t, isMirror())
	assert.NotContains(t, got.Annotations, testConfirmKey)
	assert.NotContains(t, got.Annotations, backupDemandAnnotation)
}
