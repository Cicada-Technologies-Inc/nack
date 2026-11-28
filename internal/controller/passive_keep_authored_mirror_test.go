package controller

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	jsm "github.com/nats-io/jsm.go"
	jsmapi "github.com/nats-io/jsm.go/api"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A mirror-baseline CR that already pulls its origin from the peer domain is
// kept verbatim, including an empty deliver prefix, so translation does not
// ask the server for a mirror change it can only satisfy by recreating.
func TestTranslateStreamSpecToMirrorKeepsAuthoredMirror(t *testing.T) {
	orig := &api.StreamSpec{
		Name:   "notification-center",
		Mirror: &api.StreamSource{Name: "notification-center", ExternalAPIPrefix: "$JS.prd.API"},
	}
	got := translateStreamSpecToMirror(orig, "prd")
	if got.Mirror == nil || got.Mirror.ExternalDeliverPrefix != "" || got.Mirror.ExternalAPIPrefix != "$JS.prd.API" {
		t.Fatalf("authored mirror not kept: %+v", got.Mirror)
	}
	got.Mirror.Name = "MUTATED"
	if orig.Mirror.Name != "notification-center" {
		t.Fatal("authored mirror aliased; translation must deep-copy")
	}
}

func TestTranslateStreamSpecToMirrorReplacesMismatchedAuthoredMirror(t *testing.T) {
	cases := map[string]*api.StreamSource{
		"other domain":   {Name: "s", ExternalAPIPrefix: "$JS.other.API"},
		"other origin":   {Name: "t", ExternalAPIPrefix: "$JS.prd.API"},
		"filtered":       {Name: "s", ExternalAPIPrefix: "$JS.prd.API", FilterSubject: "a.>"},
		"start sequence": {Name: "s", ExternalAPIPrefix: "$JS.prd.API", OptStartSeq: 5},
		"local mirror":   {Name: "s"},
	}
	for name, mirror := range cases {
		t.Run(name, func(t *testing.T) {
			got := translateStreamSpecToMirror(&api.StreamSpec{Name: "s", Mirror: mirror}, "prd")
			want := api.StreamSource{Name: "s", ExternalAPIPrefix: "$JS.prd.API", ExternalDeliverPrefix: "deliver.s.dr"}
			if diff := cmp.Diff(want, *got.Mirror); diff != "" {
				t.Fatalf("mirror not synthesized (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTranslateKeyValueSpecToMirrorKeepsAuthoredMirror(t *testing.T) {
	orig := &api.KeyValueSpec{
		Bucket: "offsets",
		Mirror: &api.StreamSource{Name: "KV_offsets", ExternalAPIPrefix: "$JS.prd.API"},
	}
	got := translateKeyValueSpecToMirror(orig, "prd")
	if got.Mirror == nil || got.Mirror.ExternalDeliverPrefix != "" {
		t.Fatalf("authored KV mirror not kept: %+v", got.Mirror)
	}
}

// The passive copy a mirror-baseline CR created without a deliver prefix must
// reconcile to an identical configuration once translation is on.
func TestPassiveTranslationOfExistingMirrorIsNoOp(t *testing.T) {
	live := jsmapi.StreamConfig{
		Name: "notification-center", Retention: jsmapi.LimitsPolicy, MaxConsumers: -1, MaxMsgs: -1,
		MaxBytes: 1 << 30, MaxAge: 168 * time.Hour, MaxMsgsPer: -1, MaxMsgSize: -1,
		Discard: jsmapi.DiscardOld, Storage: jsmapi.FileStorage, Replicas: 3,
		Mirror:      &jsmapi.StreamSource{Name: "notification-center", External: &jsmapi.ExternalStream{ApiPrefix: "$JS.prd.API"}},
		Compression: jsmapi.NoCompression,
	}
	spec := &api.StreamSpec{
		Name: "notification-center", Storage: "file", Retention: "limits", Replicas: 3,
		MaxAge: "168h", MaxBytes: 1 << 30, MaxConsumers: -1, MaxMsgs: -1, MaxMsgsPerSubject: -1,
		MaxMsgSize: -1, Discard: "old", Compression: "none",
		Mirror: &api.StreamSource{Name: "notification-center", ExternalAPIPrefix: "$JS.prd.API"},
	}
	translated := translateStreamSpecToMirror(spec, "prd")
	opts, err := streamSpecToConfig(&translated, &live)
	if err != nil {
		t.Fatal(err)
	}
	target, err := jsm.NewStreamConfiguration(live, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(live, *target); diff != "" {
		t.Fatalf("translation would change the live mirror (-live +request):\n%s", diff)
	}
}

func TestPreservePassiveMirrorDeliveryOnlyForEquivalentMirrors(t *testing.T) {
	start := time.Now()
	for _, tc := range []struct {
		name   string
		change func(*jsmapi.StreamConfig)
		want   string
	}{
		{"same peer", func(*jsmapi.StreamConfig) {}, "old-delivery"},
		{"implicit delivery", func(c *jsmapi.StreamConfig) { c.Mirror.External.DeliverPrefix = "" }, ""},
		{"primary", func(c *jsmapi.StreamConfig) { c.Mirror = nil }, "deliver.s.dr"},
		{"local mirror", func(c *jsmapi.StreamConfig) { c.Mirror.External = nil }, "deliver.s.dr"},
		{"other domain", func(c *jsmapi.StreamConfig) { c.Mirror.External.ApiPrefix = "$JS.other.API" }, "deliver.s.dr"},
		{"other origin", func(c *jsmapi.StreamConfig) { c.Mirror.Name = "other" }, "deliver.s.dr"},
		{"filter", func(c *jsmapi.StreamConfig) { c.Mirror.FilterSubject = "filtered.>" }, "deliver.s.dr"},
		{"transform", func(c *jsmapi.StreamConfig) {
			c.Mirror.SubjectTransforms = []jsmapi.SubjectTransformConfig{{Source: "in", Destination: "out"}}
		}, "deliver.s.dr"},
		{"start sequence", func(c *jsmapi.StreamConfig) { c.Mirror.OptStartSeq = 10 }, "deliver.s.dr"},
		{"start time", func(c *jsmapi.StreamConfig) { c.Mirror.OptStartTime = &start }, "deliver.s.dr"},
		{"consumer", func(c *jsmapi.StreamConfig) { c.Mirror.Consumer = &jsmapi.StreamConsumerSource{Name: "explicit"} }, "deliver.s.dr"},
		{"sources", func(c *jsmapi.StreamConfig) { c.Sources = []*jsmapi.StreamSource{{Name: "other"}} }, "deliver.s.dr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &jsmapi.StreamConfig{Mirror: &jsmapi.StreamSource{Name: "s", External: &jsmapi.ExternalStream{ApiPrefix: "$JS.prd.API", DeliverPrefix: "old-delivery"}}}
			tc.change(cfg)
			desired := translateStreamSpecToMirror(&api.StreamSpec{Name: "s"}, "prd")
			preservePassiveMirrorDelivery(desired.Mirror, cfg)
			require.Equal(t, tc.want, desired.Mirror.ExternalDeliverPrefix)
		})
	}
}

// Start separate JetStream domains connected over a leaf link. Using the same
// logical stream name in each domain exercises the external API prefix as deployed.
func passiveMirrorDomain(t *testing.T, domain string, leaf server.LeafNodeOpts) (*server.Server, nats.JetStreamContext) {
	t.Helper()
	opts := server.Options{Host: "127.0.0.1", Port: -1, ServerName: domain, JetStream: true,
		JetStreamDomain: domain, StoreDir: t.TempDir(), LeafNode: leaf}
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.Domain(domain))
	require.NoError(t, err)
	return srv, js
}

func TestPassiveMirrorBaselineMigrationRetainsStreamAndConsumer(t *testing.T) {
	const name = "SEF"
	origin, source := passiveMirrorDomain(t, "prd", server.LeafNodeOpts{Host: "127.0.0.1", Port: -1})
	v, err := origin.Varz(nil)
	require.NoError(t, err)
	remote, err := url.Parse(fmt.Sprintf("nats-leaf://127.0.0.1:%d", v.LeafNode.Port))
	require.NoError(t, err)
	srv, js := passiveMirrorDomain(t, "east", server.LeafNodeOpts{Remotes: []*server.RemoteLeafOpts{{URLs: []*url.URL{remote}}}})
	require.Eventually(t, func() bool { return srv.NumLeafNodes() == 1 }, 5*time.Second, 25*time.Millisecond)
	_, err = source.AddStream(&nats.StreamConfig{Name: name, Storage: nats.FileStorage, Subjects: []string{"orders.>"}})
	require.NoError(t, err)
	for _, body := range []string{"first", "second"} {
		_, err = source.Publish("orders.test", []byte(body))
		require.NoError(t, err)
	}
	external := &nats.ExternalStream{APIPrefix: "$JS.prd.API"}
	before, err := js.AddStream(&nats.StreamConfig{Name: name, Storage: nats.FileStorage, Mirror: &nats.StreamSource{Name: name, External: external}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { info, err := js.StreamInfo(name); return err == nil && info.State.Msgs == 2 }, 10*time.Second, 25*time.Millisecond)
	consumer, err := js.AddConsumer(name, &nats.ConsumerConfig{Durable: "audit", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRolePassive}}}
	stream := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{Name: "sef", Namespace: ns.Name, Generation: 2, Labels: map[string]string{scopeLabel: "true"}},
		Spec:       api.StreamSpec{Name: name, Subjects: []string{"orders.>"}, Storage: "file", Retention: "limits", MaxBytes: 16 << 20},
		Status:     api.Status{Conditions: []api.Condition{{Type: readyCondType, Status: corev1.ConditionUnknown}}},
	}
	sch := activeXlatScheme(t)
	k8s := fake.NewClientBuilder().WithScheme(sch).WithObjects(ns, stream).WithStatusSubresource(&api.Stream{}).Build()
	base, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{
		Namespace: ns.Name, EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "prd",
		MirrorRecreateOnConflict: true, RequireBackupConfirmation: true,
	})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: sch, JetStreamController: base}
	require.NoError(t, r.createOrUpdate(t.Context(), logr.Discard(), stream))
	after, err := js.StreamInfo(name)
	require.NoError(t, err)
	require.Equal(t, before.Created, after.Created)
	require.Equal(t, before.Config.Mirror, after.Config.Mirror)
	require.EqualValues(t, 2, after.State.Msgs)
	for i, body := range []string{"first", "second"} {
		m, err := js.GetMsg(name, uint64(i+1))
		require.NoError(t, err)
		require.Equal(t, body, string(m.Data))
	}
	require.Empty(t, after.Config.Subjects)
	require.EqualValues(t, 16<<20, after.Config.MaxBytes, "other requested properties must still reconcile")
	got, err := js.ConsumerInfo(name, "audit")
	require.NoError(t, err)
	require.Equal(t, consumer.Created, got.Created)
	require.Nil(t, stream.Spec.Mirror)
	require.Equal(t, []string{"orders.>"}, stream.Spec.Subjects)
}
