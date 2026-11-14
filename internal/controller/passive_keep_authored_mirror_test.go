package controller

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	jsm "github.com/nats-io/jsm.go"
	jsmapi "github.com/nats-io/jsm.go/api"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
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
