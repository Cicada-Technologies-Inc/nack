package controller

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func productionStreams(t *testing.T) map[string][]api.Stream {
	t.Helper()
	data, err := os.ReadFile("testdata/production_streams.json")
	require.NoError(t, err)
	var fixture struct {
		Regions map[string][]api.Stream `json:"regions"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	for region, objects := range fixture.Regions {
		var streams []api.Stream
		for _, obj := range objects {
			if obj.Kind == "Stream" {
				streams = append(streams, obj)
			}
		}
		require.Len(t, streams, 49, region)
		fixture.Regions[region] = streams
	}
	return fixture.Regions
}

func TestProductionStreamsActivePassiveContract(t *testing.T) {
	regions := productionStreams(t)
	primaries := map[string]api.StreamSpec{}
	for _, stream := range regions["west"] {
		primaries[stream.Spec.Name] = stream.Spec
	}
	for region, streams := range regions {
		for _, stream := range streams {
			t.Run(region+"/"+stream.Spec.Name, func(t *testing.T) {
				active, err := activeStreamSpec(&stream)
				require.NoError(t, err)
				primary := primaries[stream.Spec.Name]
				require.Equal(t, primary.Subjects, active.Subjects)
				require.Equal(t, primary.Sources, active.Sources)
				require.Equal(t, primary.Retention, active.Retention)
				require.Equal(t, primary.Discard, active.Discard)
				require.Equal(t, primary.DuplicateWindow, active.DuplicateWindow)
				passive := translateStreamSpecToMirror(&active, "peer")
				require.Nil(t, passive.Subjects)
				require.Nil(t, passive.Sources)
				require.Equal(t, stream.Spec.Name, passive.Mirror.Name)
				if active.Retention == "interest" {
					require.Equal(t, "limits", passive.Retention)
				} else {
					require.Equal(t, active.Retention, passive.Retention)
				}
			})
		}
	}
}

func TestProductionSourceResumeMetadataOnPrimaryBaseline(t *testing.T) {
	for _, stream := range productionStreams(t)["west"] {
		if len(stream.Spec.Sources) == 0 {
			continue
		}
		t.Run(stream.Spec.Name, func(t *testing.T) {
			hints := stream.Spec.DeepCopy().Sources
			for _, source := range hints {
				source.OptStartSeq = 1001
			}
			raw, err := json.Marshal(hints)
			require.NoError(t, err)
			stream.Annotations["drp.cicada.io/sources-resume-config"] = string(raw)
			active, err := activeStreamSpec(&stream)
			require.NoError(t, err)
			for i, source := range active.Sources {
				require.Equal(t, 1001, source.OptStartSeq)
				require.Zero(t, stream.Spec.Sources[i].OptStartSeq, "translation must not mutate the CR")
			}
		})
	}
}

// These are the five real production source/transform shapes, reconciled
// against embedded NATS. The second reconcile simulates Argo self-healing.
func TestProductionSourcedPromotionSurvivesSelfHeal(t *testing.T) {
	for _, baseline := range productionStreams(t)["east"] {
		if baseline.Labels["drp.cicada.io/sourced"] != "true" {
			continue
		}
		t.Run(baseline.Spec.Name, func(t *testing.T) {
			srv, _, js, cleanup := newJSTestServerWithURL(t)
			defer cleanup()
			_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"synthetic.origin"}, Storage: nats.FileStorage})
			require.NoError(t, err)
			_, err = js.Publish("synthetic.origin", []byte("synthetic"))
			require.NoError(t, err)
			policy := nats.LimitsPolicy
			if baseline.Spec.Retention == "workqueue" {
				policy = nats.WorkQueuePolicy
			}
			_, err = js.AddStream(&nats.StreamConfig{Name: baseline.Spec.Name, Mirror: &nats.StreamSource{Name: "ORIGIN"}, Storage: nats.FileStorage, Retention: policy})
			require.NoError(t, err)
			require.Eventually(t, func() bool { s, e := js.StreamInfo(baseline.Spec.Name); return e == nil && s.State.Msgs == 1 }, 3*time.Second, 20*time.Millisecond)
			before, err := js.StreamInfo(baseline.Spec.Name)
			require.NoError(t, err)
			active, err := activeStreamSpec(&baseline)
			require.NoError(t, err)
			for _, source := range active.Sources {
				// A real operator writes this before changing the namespace role.
				source.OptStartSeq = 2
				_, err = js.AddStream(&nats.StreamConfig{Name: source.Name, Subjects: []string{source.SubjectTransforms[0].Source}, Storage: nats.FileStorage})
				require.NoError(t, err)
				_, err = js.Publish(strings.ReplaceAll(source.SubjectTransforms[0].Source, "*", "x"), []byte("synthetic historical work"))
				require.NoError(t, err)
			}
			active.Replicas = 1 // The embedded server is a single node.
			active.ConnectionOpts = api.ConnectionOpts{}
			cr := baseline.DeepCopy()
			// Simulate Argo restoring its baseline after operator preparation.
			resume, err := json.Marshal(active.Sources)
			require.NoError(t, err)
			cr.Annotations["drp.cicada.io/sources-resume-config"] = string(resume)
			cr.Spec.ConnectionOpts = api.ConnectionOpts{}
			cr.Spec.Replicas = 1
			cr.Generation = 1
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRoleActive}}}
			k8s := fake.NewClientBuilder().WithScheme(activeXlatScheme(t)).WithObjects(ns, cr).WithStatusSubresource(cr).Build()
			ctl, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "peer"})
			require.NoError(t, err)
			r := &StreamReconciler{Scheme: activeXlatScheme(t), JetStreamController: ctl}
			require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
			after, err := js.StreamInfo(cr.Spec.Name)
			require.NoError(t, err)
			require.Nil(t, after.Config.Mirror)
			require.Equal(t, before.Created, after.Created)
			require.Equal(t, before.State.Msgs, after.State.Msgs)
			require.Len(t, after.Config.Sources, len(active.Sources))
			for _, source := range active.Sources {
				_, err = js.Publish(strings.ReplaceAll(source.SubjectTransforms[0].Source, "*", "x"), []byte("synthetic new work"))
				require.NoError(t, err)
			}
			expected := before.State.Msgs + uint64(len(active.Sources))
			require.Eventually(t, func() bool { info, e := js.StreamInfo(cr.Spec.Name); return e == nil && info.State.Msgs == expected }, 3*time.Second, 20*time.Millisecond)
			// Argo restores the east mirror form, without operator resume hints.
			cr.Spec = baseline.Spec
			delete(cr.Annotations, "drp.cicada.io/sources-resume-config")
			cr.Spec.Replicas = 1
			cr.Spec.ConnectionOpts = api.ConnectionOpts{}
			cr.Generation++
			require.NoError(t, k8s.Update(context.Background(), cr))
			require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
			after, err = js.StreamInfo(cr.Spec.Name)
			require.NoError(t, err)
			require.Nil(t, after.Config.Mirror)
			require.Equal(t, before.Created, after.Created)
			require.Equal(t, expected, after.State.Msgs)
			for _, source := range after.Config.Sources {
				require.EqualValues(t, 2, source.OptStartSeq)
			}
		})
	}
}

func TestSourceResumeSurvivesSelfHealWithoutResumeAnnotation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		primaryBaseline bool
		byTime          bool
	}{
		{"mirror-baseline/time", false, true},
		{"primary-baseline/time", true, true},
		{"mirror-baseline/sequence", false, false},
		{"primary-baseline/sequence", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testSourceResumeWithoutAnnotation(t, tc.primaryBaseline, tc.byTime)
		})
	}
}

func testSourceResumeWithoutAnnotation(t *testing.T, primaryBaseline, byTime bool) {
	t.Helper()
	srv, _, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()
	_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"source.event"}, Storage: nats.FileStorage})
	require.NoError(t, err)
	_, err = js.Publish("source.event", []byte("historical"))
	require.NoError(t, err)
	start := time.Now().UTC()
	source := &nats.StreamSource{Name: "ORIGIN", OptStartSeq: 2}
	if byTime {
		source.OptStartSeq, source.OptStartTime = 0, &start
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name: "DEST", Storage: nats.FileStorage,
		Sources: []*nats.StreamSource{source},
	})
	require.NoError(t, err)
	cr := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dest", Namespace: "nats", Generation: 1,
			Labels:      map[string]string{scopeLabel: "true"},
			Annotations: map[string]string{"drp.cicada.io/sources-config": `[{"name":"ORIGIN"}]`},
		},
		Spec: api.StreamSpec{Name: "DEST", Mirror: &api.StreamSource{Name: "DEST"}, Storage: "file", Retention: "limits"},
	}
	if primaryBaseline {
		cr.Spec.Mirror = nil
		cr.Spec.Sources = []*api.StreamSource{{Name: "ORIGIN"}}
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRoleActive}}}
	k8s := fake.NewClientBuilder().WithScheme(activeXlatScheme(t)).WithObjects(ns, cr).WithStatusSubresource(cr).Build()
	ctl, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "peer"})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: activeXlatScheme(t), JetStreamController: ctl}
	require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
	after, err := js.StreamInfo("DEST")
	require.NoError(t, err)
	require.Len(t, after.Config.Sources, 1)
	if byTime {
		require.NotNil(t, after.Config.Sources[0].OptStartTime)
		require.True(t, start.Equal(*after.Config.Sources[0].OptStartTime), "preserve the complete timestamp, including subsecond precision")
	} else {
		require.EqualValues(t, 2, after.Config.Sources[0].OptStartSeq)
	}
	if primaryBaseline {
		require.Zero(t, cr.Spec.Sources[0].OptStartSeq)
		require.Empty(t, cr.Spec.Sources[0].OptStartTime)
	}
	_, err = js.Publish("source.event", []byte("new"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, e := js.StreamInfo("DEST")
		return e == nil && info.State.Msgs == 1
	}, 3*time.Second, 20*time.Millisecond)
	last, err := js.GetLastMsg("DEST", "source.event")
	require.NoError(t, err)
	require.Equal(t, "new", string(last.Data))
}

func TestActiveAnnotationChangeInvalidatesConvergedState(t *testing.T) {
	srv, _, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()
	cr := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dest", Namespace: "nats", Generation: 1,
			Labels:      map[string]string{scopeLabel: "true"},
			Annotations: map[string]string{"drp.cicada.io/promote-subjects": "old.event"},
		},
		Spec: api.StreamSpec{Name: "DEST", Mirror: &api.StreamSource{Name: "DEST"}, Storage: "file", Retention: "limits"},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRoleActive}}}
	k8s := fake.NewClientBuilder().WithScheme(activeXlatScheme(t)).WithObjects(ns, cr).WithStatusSubresource(cr).Build()
	ctl, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "peer"})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: activeXlatScheme(t), JetStreamController: ctl}
	require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
	before := cr.DeepCopy()
	cr.Annotations["drp.cicada.io/promote-subjects"] = "new.event"
	require.True(t, streamConfigChangedPredicate().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: cr}))
	require.NoError(t, k8s.Update(context.Background(), cr))
	require.Equal(t, cr.Generation, cr.Status.ObservedGeneration)
	require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
	after, err := js.StreamInfo("DEST")
	require.NoError(t, err)
	require.Equal(t, []string{"new.event"}, after.Config.Subjects)
	before = cr.DeepCopy()
	cr.Annotations[drpInputStateAnnotation] = "controller-only-state"
	require.False(t, streamConfigChangedPredicate().Update(event.UpdateEvent{ObjectOld: before, ObjectNew: cr}), "controller bookkeeping must not trigger a reconcile loop")
}
