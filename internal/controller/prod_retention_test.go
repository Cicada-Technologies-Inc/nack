package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestProductionMirrorRetention(t *testing.T) {
	for _, policy := range []string{"limits", "interest", "workqueue"} {
		t.Run(policy, func(t *testing.T) {
			_, js, cleanup := newJSTestServer(t)
			defer cleanup()
			policies := map[string]nats.RetentionPolicy{"limits": nats.LimitsPolicy, "interest": nats.InterestPolicy, "workqueue": nats.WorkQueuePolicy}
			_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"test.>"}, Storage: nats.FileStorage, Retention: policies[policy]})
			require.NoError(t, err)
			_, err = js.AddConsumer("ORIGIN", &nats.ConsumerConfig{Durable: "application", AckPolicy: nats.AckExplicitPolicy})
			require.NoError(t, err)
			translated := translateStreamSpecToMirror(&api.StreamSpec{Retention: policy}, "peer")
			retention := policies[translated.Retention]
			_, err = js.AddStream(&nats.StreamConfig{Name: "REPLICA", Mirror: &nats.StreamSource{Name: "ORIGIN"}, Retention: retention, Storage: nats.FileStorage})
			require.NoError(t, err)
			_, err = js.Publish("test.event", []byte("synthetic"))
			require.NoError(t, err)
			require.Eventually(t, func() bool { s, e := js.StreamInfo("REPLICA"); return e == nil && s.State.Msgs == 1 }, 3*time.Second, 20*time.Millisecond)
			origin, err := js.StreamInfo("ORIGIN")
			require.NoError(t, err)
			require.EqualValues(t, 1, origin.State.Msgs, "replication must retain pending application work at the origin")
		})
	}
}

func TestInterestPromotionPreservesBacklogAndDefersRetention(t *testing.T) {
	for _, primaryBaseline := range []bool{false, true} {
		name := "mirror-baseline"
		if primaryBaseline {
			name = "primary-baseline-failback"
		}
		t.Run(name, func(t *testing.T) {
			srv, _, js, cleanup := newJSTestServerWithURL(t)
			defer cleanup()
			_, err := js.AddStream(&nats.StreamConfig{Name: "ORIGIN", Subjects: []string{"origin.event"}, Storage: nats.FileStorage})
			require.NoError(t, err)
			_, err = js.Publish("origin.event", []byte("pending"))
			require.NoError(t, err)
			before, err := js.AddStream(&nats.StreamConfig{Name: "DEST", Mirror: &nats.StreamSource{Name: "ORIGIN"}, Storage: nats.FileStorage})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				info, e := js.StreamInfo("DEST")
				return e == nil && info.State.Msgs == 1
			}, 3*time.Second, 20*time.Millisecond)
			cr := &api.Stream{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dest", Namespace: "nats", Generation: 1,
					Labels: map[string]string{scopeLabel: "true"},
					Annotations: map[string]string{
						"drp.cicada.io/promote-subjects":  "active.event",
						"drp.cicada.io/promote-retention": "interest",
					},
				},
				Spec: api.StreamSpec{Name: "DEST", Storage: "file", Retention: "limits", Mirror: &api.StreamSource{Name: "DEST"}},
			}
			if primaryBaseline {
				cr.Spec.Mirror = nil
				cr.Spec.Subjects = []string{"active.event"}
				cr.Spec.Retention = "interest"
			}
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRoleActive}}}
			k8s := fake.NewClientBuilder().WithScheme(activeXlatScheme(t)).WithObjects(ns, cr).WithStatusSubresource(cr).Build()
			ctl, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "peer"})
			require.NoError(t, err)
			r := &StreamReconciler{Scheme: activeXlatScheme(t), JetStreamController: ctl}
			for pass := 0; pass < 2; pass++ {
				require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
				after, err := js.StreamInfo("DEST")
				require.NoError(t, err)
				require.Nil(t, after.Config.Mirror)
				require.Equal(t, nats.LimitsPolicy, after.Config.Retention)
				require.EqualValues(t, 1, after.State.Msgs)
				require.Equal(t, before.Created, after.Created)
				require.Zero(t, after.State.Consumers)
				require.Contains(t, cr.Status.Conditions[0].Message, "Effective retention remains limits")
			}
			_, err = js.Publish("active.event", []byte("new pending work"))
			require.NoError(t, err)
			after, err := js.StreamInfo("DEST")
			require.NoError(t, err)
			require.EqualValues(t, 2, after.State.Msgs)
		})
	}
}

func TestFreshPrimaryKeepsAuthoredInterestRetention(t *testing.T) {
	srv, _, js, cleanup := newJSTestServerWithURL(t)
	defer cleanup()
	cr := &api.Stream{
		ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "nats", Generation: 1, Labels: map[string]string{scopeLabel: "true"}},
		Spec:       api.StreamSpec{Name: "FRESH", Storage: "file", Retention: "interest", Subjects: []string{"fresh.event"}},
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nats", Annotations: map[string]string{localRoleAnnotation: localRoleActive}}}
	k8s := fake.NewClientBuilder().WithScheme(activeXlatScheme(t)).WithObjects(ns, cr).WithStatusSubresource(cr).Build()
	ctl, err := NewJSController(k8s, &NatsConfig{ServerURL: srv.ClientURL()}, &Config{EnablePassiveRoleTranslation: true, CrossRegionNATSDomain: "peer"})
	require.NoError(t, err)
	r := &StreamReconciler{Scheme: activeXlatScheme(t), JetStreamController: ctl}
	require.NoError(t, r.CreateOrUpdate(context.Background(), logr.Discard(), cr))
	info, err := js.StreamInfo("FRESH")
	require.NoError(t, err)
	require.Equal(t, nats.InterestPolicy, info.Config.Retention)
}
