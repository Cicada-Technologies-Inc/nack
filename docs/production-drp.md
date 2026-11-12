# Production DR stream configuration

The control-loop controller accepts the existing DR labels and these annotations
on scope-labeled Streams when passive-role translation is enabled:

| Annotation | Purpose |
| --- | --- |
| `drp.cicada.io/promote-subjects` | Semicolon-separated subjects for a mirror baseline's active configuration. |
| `drp.cicada.io/sources-config` | JSON StreamSource array, including subject transforms, restored on promotion. |
| `drp.cicada.io/promote-retention` | Original limits, interest or workqueue retention. |
| `drp.cicada.io/sources-resume-config` | Runtime source resume hints prepared by drp-operator before activating the namespace. |

Primary translation preserves authored sources. Mirror translation clears
subjects and sources; interest retention becomes limits while passive so the
replica retains events without local consumers. An existing limits stream stays
limits during promotion and subsequent reconciliation, including a primary-form
west baseline. NATS 2.15.2 can compact a limits-to-interest backlog immediately
when no application interest exists. Deferred retention is reported in logs
and Ready.message. Restore interest only through a separately reviewed server
operation after verifying all required application consumer interest; a fresh
primary still uses its authored interest policy. Workqueue retention remains
workqueue, since NATS cannot update to/from that policy in place.

Promotion removes the server mirror in a separate update before adding sources
or subjects. The stream is never deleted on this path. Resume hints survive
Argo restoring the baseline spec, and are applied only to matching source
configurations. If the runtime annotation is absent, matching hints already
applied on the server are preserved, including OptStartTime. Changes to the DR
configuration annotations trigger reconciliation even without a Generation
change, and invalidate the stored convergence fingerprint.

The operator must prepare resume hints before switching roles;
its paired change refuses activation when a hint cannot be resolved.

`internal/controller/testdata/production_streams.json` is a metadata-only render
of all 49 production streams and two KV buckets in each region from
`gitops-platform@1814dc01b` plus the accompanying `feat/prod-stream-coverage`
branch. Tests cover configuration recovery for every Stream, passive retention,
and all five production source/transform shapes with embedded NATS and synthetic
traffic. The latter simulate Argo restoring a baseline both before and after
activation, verify the physical stream survives and confirm new work arrives
without replaying historical work.

Review order is this Cicada fork, the paired drp-operator commit pin, then
GitOps. All PRs are drafts while deployment is deferred. No server upgrade or
feature activation is part of publishing the branches. A future rollout must
install this NACK build before the paired operator and GitOps manifests. Existing primary-to-mirror migrations remain subject
to the backup gate; review current snapshots and stream state before migrating.

Live verification on 2026-11-12 confirmed all 49 Stream CRs and two KeyValue CRs
in each production region and all 51 server-side mirrors in the passive east
region. Tests also passed against the deployed NATS 2.14.2 via a temporary
modfile. The additional `KV_mpa_bridge_screens` is application-created and has
no nack CR; it remains outside this declared scope. Full read-only evidence is
in drp-operator's `docs/drp-nats/production-inventory-2026-11-12.json`.

Source resume hints do not synchronize application ACKs or remove historical
messages already retained in a passive workqueue mirror. A zero mirror lag
does not guarantee that already-acknowledged work will not replay on failover.

The paired operator adds a default-off `reconcileWorkQueueAcks` option. It uses
an exact, durable pre-demote capture and prepares application interest before
reconciling captured deleted sequences. NACK itself does not synchronize ACKs.
The operator documentation describes unsupported source-loss-before-capture,
namespace/account coordination and stream recreation races. Do not interpret
configuration coverage as an exactly-once processing guarantee.

Validation uses NATS 2.15.2 and the repository's Go 1.27 toolchain:

```sh
go test ./internal/controller ./controllers/jetstream ./pkg/roletranslate -count=1
```

The controller-runtime suite requires envtest binaries through
`KUBEBUILDER_ASSETS`; the embedded NATS cases bind local sockets. The stream
fixture contains metadata only. Production source names must be unique within
a sourced stream for the paired operator's current resume resolver; duplicate
names with independent domains/filters are rejected rather than merged.
