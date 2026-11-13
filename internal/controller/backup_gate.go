package controller

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// Backup-confirmation binding.
//
// The destructive primary→mirror recreate is gated (see
// --require-backup-confirmation) on either the cross-region peer holding
// the local data or an external backup operator attesting that it
// captured it. Both proofs are bound to the stream's sequence range at
// decision time, not to a message count alone, and an attestation can
// authorize at most one destructive recreate:
//
//   - Peer proof: the peer's [FirstSeq, LastSeq] must contain the local
//     range and hold at least as many messages. Mirrors keep the origin's
//     sequence numbers, so equal counts over different ranges no longer
//     pass.
//
//   - Fingerprinted attestation: the confirmation annotation value
//     "gen=<g>,first=<f>,last=<l>,msgs=<n>" describes the state the
//     backup captured. It is accepted when <g> equals the CR generation
//     and the captured range contains the current local range.
//
//   - Generation-only attestation (the historical "<g>" format): it
//     carries no state, so it is only accepted as the answer to a demand
//     the controller recorded itself. When the gate fires, the controller
//     writes the demand annotation (generation + local fingerprint) and
//     removes any confirmation present at that moment in the same update,
//     so a "<g>" value observed later was necessarily written after the
//     demand. It is accepted while the local fingerprint still equals the
//     demanded one; if the stream moved, the demand is re-armed and the
//     stale confirmation removed, which makes the backup operator take a
//     fresh backup.
//
// Before any destructive recreate the controller removes both annotations
// (persisted before the delete), so a confirmation is single-use and a
// leftover from an earlier cycle cannot authorize a later recreate.

// backupDemandAnnotation records the generation and local fingerprint
// the controller demanded a backup for. Owned by the controller.
const backupDemandAnnotation = "jetstream.nats.io/backup-demand"

// streamFingerprint is the part of a stream's state a backup is bound to.
type streamFingerprint struct {
	First uint64
	Last  uint64
	Msgs  uint64
}

func (fp streamFingerprint) String() string {
	return fmt.Sprintf("first=%d,last=%d,msgs=%d", fp.First, fp.Last, fp.Msgs)
}

// coveredBy reports whether a capture described by `outer` contains
// every message of `fp`: its sequence range encloses fp's range and it
// holds at least as many messages.
func (fp streamFingerprint) coveredBy(outer streamFingerprint) bool {
	return outer.First <= fp.First && fp.Last <= outer.Last && fp.Msgs <= outer.Msgs
}

// backupRecord is a parsed confirmation or demand annotation value.
type backupRecord struct {
	Generation  int64
	Fingerprint streamFingerprint
	// HasFingerprint is false for the generation-only format.
	HasFingerprint bool
}

// formatBackupRecord renders the fingerprinted format.
func formatBackupRecord(generation int64, fp streamFingerprint) string {
	return fmt.Sprintf("gen=%d,%s", generation, fp)
}

// parseBackupRecord accepts "<generation>" or
// "gen=<g>,first=<f>,last=<l>,msgs=<n>" (keys in any order, all
// required, no duplicates). Anything else is rejected.
func parseBackupRecord(v string) (backupRecord, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return backupRecord{}, false
	}
	if !strings.Contains(v, "=") {
		gen, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return backupRecord{}, false
		}
		return backupRecord{Generation: gen}, true
	}
	fields := map[string]string{}
	for _, part := range strings.Split(v, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return backupRecord{}, false
		}
		if _, dup := fields[k]; dup {
			return backupRecord{}, false
		}
		fields[k] = val
	}
	if len(fields) != 4 {
		return backupRecord{}, false
	}
	gen, err := strconv.ParseInt(fields["gen"], 10, 64)
	if err != nil {
		return backupRecord{}, false
	}
	var nums [3]uint64
	for i, k := range []string{"first", "last", "msgs"} {
		n, err := strconv.ParseUint(fields[k], 10, 64)
		if err != nil {
			return backupRecord{}, false
		}
		nums[i] = n
	}
	return backupRecord{
		Generation:     gen,
		Fingerprint:    streamFingerprint{First: nums[0], Last: nums[1], Msgs: nums[2]},
		HasFingerprint: true,
	}, true
}

// backupConfirmed reports whether the CR's annotations authorize a
// destructive recreate of a stream whose current state is `local`.
func backupConfirmed(annotations map[string]string, confirmKey string, generation int64, local streamFingerprint) bool {
	conf, ok := parseBackupRecord(annotations[confirmKey])
	if !ok || conf.Generation != generation {
		return false
	}
	if conf.HasFingerprint {
		return local.coveredBy(conf.Fingerprint)
	}
	demand, ok := parseBackupRecord(annotations[backupDemandAnnotation])
	return ok && demand.HasFingerprint && demand.Generation == generation && demand.Fingerprint == local
}

// withBackupAnnotationsConsumed returns a copy of annotations without
// the confirmation and demand keys, or nil when neither is present.
func withBackupAnnotationsConsumed(annotations map[string]string, confirmKey string) map[string]string {
	_, hasConf := annotations[confirmKey]
	_, hasDemand := annotations[backupDemandAnnotation]
	if !hasConf && !hasDemand {
		return nil
	}
	out := maps.Clone(annotations)
	delete(out, confirmKey)
	delete(out, backupDemandAnnotation)
	return out
}

// withBackupDemandArmed returns a copy of annotations whose demand
// records (generation, local) and from which any confirmation that does
// not authorize this state is removed, or nil when nothing changes.
func withBackupDemandArmed(annotations map[string]string, confirmKey string, generation int64, local streamFingerprint) map[string]string {
	want := formatBackupRecord(generation, local)
	changed := annotations[backupDemandAnnotation] != want
	if _, hasConf := annotations[confirmKey]; hasConf && (changed || !backupConfirmed(annotations, confirmKey, generation, local)) {
		changed = true
	}
	if !changed {
		return nil
	}
	out := maps.Clone(annotations)
	if out == nil {
		out = map[string]string{}
	}
	delete(out, confirmKey)
	out[backupDemandAnnotation] = want
	return out
}

// backupGateResult is the outcome of a backup-gate evaluation.
type backupGateResult struct {
	// Fires means the destructive recreate must NOT run; Reason and
	// Message describe why for the BackupRequired condition.
	Fires   bool
	Reason  string
	Message string
	// Annotations, when non-nil, is the CR annotation set the caller
	// must persist BEFORE acting on the decision (before setting the
	// condition when Fires, before deleting the stream otherwise). A
	// failed persist must abort the reconcile without deleting.
	Annotations map[string]string
}

// peerCoverage is the cross-region probe result.
type peerCoverage struct {
	Exists      bool
	Fingerprint streamFingerprint
}

// decideBackupGate is the pure decision once local state is known and
// the gate is enabled. probe is only called when no confirmation applies;
// a nil probe means no cross-region URL is configured.
func decideBackupGate(annotations map[string]string, confirmKey string, generation int64, local streamFingerprint, subject string, probe func() (peerCoverage, error)) backupGateResult {
	if local.Msgs == 0 {
		return backupGateResult{Annotations: withBackupAnnotationsConsumed(annotations, confirmKey)}
	}
	if backupConfirmed(annotations, confirmKey, generation, local) {
		return backupGateResult{Annotations: withBackupAnnotationsConsumed(annotations, confirmKey)}
	}
	fire := func(reason, msg string) backupGateResult {
		return backupGateResult{
			Fires:       true,
			Reason:      reason,
			Message:     msg,
			Annotations: withBackupDemandArmed(annotations, confirmKey, generation, local),
		}
	}
	if probe == nil {
		return fire("NoProbeConfigured",
			fmt.Sprintf("local %s has %s and --cross-region-nats-url is unset", subject, local))
	}
	peer, err := probe()
	switch {
	case err != nil:
		return fire("PeerUnreachable",
			fmt.Sprintf("local %s has %s; cross-region probe failed: %v", subject, local, err))
	case !peer.Exists:
		return fire("PeerMissing",
			fmt.Sprintf("local %s has %s; cross-region peer stream not found", subject, local))
	case !local.coveredBy(peer.Fingerprint):
		return fire("PeerStale",
			fmt.Sprintf("local %s has %s; cross-region peer has %s", subject, local, peer.Fingerprint))
	}
	return backupGateResult{Annotations: withBackupAnnotationsConsumed(annotations, confirmKey)}
}
