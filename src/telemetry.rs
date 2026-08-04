//! Durable connection telemetry: does a lost session actually come back?
//!
//! Fabric already wrote a log line for every loss, reconnect attempt, and
//! resume. A line is enough to reconstruct one incident by hand, and it is
//! useless for the question that actually matters: does resumption work in
//! daily use? Answering that from logs meant grepping megabytes of text, and
//! the answer died with the next log rotation. These counters survive a daemon
//! restart, so the question has an answer that does not depend on somebody
//! still holding the right log file.
//!
//! Three things are recorded that the log could not answer:
//!
//! - **A durable count** of losses, resumes, and failed resumes, per peer.
//! - **The measured reconnect time.** The log carried the *backoff delay* for
//!   the next attempt, which is not the same number and is never the total.
//! - **The path in use**, direct or relay, beside each loss and each resume.
//!   A resume count that omits the path sends the next reader back to grepping,
//!   because "it came back" and "it came back on the relay" are different
//!   outcomes.
//!
//! Probe latency is retained here too, per peer and per path. The liveness
//! probe already measured a round trip time for whichever path it used and then
//! threw it away, so the only way to compare direct against relay was to parse
//! days of log text. Keeping it is what lets a later change compare a live path
//! against the relay. This module deliberately makes no such decision itself.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Upper bounds, in microseconds, for every latency bucket but the last.
///
/// One ladder serves two very different scales on purpose. A probe round trip
/// lives in the tens of milliseconds and a reconnect lives in seconds, so the
/// bounds stay dense where probes land and keep coarse steps out to a minute so
/// a slow reconnect still lands somewhere meaningful instead of in an overflow
/// bucket that reports nothing.
pub const LATENCY_BUCKET_BOUNDS_MICROS: [u64; 15] = [
    1_000,      // 1ms
    2_000,      // 2ms
    5_000,      // 5ms
    10_000,     // 10ms
    20_000,     // 20ms
    50_000,     // 50ms
    100_000,    // 100ms
    200_000,    // 200ms
    500_000,    // 500ms
    1_000_000,  // 1s
    2_000_000,  // 2s
    5_000_000,  // 5s
    10_000_000, // 10s
    30_000_000, // 30s
    60_000_000, // 60s
];

/// The path a session or probe used. Mirrors the daemon's own classification so
/// a reader does not have to learn a second vocabulary.
pub const PATH_UNKNOWN: &str = "unknown";

/// A bounded latency distribution.
///
/// Storing every sample would grow without limit on a daemon that runs for
/// weeks, so this keeps counts per bucket instead. That is enough to answer
/// "is the direct path worse than the relay at the tail", which is the question
/// the hand analysis had to answer by sorting raw samples.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    #[serde(default)]
    pub samples: u64,
    /// Sum of every sample, in microseconds.
    ///
    /// `u64`, NOT `u128`, and the difference is load-bearing. The control
    /// protocol is JSON, which cannot carry a `u128`, so a `u128` here made
    /// `fabric status` fail with "u128 is not supported" as soon as any peer had
    /// been probed. `u64` microseconds overflows after roughly 584,000 years.
    #[serde(default)]
    pub total_micros: u64,
    #[serde(default)]
    pub max_micros: u64,
    /// Counts aligned with [`LATENCY_BUCKET_BOUNDS_MICROS`], plus one final
    /// bucket for everything above the last bound.
    #[serde(default)]
    pub buckets: Vec<u64>,
}

impl LatencySummary {
    pub fn record(&mut self, micros: u64) {
        if self.buckets.len() != LATENCY_BUCKET_BOUNDS_MICROS.len() + 1 {
            self.buckets
                .resize(LATENCY_BUCKET_BOUNDS_MICROS.len() + 1, 0);
        }
        let index = LATENCY_BUCKET_BOUNDS_MICROS
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(LATENCY_BUCKET_BOUNDS_MICROS.len());
        self.buckets[index] += 1;
        self.samples += 1;
        self.total_micros = self.total_micros.saturating_add(micros);
        self.max_micros = self.max_micros.max(micros);
    }

    pub fn mean_micros(&self) -> Option<u64> {
        if self.samples == 0 {
            return None;
        }
        Some(self.total_micros / self.samples)
    }

    /// Approximate quantile, reported as the upper bound of the bucket the
    /// quantile falls in.
    ///
    /// The result is clamped to the largest sample actually seen. A bucket
    /// bound alone would let an empty tail report a latency that never
    /// happened, which is exactly the kind of number that starts a wrong
    /// investigation.
    pub fn quantile_micros(&self, quantile: f64) -> Option<u64> {
        if self.samples == 0 {
            return None;
        }
        let target = (self.samples as f64 * quantile).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                let bound = LATENCY_BUCKET_BOUNDS_MICROS
                    .get(index)
                    .copied()
                    .unwrap_or(self.max_micros);
                return Some(bound.min(self.max_micros));
            }
        }
        Some(self.max_micros)
    }
}

/// Everything recorded for one peer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerTelemetry {
    /// Transports lost. Counted once per loss, not once per retry.
    #[serde(default)]
    pub losses: u64,
    /// Losses that came back.
    #[serde(default)]
    pub resumes: u64,
    /// Losses that gave up.
    #[serde(default)]
    pub resume_failures: u64,
    /// Retry attempts across every loss. A loss that took 4 tries to come back
    /// counts once in `losses` and 4 times here.
    #[serde(default)]
    pub reconnect_attempts: u64,
    /// Wall time from a loss to its resume. This is the number the log never
    /// had: it carried the delay before the *next* attempt, never the total.
    #[serde(default)]
    pub reconnect: LatencySummary,
    /// Losses keyed by the path that was in use when the transport died.
    #[serde(default)]
    pub losses_by_path: BTreeMap<String, u64>,
    /// Resumes keyed by the path the session came back on. This may differ from
    /// the path it was lost on, and that difference is the interesting case.
    #[serde(default)]
    pub resumes_by_path: BTreeMap<String, u64>,
    /// Liveness probes that answered, keyed by path.
    #[serde(default)]
    pub probe_latency: BTreeMap<String, LatencySummary>,
    #[serde(default)]
    pub probes_reachable: u64,
    #[serde(default)]
    pub probes_unreachable: u64,
}

/// The persisted form. Versioned so a later shape change can be detected rather
/// than silently misread.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub peers: BTreeMap<String, PeerTelemetry>,
}

const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Default)]
struct Inner {
    peers: BTreeMap<String, PeerTelemetry>,
    /// When each peer's current loss started. Deliberately not persisted: a
    /// restart destroys the sessions these refer to, so a stale start time
    /// could only ever produce a fabricated reconnect duration.
    losses_in_flight: BTreeMap<String, Instant>,
}

/// Durable per-peer connection counters.
///
/// Every recorded event writes the file under the lock, rather than batching
/// behind a flush timer. A loss is rare and is exactly the incident these
/// counters exist to record, so a flush window would lose the one event that
/// mattered. Probes are not rare, and they pay the same write: at the default
/// 20 second interval that is a few kilobytes per peer per probe, alongside the
/// log line the same probe already writes, so it adds no new order of
/// magnitude.
///
/// The file cannot grow without bound. Peers come from the peer book, paths come
/// from a fixed set, and latency is bucketed rather than sampled, so the size is
/// a product of three bounded things and not of uptime.
#[derive(Debug)]
pub struct TelemetryStore {
    path: Option<PathBuf>,
    inner: Mutex<Inner>,
}

impl TelemetryStore {
    /// Load from disk, or start empty when the file is absent, unreadable, or
    /// written by a different layout.
    ///
    /// A corrupt telemetry file must never stop a daemon from starting. These
    /// are counters, not state the product depends on, so a bad read restarts
    /// the counts and says so.
    ///
    /// The version check is not ceremony. A bucket holds a COUNT, and its
    /// meaning lives entirely in [`LATENCY_BUCKET_BOUNDS_MICROS`], which is not
    /// stored beside it. Change those bounds and every historical count is
    /// silently reattributed: 100 probes recorded at 64ms sit in the bucket
    /// whose bound is 100ms, and under a plausible refinement that same index
    /// means 60ms, so the daemon would report a latency that never happened.
    /// Nothing errors, because a same-length change never triggers a resize.
    /// Refusing a foreign version is what keeps that from being silent.
    pub fn load(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let peers = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<TelemetrySnapshot>(&bytes) {
                Ok(snapshot) if snapshot.version == SNAPSHOT_VERSION => snapshot.peers,
                Ok(snapshot) => {
                    eprintln!(
                        "fabric: telemetry at {} is version {}, expected {}; starting the counts over rather than misreading its latency buckets",
                        path.display(),
                        snapshot.version,
                        SNAPSHOT_VERSION
                    );
                    BTreeMap::new()
                }
                Err(error) => {
                    eprintln!(
                        "fabric: ignoring unreadable telemetry at {}: {error}",
                        path.display()
                    );
                    BTreeMap::new()
                }
            },
            Err(_) => BTreeMap::new(),
        };
        Self {
            path: Some(path),
            inner: Mutex::new(Inner {
                peers,
                losses_in_flight: BTreeMap::new(),
            }),
        }
    }

    /// An in-memory store that never touches a disk. For tests and for a daemon
    /// with no writable home.
    pub fn ephemeral() -> Self {
        Self {
            path: None,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Record a lost transport.
    ///
    /// `attempt` is the session's running retry count and it does NOT reset per
    /// loss, so it cannot be used to tell a new loss from a retry. The in-flight
    /// entry is what separates them: the first event opens it and only a resume
    /// or a failure closes it. Every later retry for the same break therefore
    /// adds to `reconnect_attempts` without inventing a second loss.
    pub fn record_loss(&self, peer: &str, path: Option<&str>, attempt: u64, now: Instant) {
        let _ = attempt;
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let fresh = !inner.losses_in_flight.contains_key(peer);
        if fresh {
            inner.losses_in_flight.insert(peer.to_string(), now);
        }
        let entry = inner.peers.entry(peer.to_string()).or_default();
        entry.reconnect_attempts += 1;
        if fresh {
            entry.losses += 1;
            *entry
                .losses_by_path
                .entry(path.unwrap_or(PATH_UNKNOWN).to_string())
                .or_default() += 1;
        }
        self.persist_locked(&inner);
    }

    /// Record a transport that came back, and how long it took.
    ///
    /// A resume with no matching in-flight loss records no duration rather than
    /// a zero. A zero would be a measurement, and there was none.
    pub fn record_resume(&self, peer: &str, path: Option<&str>, now: Instant) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let started = inner.losses_in_flight.remove(peer);
        let entry = inner.peers.entry(peer.to_string()).or_default();
        entry.resumes += 1;
        *entry
            .resumes_by_path
            .entry(path.unwrap_or(PATH_UNKNOWN).to_string())
            .or_default() += 1;
        if let Some(started) = started {
            let elapsed = now.saturating_duration_since(started);
            entry.reconnect.record(duration_micros(elapsed));
        }
        self.persist_locked(&inner);
    }

    /// Record a loss that gave up. Closes the in-flight loss without recording a
    /// reconnect time, because the reconnect never finished.
    pub fn record_resume_failure(&self, peer: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner.losses_in_flight.remove(peer);
        inner
            .peers
            .entry(peer.to_string())
            .or_default()
            .resume_failures += 1;
        self.persist_locked(&inner);
    }

    /// Record one liveness probe.
    ///
    /// This is the measurement the probe used to compute and discard. Keeping it
    /// per path is what makes a later direct-against-relay comparison possible
    /// without parsing log text.
    pub fn record_probe(
        &self,
        peer: &str,
        reachable: bool,
        path: Option<&str>,
        round_trip: Option<Duration>,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner.peers.entry(peer.to_string()).or_default();
        if reachable {
            entry.probes_reachable += 1;
            if let Some(round_trip) = round_trip {
                entry
                    .probe_latency
                    .entry(path.unwrap_or(PATH_UNKNOWN).to_string())
                    .or_default()
                    .record(duration_micros(round_trip));
            }
        } else {
            entry.probes_unreachable += 1;
        }
        self.persist_locked(&inner);
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        TelemetrySnapshot {
            version: SNAPSHOT_VERSION,
            peers: inner.peers.clone(),
        }
    }

    pub fn peer(&self, peer: &str) -> Option<PeerTelemetry> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner.peers.get(peer).cloned()
    }

    /// Write the current counts. Failure is reported and never propagated: a
    /// full disk must not take down a shell session that is otherwise fine.
    fn persist_locked(&self, inner: &Inner) {
        let Some(path) = self.path.as_ref() else {
            return;
        };
        let snapshot = TelemetrySnapshot {
            version: SNAPSHOT_VERSION,
            peers: inner.peers.clone(),
        };
        if let Err(error) = write_snapshot(path, &snapshot) {
            eprintln!(
                "fabric: failed to persist telemetry to {}: {error:#}",
                path.display()
            );
        }
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

/// Write to a temp sibling then rename, so a crash mid-write leaves the previous
/// counts rather than a truncated file that the next load would discard.
fn write_snapshot(path: &Path, snapshot: &TelemetrySnapshot) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    let tmp = path.with_extension("json.fabric-tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename into {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    #[test]
    fn a_loss_and_resume_counts_once_and_measures_the_total() {
        let store = TelemetryStore::ephemeral();
        let base = Instant::now();
        store.record_loss("droppy", Some("direct"), 1, base);
        store.record_resume("droppy", Some("relay"), at(base, 2_500));

        let peer = store.peer("droppy").expect("peer recorded");
        assert_eq!(peer.losses, 1);
        assert_eq!(peer.resumes, 1);
        assert_eq!(peer.reconnect.samples, 1);
        assert_eq!(peer.losses_by_path.get("direct"), Some(&1));
        assert_eq!(
            peer.resumes_by_path.get("relay"),
            Some(&1),
            "the path it came back on must be recorded separately from the one it lost"
        );
        let measured = peer.reconnect.max_micros;
        assert!(
            (2_400_000..=2_600_000).contains(&measured),
            "reconnect total should be about 2.5s, got {measured}us"
        );
    }

    /// The negative control. Every wrong finding in this repo's history came
    /// from never proving the tested condition happened, so prove the counter
    /// stays still when nothing breaks.
    #[test]
    fn nothing_moves_when_no_loss_happens() {
        let store = TelemetryStore::ephemeral();
        store.record_probe(
            "droppy",
            true,
            Some("direct"),
            Some(Duration::from_millis(51)),
        );
        store.record_probe(
            "droppy",
            true,
            Some("relay"),
            Some(Duration::from_millis(64)),
        );

        let peer = store.peer("droppy").expect("peer recorded");
        assert_eq!(peer.losses, 0, "a healthy probe must not record a loss");
        assert_eq!(peer.resumes, 0, "a healthy probe must not record a resume");
        assert_eq!(peer.resume_failures, 0);
        assert_eq!(peer.reconnect_attempts, 0);
        assert_eq!(
            peer.reconnect.samples, 0,
            "no loss means no reconnect duration, not a zero-length one"
        );
        assert_eq!(peer.probes_reachable, 2);
    }

    #[test]
    fn retries_for_one_break_do_not_invent_extra_losses() {
        let store = TelemetryStore::ephemeral();
        let base = Instant::now();
        // `attempt` is monotonic per session and never resets, so a second
        // break arrives with a higher number, not with 1.
        store.record_loss("hetz", Some("direct"), 7, base);
        store.record_loss("hetz", Some("direct"), 8, at(base, 500));
        store.record_loss("hetz", Some("direct"), 9, at(base, 1_500));
        store.record_resume("hetz", Some("direct"), at(base, 3_000));

        let peer = store.peer("hetz").expect("peer recorded");
        assert_eq!(peer.losses, 1, "three retries are one break");
        assert_eq!(peer.reconnect_attempts, 3);
        assert_eq!(peer.resumes, 1);
        assert_eq!(peer.reconnect.samples, 1);
        assert_eq!(
            peer.losses_by_path.get("direct"),
            Some(&1),
            "the path must be counted once per break, not once per retry"
        );
    }

    #[test]
    fn a_failed_resume_records_no_duration_and_frees_the_next_loss() {
        let store = TelemetryStore::ephemeral();
        let base = Instant::now();
        store.record_loss("bluey", Some("direct"), 1, base);
        store.record_resume_failure("bluey");

        let peer = store.peer("bluey").expect("peer recorded");
        assert_eq!(peer.resume_failures, 1);
        assert_eq!(
            peer.reconnect.samples, 0,
            "a reconnect that never finished has no duration to report"
        );

        // The next genuine break must still count, which only works if the
        // failure cleared the in-flight entry.
        store.record_loss("bluey", Some("relay"), 2, at(base, 10_000));
        assert_eq!(store.peer("bluey").unwrap().losses, 2);
    }

    #[test]
    fn a_resume_with_no_recorded_loss_reports_no_duration() {
        let store = TelemetryStore::ephemeral();
        store.record_resume("droppy", Some("direct"), Instant::now());
        let peer = store.peer("droppy").unwrap();
        assert_eq!(peer.resumes, 1);
        assert_eq!(
            peer.reconnect.samples, 0,
            "an unmatched resume must not report a zero-length reconnect"
        );
    }

    #[test]
    fn counts_survive_a_reload_from_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");
        let base = Instant::now();
        {
            let store = TelemetryStore::load(&path);
            store.record_loss("droppy", Some("direct"), 1, base);
            store.record_resume("droppy", Some("relay"), at(base, 1_200));
            store.record_probe(
                "droppy",
                true,
                Some("relay"),
                Some(Duration::from_millis(64)),
            );
        }

        let reloaded = TelemetryStore::load(&path);
        let peer = reloaded.peer("droppy").expect("counts survive a restart");
        assert_eq!(peer.losses, 1);
        assert_eq!(peer.resumes, 1);
        assert_eq!(peer.resumes_by_path.get("relay"), Some(&1));
        assert_eq!(peer.reconnect.samples, 1);
        assert_eq!(peer.probe_latency.get("relay").map(|l| l.samples), Some(1));
    }

    #[test]
    fn an_in_flight_loss_does_not_survive_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");
        {
            let store = TelemetryStore::load(&path);
            store.record_loss("droppy", Some("direct"), 1, Instant::now());
        }

        // The session this loss belonged to died with the old process, so a
        // resume after restart must not measure across the restart boundary.
        let reloaded = TelemetryStore::load(&path);
        reloaded.record_resume("droppy", Some("direct"), Instant::now());
        let peer = reloaded.peer("droppy").unwrap();
        assert_eq!(peer.losses, 1);
        assert_eq!(peer.resumes, 1);
        assert_eq!(
            peer.reconnect.samples, 0,
            "a duration measured across a restart would be fiction"
        );
    }

    /// Changing the bucket bounds MUST bump the snapshot version.
    ///
    /// This pins both together on purpose, because the coupling is otherwise
    /// invisible. A bucket stores only a count; its meaning lives in the bounds,
    /// and the bounds are not written to the file. Change them without bumping
    /// the version and every historical count is silently reattributed — a
    /// same-length change does not even trigger a resize, so nothing errors and
    /// the daemon reports latencies that never occurred.
    ///
    /// If this test fails you changed one of the two. Change the other.
    #[test]
    fn changing_the_latency_buckets_requires_a_new_snapshot_version() {
        assert_eq!(
            LATENCY_BUCKET_BOUNDS_MICROS,
            [
                1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000, 1_000_000,
                2_000_000, 5_000_000, 10_000_000, 30_000_000, 60_000_000
            ],
            "the latency buckets changed; bump SNAPSHOT_VERSION so old files are \
             discarded instead of silently reinterpreted, then update this test"
        );
        assert_eq!(
            SNAPSHOT_VERSION, 1,
            "the snapshot version changed; confirm the bucket bounds above still \
             match what that version means"
        );
    }

    /// A file from another layout is discarded, not misread.
    #[test]
    fn a_foreign_version_starts_clean_rather_than_reinterpreting_buckets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");

        // A well-formed file that a future layout could plausibly have written:
        // the counts are real, but the bounds that gave them meaning are gone.
        let store = TelemetryStore::load(&path);
        store.record_probe(
            "droppy",
            true,
            Some("relay"),
            Some(Duration::from_millis(64)),
        );
        let mut snapshot = store.snapshot();
        snapshot.version = SNAPSHOT_VERSION + 1;
        std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();

        let reloaded = TelemetryStore::load(&path);
        assert!(
            reloaded.peer("droppy").is_none(),
            "counts from an unknown layout must be dropped; keeping them would \
             report latencies computed against bounds that never applied"
        );

        // And it still records normally afterwards, rather than staying broken.
        reloaded.record_probe(
            "droppy",
            true,
            Some("relay"),
            Some(Duration::from_millis(70)),
        );
        assert_eq!(
            reloaded.peer("droppy").unwrap().probe_latency["relay"].samples,
            1
        );
    }

    #[test]
    fn a_matching_version_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");
        {
            let store = TelemetryStore::load(&path);
            store.record_probe(
                "hetz",
                true,
                Some("direct"),
                Some(Duration::from_millis(64)),
            );
        }
        let reloaded = TelemetryStore::load(&path);
        assert_eq!(
            reloaded.peer("hetz").map(|p| p.probes_reachable),
            Some(1),
            "the current version must survive a reload, or the check is too strict \
             and quietly discards every restart"
        );
    }

    #[test]
    fn a_corrupt_file_starts_clean_instead_of_failing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");
        std::fs::write(&path, b"{ this is not json").expect("write");
        let store = TelemetryStore::load(&path);
        assert!(store.snapshot().peers.is_empty());
        store.record_loss("droppy", Some("direct"), 1, Instant::now());
        assert_eq!(store.peer("droppy").unwrap().losses, 1);
    }

    #[test]
    fn probe_latency_separates_the_paths_it_measured() {
        let store = TelemetryStore::ephemeral();
        for _ in 0..10 {
            store.record_probe(
                "droppy",
                true,
                Some("direct"),
                Some(Duration::from_millis(50)),
            );
        }
        for _ in 0..10 {
            store.record_probe(
                "droppy",
                true,
                Some("relay"),
                Some(Duration::from_millis(64)),
            );
        }
        store.record_probe("droppy", false, None, None);

        let peer = store.peer("droppy").unwrap();
        assert_eq!(peer.probes_reachable, 20);
        assert_eq!(peer.probes_unreachable, 1);
        let direct = peer.probe_latency.get("direct").expect("direct measured");
        let relay = peer.probe_latency.get("relay").expect("relay measured");
        assert_eq!(direct.samples, 10);
        assert_eq!(relay.samples, 10);
        assert!(
            direct.mean_micros() < relay.mean_micros(),
            "the two paths must be summarized separately, not pooled"
        );
    }

    #[test]
    fn an_unreachable_probe_records_no_latency() {
        let store = TelemetryStore::ephemeral();
        store.record_probe("bluey", false, None, None);
        let peer = store.peer("bluey").unwrap();
        assert_eq!(peer.probes_unreachable, 1);
        assert!(
            peer.probe_latency.is_empty(),
            "a probe that never answered has no round trip to record"
        );
    }

    #[test]
    fn quantiles_never_exceed_the_largest_sample_seen() {
        let mut summary = LatencySummary::default();
        for _ in 0..99 {
            summary.record(50_000);
        }
        summary.record(600_000);
        let p99 = summary.quantile_micros(0.99).expect("p99");
        assert!(
            p99 <= summary.max_micros,
            "p99 {p99} must not exceed the observed max {}",
            summary.max_micros
        );
        assert!(summary.quantile_micros(0.5).unwrap() <= 50_000);
    }

    #[test]
    fn an_empty_summary_reports_nothing_rather_than_zero() {
        let summary = LatencySummary::default();
        assert_eq!(summary.mean_micros(), None);
        assert_eq!(summary.quantile_micros(0.5), None);
    }

    /// A populated snapshot must round-trip as plain JSON.
    ///
    /// Necessary but NOT sufficient: this passed while `fabric status` was
    /// broken, because the real wire type wraps this in an internally tagged
    /// enum. The test that actually catches that lives in `control`, and this
    /// one is kept only to pin the inner shape.
    #[test]
    fn a_populated_snapshot_survives_plain_json() {
        let store = TelemetryStore::ephemeral();
        let base = Instant::now();
        store.record_loss("droppy", Some("direct"), 1, base);
        store.record_resume("droppy", Some("relay"), base + Duration::from_millis(1_500));
        store.record_probe(
            "droppy",
            true,
            Some("relay"),
            Some(Duration::from_millis(64)),
        );
        let snapshot = store.snapshot();
        assert!(
            !snapshot.peers.is_empty(),
            "an empty snapshot is the case that never broke; this must be populated"
        );

        let bytes = serde_json::to_vec(&snapshot).expect("a snapshot must be serializable");
        let decoded: TelemetrySnapshot =
            serde_json::from_slice(&bytes).expect("a snapshot must round-trip");
        assert_eq!(
            decoded, snapshot,
            "every counter must survive the control protocol unchanged"
        );

        let peer = &decoded.peers["droppy"];
        assert_eq!(peer.losses, 1);
        assert_eq!(peer.reconnect.samples, 1);
        assert!(
            peer.reconnect.total_micros > 0,
            "the measured total must cross the wire, not just its sample count"
        );
        assert_eq!(peer.probe_latency["relay"].samples, 1);
    }

    /// The file size must be a product of peers and paths, not of uptime.
    ///
    /// This daemon runs for weeks, and it writes the whole file on every event.
    /// Bucketing latency instead of keeping samples is what makes that safe, so
    /// prove the property rather than trusting the intent.
    #[test]
    fn the_file_stays_bounded_as_events_accumulate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("telemetry.json");
        let store = TelemetryStore::load(&path);
        let peers = ["hetz", "droppy", "bluey", "mac"];
        for round in 0..500u64 {
            for peer in peers {
                let via = if round % 2 == 0 { "direct" } else { "relay" };
                store.record_probe(
                    peer,
                    true,
                    Some(via),
                    Some(Duration::from_millis(40 + round % 90)),
                );
            }
        }
        let after_probes = std::fs::metadata(&path).expect("written").len();

        for round in 0..500u64 {
            for peer in peers {
                store.record_loss(peer, Some("direct"), round, Instant::now());
                store.record_resume(peer, Some("relay"), Instant::now());
            }
        }
        let after_losses = std::fs::metadata(&path).expect("written").len();

        assert!(
            after_losses < 16_384,
            "4 peers and 4000 events must stay small; got {after_losses} bytes"
        );
        // Growth comes from newly-seen (peer, path) pairs, not from event count.
        assert!(
            after_losses < after_probes * 3,
            "size grew with events rather than with distinct peers and paths: \
             {after_probes} -> {after_losses}"
        );
    }
}
