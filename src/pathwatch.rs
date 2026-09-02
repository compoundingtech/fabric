//! Path-quality classification and instrumentation for the shared peer
//! connection.
//!
//! iroh 1.0 connections are **multipath**: `Connection::paths()` yields several
//! concurrent paths (direct IPv4, direct IPv6, relay), one of which is
//! *selected* for application data. Each path exposes its own RTT, its
//! direct-vs-relay class, and its remote/local transport address (the UDP
//! 4-tuple). The daemon's periodic health poll only checks `Endpoint::online()`
//! (which stays true through a degradation) and the endpoint snapshot logs only
//! address *counts* — neither can show which path is hot or how slow it is.
//!
//! The daemon samples the real shared connection. It records every path and
//! redials only after a high absolute and relative delay persists.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use iroh::{EndpointId, endpoint::Connection};
use tracing::info;

/// The validation-log target, matching the daemon's other diagnostics.
const PATHWATCH_TARGET: &str = "fabric::validation";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathQualityAction {
    None,
    Redial {
        class: String,
        baseline: Duration,
        observed: Duration,
    },
}

#[derive(Debug, Clone)]
struct ClassQuality {
    baseline: Duration,
    consecutive_degraded: usize,
}

#[derive(Debug, Clone)]
struct PeerQuality {
    generation: u64,
    warmup_remaining: usize,
    classes: HashMap<String, ClassQuality>,
    cooldown_until: Option<Instant>,
}

/// A conservative per-peer degraded-path classifier.
pub struct PathQualityTracker {
    absolute_floor: Duration,
    baseline_multiplier: u32,
    consecutive_required: usize,
    warmup_samples: usize,
    cooldown: Duration,
    peers: HashMap<EndpointId, PeerQuality>,
}

impl PathQualityTracker {
    pub fn new(
        absolute_floor: Duration,
        baseline_multiplier: u32,
        consecutive_required: usize,
        warmup_samples: usize,
        cooldown: Duration,
    ) -> Self {
        Self {
            absolute_floor,
            baseline_multiplier: baseline_multiplier.max(1),
            consecutive_required: consecutive_required.max(1),
            warmup_samples,
            cooldown,
            peers: HashMap::new(),
        }
    }

    pub fn on_sample(
        &mut self,
        peer: EndpointId,
        generation: u64,
        class: &str,
        observed: Duration,
        now: Instant,
    ) -> PathQualityAction {
        let state = self.peers.entry(peer).or_insert_with(|| PeerQuality {
            generation,
            warmup_remaining: self.warmup_samples,
            classes: HashMap::new(),
            cooldown_until: None,
        });
        if state.generation != generation {
            *state = PeerQuality {
                generation,
                warmup_remaining: self.warmup_samples,
                classes: HashMap::new(),
                cooldown_until: None,
            };
        }

        let quality = state
            .classes
            .entry(class.to_string())
            .or_insert(ClassQuality {
                baseline: observed,
                consecutive_degraded: 0,
            });
        if state.warmup_remaining > 0 {
            state.warmup_remaining -= 1;
            quality.baseline = quality.baseline.min(observed);
            quality.consecutive_degraded = 0;
            return PathQualityAction::None;
        }
        if state.cooldown_until.is_some_and(|until| now < until) {
            quality.consecutive_degraded = 0;
            return PathQualityAction::None;
        }

        let degraded = observed >= self.absolute_floor
            && observed >= quality.baseline.saturating_mul(self.baseline_multiplier);
        if !degraded {
            quality.baseline = quality.baseline.min(observed);
            quality.consecutive_degraded = 0;
            return PathQualityAction::None;
        }
        quality.consecutive_degraded = quality.consecutive_degraded.saturating_add(1);
        if quality.consecutive_degraded < self.consecutive_required {
            return PathQualityAction::None;
        }

        quality.consecutive_degraded = 0;
        state.cooldown_until = Some(now + self.cooldown);
        PathQualityAction::Redial {
            class: class.to_string(),
            baseline: quality.baseline,
            observed,
        }
    }
}

/// A snapshot of one iroh path at one instant.
#[derive(Debug, Clone)]
pub struct PathObservation {
    pub id: String,
    pub selected: bool,
    /// "direct" (IP), "relay", or "other".
    pub class: &'static str,
    pub remote: String,
    pub local: String,
    pub rtt: Duration,
}

/// All paths of a connection at one instant, with derived aggregates.
#[derive(Debug, Clone, Default)]
pub struct PathObservations {
    pub paths: Vec<PathObservation>,
    /// "class:remote_addr" of the selected path, if any.
    pub selected: Option<String>,
    /// Minimum RTT across all paths.
    pub min_rtt: Option<Duration>,
}

/// Observe a connection's current multipath state. Pure read of the iroh path
/// API — the log/diff wrappers build on this, and tests assert on it directly.
pub fn observe_paths(connection: &Connection) -> PathObservations {
    let mut out = PathObservations::default();
    for path in connection.paths().iter() {
        let rtt = path.rtt();
        out.min_rtt = Some(out.min_rtt.map_or(rtt, |m| m.min(rtt)));
        let class = if path.is_ip() {
            "direct"
        } else if path.is_relay() {
            "relay"
        } else {
            "other"
        };
        if path.is_selected() {
            out.selected = Some(format!("{class}:{}", path.remote_addr()));
        }
        out.paths.push(PathObservation {
            id: format!("{:?}", path.id()),
            selected: path.is_selected(),
            class,
            remote: format!("{}", path.remote_addr()),
            local: format!("{:?}", path.local_addr()),
            rtt,
        });
    }
    out
}

/// Log every path's state plus an aggregate, flagging a selected-path change.
pub fn log_paths(
    connection: &Connection,
    peer_label: &str,
    app_rtt: Duration,
    last_selected: &mut Option<String>,
) {
    let observed = observe_paths(connection);

    // Per-path lines: the granular record for correlation.
    for path in &observed.paths {
        info!(
            target: PATHWATCH_TARGET,
            event = "pathwatch_path",
            peer = %peer_label,
            path_id = %path.id,
            selected = path.selected,
            class = path.class,
            remote_addr = %path.remote,
            local_addr = %path.local,
            rtt_ms = path.rtt.as_secs_f64() * 1000.0,
        );
    }

    // Aggregate line: the at-a-glance signal, with a selected-path change flag
    // (a re-selection is exactly the recovery behaviour we are checking for).
    let selected_changed = observed.selected != *last_selected;
    info!(
        target: PATHWATCH_TARGET,
        event = "pathwatch_snapshot",
        peer = %peer_label,
        path_count = observed.paths.len(),
        selected = observed.selected.as_deref().unwrap_or("none"),
        selected_changed,
        min_path_rtt_ms = observed
            .min_rtt
            .map(|r| r.as_secs_f64() * 1000.0)
            .unwrap_or(f64::NAN),
        app_rtt_ms = app_rtt.as_secs_f64() * 1000.0,
    );
    *last_selected = observed.selected;
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn tracker() -> PathQualityTracker {
        PathQualityTracker::new(Duration::from_secs(1), 8, 3, 2, Duration::from_secs(60))
    }

    #[test]
    fn normal_direct_path_jitter_keeps_a_stable_baseline() {
        let peer = SecretKey::generate().public();
        let now = Instant::now();
        let mut tracker = tracker();
        for (offset, rtt) in [40, 150, 70, 110].into_iter().enumerate() {
            assert_eq!(
                tracker.on_sample(
                    peer,
                    1,
                    "direct",
                    Duration::from_millis(rtt),
                    now + Duration::from_secs(offset as u64),
                ),
                PathQualityAction::None
            );
        }
    }

    #[test]
    fn persistent_five_second_latency_requests_one_redial() {
        let peer = SecretKey::generate().public();
        let now = Instant::now();
        let mut tracker = tracker();
        for offset in 0..2 {
            assert_eq!(
                tracker.on_sample(
                    peer,
                    4,
                    "direct",
                    Duration::from_millis(50),
                    now + Duration::from_secs(offset),
                ),
                PathQualityAction::None
            );
        }
        for offset in 2..4 {
            assert_eq!(
                tracker.on_sample(
                    peer,
                    4,
                    "direct",
                    Duration::from_secs(5),
                    now + Duration::from_secs(offset),
                ),
                PathQualityAction::None
            );
        }
        assert!(matches!(
            tracker.on_sample(
                peer,
                4,
                "direct",
                Duration::from_secs(5),
                now + Duration::from_secs(4),
            ),
            PathQualityAction::Redial { .. }
        ));
    }

    #[test]
    fn an_endpoint_generation_change_suppresses_old_degradation() {
        let peer = SecretKey::generate().public();
        let now = Instant::now();
        let mut tracker = tracker();
        for offset in 0..2 {
            tracker.on_sample(
                peer,
                8,
                "direct",
                Duration::from_millis(50),
                now + Duration::from_secs(offset),
            );
        }
        for offset in 2..4 {
            tracker.on_sample(
                peer,
                8,
                "direct",
                Duration::from_secs(5),
                now + Duration::from_secs(offset),
            );
        }
        assert_eq!(
            tracker.on_sample(
                peer,
                9,
                "direct",
                Duration::from_secs(5),
                now + Duration::from_secs(4),
            ),
            PathQualityAction::None
        );
    }

    #[test]
    fn the_redial_cooldown_prevents_a_peer_storm() {
        let peer = SecretKey::generate().public();
        let now = Instant::now();
        let mut tracker = tracker();
        for offset in 0..2 {
            tracker.on_sample(
                peer,
                1,
                "relay",
                Duration::from_millis(100),
                now + Duration::from_secs(offset),
            );
        }
        for offset in 2..=4 {
            tracker.on_sample(
                peer,
                1,
                "relay",
                Duration::from_secs(5),
                now + Duration::from_secs(offset),
            );
        }
        for offset in 5..20 {
            assert_eq!(
                tracker.on_sample(
                    peer,
                    1,
                    "relay",
                    Duration::from_secs(5),
                    now + Duration::from_secs(offset),
                ),
                PathQualityAction::None
            );
        }
    }
}
