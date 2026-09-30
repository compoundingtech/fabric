//! Cached peer presence and a bounded, cursor-based local event journal.
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEvent {
    pub sequence: u64,
    pub peer_id: String,
    pub online: bool,
    pub path: Option<String>,
    pub cause: String,
    pub observed_at_unix_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEvents {
    pub instance: String,
    pub cursor: u64,
    pub reset: bool,
    pub events: Vec<PeerEvent>,
}
#[derive(Debug)]
struct Entry {
    online: bool,
    connection_id: Option<usize>,
    retry_at: Instant,
    failures: u32,
    latest: PeerEvent,
}
#[derive(Debug, Default)]
struct State {
    entries: HashMap<EndpointId, Entry>,
    events: VecDeque<PeerEvent>,
    sequence: u64,
}
#[derive(Debug)]
pub struct Presence {
    instance: String,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}
impl Default for Presence {
    fn default() -> Self {
        Self {
            instance: format!("{:032x}", rand::random::<u128>()),
            state: Mutex::new(State::default()),
            changed: watch::channel(0).0,
        }
    }
}
impl Presence {
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn offline(&self, peer: EndpointId) -> bool {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(&peer)
            .is_some_and(|entry| !entry.online)
    }
    pub fn probe_due(&self, peer: EndpointId) -> bool {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(&peer)
            .is_none_or(|entry| entry.online || entry.retry_at <= Instant::now())
    }
    pub fn peer_sequence(&self, peer: EndpointId) -> u64 {
        self.state
            .lock()
            .unwrap()
            .entries
            .get(&peer)
            .map_or(0, |entry| entry.latest.sequence)
    }
    /// A local start, wake or address change gets one fresh announcement round.
    pub fn retry_peer_now(&self, peer: EndpointId) {
        if let Some(entry) = self.state.lock().unwrap().entries.get_mut(&peer) {
            entry.retry_at = Instant::now();
        }
    }
    pub fn retry_now(&self) {
        for entry in self.state.lock().unwrap().entries.values_mut() {
            entry.retry_at = Instant::now();
        }
    }
    pub fn online(
        &self,
        peer: EndpointId,
        connection_id: usize,
        path: Option<String>,
        cause: &str,
    ) {
        self.observe(peer, true, Some(connection_id), path, cause);
    }
    pub fn legacy_online(&self, peer: EndpointId, path: Option<String>) {
        self.observe(peer, true, None, path, "connected");
    }
    pub fn away(&self, peer: EndpointId) {
        self.observe(peer, false, None, None, "unreachable");
    }
    /// Deliberate replacement is not evidence of a remote outage.
    pub fn replacing(&self, peer: EndpointId, connection_id: usize) {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.entries.get_mut(&peer)
            && entry.connection_id == Some(connection_id)
        {
            entry.connection_id = None;
        }
    }
    pub fn closed(&self, peer: EndpointId, connection_id: usize) {
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .get(&peer)
            .is_some_and(|entry| entry.connection_id == Some(connection_id))
        {
            self.observe_locked(&mut state, peer, false, None, None, "connection_closed");
            // One reconnect distinguishes a transient path loss from absence.
            // Failed reconnects then back off; new application requests still
            // close immediately while this fact is offline.
            state.entries.get_mut(&peer).unwrap().retry_at = Instant::now();
        }
    }
    fn observe(
        &self,
        peer: EndpointId,
        online: bool,
        connection_id: Option<usize>,
        path: Option<String>,
        cause: &str,
    ) {
        self.observe_locked(
            &mut self.state.lock().unwrap(),
            peer,
            online,
            connection_id,
            path,
            cause,
        );
    }
    fn observe_locked(
        &self,
        state: &mut State,
        peer: EndpointId,
        online: bool,
        connection_id: Option<usize>,
        path: Option<String>,
        cause: &str,
    ) {
        let previous = state.entries.get(&peer);
        // A failed concurrent dial must not overwrite a connection admitted meanwhile.
        if !online
            && connection_id.is_none()
            && previous.is_some_and(|entry| entry.online && entry.connection_id.is_some())
            && cause == "unreachable"
        {
            return;
        }
        // A new authenticated connection is useful evidence even if the old
        // handle had not timed out yet. Wake consumers that already backed off.
        let transition = previous.is_none_or(|entry| {
            entry.online != online || (online && entry.connection_id != connection_id)
        });
        let failures = if online {
            0
        } else {
            previous.map_or(1, |entry| entry.failures.saturating_add(1))
        };
        let delay_secs = (20u64.saturating_mul(1u64 << failures.min(10))).min(3600);
        let delay = Duration::from_millis(delay_secs * (800 + rand::random::<u64>() % 401));
        let event = if transition {
            state.sequence += 1;
            let event = PeerEvent {
                sequence: state.sequence,
                peer_id: peer.to_string(),
                online,
                path,
                cause: cause.into(),
                observed_at_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            };
            state.events.push_back(event.clone());
            while state.events.len() > 256 {
                state.events.pop_front();
            }
            self.changed.send_replace(state.sequence);
            event
        } else {
            previous.unwrap().latest.clone()
        };
        state.entries.insert(
            peer,
            Entry {
                online,
                connection_id,
                failures,
                retry_at: Instant::now() + delay,
                latest: event,
            },
        );
    }
    pub fn batch(&self, instance: Option<&str>, after: u64) -> PeerEvents {
        let state = self.state.lock().unwrap();
        let reset = instance != Some(self.instance.as_str())
            || after > state.sequence
            || state
                .events
                .front()
                .is_some_and(|event| after.saturating_add(1) < event.sequence);
        let mut events: Vec<_> = if reset {
            state
                .entries
                .values()
                .map(|entry| entry.latest.clone())
                .collect()
        } else {
            state
                .events
                .iter()
                .filter(|event| event.sequence > after)
                .cloned()
                .collect()
        };
        events.sort_by_key(|event| event.sequence);
        PeerEvents {
            instance: self.instance.clone(),
            cursor: state.sequence,
            reset,
            events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn minutes_and_hours_away_are_quiet_and_a_return_wakes_immediately() {
        for outage in [
            Duration::from_secs(5 * 60),
            Duration::from_secs(4 * 60 * 60),
        ] {
            let presence = Presence::default();
            let peer = iroh::SecretKey::generate().public();
            presence.online(peer, 1, Some("direct".into()), "connected");
            presence.closed(peer, 1);
            let mut changes = presence.subscribe();
            let before = presence.batch(None, 0);
            for _ in 0..1000 {
                assert!(presence.offline(peer));
            }
            assert!(
                !changes.has_changed().unwrap(),
                "consumer checks emitted events"
            );
            for _ in 0..12 {
                tokio::time::advance(Duration::from_secs(2 * 3600)).await;
                presence.away(peer);
            }
            let deadline = presence.state.lock().unwrap().entries[&peer].retry_at;
            assert!(deadline - Instant::now() >= Duration::from_secs(48 * 60));
            assert!(deadline - Instant::now() <= Duration::from_secs(72 * 60));
            tokio::time::advance(outage).await;
            assert_eq!(
                presence
                    .batch(Some(&before.instance), before.cursor)
                    .events
                    .len(),
                0,
                "absence emitted repeated events"
            );
            presence.online(peer, 2, Some("relay".into()), "connected");
            changes.changed().await.unwrap();
            assert!(!presence.offline(peer));
            assert!(presence.probe_due(peer));
            assert_eq!(presence.state.lock().unwrap().entries[&peer].failures, 0);
            let returned = presence.batch(Some(&before.instance), before.cursor);
            assert_eq!(returned.events.len(), 1);
            assert!(returned.events[0].online);
        }
    }
    #[tokio::test]
    async fn replacing_a_connection_does_not_emit_an_offline_transition() {
        let presence = Presence::default();
        let peer = iroh::SecretKey::generate().public();
        presence.online(peer, 1, None, "connected");
        let before = presence.batch(None, 0);
        presence.replacing(peer, 1);
        presence.closed(peer, 1);
        presence.online(peer, 2, None, "connected");
        presence.closed(peer, 1);
        assert!(!presence.offline(peer));
        let events = presence.batch(Some(&before.instance), before.cursor).events;
        assert_eq!(events.len(), 1);
        assert!(
            events[0].online,
            "replacement must not announce a false outage"
        );
    }
    #[tokio::test]
    async fn stale_cursor_and_daemon_restart_return_a_current_snapshot() {
        let presence = Presence::default();
        let peer = iroh::SecretKey::generate().public();
        for id in 0..300 {
            presence.online(peer, id, None, "connected");
            presence.closed(peer, id);
        }
        assert_eq!(presence.state.lock().unwrap().events.len(), 256);
        let snapshot = presence.batch(None, 0);
        let stale = presence.batch(Some(&snapshot.instance), 1);
        assert!(stale.reset);
        assert_eq!(stale.events.len(), 1);
        assert!(!stale.events[0].online);
        let fresh = Presence::default().batch(Some(&snapshot.instance), snapshot.cursor);
        assert!(fresh.reset);
    }
}
