//! Local observations of the shared connection carrying a tunnel session.
//! These snapshots describe attach/close instants, never every payload byte.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use iroh::{
    EndpointId,
    endpoint::{Connection, PathEvent},
};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::presence::Presence;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPath {
    pub connection_id: u64,
    pub class: Option<String>,
    /// Present when exactly one path is selected.
    pub remote_addr: Option<String>,
    /// Every selected remote, including both classes for a mixed selection.
    pub remote_addrs: Vec<String>,
}
impl SessionPath {
    fn from_selected(
        connection_id: u64,
        selected: impl IntoIterator<Item = (impl AsRef<str>, String)>,
    ) -> Self {
        let mut direct = false;
        let mut relay = false;
        let mut remotes = Vec::new();
        for (class, remote) in selected {
            direct |= class.as_ref() == "direct";
            relay |= class.as_ref() == "relay";
            remotes.push(remote);
        }
        remotes.sort();
        remotes.dedup();
        Self {
            connection_id,
            class: match (direct, relay) {
                (true, true) => Some("mixed".into()),
                (true, false) => Some("direct".into()),
                (false, true) => Some("relay".into()),
                _ => None,
            },
            remote_addr: (remotes.len() == 1).then(|| remotes[0].clone()),
            remote_addrs: remotes,
        }
    }
}

/// Internal path identity deduplicates event and boundary observations without
/// exposing an upstream debug-format identifier as part of the JSON contract.
#[derive(Clone)]
struct ObservedPath {
    path: SessionPath,
    ids: Vec<String>,
}
impl ObservedPath {
    fn observe(connection: &Connection) -> Self {
        let paths = connection.paths();
        let selected: Vec<_> = paths.iter().filter(|path| path.is_selected()).collect();
        let mut ids: Vec<_> = selected
            .iter()
            .map(|path| format!("{:?}", path.id()))
            .collect();
        ids.sort();
        Self {
            path: SessionPath::from_selected(
                connection.stable_id() as u64,
                selected.iter().map(|path| remote(path.remote_addr())),
            ),
            ids,
        }
    }
    fn selected(connection: &Connection, id: String, addr: &iroh::TransportAddr) -> Self {
        Self {
            path: SessionPath::from_selected(connection.stable_id() as u64, [remote(addr)]),
            ids: vec![id],
        }
    }
}
fn remote(addr: &iroh::TransportAddr) -> (&'static str, String) {
    match addr {
        iroh::TransportAddr::Ip(addr) => ("direct", addr.to_string()),
        iroh::TransportAddr::Relay(url) => ("relay", url.to_string()),
        other => ("other", other.to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPathStatus {
    pub session_id: String,
    pub peer_id: String,
    pub direction: SessionDirection,
    pub attached: bool,
    pub attach_count: u64,
    pub active_attaches: usize,
    /// Path on the most recent accepted attach.
    pub path_at_attach: Option<SessionPath>,
    /// Latest snapshot of the most recent attach, retained while detached.
    pub current_path: Option<SessionPath>,
    pub path_at_close: Option<SessionPath>,
    /// Changes of selected connection/path identity observed at boundaries
    /// or in the path-event stream.
    pub path_changes: u64,
    /// A nonzero value means the watcher lost notifications; the count above
    /// is then explicitly incomplete.
    pub missed_path_events: u64,
}

/// Carries only local observation context across an accepted server stream.
#[derive(Clone)]
pub(crate) struct AttachContext {
    pub registry: Arc<SessionPaths>,
    pub connection: Connection,
    pub direction: SessionDirection,
}
tokio::task_local! { static ATTACH_CONTEXT: AttachContext; }
impl AttachContext {
    pub fn current() -> Option<Self> {
        ATTACH_CONTEXT.try_with(Clone::clone).ok()
    }
    pub async fn scope<F: std::future::Future>(self, future: F) -> F::Output {
        ATTACH_CONTEXT.scope(self, future).await
    }
}

type Key = (SessionDirection, String);
#[derive(Debug)]
struct Entry {
    identity: Arc<()>,
    status: SessionPathStatus,
    attaches: BTreeSet<u64>,
    selected: Option<(u64, Vec<String>)>,
}
#[derive(Debug)]
pub(crate) struct SessionPaths {
    presence: Arc<Presence>,
    active: Mutex<BTreeMap<Key, Entry>>,
}
impl SessionPaths {
    pub fn new(presence: Arc<Presence>) -> Arc<Self> {
        Arc::new(Self {
            presence,
            active: Mutex::new(BTreeMap::new()),
        })
    }
    pub fn snapshot(&self) -> Vec<SessionPathStatus> {
        self.active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(|entry| entry.status.clone())
            .collect()
    }
    pub fn session(
        self: &Arc<Self>,
        session_id: String,
        peer: EndpointId,
        direction: SessionDirection,
    ) -> SessionPathHandle {
        let key = (direction, session_id.clone());
        let identity = Arc::new(());
        self.active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                key.clone(),
                Entry {
                    identity: identity.clone(),
                    attaches: BTreeSet::new(),
                    selected: None,
                    status: SessionPathStatus {
                        session_id,
                        peer_id: peer.to_string(),
                        direction,
                        attached: false,
                        attach_count: 0,
                        active_attaches: 0,
                        path_at_attach: None,
                        current_path: None,
                        path_at_close: None,
                        path_changes: 0,
                        missed_path_events: 0,
                    },
                },
            );
        SessionPathHandle {
            inner: Arc::new(SessionObservation {
                registry: self.clone(),
                identity,
                key,
                peer,
                closed: AtomicBool::new(false),
                connection: Mutex::new(Weak::new()),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionPathHandle {
    inner: Arc<SessionObservation>,
}
#[derive(Debug)]
struct SessionObservation {
    registry: Arc<SessionPaths>,
    identity: Arc<()>,
    key: Key,
    peer: EndpointId,
    closed: AtomicBool,
    connection: Mutex<Weak<Connection>>,
}
impl SessionPathHandle {
    pub fn attach(&self, connection: &Connection) -> PathAttachment {
        // Subscribe before the attach snapshot so changes cannot fall into an
        // unobserved subscription gap. Lag is recorded explicitly.
        let mut events = connection.path_events();
        let attached_connection = Arc::new(connection.clone());
        {
            let mut held = self
                .inner
                .connection
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if !self.inner.closed.load(Ordering::SeqCst) {
                *held = Arc::downgrade(&attached_connection);
            }
        }
        let generation = self.begin_observation(ObservedPath::observe(connection));
        let cancel = CancellationToken::new();
        let stopped = cancel.clone();
        let handle = self.clone();
        let observed = attached_connection.clone();
        if generation != 0 {
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = stopped.cancelled() => break,
                        event = events.next() => {
                            let Some(event) = event else { break; };
                            match event {
                                PathEvent::Selected { id, remote_addr, .. } => handle.observe(generation,
                                    ObservedPath::selected(&observed, format!("{id:?}"), &remote_addr), 0),
                                PathEvent::Lagged { missed, .. } => handle.observe(generation, ObservedPath::observe(&observed), missed),
                                _ => {}
                            }
                        }
                    }
                }
            });
        }
        PathAttachment {
            handle: self.clone(),
            connection: attached_connection,
            generation,
            cancel,
        }
    }
    fn begin_observation(&self, path: ObservedPath) -> u64 {
        let mut active = self
            .inner
            .registry
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if self.inner.closed.load(Ordering::SeqCst) {
            return 0;
        }
        let Some(entry) = active
            .get_mut(&self.inner.key)
            .filter(|entry| Arc::ptr_eq(&entry.identity, &self.inner.identity))
        else {
            return 0;
        };
        let status = &mut entry.status;
        update_path(status, &mut entry.selected, path.clone(), 0);
        status.attach_count += 1;
        entry.attaches.insert(status.attach_count);
        status.active_attaches = entry.attaches.len();
        status.attached = true;
        status.path_at_attach = Some(path.path);
        self.inner.registry.presence.session_event(
            self.inner.peer,
            "session_attached",
            status.clone(),
        );
        status.attach_count
    }
    fn observe(&self, generation: u64, path: ObservedPath, missed: u64) {
        let mut active = self
            .inner
            .registry
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = active
            .get_mut(&self.inner.key)
            .filter(|entry| Arc::ptr_eq(&entry.identity, &self.inner.identity))
        {
            // Older attaches cannot overwrite the most recent connection.
            if entry.status.attach_count == generation {
                update_path(&mut entry.status, &mut entry.selected, path, missed);
            }
        }
    }
    fn detach(&self, generation: u64, path: ObservedPath) {
        let mut active = self
            .inner
            .registry
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = active
            .get_mut(&self.inner.key)
            .filter(|entry| Arc::ptr_eq(&entry.identity, &self.inner.identity))
        {
            entry.attaches.remove(&generation);
            entry.status.active_attaches = entry.attaches.len();
            entry.status.attached = !entry.attaches.is_empty();
            if entry.status.attach_count == generation {
                update_path(&mut entry.status, &mut entry.selected, path, 0);
            }
        }
    }
    pub fn close(&self) {
        self.finish_close(|| {
            // The attachment owns the strong reference. A detached session
            // never keeps an old QUIC connection alive just for diagnostics.
            let weak = std::mem::take(
                &mut *self
                    .inner
                    .connection
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()),
            );
            weak.upgrade().as_deref().map(ObservedPath::observe)
        });
    }
    fn close_with_path(&self, path: Option<ObservedPath>) {
        self.finish_close(|| path);
    }
    fn finish_close(&self, snapshot: impl FnOnce() -> Option<ObservedPath>) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let path = snapshot();
        let mut active = self
            .inner
            .registry
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !active
            .get(&self.inner.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.identity, &self.inner.identity))
        {
            return;
        }
        let mut entry = active.remove(&self.inner.key).unwrap();
        let status = &mut entry.status;
        if status.attach_count == 0 {
            return;
        }
        if let Some(path) = path.clone() {
            update_path(status, &mut entry.selected, path, 0);
        }
        status.attached = false;
        status.active_attaches = 0;
        status.path_at_close = path.map(|observed| observed.path);
        self.inner
            .registry
            .presence
            .session_event(self.inner.peer, "session_closed", entry.status);
    }
}

fn update_path(
    status: &mut SessionPathStatus,
    selected: &mut Option<(u64, Vec<String>)>,
    observed: ObservedPath,
    missed: u64,
) {
    let key = (observed.path.connection_id, observed.ids);
    if selected.as_ref().is_some_and(|previous| previous != &key) {
        status.path_changes = status.path_changes.saturating_add(1);
    }
    *selected = Some(key);
    status.missed_path_events = status.missed_path_events.saturating_add(missed);
    status.current_path = Some(observed.path);
}

pub(crate) struct PathAttachment {
    handle: SessionPathHandle,
    connection: Arc<Connection>,
    generation: u64,
    cancel: CancellationToken,
}
impl Drop for PathAttachment {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.handle
            .detach(self.generation, ObservedPath::observe(&self.connection));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn direct(port: u16) -> ObservedPath {
        ObservedPath {
            path: SessionPath::from_selected(7, [("direct", format!("127.0.0.1:{port}"))]),
            ids: vec![port.to_string()],
        }
    }
    fn peer() -> EndpointId {
        iroh::SecretKey::from_bytes(&[71; 32]).public()
    }

    #[test]
    fn no_selected_path_is_unknown_and_mixed_selection_names_every_remote() {
        let unknown = SessionPath::from_selected(7, Vec::<(&str, String)>::new());
        assert!(unknown.class.is_none());
        assert!(unknown.remote_addr.is_none());
        assert!(unknown.remote_addrs.is_empty());
        let mixed = SessionPath::from_selected(
            8,
            [
                ("relay", "https://relay.example.invalid".into()),
                ("direct", "127.0.0.1:4000".into()),
            ],
        );
        assert_eq!(mixed.class.as_deref(), Some("mixed"));
        assert!(mixed.remote_addr.is_none());
        assert_eq!(mixed.remote_addrs.len(), 2);
    }

    #[test]
    fn resume_keeps_one_record_counts_observed_changes_and_ignores_old_attach_updates() {
        let presence = Arc::new(Presence::default());
        presence.online(peer(), 7, Some("direct".into()), "connected");
        let sequence = presence.peer_sequence(peer());
        let registry = SessionPaths::new(presence.clone());
        let session = registry.session("one".into(), peer(), SessionDirection::Outbound);
        let first = session.begin_observation(direct(4000));
        session.observe(first, direct(4001), 0);
        session.observe(first, direct(4001), 4);
        let second = session.begin_observation(direct(4001));
        session.detach(first, direct(4000));
        session.observe(first, direct(4000), 0);
        let live = registry.snapshot();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].attach_count, 2);
        assert_eq!(live[0].active_attaches, 1);
        assert_eq!(live[0].path_changes, 1);
        assert_eq!(live[0].missed_path_events, 4);
        assert_eq!(
            live[0]
                .current_path
                .as_ref()
                .unwrap()
                .remote_addr
                .as_deref(),
            Some("127.0.0.1:4001")
        );
        session.detach(second, direct(4001));
        assert!(!registry.snapshot()[0].attached);
        session.close_with_path(Some(direct(4002)));
        session.close_with_path(Some(direct(4003)));
        assert!(registry.snapshot().is_empty());
        assert_eq!(presence.peer_sequence(peer()), sequence);
        assert!(!presence.offline(peer()));
        let events = presence.batch(None, 0);
        let sessions: Vec<_> = events
            .events
            .iter()
            .filter(|event| event.session.is_some())
            .collect();
        assert_eq!(
            sessions
                .iter()
                .map(|event| event.cause.as_str())
                .collect::<Vec<_>>(),
            ["session_attached", "session_attached", "session_closed"]
        );
        let closed = sessions[2].session.as_ref().unwrap();
        assert_eq!(closed.path_changes, 2);
        assert_eq!(
            closed
                .path_at_close
                .as_ref()
                .unwrap()
                .remote_addr
                .as_deref(),
            Some("127.0.0.1:4002")
        );
        assert_eq!(
            closed
                .path_at_attach
                .as_ref()
                .unwrap()
                .remote_addr
                .as_deref(),
            Some("127.0.0.1:4001")
        );
    }

    #[test]
    fn path_identity_changes_count_even_when_the_remote_address_is_unchanged() {
        let registry = SessionPaths::new(Arc::new(Presence::default()));
        let session = registry.session("same-remote".into(), peer(), SessionDirection::Outbound);
        let generation = session.begin_observation(direct(4000));
        let mut selected = direct(4000);
        selected.ids = vec!["new-path".into()];
        session.observe(generation, selected.clone(), 0);
        session.observe(generation, selected.clone(), 0);
        session.detach(generation, selected.clone());
        assert_eq!(registry.snapshot()[0].path_changes, 1);
        session.close_with_path(Some(selected));
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn stale_handles_cannot_close_or_update_a_reused_session_id() {
        let registry = SessionPaths::new(Arc::new(Presence::default()));
        let old = registry.session("same".into(), peer(), SessionDirection::Inbound);
        let prior = old.begin_observation(direct(4000));
        let new = registry.session("same".into(), peer(), SessionDirection::Inbound);
        new.begin_observation(direct(5000));
        old.observe(prior, direct(6000), 0);
        old.close_with_path(Some(direct(6000)));
        assert_eq!(registry.snapshot().len(), 1);
        assert_eq!(
            registry.snapshot()[0]
                .current_path
                .as_ref()
                .unwrap()
                .remote_addr
                .as_deref(),
            Some("127.0.0.1:5000")
        );
        new.close_with_path(Some(direct(5000)));
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn session_event_overflow_is_bounded_and_does_not_overwrite_presence() {
        let presence = Arc::new(Presence::default());
        presence.online(peer(), 7, Some("direct".into()), "connected");
        let initial = presence.batch(None, 0);
        let registry = SessionPaths::new(presence.clone());
        for index in 0..200 {
            let session = registry.session(index.to_string(), peer(), SessionDirection::Inbound);
            session.begin_observation(direct(4000));
            session.close_with_path(Some(direct(4000)));
        }
        let reset = presence.batch(Some(&initial.instance), initial.cursor);
        assert!(reset.reset);
        assert_eq!(
            reset
                .events
                .iter()
                .filter(|event| event.session.is_some())
                .count(),
            256
        );
        assert_eq!(presence.peer_sequence(peer()), initial.cursor);
        assert!(registry.snapshot().is_empty());
        assert_eq!(
            reset
                .events
                .iter()
                .filter(|event| event.session.is_none())
                .count(),
            1
        );
    }
}
