//! What a peer was admitted to, kept for as long as it runs, so a peer book
//! that takes the access away can end it.
//!
//! Admission checks a grant once, when a connection or stream starts. Without
//! this record a session admitted before a reload outlives the grant that let
//! it in. Only a reload that succeeds revokes anything: a failed one closes the
//! book to new connections and leaves live work alone, so one bad edit to the
//! peer file cannot cut every session on the machine.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use iroh::EndpointId;
use tokio_util::sync::CancellationToken;

use crate::config::PeerBook;

/// The grant that let a peer in, which is what a new peer book must still give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grant {
    /// Trust alone: the shared connection, or a service that checks a narrower
    /// grant itself on every request.
    Trust,
    /// The peer's grant for this service, by the name a person writes in
    /// `allow`.
    Service(String),
}

impl Grant {
    /// Does `book` still give `peer` this grant?
    pub fn permitted(&self, book: &PeerBook, peer: &EndpointId) -> bool {
        match self {
            Grant::Trust => book.accepts_inbound(peer),
            Grant::Service(service) => book.may(peer, service).is_ok(),
        }
    }
}

/// Every live admission on this machine.
#[derive(Debug, Default)]
pub struct Admissions {
    live: Mutex<Live>,
}

#[derive(Debug, Default)]
struct Live {
    next: u64,
    entries: HashMap<u64, Entry>,
}

#[derive(Debug)]
struct Entry {
    peer: EndpointId,
    grant: Grant,
    revoked: CancellationToken,
}

impl Admissions {
    /// Record an admission before its grant is checked. A reload that lands
    /// between the check and the record would otherwise miss it; recorded
    /// first, it is either revoked by that reload or refused by the new book.
    pub fn admit(self: &Arc<Self>, peer: EndpointId, grant: Grant) -> Admission {
        let revoked = CancellationToken::new();
        let mut live = self.live.lock().unwrap();
        let id = live.next;
        live.next += 1;
        live.entries.insert(
            id,
            Entry {
                peer,
                grant: grant.clone(),
                revoked: revoked.clone(),
            },
        );
        Admission {
            id,
            peer,
            grant,
            revoked,
            admissions: self.clone(),
        }
    }

    /// Revoke every admission `book` no longer gives. Returns how many.
    pub fn revoke(&self, book: &PeerBook) -> usize {
        let live = self.live.lock().unwrap();
        let mut revoked = 0;
        for entry in live.entries.values() {
            if !entry.revoked.is_cancelled() && !entry.grant.permitted(book, &entry.peer) {
                entry.revoked.cancel();
                revoked += 1;
            }
        }
        revoked
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.live.lock().unwrap().entries.len()
    }
}

/// One live admission. Dropping it forgets the record.
#[derive(Debug)]
pub struct Admission {
    id: u64,
    peer: EndpointId,
    grant: Grant,
    revoked: CancellationToken,
    admissions: Arc<Admissions>,
}

impl Admission {
    pub fn peer(&self) -> EndpointId {
        self.peer
    }

    pub fn grant(&self) -> &Grant {
        &self.grant
    }

    /// Cancelled when a reload takes this admission's grant away.
    pub fn revoked(&self) -> CancellationToken {
        self.revoked.clone()
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.admissions
            .live
            .lock()
            .unwrap()
            .entries
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const SERVICES: [&str; 3] = ["shell", "exec", "web"];

    /// Four peers. Each case gets fresh ones; only their positions matter.
    fn peers() -> Vec<EndpointId> {
        (0..4)
            .map(|_| iroh::SecretKey::generate().public())
            .collect()
    }

    /// 0 is trust alone; 1..=3 is one service.
    fn grant(index: usize) -> Grant {
        match index {
            0 => Grant::Trust,
            index => Grant::Service(SERVICES[index - 1].to_string()),
        }
    }

    /// A book in which each peer is absent, or present with some services.
    fn book(ids: &[EndpointId], model: &[Option<Vec<bool>>]) -> PeerBook {
        let mut book = PeerBook::default();
        for (id, entry) in ids.iter().zip(model) {
            if let Some(allowed) = entry {
                let allow = SERVICES
                    .iter()
                    .zip(allowed)
                    .filter(|(_, allowed)| **allowed)
                    .map(|(service, _)| service.to_string())
                    .collect();
                book.add_with_allow(*id, None, None, Some(allow));
            }
        }
        book
    }

    fn models() -> impl Strategy<Value = Vec<Option<Vec<bool>>>> {
        proptest::collection::vec(
            proptest::option::of(proptest::collection::vec(any::<bool>(), SERVICES.len())),
            4,
        )
    }

    proptest! {
        /// A reload revokes exactly the admissions the new book no longer
        /// gives, whatever was admitted before and whatever the book says. The
        /// expected answer is read from the model, not from the book.
        #[test]
        fn a_reload_revokes_exactly_what_the_new_book_withholds(
            admitted in proptest::collection::vec((0usize..4, 0usize..=SERVICES.len()), 0..12),
            model in models(),
        ) {
            let ids = peers();
            let admissions = Arc::new(Admissions::default());
            let held: Vec<Admission> = admitted
                .iter()
                .map(|(peer, index)| admissions.admit(ids[*peer], grant(*index)))
                .collect();

            let revoked = admissions.revoke(&book(&ids, &model));

            let expected: Vec<bool> = admitted
                .iter()
                .map(|(peer, index)| match (&model[*peer], index) {
                    (None, _) => true,
                    (Some(_), 0) => false,
                    (Some(allowed), index) => !allowed[index - 1],
                })
                .collect();
            let actual: Vec<bool> = held
                .iter()
                .map(|admission| admission.revoked().is_cancelled())
                .collect();
            prop_assert_eq!(&actual, &expected);
            prop_assert_eq!(revoked, expected.iter().filter(|revoked| **revoked).count());
            // A second sweep with the same book has nothing left to revoke.
            prop_assert_eq!(admissions.revoke(&book(&ids, &model)), 0);
        }
    }

    #[test]
    fn trust_survives_a_book_that_keeps_the_peer_with_no_grants() {
        let ids = peers();
        let admissions = Arc::new(Admissions::default());
        let trusted = admissions.admit(ids[0], Grant::Trust);
        let service = admissions.admit(ids[0], Grant::Service("shell".into()));
        let no_grants = book(&ids, &[Some(vec![false; 3]), None, None, None]);
        assert_eq!(admissions.revoke(&no_grants), 1);
        assert!(!trusted.revoked().is_cancelled());
        assert!(service.revoked().is_cancelled());
        assert_eq!(admissions.revoke(&PeerBook::default()), 1);
        assert!(trusted.revoked().is_cancelled());
    }

    #[test]
    fn a_finished_admission_is_forgotten() {
        let admissions = Arc::new(Admissions::default());
        let admission = admissions.admit(peers()[0], Grant::Trust);
        assert_eq!(admissions.len(), 1);
        drop(admission);
        assert_eq!(admissions.len(), 0);
        assert_eq!(admissions.revoke(&PeerBook::default()), 0);
    }
}
