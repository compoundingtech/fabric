//! Bounded admission shared by every generic dial listener to one peer.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use anyhow::Result;
use fabric_config::error::{ErrorCode, FabricError};
use iroh::EndpointId;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::config::DEFAULT_SERVER_SESSION_MAX_PER_PEER;

const MAX_WAITING_PER_PEER: usize = 128;
pub(crate) const DEFAULT_QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Default)]
pub(crate) struct DialQueues(Mutex<HashMap<EndpointId, Weak<DialQueue>>>);

impl DialQueues {
    pub(crate) fn peer(&self, peer: EndpointId) -> Arc<DialQueue> {
        let mut queues = self.0.lock().unwrap();
        queues.retain(|_, queue| queue.strong_count() > 0);
        if let Some(queue) = queues.get(&peer).and_then(Weak::upgrade) {
            return queue;
        }
        let queue = Arc::new(DialQueue::new(
            DEFAULT_SERVER_SESSION_MAX_PER_PEER,
            MAX_WAITING_PER_PEER,
        ));
        queues.insert(peer, Arc::downgrade(&queue));
        queue
    }
}

#[derive(Debug)]
pub(crate) struct DialQueue {
    active: Arc<Semaphore>,
    outstanding: Arc<Semaphore>,
}

impl DialQueue {
    fn new(active: usize, waiting: usize) -> Self {
        Self {
            active: Arc::new(Semaphore::new(active)),
            outstanding: Arc::new(Semaphore::new(active + waiting)),
        }
    }

    /// Reserve before spawning: waiting sockets/tasks cannot grow without bound.
    pub(crate) fn reserve(self: &Arc<Self>, timeout: Duration) -> Result<DialReservation> {
        let permit = self
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| limit_error("dial queue is full"))?;
        Ok(DialReservation {
            queue: self.clone(),
            _outstanding: permit,
            deadline: Instant::now() + timeout,
        })
    }
}

pub(crate) fn limit_error(message: &str) -> anyhow::Error {
    FabricError::new(ErrorCode::SessionLimitReached, message).into()
}

pub(crate) struct DialReservation {
    queue: Arc<DialQueue>,
    _outstanding: OwnedSemaphorePermit,
    pub(crate) deadline: Instant,
}

/// Holds the per-peer and daemon-wide session budgets until the stream ends.
pub(crate) struct DialPermit {
    _reservation: DialReservation,
    _active: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl DialReservation {
    pub(crate) async fn acquire(
        self,
        global: Arc<Semaphore>,
        cancel: &CancellationToken,
    ) -> Result<Option<DialPermit>> {
        let wait = async {
            let active = self.queue.active.clone().acquire_owned().await?;
            let global = global.acquire_owned().await?;
            Ok::<_, anyhow::Error>((active, global))
        };
        let (active, global) = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(None),
            _ = tokio::time::sleep_until(self.deadline) => return Err(limit_error("dial queue timeout expired")),
            permits = wait => permits?,
        };
        Ok(Some(DialPermit {
            _reservation: self,
            _active: active,
            _global: global,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn listeners_share_a_peer_budget_but_other_peers_do_not() -> Result<()> {
        let queues = DialQueues::default();
        let peer = iroh::SecretKey::generate().public();
        let first = queues.peer(peer);
        let second = queues.peer(peer);
        assert!(Arc::ptr_eq(&first, &second));
        let other = queues.peer(iroh::SecretKey::generate().public());
        let held = first
            .active
            .clone()
            .acquire_many_owned(DEFAULT_SERVER_SESSION_MAX_PER_PEER as u32)
            .await?;
        let global = Arc::new(Semaphore::new(32));
        let cancel = CancellationToken::new();
        let waiting = tokio::spawn({
            let reservation = second.reserve(DEFAULT_QUEUE_TIMEOUT)?;
            let global = global.clone();
            let cancel = cancel.clone();
            async move { reservation.acquire(global, &cancel).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let other_permit = other
            .reserve(DEFAULT_QUEUE_TIMEOUT)?
            .acquire(global, &cancel)
            .await?;
        assert!(other_permit.is_some());
        drop(held);
        assert!(waiting.await??.is_some());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn full_queue_timeout_and_cancellation_release_capacity() -> Result<()> {
        let queue = Arc::new(DialQueue::new(1, 1));
        let global = Arc::new(Semaphore::new(1));
        let cancel = CancellationToken::new();
        let live = queue
            .reserve(DEFAULT_QUEUE_TIMEOUT)?
            .acquire(global.clone(), &cancel)
            .await?;
        let waiting = queue.reserve(DEFAULT_QUEUE_TIMEOUT)?;
        assert_eq!(
            fabric_config::error::code_of(&queue.reserve(DEFAULT_QUEUE_TIMEOUT).err().unwrap()),
            ErrorCode::SessionLimitReached
        );
        let error = waiting
            .acquire(global.clone(), &cancel)
            .await
            .err()
            .unwrap();
        assert_eq!(
            fabric_config::error::code_of(&error),
            ErrorCode::SessionLimitReached
        );
        let waiting = queue.reserve(DEFAULT_QUEUE_TIMEOUT)?;
        cancel.cancel();
        assert!(waiting.acquire(global.clone(), &cancel).await?.is_none());
        drop(live);
        let fresh = CancellationToken::new();
        assert!(
            queue
                .reserve(DEFAULT_QUEUE_TIMEOUT)?
                .acquire(global, &fresh)
                .await?
                .is_some()
        );
        assert_eq!(queue.outstanding.available_permits(), 2);
        Ok(())
    }
}
