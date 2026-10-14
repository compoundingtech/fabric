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
#[cfg(test)]
pub(crate) const DEFAULT_QUEUE_TIMEOUT: Duration =
    Duration::from_millis(crate::config::DEFAULT_DIAL_QUEUE_TIMEOUT_MS);

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
        let deadline = if timeout.is_zero() {
            None
        } else {
            Some(Instant::now().checked_add(timeout).ok_or_else(|| {
                FabricError::new(ErrorCode::InvalidArgument, "queue timeout is too large")
            })?)
        };
        let permit = self
            .outstanding
            .clone()
            .try_acquire_owned()
            .map_err(|_| limit_error("dial queue is full"))?;
        Ok(DialReservation {
            queue: self.clone(),
            _outstanding: permit,
            deadline,
        })
    }
}

pub(crate) fn limit_error(message: &str) -> anyhow::Error {
    FabricError::new(ErrorCode::SessionLimitReached, message).into()
}

/// Typed local errors display prose alone. Preserve their code in listener
/// diagnostics just as the remote's coded rejection preserves its code.
pub(crate) fn diagnostic(error: &anyhow::Error) -> String {
    fabric_config::error::code_of(error)
        .reason(&fabric_config::error::without_tokens(&format!("{error:#}")))
}

pub(crate) struct DialReservation {
    queue: Arc<DialQueue>,
    _outstanding: OwnedSemaphorePermit,
    pub(crate) deadline: Option<Instant>,
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
        if cancel.is_cancelled() {
            return Ok(None);
        }
        let Some(deadline) = self.deadline else {
            // Zero disables waiting and remote admission retries, while a
            // connection with immediately available capacity can still run.
            let active = self
                .queue
                .active
                .clone()
                .try_acquire_owned()
                .map_err(|_| limit_error("dial peer capacity is full"))?;
            let global = global
                .try_acquire_owned()
                .map_err(|_| limit_error("dial daemon capacity is full"))?;
            return Ok(Some(DialPermit {
                _reservation: self,
                _active: active,
                _global: global,
            }));
        };
        let wait = async {
            let active = self.queue.active.clone().acquire_owned().await?;
            let global = global.acquire_owned().await?;
            Ok::<_, anyhow::Error>((active, global))
        };
        let (active, global) = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(None),
            _ = tokio::time::sleep_until(deadline) => return Err(limit_error("dial queue timeout expired")),
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
    async fn zero_timeout_admits_available_capacity_and_never_waits() -> Result<()> {
        let queue = Arc::new(DialQueue::new(1, 1));
        let global = Arc::new(Semaphore::new(1));
        let cancel = CancellationToken::new();
        let reservation = queue.reserve(Duration::ZERO)?;
        assert!(reservation.deadline.is_none());
        let live = reservation.acquire(global.clone(), &cancel).await?;
        let start = Instant::now();
        let error = queue
            .reserve(Duration::ZERO)?
            .acquire(global.clone(), &cancel)
            .await
            .err()
            .unwrap();
        assert_eq!(Instant::now(), start);
        assert_eq!(
            fabric_config::error::code_of(&error),
            ErrorCode::SessionLimitReached
        );
        drop(live);
        let held = global.clone().acquire_owned().await?;
        assert!(
            queue
                .reserve(Duration::ZERO)?
                .acquire(global.clone(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(queue.active.available_permits(), 1);
        drop(held);
        assert!(
            queue
                .reserve(Duration::ZERO)?
                .acquire(global, &cancel)
                .await?
                .is_some()
        );
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
