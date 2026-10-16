//! Bounded, best-effort diagnostic output. Producers never touch the sink.
use std::{
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Counters {
    bytes: AtomicUsize,
    records: AtomicUsize,
    dropped: AtomicU64,
    pending_loss: AtomicU64,
    writing_summary: AtomicBool,
}

/// Clones share one queue. The byte bound includes the record being written;
/// the entry bound is the waiting queue plus one record on the writer thread.
/// Drop never joins the worker: a stuck disk must not delay daemon shutdown.
#[derive(Clone, Debug)]
pub struct BackgroundWriter {
    sender: SyncSender<Vec<u8>>,
    counters: Arc<Counters>,
    max_bytes: usize,
}

impl BackgroundWriter {
    pub fn spawn(
        name: &str,
        sink: impl Write + Send + 'static,
        max_entries: usize,
        max_bytes: usize,
    ) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(max_entries);
        let counters = Arc::new(Counters::default());
        let worker_counters = counters.clone();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                let mut sink = sink;
                loop {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(bytes) => {
                            if sink.write_all(&bytes).and_then(|()| sink.flush()).is_err() {
                                worker_counters.dropped.fetch_add(1, Ordering::Relaxed);
                                worker_counters.pending_loss.fetch_add(1, Ordering::Relaxed);
                            }
                            worker_counters
                                .bytes
                                .fetch_sub(bytes.len(), Ordering::Release);
                            write_loss_summary(&mut sink, &worker_counters);
                            worker_counters.records.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            write_loss_summary(&mut sink, &worker_counters);
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    write_loss_summary(&mut sink, &worker_counters);
                }
                let _ = sink.flush();
            })?;
        Ok(Self {
            sender,
            counters,
            max_bytes,
        })
    }

    fn lose(&self) {
        self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        self.counters.pending_loss.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dropped(&self) -> u64 {
        self.counters.dropped.load(Ordering::Relaxed)
    }

    /// Queue a whole record, or count its loss. An oversized record is dropped
    /// whole so a truncated JSON or framed diagnostic cannot masquerade as valid.
    pub fn enqueue(&self, bytes: &[u8]) {
        if self
            .counters
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes.len())
                    .filter(|next| *next <= self.max_bytes)
            })
            .is_err()
        {
            self.lose();
            return;
        }
        // Increment before sending: the worker may finish before try_send returns.
        self.counters.records.fetch_add(1, Ordering::Release);
        if self.sender.try_send(bytes.to_vec()).is_err() {
            self.counters
                .bytes
                .fetch_sub(bytes.len(), Ordering::Release);
            self.counters.records.fetch_sub(1, Ordering::Release);
            self.lose();
        }
    }

    /// Best effort, with a fixed caller deadline even when the sink never returns.
    /// Concurrent producers can keep it busy; the deadline still bounds the wait.
    pub async fn drain(&self, bound: Duration) -> bool {
        tokio::time::timeout(bound, async {
            while self.counters.records.load(Ordering::Acquire) != 0
                || self.counters.pending_loss.load(Ordering::Acquire) != 0
                || self.counters.writing_summary.load(Ordering::Acquire)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }
}

fn write_loss_summary(sink: &mut impl Write, counters: &Counters) {
    counters.writing_summary.store(true, Ordering::Release);
    let loss = counters.pending_loss.swap(0, Ordering::AcqRel);
    if loss == 0 {
        counters.writing_summary.store(false, Ordering::Release);
        return;
    }
    let summary = format!(
        "{} fabric: {loss} diagnostic records dropped (writer queue full or I/O failed)\n",
        time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    );
    if sink
        .write_all(summary.as_bytes())
        .and_then(|()| sink.flush())
        .is_err()
    {
        counters.pending_loss.fetch_add(loss, Ordering::Relaxed);
    }
    counters.writing_summary.store(false, Ordering::Release);
}

impl Write for BackgroundWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.enqueue(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Flush is performed on the worker after every record.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct GatedSink {
        entered: SyncSender<()>,
        release: mpsc::Receiver<()>,
        output: Arc<Mutex<Vec<u8>>>,
        first: bool,
    }

    impl Write for GatedSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.first {
                self.first = false;
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.output.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn queue_entry_bound_is_independent_of_the_byte_bound() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = BackgroundWriter::spawn(
            "test-log-entries",
            GatedSink {
                entered: entered_tx,
                release: release_rx,
                output: output.clone(),
                first: true,
            },
            1,
            1024,
        )
        .unwrap();
        writer.enqueue(b"first\n");
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();
        writer.enqueue(b"second\n");
        writer.enqueue(b"third\n");
        assert_eq!(writer.dropped(), 1);
        assert_eq!(writer.counters.bytes.load(Ordering::Acquire), 13);
        release_tx.send(()).unwrap();
        assert!(writer.drain(Duration::from_secs(2)).await);
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(output.contains("1 diagnostic records dropped"));
        assert!(!output.contains("third"));
    }

    #[tokio::test]
    async fn io_failure_is_counted_and_reported_without_a_new_record() {
        struct FailOnce {
            first: bool,
            output: Arc<Mutex<Vec<u8>>>,
        }
        impl Write for FailOnce {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.first {
                    self.first = false;
                    return Err(io::Error::other("injected disk failure"));
                }
                self.output.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = BackgroundWriter::spawn(
            "test-log-error",
            FailOnce {
                first: true,
                output: output.clone(),
            },
            1,
            1024,
        )
        .unwrap();
        writer.enqueue(b"lost\n");
        assert!(writer.drain(Duration::from_secs(2)).await);
        assert_eq!(writer.dropped(), 1);
        assert!(
            String::from_utf8_lossy(&output.lock().unwrap())
                .contains("1 diagnostic records dropped")
        );
    }

    #[tokio::test]
    async fn blocked_sink_bounds_memory_counts_loss_and_recovers_without_blocking_producers() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = BackgroundWriter::spawn(
            "test-log-writer",
            GatedSink {
                entered: entered_tx,
                release: release_rx,
                output: output.clone(),
                first: true,
            },
            2,
            16,
        )
        .unwrap();
        writer.enqueue(b"first\n");
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();
        writer.enqueue(b"second\n");
        writer.enqueue(b"over-byte-bound\n");
        assert_eq!(writer.dropped(), 1);
        assert_eq!(writer.counters.bytes.load(Ordering::Relaxed), 13);
        assert!(!writer.drain(Duration::from_millis(20)).await);
        release_tx.send(()).unwrap();
        assert!(writer.drain(Duration::from_secs(2)).await);
        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(output.starts_with("first\n"));
        assert!(output.contains("1 diagnostic records dropped"));
        assert!(output.ends_with("second\n"));
    }
}
