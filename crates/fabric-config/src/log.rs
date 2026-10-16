//! Timestamp process output that the service manager redirects to its logs.

use crate::background_writer::BackgroundWriter;
use std::io::{self, Write};
use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
};

static DAEMON_STDERR: OnceLock<BackgroundWriter> = OnceLock::new();
static DAEMON_STDERR_USERS: AtomicUsize = AtomicUsize::new(0);

pub struct DaemonStderrGuard {
    _private: (),
}

impl Drop for DaemonStderrGuard {
    fn drop(&mut self) {
        DAEMON_STDERR_USERS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Only the daemon installs this route; CLI diagnostics remain synchronous.
pub fn init_daemon_stderr() -> io::Result<DaemonStderrGuard> {
    if DAEMON_STDERR.get().is_none() {
        let writer = BackgroundWriter::spawn("fabric-stderr", io::stderr(), 1024, 1024 * 1024)?;
        let _ = DAEMON_STDERR.set(writer);
    }
    DAEMON_STDERR_USERS.fetch_add(1, Ordering::AcqRel);
    Ok(DaemonStderrGuard { _private: () })
}

pub async fn drain_daemon_stderr(bound: std::time::Duration) -> bool {
    match DAEMON_STDERR.get() {
        Some(writer) => writer.drain(bound).await,
        None => true,
    }
}

use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn write_lines(mut writer: impl Write, timestamp: &str, message: &str) -> io::Result<()> {
    for line in message.lines() {
        writeln!(writer, "{timestamp} {line}")?;
    }
    Ok(())
}

/// Write each line as a separate UTC-stamped log record, including multiline errors.
pub fn stderr(message: &str) {
    if DAEMON_STDERR_USERS.load(Ordering::Acquire) != 0
        && let Some(writer) = DAEMON_STDERR.get()
    {
        let mut record = Vec::new();
        let _ = write_message(&mut record, message);
        writer.enqueue(&record);
        return;
    }
    let mut output = io::stderr().lock();
    let _ = write_message(&mut output, message);
}

pub fn stdout(message: &str) {
    let mut output = io::stdout().lock();
    let _ = write_message(&mut output, message);
}

fn write_message(writer: &mut impl Write, message: &str) -> io::Result<()> {
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("UTC timestamp is representable");
    write_lines(writer, &timestamp, message)
}

#[macro_export]
macro_rules! log_eprintln {
    ($($arg:tt)*) => {
        $crate::log::stderr(&format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_every_line_of_multiline_output() {
        let mut output = Vec::new();
        write_lines(&mut output, "2026-09-26T08:30:00Z", "first\nsecond\n").unwrap();
        assert_eq!(
            String::from_utf8_lossy(&output),
            "2026-09-26T08:30:00Z first\n2026-09-26T08:30:00Z second\n"
        );
        output.clear();
        write_message(&mut output, "first\nsecond").unwrap();
        for line in String::from_utf8(output).unwrap().lines() {
            let (timestamp, _) = line.split_once(' ').unwrap();
            assert!(timestamp.ends_with('Z'), "not a UTC timestamp: {timestamp}");
        }
    }
}
