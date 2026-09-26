//! Timestamp process output that the service manager redirects to its logs.

use std::io::{self, Write};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn write_lines(mut writer: impl Write, timestamp: &str, message: &str) -> io::Result<()> {
    for line in message.lines() {
        writeln!(writer, "{timestamp} {line}")?;
    }
    Ok(())
}

/// Write each line as a separate UTC-stamped log record, including multiline errors.
pub fn stderr(message: &str) {
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
