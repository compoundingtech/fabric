//! Write an exec service for fabric.
//!
//! `fabric expose NAME --exec -- COMMAND` starts COMMAND once per incoming
//! stream and gives it the stream on stdin and stdout. A service that is really a
//! front for something else (a terminal, a database socket, a log follower) is a
//! bridge: bytes from stdin go to a socket, bytes from the socket go to stdout.
//! This crate is that bridge, and it is written the way such a bridge has to be.
//!
//! # Why not `std::io::copy`
//!
//! On Linux `std::io::copy` is not a read/write loop when it can avoid one. A
//! copy from a socket to a pipe becomes `splice(2)`, and a copy from a pipe to a
//! socket becomes one as well. `splice` from an idle socket sleeps in the kernel
//! **with the output pipe's lock held**, for as long as the socket stays quiet.
//! Anything that then reads or writes the other end of that pipe, even on a
//! non-blocking descriptor, waits for the lock: the daemon that started the
//! service was parked by it, and a terminal that was sitting at a prompt was
//! enough to do it. Every function here moves bytes with an explicit `read`, a
//! `write_all` and a `flush`, which never reaches `splice`.
//!
//! # What it does
//!
//! * [`pump`] copies one direction, flushing after every chunk, so output appears
//!   when it is produced and not when a buffer fills.
//! * [`bridge`] runs two pumps (one thread each) between a socket and a pair of
//!   stdio-like ends, and carries end-of-file across: when stdin ends the socket's
//!   write side is shut down, so the service on the other end sees the request
//!   finish and can still answer; when the socket ends, stdout is closed, so the
//!   peer sees the response finish. It returns when the socket has ended.
//! * [`serve_stdio`] is `bridge` on the process's own stdin and stdout, on
//!   descriptors of their own (a `dup`), so nothing else in the process shares
//!   their buffering.
//!
//! The crate has no dependencies and no unsafe code.

use std::{
    io::{self, Read, Write},
    net::Shutdown,
    os::{fd::AsFd, unix::net::UnixStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

/// How much one read takes. Large enough to move a terminal screen in one piece,
/// small enough that a slow peer is not buffered far ahead of.
pub const CHUNK: usize = 16 * 1024;

/// Copy `reader` to `writer` until the reader ends, flushing after every chunk.
/// Returns the number of bytes moved.
///
/// This is the loop `std::io::copy` is not: it never uses a kernel shortcut, so
/// it never holds a pipe's lock while it waits for the reader.
pub fn pump<R: Read, W: Write>(mut reader: R, mut writer: W) -> io::Result<u64> {
    let mut buf = vec![0u8; CHUNK];
    let mut moved = 0u64;
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => return Ok(moved),
            Ok(n) => n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        writer.write_all(&buf[..n])?;
        writer.flush()?;
        moved += n as u64;
    }
}

/// What a finished bridge moved, in each direction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Moved {
    /// Bytes from the input end to the socket.
    pub to_socket: u64,
    /// Bytes from the socket to the output end.
    pub from_socket: u64,
}

/// [`pump`] that also keeps a running count where another thread can read it.
fn pump_counting<R: Read, W: Write>(
    mut reader: R,
    mut writer: W,
    moved: &AtomicU64,
) -> io::Result<u64> {
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) => return Ok(moved.load(Ordering::SeqCst)),
            Ok(n) => n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        writer.write_all(&buf[..n])?;
        writer.flush()?;
        moved.fetch_add(n as u64, Ordering::SeqCst);
    }
}

/// Bridge `socket` with an input end and an output end. Each direction ends on
/// its reader's end-of-file or error and carries it to the far side: the
/// socket's write half is shut down when `input` ends (the request is finished,
/// the service may still answer), and `output` is closed when the socket ends
/// (the response is finished).
///
/// Returns when the **socket** has ended, which is when the exchange is over. The
/// input pump may still be waiting for input at that point, and a blocking read
/// of a pipe cannot be interrupted; the caller is a process that is about to
/// exit, and exiting ends that thread. If the input pump has already finished its
/// result is reported (an error in it is returned); if it has not, what it moved
/// so far is.
pub fn bridge<I, O>(socket: UnixStream, input: I, output: O) -> io::Result<Moved>
where
    I: Read + Send + 'static,
    O: Write + Send + 'static,
{
    let read_half = socket.try_clone()?;
    let write_half = socket.try_clone()?;

    let to_socket = Arc::new(AtomicU64::new(0));
    let from_socket = Arc::new(AtomicU64::new(0));
    let upstream_result: Arc<Mutex<Option<io::Result<u64>>>> = Arc::new(Mutex::new(None));

    {
        let (to_socket, result) = (to_socket.clone(), upstream_result.clone());
        thread::Builder::new()
            .name("fabric-stdio-to-socket".to_string())
            .spawn(move || {
                let mut write_half = write_half;
                let outcome = pump_counting(input, &mut write_half, &to_socket);
                // End of input is the end of the request: tell the socket's service.
                let _ = write_half.shutdown(Shutdown::Write);
                if let Ok(mut slot) = result.lock() {
                    *slot = Some(outcome);
                }
            })?;
    }
    let downstream = {
        let from_socket = from_socket.clone();
        thread::Builder::new()
            .name("fabric-stdio-from-socket".to_string())
            .spawn(move || pump_counting(read_half, output, &from_socket))?
    };

    let downstream_result = downstream
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("a bridge thread panicked")));
    // Nothing more will come back: closing the socket makes the input pump's next
    // write fail and the service on the other end stop.
    let _ = socket.shutdown(Shutdown::Both);

    // A short grace for the input pump to report; it usually has by now.
    let waited = std::time::Instant::now();
    loop {
        if upstream_result.lock().map(|r| r.is_some()).unwrap_or(true)
            || waited.elapsed() >= UPSTREAM_GRACE
        {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let upstream = upstream_result.lock().ok().and_then(|mut slot| slot.take());

    downstream_result?;
    if let Some(Err(error)) = upstream {
        // A write into a socket the service has already closed is how an input
        // pump learns the exchange is over; it is not a failure.
        if error.kind() != io::ErrorKind::BrokenPipe
            && error.kind() != io::ErrorKind::ConnectionReset
        {
            return Err(error);
        }
    }
    Ok(Moved {
        to_socket: to_socket.load(Ordering::SeqCst),
        from_socket: from_socket.load(Ordering::SeqCst),
    })
}

/// How long [`bridge`] waits, after the socket has ended, for the input pump to
/// report before returning what it has moved so far.
const UPSTREAM_GRACE: Duration = Duration::from_millis(200);

/// [`bridge`] between `socket` and this process's stdin and stdout. The two are
/// duplicated first, so the bridge owns its own descriptors and bypasses the
/// standard library's stdout buffering.
pub fn serve_stdio(socket: UnixStream) -> io::Result<Moved> {
    let input = std::fs::File::from(io::stdin().as_fd().try_clone_to_owned()?);
    let output = std::fs::File::from(io::stdout().as_fd().try_clone_to_owned()?);
    bridge(socket, input, output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Cursor,
        sync::mpsc,
        time::{Duration, Instant},
    };

    /// A reader that returns one byte at a time, and a writer that accepts at most
    /// three: both must still move every byte.
    struct Trickle<T>(T);
    impl<T: Read> Read for Trickle<T> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let limit = buf.len().min(1);
            self.0.read(&mut buf[..limit])
        }
    }
    struct ShortWrites(Vec<u8>);
    impl Write for ShortWrites {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let n = buf.len().min(3);
            self.0.extend_from_slice(&buf[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn pump_moves_every_byte_through_short_reads_and_writes() {
        let data: Vec<u8> = (0..5000u32).map(|n| (n % 241) as u8).collect();
        let mut out = ShortWrites(Vec::new());
        let moved = pump(Trickle(Cursor::new(data.clone())), &mut out).unwrap();
        assert_eq!(moved, data.len() as u64);
        assert_eq!(out.0, data);
    }

    #[test]
    fn pump_flushes_after_every_chunk() {
        struct CountFlush(usize);
        impl Write for CountFlush {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut out = CountFlush(0);
        pump(Trickle(Cursor::new(vec![1u8; 10])), &mut out).unwrap();
        assert_eq!(out.0, 10);
    }

    #[test]
    fn pump_returns_a_read_error() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::ConnectionReset))
            }
        }
        let error = pump(Broken, io::sink()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    }

    /// The service end of the socket: echoes what it is sent, then a trailer once
    /// its input ends, to show that end-of-input reached it and that it could
    /// still answer afterwards.
    fn echo_service(service: UnixStream) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut service = service;
            let mut buf = [0u8; 4096];
            loop {
                match service.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if service.write_all(&buf[..n]).is_err() {
                            return;
                        }
                    }
                }
            }
            let _ = service.write_all(b"|eof-seen");
            let _ = service.shutdown(Shutdown::Write);
        })
    }

    /// A pipe pair standing in for stdin/stdout of the exposed process.
    fn pipe() -> (UnixStream, UnixStream) {
        UnixStream::pair().unwrap()
    }

    #[test]
    fn bridge_carries_data_and_end_of_file_both_ways() {
        let (socket, service) = pipe();
        let service = echo_service(service);
        let (mut feed, input) = pipe();
        let (output, mut collect) = pipe();
        let bridge = thread::spawn(move || bridge(socket, input, output));

        // Feed from a thread: the echo comes back through `collect`, which is
        // only read below, so feeding on this thread would wait on itself.
        let feeder = thread::spawn(move || {
            feed.write_all(b"hello ").unwrap();
            feed.write_all(&vec![7u8; 100_000]).unwrap();
            feed.shutdown(Shutdown::Write).unwrap();
        });
        let mut got = Vec::new();
        collect.read_to_end(&mut got).unwrap();
        feeder.join().unwrap();
        let moved = bridge.join().unwrap().unwrap();
        service.join().unwrap();

        assert!(got.starts_with(b"hello "));
        assert!(got.ends_with(b"|eof-seen"));
        assert_eq!(got.len(), 6 + 100_000 + 9);
        assert_eq!(moved.to_socket, 100_006);
        assert_eq!(moved.from_socket, 100_015);
    }

    #[test]
    fn an_idle_socket_does_not_hold_up_the_other_direction() {
        let (socket, service) = pipe();
        let (mut feed, input) = pipe();
        let (output, mut collect) = pipe();
        let bridge = thread::spawn(move || bridge(socket, input, output));
        let mut service = service;

        // The service says nothing for a while; input still reaches it at once.
        feed.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        service
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        service.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");

        // Then the service speaks after a long silence and it comes out.
        thread::sleep(Duration::from_millis(300));
        service.write_all(b"pong").unwrap();
        collect
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        collect.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"pong");

        feed.shutdown(Shutdown::Write).unwrap();
        service.shutdown(Shutdown::Write).unwrap();
        bridge.join().unwrap().unwrap();
    }

    #[test]
    fn a_service_that_closes_abruptly_ends_the_bridge() {
        let (socket, service) = pipe();
        let (feed, input) = pipe();
        let (output, mut collect) = pipe();
        let bridge = thread::spawn(move || bridge(socket, input, output));

        drop(service); // the service goes away without a word
        let started = Instant::now();
        let mut rest = Vec::new();
        collect.read_to_end(&mut rest).unwrap(); // output is closed
        assert!(rest.is_empty());
        // The input side was never closed: the bridge must still finish.
        let result = bridge.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        drop(feed);
        let _ = result;
    }

    #[test]
    fn a_reader_that_stops_reading_does_not_strand_the_bridge() {
        let (socket, service) = pipe();
        let (feed, input) = pipe();
        let (output, collect) = pipe();
        let (done, finished) = mpsc::channel();
        thread::spawn(move || {
            let _ = done.send(bridge(socket, input, output));
        });
        drop(collect); // nobody will read the output
        // The service keeps talking into a bridge that has nowhere to put it.
        let talker = thread::spawn(move || {
            let mut service = service;
            let _ = service.write_all(&vec![1u8; 100_000]);
        });
        finished
            .recv_timeout(Duration::from_secs(5))
            .expect("the bridge did not finish")
            .ok();
        drop(feed);
        let _ = talker;
    }

    /// The point of the crate, kept from rotting: nothing here may call
    /// `std::io::copy`, which can become a lock-holding `splice`.
    #[test]
    fn the_crate_never_calls_io_copy() {
        let source = include_str!("lib.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();
        for (number, line) in production.lines().enumerate() {
            let code = line.split("//").next().unwrap();
            assert!(
                !code.contains("io::copy(") && !code.contains("copy_bidirectional"),
                "line {}: {}",
                number + 1,
                line.trim()
            );
        }
    }
}
