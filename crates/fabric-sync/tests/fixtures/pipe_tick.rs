use std::{
    io::{self, Write},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn main() -> io::Result<()> {
    let (send, receive) = mpsc::channel();
    thread::spawn(move || {
        let started = Instant::now();
        for sequence in 0..1_000_u64 {
            let deadline = Duration::from_millis(sequence * 10);
            while started.elapsed() < deadline {
                std::hint::spin_loop();
            }
            if send.send((sequence, started.elapsed().as_nanos())).is_err() {
                return;
            }
        }
    });
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for (sequence, source_nanos) in receive {
        writeln!(stdout, "{sequence} {source_nanos}")?;
        stdout.flush()?;
    }
    Ok(())
}
