//! Notice a daemon whose async runtime has stopped running, say what every
//! thread was waiting on, and end the process so its service manager starts a
//! fresh one.
//!
//! A daemon that dies is restarted. A daemon that is alive and does nothing is
//! not: its connections time out, its peers see it offline, its service manager
//! sees a running process, and it stays that way until a person logs in. That is
//! the worst way for the one daemon every other machine reaches the host through
//! to fail, so a frozen daemon is turned into a dead one.
//!
//! Two pieces. A task on the runtime writes the time to an atomic every few
//! seconds. A plain OS thread, which no runtime stall can stop, reads it. When
//! the runtime has not run for [`StallConfig::report_after`] the thread logs how
//! long and what each thread is waiting on; when it has not run for
//! [`StallConfig::abort_after`] the thread ends the process.
//!
//! The thread judges only time during which it was itself running. If it was
//! paused too (a suspended laptop, a frozen virtual machine, a stopped process),
//! it has no evidence about the runtime, so it starts counting again instead of
//! killing a daemon that is merely waking up.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

const VALIDATION_LOG_TARGET: &str = "fabric::validation";

/// The exit status of a daemon that ended itself because it was frozen
/// (EX_SOFTWARE). Any non-zero status makes a service manager restart it.
pub const STALL_EXIT_STATUS: i32 = 70;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StallConfig {
    /// How often the runtime writes its heartbeat and the thread reads it.
    pub beat: Duration,
    /// Log how long the runtime has been frozen, once, after this long.
    pub report_after: Duration,
    /// End the process after this long. `None` reports and never ends it.
    pub abort_after: Option<Duration>,
}

impl Default for StallConfig {
    fn default() -> Self {
        Self {
            beat: Duration::from_secs(10),
            report_after: Duration::from_secs(60),
            abort_after: Some(Duration::from_secs(180)),
        }
    }
}

impl StallConfig {
    /// The defaults, with `FABRIC_STALL_REPORT_SECS` and `FABRIC_STALL_ABORT_SECS`
    /// overriding them. An abort of `0` means never abort.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        let secs = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
        };
        if let Some(report) = secs("FABRIC_STALL_REPORT_SECS").filter(|secs| *secs > 0) {
            config.report_after = Duration::from_secs(report);
        }
        match secs("FABRIC_STALL_ABORT_SECS") {
            Some(0) => config.abort_after = None,
            Some(abort) => config.abort_after = Some(Duration::from_secs(abort)),
            None => {}
        }
        // Never abort before reporting, or the report is the thing that is lost.
        if let Some(abort) = config.abort_after {
            config.abort_after = Some(abort.max(config.report_after));
        }
        config
    }
}

/// What the thread concluded from one look at the heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    /// The runtime had stopped for this long, as far as the thread can tell, and
    /// this is the first look to say so.
    Stalled(Duration),
    /// Still stopped, and past the point of waiting.
    Abort(Duration),
    /// The runtime ran again after having been reported stopped.
    Recovered(Duration),
}

/// The decision, separate from clocks and threads so it can be tested without
/// either. Times are offsets from one origin.
#[derive(Debug)]
pub struct Judge {
    config: StallConfig,
    last_check: Option<Duration>,
    /// Evidence older than this was gathered while the thread itself may have
    /// been paused, and counts for nothing.
    floor: Duration,
    reported_since: Option<Duration>,
}

impl Judge {
    pub fn new(config: StallConfig) -> Self {
        Self {
            config,
            last_check: None,
            floor: Duration::ZERO,
            reported_since: None,
        }
    }

    /// `now` is the thread's clock; `beat` is the runtime's last heartbeat on the
    /// same clock.
    pub fn look(&mut self, now: Duration, beat: Duration) -> Verdict {
        // The thread is meant to look once per `beat`. A much longer gap means the
        // thread was not running either, so nothing before `now` says anything
        // about the runtime.
        if let Some(last) = self.last_check
            && now.saturating_sub(last) > self.config.beat * 3
        {
            self.floor = now;
        }
        self.last_check = Some(now);

        let since = beat.max(self.floor);
        let age = now.saturating_sub(since);
        if age < self.config.report_after {
            return match self.reported_since.take() {
                Some(started) => Verdict::Recovered(now.saturating_sub(started)),
                None => Verdict::Healthy,
            };
        }
        if let Some(abort) = self.config.abort_after
            && age >= abort
        {
            self.reported_since.get_or_insert(since);
            return Verdict::Abort(age);
        }
        if self.reported_since.is_none() {
            self.reported_since = Some(since);
            return Verdict::Stalled(age);
        }
        Verdict::Healthy
    }
}

/// One thread, as `/proc` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadSample {
    pub name: String,
    pub state: char,
    pub wait: String,
}

/// One line saying where the process's threads are: how many wait in each place,
/// and which are not simply idle. A frozen runtime with every thread parked
/// reads very differently from one with a thread stuck in a system call.
pub fn summarize(samples: &[ThreadSample]) -> String {
    use std::collections::BTreeMap;
    let mut waits: BTreeMap<String, usize> = BTreeMap::new();
    for sample in samples {
        let wait = if sample.wait.is_empty() || sample.wait == "0" {
            "running".to_string()
        } else {
            sample.wait.clone()
        };
        *waits.entry(format!("{}/{wait}", sample.state)).or_default() += 1;
    }
    let counts = waits
        .iter()
        .map(|(place, count)| format!("{place}={count}"))
        .collect::<Vec<_>>()
        .join(" ");
    let mut odd: Vec<String> = samples
        .iter()
        .filter(|sample| !matches!(sample.state, 'S' | 'I'))
        .map(|sample| format!("{}:{}/{}", sample.name, sample.state, sample.wait))
        .collect();
    odd.truncate(12);
    if odd.is_empty() {
        format!("threads={} {counts}", samples.len())
    } else {
        format!(
            "threads={} {counts} not-idle=[{}]",
            samples.len(),
            odd.join(" ")
        )
    }
}

/// Read this process's threads. Empty where `/proc` is not available.
pub fn sample_threads() -> Vec<ThreadSample> {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    let mut samples = Vec::new();
    for task in tasks.flatten() {
        let path = task.path();
        let read = |file: &str| {
            std::fs::read_to_string(path.join(file))
                .map(|text| text.trim().to_string())
                .unwrap_or_default()
        };
        let stat = read("stat");
        // The state follows the closing parenthesis of the command name, which
        // can itself contain parentheses and spaces.
        let state = stat
            .rsplit_once(") ")
            .and_then(|(_, rest)| rest.chars().next())
            .unwrap_or('?');
        samples.push(ThreadSample {
            name: read("comm"),
            state,
            wait: read("wchan"),
        });
    }
    samples
}

/// The kernel's own account of stall, from `/proc/pressure/{cpu,io,memory}`:
/// the share of the last 10 s and 60 s that some task, and for io and memory all
/// tasks, could not run for want of that resource.
pub fn summarize_pressure(cpu: &str, io: &str, memory: &str) -> String {
    let line = |name: &str, text: &str, kind: &str| {
        let Some(found) = text.lines().find(|line| line.starts_with(kind)) else {
            return format!("{name}=?");
        };
        let field = |key: &str| {
            found
                .split_whitespace()
                .find_map(|part| part.strip_prefix(key))
                .unwrap_or("?")
        };
        format!("{name}.{kind}={}/{}", field("avg10="), field("avg60="))
    };
    [
        line("cpu", cpu, "some"),
        line("io", io, "some"),
        line("io", io, "full"),
        line("mem", memory, "some"),
        line("mem", memory, "full"),
    ]
    .join(" ")
}

/// How much of the time this cgroup has been held back by its CPU quota, from
/// its `cpu.stat`. A quota that throttles a daemon looks, from inside, exactly
/// like the daemon freezing.
pub fn summarize_throttling(cpu_stat: &str) -> String {
    let value = |key: &str| {
        cpu_stat
            .lines()
            .find_map(|line| line.strip_prefix(key)?.trim().parse::<u64>().ok())
    };
    match (value("nr_throttled"), value("throttled_usec")) {
        (Some(periods), Some(usec)) => {
            format!(
                "throttled_periods={periods} throttled_secs={}",
                usec / 1_000_000
            )
        }
        _ => "unthrottled-or-unknown".to_string(),
    }
}

/// Every process on the machine in uninterruptible sleep, and what it waits in.
/// Several unrelated processes stuck in the same place name a shared cause that
/// no single process's own view can.
pub fn summarize_blocked(stats: &[(String, char, String)]) -> String {
    let blocked: Vec<String> = stats
        .iter()
        .filter(|(_, state, _)| *state == 'D')
        .map(|(name, _, wait)| format!("{name}:{wait}"))
        .collect();
    let shown = blocked
        .iter()
        .take(15)
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    format!("blocked_processes={} [{shown}]", blocked.len())
}

/// One process, as the first line of `/proc/<pid>/stat` gives it: its parent, its
/// state, and the name the kernel holds for it. The name is parsed from between
/// the first `(` and the last `)`, because it may itself contain either.
pub fn parse_stat(stat: &str) -> Option<(u32, char, String)> {
    let open = stat.find('(')?;
    let close = stat.rfind(')')?;
    let name = stat.get(open + 1..close)?.to_string();
    let mut rest = stat.get(close + 1..)?.split_whitespace();
    let state = rest.next()?.chars().next()?;
    let parent = rest.next()?.parse().ok()?;
    Some((parent, state, name))
}

/// The direct children of `me` among `(pid, stat)` lines.
pub fn children_of(me: u32, processes: &[(u32, String)]) -> Vec<(u32, char, String)> {
    processes
        .iter()
        .filter_map(|(pid, stat)| {
            let (parent, state, name) = parse_stat(stat)?;
            (parent == me).then_some((*pid, state, name))
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn all_processes() -> Vec<(u32, String)> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let pid: u32 = entry.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
            Some((pid, stat))
        })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn all_processes() -> Vec<(u32, String)> {
    Vec::new()
}

/// The daemon's own children and what each is doing. A child that is still
/// running the daemon's own program (same name) and has not become the command
/// it was started for is a fork that never reached exec, which leaves the thread
/// that started it waiting on a pipe for ever.
pub fn summarize_children(own_name: &str, children: &[(u32, char, String)]) -> String {
    let shown = children
        .iter()
        .take(12)
        .map(|(pid, state, name)| {
            let note = if name == own_name {
                "!same-program"
            } else {
                ""
            };
            format!("{pid}:{state}/{name}{note}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("children={} [{shown}]", children.len())
}

/// Kill every direct child of this process. A frozen daemon's children keep the
/// ends of its pipes open (a unit with `KillMode=process` leaves them alive on
/// purpose), and a thread of the daemon blocked on one of those pipes keeps the
/// whole process from ending, so the service manager never sees it exit and never
/// starts another. Killing them closes the pipes. Returns how many.
pub fn kill_children() -> usize {
    let children = children_of(std::process::id(), &all_processes());
    #[cfg(unix)]
    for (pid, _, _) in &children {
        // SAFETY: signalling a process by number; the worst outcome is ESRCH.
        unsafe {
            libc::kill(*pid as libc::pid_t, libc::SIGKILL);
        }
    }
    children.len()
}

#[cfg(target_os = "linux")]
fn machine_view() -> String {
    let read = |path: &str| std::fs::read_to_string(path).unwrap_or_default();
    let pressure = summarize_pressure(
        &read("/proc/pressure/cpu"),
        &read("/proc/pressure/io"),
        &read("/proc/pressure/memory"),
    );
    let cgroup = read("/proc/self/cgroup");
    let path = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or("/");
    let throttling = summarize_throttling(&read(&format!("/sys/fs/cgroup{path}/cpu.stat")));
    let mut stats = Vec::new();
    if let Ok(processes) = std::fs::read_dir("/proc") {
        for process in processes.flatten() {
            if !process
                .file_name()
                .to_string_lossy()
                .bytes()
                .all(|b| b.is_ascii_digit())
            {
                continue;
            }
            let dir = process.path();
            let stat = std::fs::read_to_string(dir.join("stat")).unwrap_or_default();
            let Some((head, rest)) = stat.split_once(") ") else {
                continue;
            };
            let state = rest.chars().next().unwrap_or('?');
            if state != 'D' {
                continue;
            }
            let name = head
                .split_once(" (")
                .map_or("?", |(_, name)| name)
                .to_string();
            let wait = std::fs::read_to_string(dir.join("wchan"))
                .map(|text| text.trim().to_string())
                .unwrap_or_default();
            stats.push((name, state, wait));
        }
    }
    let me = std::process::id();
    let own_name = parse_stat(&read("/proc/self/stat"))
        .map(|(_, _, name)| name)
        .unwrap_or_default();
    let children = summarize_children(&own_name, &children_of(me, &all_processes()));
    format!(
        "pressure(10s/60s): {pressure} | own-cgroup: {throttling} | {} | {children}",
        summarize_blocked(&stats)
    )
}

#[cfg(not(target_os = "linux"))]
fn machine_view() -> String {
    String::new()
}

/// What the watchdog does when it decides. Replaceable so a test can watch a
/// runtime freeze without the process ending.
pub trait Reaction: Send + 'static {
    fn stalled(&mut self, frozen_for: Duration, threads: &str);
    fn recovered(&mut self, frozen_for: Duration);
    /// End the process. Never returns in production.
    fn abort(&mut self, frozen_for: Duration);
}

/// The daemon's own reaction: log, and on abort leave at once. `_exit` rather
/// than `exit`: a frozen process may be frozen holding the locks `exit` would
/// take.
struct Daemon;

impl Reaction for Daemon {
    fn stalled(&mut self, frozen_for: Duration, threads: &str) {
        let machine = machine_view();
        fabric_config::log_eprintln!(
            "fabric: the async runtime has not run for {}s; threads: {threads}; machine: {machine}",
            frozen_for.as_secs()
        );
        tracing::warn!(
            target: VALIDATION_LOG_TARGET,
            event = "runtime_stalled",
            frozen_secs = frozen_for.as_secs(),
            threads = %threads,
            machine = %machine,
            "the daemon's async runtime has stopped running"
        );
    }

    fn recovered(&mut self, frozen_for: Duration) {
        fabric_config::log_eprintln!(
            "fabric: the async runtime ran again after {}s",
            frozen_for.as_secs()
        );
        tracing::warn!(
            target: VALIDATION_LOG_TARGET,
            event = "runtime_recovered",
            frozen_secs = frozen_for.as_secs(),
            "the daemon's async runtime ran again"
        );
    }

    fn abort(&mut self, frozen_for: Duration) {
        // First the children, which may be holding the pipe a stuck thread waits
        // on; without this the process can end up half dead and never be replaced.
        let killed = kill_children();
        fabric_config::log_eprintln!(
            "fabric: the async runtime has not run for {}s; killed {killed} child processes and \
             ending the process so the service manager starts a fresh one",
            frozen_for.as_secs()
        );
        #[cfg(unix)]
        unsafe {
            libc::_exit(STALL_EXIT_STATUS);
        }
        #[cfg(not(unix))]
        std::process::exit(STALL_EXIT_STATUS);
    }
}

/// Dropping this stops the watching.
pub struct Watchdog {
    stop: Arc<AtomicBool>,
    heartbeat: tokio::task::JoinHandle<()>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    /// Watch the runtime this is called from.
    pub fn start(config: StallConfig) -> Self {
        Self::start_with(config, Daemon)
    }

    pub fn start_with(config: StallConfig, mut reaction: impl Reaction) -> Self {
        let origin = Instant::now();
        let beat_ms = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let writer = beat_ms.clone();
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(config.beat);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                writer.store(origin.elapsed().as_millis() as u64, Ordering::Relaxed);
            }
        });

        let stopping = stop.clone();
        let thread = std::thread::Builder::new()
            .name("fabric-stall".into())
            .spawn(move || {
                let mut judge = Judge::new(config);
                while !stopping.load(Ordering::Relaxed) {
                    std::thread::park_timeout(config.beat);
                    if stopping.load(Ordering::Relaxed) {
                        return;
                    }
                    let now = origin.elapsed();
                    let beat = Duration::from_millis(beat_ms.load(Ordering::Relaxed));
                    match judge.look(now, beat) {
                        Verdict::Healthy => {}
                        Verdict::Stalled(age) => {
                            reaction.stalled(age, &summarize(&sample_threads()));
                        }
                        Verdict::Abort(age) => reaction.abort(age),
                        Verdict::Recovered(age) => reaction.recovered(age),
                    }
                }
            })
            .ok();

        Self {
            stop,
            heartbeat,
            thread,
        }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.heartbeat.abort();
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    fn config() -> StallConfig {
        StallConfig {
            beat: secs(10),
            report_after: secs(60),
            abort_after: Some(secs(180)),
        }
    }

    /// Looks every `beat`, as the thread does, with the runtime beating until
    /// `runtime_stops_at` and not again until `runtime_resumes_at`.
    fn run(
        config: StallConfig,
        until: u64,
        runtime_stops_at: u64,
        runtime_resumes_at: u64,
    ) -> Vec<(u64, Verdict)> {
        let mut judge = Judge::new(config);
        let mut verdicts = Vec::new();
        let mut last_beat = 0;
        let mut now = 0;
        while now < until {
            now += config.beat.as_secs();
            if now <= runtime_stops_at || now >= runtime_resumes_at {
                last_beat = now;
            }
            let verdict = judge.look(secs(now), secs(last_beat));
            if verdict != Verdict::Healthy {
                verdicts.push((now, verdict));
            }
        }
        verdicts
    }

    #[test]
    fn a_runtime_that_keeps_beating_is_never_judged() {
        assert!(run(config(), 3600, u64::MAX, u64::MAX).is_empty());
    }

    #[test]
    fn a_frozen_runtime_is_reported_once_and_then_ended() {
        // Beats until 100 s, then nothing for good.
        let verdicts = run(config(), 400, 100, u64::MAX);
        let kinds: Vec<_> = verdicts
            .iter()
            .map(|(_, verdict)| match verdict {
                Verdict::Stalled(_) => "stalled",
                Verdict::Abort(_) => "abort",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds.first(), Some(&"stalled"), "{verdicts:?}");
        assert_eq!(
            kinds.iter().filter(|kind| **kind == "stalled").count(),
            1,
            "a stall is reported once, not every look: {verdicts:?}"
        );
        let (reported_at, _) = verdicts[0];
        assert!(
            (160..=170).contains(&reported_at),
            "reported {reported_at}s in, after 60s frozen from 100s: {verdicts:?}"
        );
        let (ended_at, _) = verdicts
            .iter()
            .find(|(_, verdict)| matches!(verdict, Verdict::Abort(_)))
            .copied()
            .expect("a runtime frozen for good is ended");
        assert!(
            (280..=290).contains(&ended_at),
            "ended {ended_at}s in, after 180s frozen from 100s: {verdicts:?}"
        );
    }

    #[test]
    fn a_stall_that_ends_on_its_own_is_reported_then_recovered_and_never_ended() {
        // Frozen 100 s to 200 s: past the report, short of the abort.
        let verdicts = run(config(), 400, 100, 200);
        assert!(
            matches!(verdicts.first(), Some((_, Verdict::Stalled(_)))),
            "{verdicts:?}"
        );
        assert!(
            verdicts
                .iter()
                .any(|(_, verdict)| matches!(verdict, Verdict::Recovered(_))),
            "{verdicts:?}"
        );
        assert!(
            !verdicts
                .iter()
                .any(|(_, verdict)| matches!(verdict, Verdict::Abort(_))),
            "{verdicts:?}"
        );
    }

    #[test]
    fn a_short_stall_is_not_reported() {
        // 40 s frozen is under the 60 s report.
        assert!(run(config(), 400, 100, 140).is_empty());
    }

    /// The thread being paused too proves nothing about the runtime. A laptop
    /// that slept, a virtual machine that was frozen, a process that was
    /// stopped: on waking, the thread sees a heartbeat hours old and must not
    /// treat it as a frozen runtime.
    #[test]
    fn a_pause_of_the_watching_thread_is_not_evidence() {
        let mut judge = Judge::new(config());
        assert_eq!(judge.look(secs(10), secs(10)), Verdict::Healthy);
        assert_eq!(judge.look(secs(20), secs(20)), Verdict::Healthy);
        // Everything stops for an hour; the thread wakes first.
        assert_eq!(
            judge.look(secs(3620), secs(20)),
            Verdict::Healthy,
            "ending a daemon that was only asleep"
        );
        // The runtime then has a normal chance to beat, and the thread keeps
        // looking every `beat`.
        assert_eq!(judge.look(secs(3630), secs(3625)), Verdict::Healthy);
        // If it truly never beats again it is judged from the wake-up, not from
        // the stale heartbeat: 60 s after waking, and not before.
        let mut verdicts = Vec::new();
        for now in (3640..=3700).step_by(10) {
            verdicts.push((now, judge.look(secs(now), secs(3625))));
        }
        let first = verdicts
            .iter()
            .find(|(_, verdict)| *verdict != Verdict::Healthy)
            .copied();
        assert!(
            matches!(first, Some((3680..=3690, Verdict::Stalled(_)))),
            "judged from the wake-up at 3620 s: {verdicts:?}"
        );
    }

    #[test]
    fn the_abort_never_comes_before_the_report() {
        let _guard = EnvGuard::set(&[
            ("FABRIC_STALL_REPORT_SECS", "120"),
            ("FABRIC_STALL_ABORT_SECS", "30"),
        ]);
        let config = StallConfig::from_env();
        assert_eq!(config.report_after, secs(120));
        assert_eq!(config.abort_after, Some(secs(120)));
    }

    #[test]
    fn an_abort_of_zero_means_report_only() {
        let _guard = EnvGuard::set(&[("FABRIC_STALL_ABORT_SECS", "0")]);
        assert_eq!(StallConfig::from_env().abort_after, None);
        let mut report_only = config();
        report_only.abort_after = None;
        let verdicts = run(report_only, 3600, 100, u64::MAX);
        assert!(
            !verdicts
                .iter()
                .any(|(_, verdict)| matches!(verdict, Verdict::Abort(_))),
            "{verdicts:?}"
        );
    }

    /// Environment variables are process-wide and the tests run in parallel.
    struct EnvGuard {
        names: Vec<&'static str>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
            let lock = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            for (name, value) in vars {
                // SAFETY: serialised by LOCK against every other test that sets one.
                unsafe { std::env::set_var(name, value) };
            }
            Self {
                names: vars.iter().map(|(name, _)| *name).collect(),
                _lock: lock,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for name in &self.names {
                // SAFETY: still holding LOCK.
                unsafe { std::env::remove_var(name) };
            }
        }
    }

    #[test]
    fn the_thread_summary_says_where_threads_wait_and_which_are_not_idle() {
        let sample = |name: &str, state, wait: &str| ThreadSample {
            name: name.into(),
            state,
            wait: wait.into(),
        };
        let line = summarize(&[
            sample("tokio-rt-worker", 'S', "futex_wait_queue"),
            sample("tokio-rt-worker", 'S', "futex_wait_queue"),
            sample("tokio-rt-worker", 'S', "do_epoll_wait"),
            sample("tokio-rt-worker", 'D', "io_schedule"),
        ]);
        assert!(line.contains("threads=4"), "{line}");
        assert!(line.contains("S/futex_wait_queue=2"), "{line}");
        assert!(line.contains("S/do_epoll_wait=1"), "{line}");
        assert!(line.contains("D/io_schedule=1"), "{line}");
        assert!(
            line.contains("not-idle=[tokio-rt-worker:D/io_schedule]"),
            "{line}"
        );

        let idle = summarize(&[sample("a", 'S', "0")]);
        assert!(!idle.contains("not-idle"), "{idle}");
        assert!(
            idle.contains("S/running=1"),
            "a zero wchan reads as running: {idle}"
        );
    }

    #[test]
    fn a_stat_line_is_parsed_whatever_the_process_is_called() {
        assert_eq!(
            parse_stat("4242 (fabric) S 1 4242 4242 0 -1 4194560"),
            Some((1, 'S', "fabric".to_string()))
        );
        // A name may hold spaces and parentheses.
        assert_eq!(
            parse_stat("77 (tokio (rt) worker) D 4242 4242 4242 0 -1"),
            Some((4242, 'D', "tokio (rt) worker".to_string()))
        );
        assert_eq!(parse_stat("garbage"), None);
    }

    #[test]
    fn only_direct_children_are_found_and_a_stuck_fork_is_marked() {
        let processes = vec![
            (10, "10 (fabric) S 1 10 10 0 -1".to_string()),
            (11, "11 (st3) S 10 11 11 0 -1".to_string()),
            (12, "12 (fabric) D 10 12 12 0 -1".to_string()),
            (13, "13 (sleep) S 11 13 13 0 -1".to_string()),
        ];
        let children = children_of(10, &processes);
        assert_eq!(
            children.iter().map(|c| c.0).collect::<Vec<_>>(),
            vec![11, 12]
        );
        let line = summarize_children("fabric", &children);
        assert!(line.starts_with("children=2"), "{line}");
        assert!(line.contains("11:S/st3 "), "{line}");
        assert!(
            line.contains("12:D/fabric!same-program"),
            "a fork that never became its command must stand out: {line}"
        );
        assert_eq!(summarize_children("fabric", &[]), "children=0 []");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_frozen_daemons_children_are_killed_so_their_pipes_close() {
        use std::process::{Command, Stdio};
        let mut child = Command::new("sleep")
            .arg("60")
            .stdin(Stdio::piped())
            .spawn()
            .expect("sleep is available");
        let killed = kill_children();
        assert!(killed >= 1, "the child was not found among {killed}");
        let status = child.wait().expect("the child can be waited for");
        assert!(!status.success(), "the child survived: {status:?}");
    }

    #[test]
    fn the_kernel_pressure_figures_are_read_from_their_files() {
        let cpu = "some avg10=1.50 avg60=0.75 avg300=0.10 total=123456\n";
        let io = "some avg10=40.00 avg60=12.00 avg300=3.00 total=9\nfull avg10=38.00 avg60=11.00 avg300=2.00 total=8\n";
        let memory = "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        let line = summarize_pressure(cpu, io, memory);
        assert!(line.contains("cpu.some=1.50/0.75"), "{line}");
        assert!(line.contains("io.some=40.00/12.00"), "{line}");
        assert!(line.contains("io.full=38.00/11.00"), "{line}");
        assert!(line.contains("mem.full=0.00/0.00"), "{line}");
        // A kernel without pressure accounting still produces a line.
        assert!(summarize_pressure("", "", "").contains("cpu=?"));
    }

    #[test]
    fn cpu_throttling_is_read_from_the_cgroup_file() {
        let stat = "usage_usec 100\nnr_periods 500\nnr_throttled 42\nthrottled_usec 7500000\n";
        assert_eq!(
            summarize_throttling(stat),
            "throttled_periods=42 throttled_secs=7"
        );
        assert_eq!(
            summarize_throttling("usage_usec 1\n"),
            "unthrottled-or-unknown"
        );
    }

    #[test]
    fn processes_stuck_in_the_same_wait_are_listed_together() {
        let stats = vec![
            ("fabric".to_string(), 'S', "futex_wait_queue".to_string()),
            ("serve-fabric".to_string(), 'D', "io_schedule".to_string()),
            ("git".to_string(), 'D', "io_schedule".to_string()),
        ];
        let line = summarize_blocked(&stats);
        assert!(line.starts_with("blocked_processes=2"), "{line}");
        assert!(line.contains("serve-fabric:io_schedule"), "{line}");
        assert!(line.contains("git:io_schedule"), "{line}");
        assert!(
            !line.contains("fabric:futex"),
            "an idle process was listed: {line}"
        );
        assert_eq!(summarize_blocked(&[]), "blocked_processes=0 []");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_machine_view_reads_this_machine() {
        let view = machine_view();
        assert!(view.contains("pressure(10s/60s):"), "{view}");
        assert!(view.contains("own-cgroup:"), "{view}");
        assert!(view.contains("blocked_processes="), "{view}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn this_process_can_be_sampled() {
        let samples = sample_threads();
        assert!(!samples.is_empty(), "no threads read from /proc/self/task");
        assert!(
            samples.iter().all(|sample| sample.state != '?'),
            "a thread's state did not parse: {samples:?}"
        );
    }

    /// Only does anything when the parent test starts it as a child process.
    #[test]
    fn child_that_ends_itself_the_way_a_frozen_daemon_does() {
        if std::env::var_os("FABRIC_STALL_TEST_CHILD").is_some() {
            Daemon.abort(Duration::from_secs(181));
            unreachable!("the abort returned");
        }
    }

    /// The abort has to end the process with a status a service manager treats
    /// as a failure, or a frozen daemon is replaced by a stopped one.
    #[cfg(unix)]
    #[test]
    fn a_frozen_daemon_ends_with_the_stall_status() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stall::tests::child_that_ends_itself_the_way_a_frozen_daemon_does",
                "--nocapture",
            ])
            .env("FABRIC_STALL_TEST_CHILD", "1")
            .output()
            .unwrap();
        assert_eq!(
            status.status.code(),
            Some(STALL_EXIT_STATUS),
            "stdout: {} stderr: {}",
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
        assert!(
            String::from_utf8_lossy(&status.stderr).contains("ending the process"),
            "the abort did not say why: {}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

    struct Recorder(mpsc::Sender<String>);

    impl Reaction for Recorder {
        fn stalled(&mut self, frozen_for: Duration, threads: &str) {
            let _ = self
                .0
                .send(format!("stalled {} {threads}", frozen_for.as_millis()));
        }
        fn recovered(&mut self, frozen_for: Duration) {
            let _ = self.0.send(format!("recovered {}", frozen_for.as_millis()));
        }
        fn abort(&mut self, frozen_for: Duration) {
            let _ = self.0.send(format!("abort {}", frozen_for.as_millis()));
        }
    }

    /// The real thing on a real runtime: block the only worker thread, and the
    /// separate watching thread must notice, report, and want to end the process.
    /// Then let the runtime run again, and it must say so.
    #[tokio::test(flavor = "current_thread")]
    async fn a_runtime_that_is_really_blocked_is_noticed_by_its_watcher() {
        // Wide margins: a busy machine delays the watching thread, and a delay of
        // more than three beats reads as the thread itself having been paused.
        let config = StallConfig {
            beat: Duration::from_millis(100),
            report_after: Duration::from_millis(800),
            abort_after: Some(Duration::from_millis(1600)),
        };
        let (tx, rx) = mpsc::channel();
        let watchdog = Watchdog::start_with(config, Recorder(tx));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(rx.try_recv().is_err(), "a healthy runtime was reported");

        // Hold the runtime's only thread: nothing on it, the heartbeat included,
        // runs for 3 s.
        std::thread::sleep(Duration::from_millis(3000));
        tokio::time::sleep(Duration::from_millis(600)).await;
        let mut seen = Vec::new();
        while let Ok(line) = rx.try_recv() {
            seen.push(line);
        }
        drop(watchdog);

        assert!(
            seen.iter().any(|line| line.starts_with("stalled ")),
            "the blocked runtime was never reported: {seen:?}"
        );
        assert!(
            seen.iter().any(|line| line.starts_with("abort ")),
            "a runtime blocked past the limit was not ended: {seen:?}"
        );
        assert!(
            seen.iter().any(|line| line.starts_with("recovered ")),
            "the runtime running again was not reported: {seen:?}"
        );
        #[cfg(target_os = "linux")]
        assert!(
            seen.iter()
                .find(|line| line.starts_with("stalled "))
                .is_some_and(|line| line.contains("threads=")),
            "the report did not say where the threads were: {seen:?}"
        );
    }
}
