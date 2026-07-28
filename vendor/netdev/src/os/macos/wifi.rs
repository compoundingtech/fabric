use objc2::rc::autoreleasepool;
use objc2_core_wlan::CWWiFiClient;
use objc2_foundation::NSString;
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex, mpsc},
    thread,
    time::Duration,
};

const WIFI_PROBE_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeState {
    InFlight,
    Complete(Option<u64>),
}

static WIFI_PROBES: LazyLock<Mutex<HashMap<String, ProbeState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Returns the macOS Wi-Fi transmit rate in bps for the given interface name.
///
/// The CoreWLAN calls below produce autoreleased Obj-C/XPC objects
/// (`CWFRequestParameters`, `CWFInterface`, the `CWFXPCRequestProtocolCoreWLAN`
/// proxy). This is called from long-lived threads with no draining autorelease
/// pool (e.g. `netwatch`'s interface monitor re-enumerates on every network
/// event), so without an explicit pool those objects are added to a pool that
/// never drains and accumulate without bound — a steady multi-MB/hour macOS
/// leak. A scoped pool per call frees them immediately.
pub(crate) fn get_wifi_transmit_rate(iface_name: &str) -> Option<u64> {
    bounded_wifi_probe(iface_name, WIFI_PROBE_TIMEOUT, query_wifi_transmit_rate)
}

/// CoreWLAN uses a synchronous XPC request for this optional metadata. A
/// wedged `airportd`/CoreWLAN service must not prevent callers from enumerating
/// interfaces, so run at most one request per interface in a detached worker
/// and fall back to `None` after a short deadline. A timed-out worker remains
/// marked in flight, preventing repeated network refreshes from accumulating
/// more blocked threads. If it eventually completes, later reads use its cache.
fn bounded_wifi_probe<F>(iface_name: &str, timeout: Duration, probe: F) -> Option<u64>
where
    F: FnOnce(&str) -> Option<u64> + Send + 'static,
{
    {
        let probes = WIFI_PROBES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match probes.get(iface_name) {
            Some(ProbeState::Complete(rate)) => return *rate,
            Some(ProbeState::InFlight) => return None,
            None => {}
        }
    }

    {
        let mut probes = WIFI_PROBES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match probes.entry(iface_name.to_owned()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                return match entry.get() {
                    ProbeState::Complete(rate) => *rate,
                    ProbeState::InFlight => None,
                };
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(ProbeState::InFlight);
            }
        }
    }

    let interface = iface_name.to_owned();
    let worker_interface = interface.clone();
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let spawn = thread::Builder::new()
        .name(format!("netdev-wifi-{iface_name}"))
        .spawn(move || {
            let rate = probe(&worker_interface);
            let mut probes = WIFI_PROBES
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            probes.insert(worker_interface, ProbeState::Complete(rate));
            let _ = result_tx.send(rate);
        });

    if spawn.is_err() {
        let mut probes = WIFI_PROBES
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        probes.insert(interface, ProbeState::Complete(None));
        return None;
    }

    result_rx.recv_timeout(timeout).ok().flatten()
}

fn query_wifi_transmit_rate(iface_name: &str) -> Option<u64> {
    autoreleasepool(|_pool| {
        let client = unsafe { CWWiFiClient::sharedWiFiClient() };
        let name = NSString::from_str(iface_name);

        let wifi_iface = unsafe { client.interfaceWithName(Some(&name)) };
        wifi_iface.map(|i| {
            let transmit_rate = unsafe { i.transmitRate() };
            (transmit_rate * 1e6) as u64
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Instant,
    };

    fn test_interface(name: &str) -> String {
        format!("fabric-test-{name}-{}", std::process::id())
    }

    #[test]
    fn blocked_probe_is_bounded_and_not_spawned_twice() {
        let interface = test_interface("blocked");
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_calls = calls.clone();
        let started = Instant::now();

        let rate = bounded_wifi_probe(&interface, Duration::from_millis(10), move |_| {
            worker_calls.fetch_add(1, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(100));
            Some(123)
        });

        assert_eq!(rate, None);
        assert!(started.elapsed() < Duration::from_millis(80));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let duplicate_calls = calls.clone();
        let second = bounded_wifi_probe(&interface, Duration::from_millis(10), move |_| {
            duplicate_calls.fetch_add(1, Ordering::SeqCst);
            Some(456)
        });
        assert_eq!(second, None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn completed_probe_is_cached() {
        let interface = test_interface("complete");
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_calls = calls.clone();

        let first = bounded_wifi_probe(&interface, Duration::from_secs(1), move |_| {
            worker_calls.fetch_add(1, Ordering::SeqCst);
            Some(789)
        });
        assert_eq!(first, Some(789));

        let duplicate_calls = calls.clone();
        let second = bounded_wifi_probe(&interface, Duration::from_secs(1), move |_| {
            duplicate_calls.fetch_add(1, Ordering::SeqCst);
            Some(999)
        });
        assert_eq!(second, Some(789));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
