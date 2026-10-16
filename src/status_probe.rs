//! Fresh exact-service samples for status. The daemon's cached echo sample
//! describes echo alone; a failed control request must not masquerade as an
//! unsupported service. Each query is bounded locally as well as remotely.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Result, ensure};
use fabric::{
    config::FabricHome,
    control::{ControlRequest, ControlResponse, PeerReachability},
    daemon::send_control,
    error::code_of,
};
use tokio::task::JoinSet;

const MAX_CONCURRENT: usize = 8;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServiceSample {
    pub measured: Option<String>,
    pub outcome: String,
    pub round_trip_micros: Option<u64>,
    pub transport: Option<String>,
    pub error: Option<String>,
    pub code: Option<String>,
}

impl ServiceSample {
    fn empty(measured: Option<String>, outcome: &str) -> Self {
        Self {
            measured,
            outcome: outcome.into(),
            round_trip_micros: None,
            transport: None,
            error: None,
            code: None,
        }
    }
}

pub(super) async fn sample_services(
    home: &FabricHome,
    peers: &[PeerReachability],
    protocol: &str,
) -> Result<BTreeMap<String, ServiceSample>> {
    sample_with_timeout(home, peers, protocol, SAMPLE_TIMEOUT).await
}

async fn sample_with_timeout(
    home: &FabricHome,
    peers: &[PeerReachability],
    protocol: &str,
    timeout: Duration,
) -> Result<BTreeMap<String, ServiceSample>> {
    ensure!(
        !protocol.is_empty(),
        "service must be a non-empty ALPN string"
    );
    let mut samples = BTreeMap::new();
    let mut pending = peers
        .iter()
        .filter_map(|peer| {
            if peer.inbound_only {
                samples.insert(peer.id.clone(), ServiceSample::empty(None, "not_probed"));
                None
            } else {
                Some(peer.id.clone())
            }
        })
        .collect::<Vec<_>>()
        .into_iter();
    let mut running = JoinSet::new();
    loop {
        while running.len() < MAX_CONCURRENT {
            let Some(id) = pending.next() else { break };
            let home = home.clone();
            let protocol = protocol.to_owned();
            running.spawn(async move {
                let sample = sample_one(&home, &id, &protocol, timeout).await;
                (id, sample)
            });
        }
        let Some(completed) = running.join_next().await else {
            break;
        };
        let (id, sample) = completed?;
        samples.insert(id, sample);
    }
    Ok(samples)
}

async fn sample_one(
    home: &FabricHome,
    id: &str,
    protocol: &str,
    timeout: Duration,
) -> ServiceSample {
    let mut sample = ServiceSample::empty(Some(protocol.into()), "error");
    let request = ControlRequest::Probe {
        peer: id.into(),
        protocol: protocol.into(),
        timeout_ms: timeout.as_millis().try_into().unwrap_or(u64::MAX),
    };
    match tokio::time::timeout(timeout, send_control(home, request)).await {
        Err(_) => {
            sample.outcome = "timeout".into();
            sample.code = Some("timeout".into());
            sample.error = Some("status service sample exceeded its deadline".into());
        }
        Ok(Err(error)) => {
            sample.code = Some(code_of(&error).as_str().into());
            sample.error = Some(format!("{error:#}"));
        }
        Ok(Ok(ControlResponse::ProbeResult {
            peer_id,
            protocol: reported,
            outcome,
            round_trip_micros,
            transport,
            error,
            code,
            ..
        })) if peer_id == id
            && reported == protocol
            && matches!(
                outcome.as_str(),
                "supported" | "unsupported" | "unreachable" | "timeout"
            ) =>
        {
            // Only a successful sample has a measured RTT and path. In
            // particular, never retain the cached echo metrics on refusal.
            if outcome == "supported" {
                sample.round_trip_micros = round_trip_micros;
                sample.transport = transport;
            }
            sample.outcome = outcome;
            sample.error = error;
            sample.code = code;
        }
        Ok(Ok(_)) => {
            sample.code = Some("internal".into());
            sample.error = Some("daemon returned an unexpected service sample".into());
        }
    }
    sample
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    fn peer(id: &str, inbound_only: bool) -> PeerReachability {
        serde_json::from_value(serde_json::json!({
            "id": id, "name": null, "inbound_only": inbound_only,
            "reachable": true, "bytes": 32, "round_trip_micros": 999,
            "transport": "relay", "error": null
        }))
        .unwrap()
    }

    async fn fixture() -> Result<(tempfile::TempDir, FabricHome, UnixListener)> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        std::fs::create_dir_all(home.control_socket_path().parent().unwrap())?;
        let listener = UnixListener::bind(home.control_socket_path())?;
        Ok((dir, home, listener))
    }

    async fn answer(stream: tokio::net::UnixStream) -> Result<()> {
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        let request: ControlRequest = serde_json::from_str(&line)?;
        let ControlRequest::Probe { peer, protocol, .. } = request else {
            panic!("unexpected mutation/request: {request:?}")
        };
        assert_eq!(
            protocol, "work/0",
            "status must query the exact service, never echo"
        );
        let outcome = match peer.as_str() {
            "allowed" => "supported",
            "missing" => "unsupported",
            _ => "unreachable",
        };
        let supported = outcome == "supported";
        let response = ControlResponse::ProbeResult {
            peer: peer.clone(),
            peer_id: peer,
            protocol,
            outcome: outcome.into(),
            round_trip_micros: supported.then_some(42),
            transport: supported.then(|| "direct".into()),
            error: (!supported).then(|| "sample not supported or permitted".into()),
            code: (outcome == "unreachable").then(|| "peer_not_permitted_for_service".into()),
        };
        stream
            .get_mut()
            .write_all(&serde_json::to_vec(&response)?)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn exact_service_answers_clear_echo_metrics_and_skip_inbound_peers() -> Result<()> {
        let (_dir, home, listener) = fixture().await?;
        let daemon = tokio::spawn(async move {
            for _ in 0..3 {
                answer(listener.accept().await?.0).await?;
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err(),
                "inbound peer was probed"
            );
            Ok::<_, anyhow::Error>(())
        });
        let samples = sample_services(
            &home,
            &[
                peer("allowed", false),
                peer("missing", false),
                peer("denied", false),
                peer("incoming", true),
            ],
            "work/0",
        )
        .await?;
        assert_eq!(samples["allowed"].round_trip_micros, Some(42));
        assert_eq!(samples["allowed"].transport.as_deref(), Some("direct"));
        assert_eq!(samples["missing"].outcome, "unsupported");
        assert_eq!(
            samples["denied"].code.as_deref(),
            Some("peer_not_permitted_for_service")
        );
        for id in ["missing", "denied", "incoming"] {
            assert_eq!(samples[id].round_trip_micros, None);
            assert_eq!(samples[id].transport, None);
        }
        assert_eq!(samples["incoming"].measured, None);
        assert_eq!(samples["incoming"].outcome, "not_probed");
        assert_eq!(samples["allowed"].measured.as_deref(), Some("work/0"));
        daemon.await??;
        Ok(())
    }

    #[tokio::test]
    async fn unanswered_and_wrong_service_replies_never_mean_unsupported() -> Result<()> {
        let (_dir, home, listener) = fixture().await?;
        let daemon = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await?;
            stream.get_mut().write_all(br#"{"type":"probe_result","peer":"p","peer_id":"wrong","protocol":"echo","outcome":"unsupported","round_trip_micros":null,"transport":null,"error":null}"#).await?;
            drop(stream);
            let (held, _) = listener.accept().await?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(held);
            Ok::<_, anyhow::Error>(())
        });
        let wrong = sample_with_timeout(
            &home,
            &[peer("p", false)],
            "work/0",
            Duration::from_millis(100),
        )
        .await?;
        assert_eq!(wrong["p"].outcome, "error");
        let timeout = sample_with_timeout(
            &home,
            &[peer("p", false)],
            "work/0",
            Duration::from_millis(50),
        )
        .await?;
        assert_eq!(timeout["p"].outcome, "timeout");
        daemon.await??;
        // No daemon is a local error, never evidence about service support.
        let absent = sample_services(&home, &[peer("p", false)], "work/0").await?;
        assert_eq!(absent["p"].outcome, "error");
        Ok(())
    }

    #[tokio::test]
    async fn large_peer_lists_have_bounded_probe_fanout() -> Result<()> {
        let (_dir, home, listener) = fixture().await?;
        let daemon = tokio::spawn(async move {
            let mut first = Vec::new();
            for _ in 0..MAX_CONCURRENT {
                first.push(listener.accept().await?.0);
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err(),
                "too many concurrent probes"
            );
            for stream in first {
                answer(stream).await?;
            }
            for _ in MAX_CONCURRENT..20 {
                answer(listener.accept().await?.0).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        let peers = (0..20)
            .map(|i| peer(&format!("peer-{i}"), false))
            .collect::<Vec<_>>();
        let samples = sample_services(&home, &peers, "work/0").await?;
        assert_eq!(samples.len(), 20);
        daemon.await??;
        Ok(())
    }
}
