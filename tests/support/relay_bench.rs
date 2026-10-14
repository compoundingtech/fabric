//! Manual local proof using the actual CLI binary and an owned iroh relay.
use anyhow::{Context, Result, bail};
use fabric::config::FabricHome;
use iroh_relay::server::{RelayConfig, Server, ServerConfig};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct Foreground {
    child: Child,
}
impl Drop for Foreground {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn cli(binary: &Path, home: &FabricHome, args: &[&str]) -> Result<String> {
    let output = Command::new(binary)
        .arg("--home")
        .arg(home.root())
        .args(args)
        .output()?;
    if !output.status.success() {
        bail!(
            "CLI {:?}: {}",
            args.first(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}
async fn start(binary: &Path, home: &FabricHome, mode: &str) -> Result<Foreground> {
    let log = fs::File::create(home.root().join("foreground.log"))?;
    let daemon = Foreground {
        child: Command::new(binary)
            .arg("--home")
            .arg(home.root())
            .args(["up", "--foreground", "--diagnostic-path-mode", mode])
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?,
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while !home.control_socket_path().exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("benchmark daemon did not start")?;
    Ok(daemon)
}
async fn stream(addr: String, bytes: usize) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_u64(bytes as u64).await?;
    let block = vec![b'r'; 64 * 1024];
    let mut remaining = bytes;
    while remaining > 0 {
        let n = remaining.min(block.len());
        stream.write_all(&block[..n]).await?;
        remaining -= n;
    }
    stream.shutdown().await?;
    let received = stream.read_u64().await?;
    assert_eq!(
        received, bytes as u64,
        "sink did not acknowledge all verified payload bytes"
    );
    Ok(())
}
async fn measure(
    addr: &str,
    bytes: usize,
    streams: usize,
    trials: usize,
) -> Result<serde_json::Value> {
    let mut seconds = Vec::new();
    // Include local socket/session setup, but warm the shared peer transport.
    stream(addr.to_string(), 1024).await?;
    for _ in 0..trials {
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..streams {
            tasks.spawn(stream(addr.to_string(), bytes));
        }
        tokio::time::timeout(Duration::from_secs(120), async {
            while let Some(result) = tasks.join_next().await {
                result??;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("bulk transfer timed out")??;
        seconds.push(start.elapsed().as_secs_f64());
    }
    let mut sorted = seconds.clone();
    sorted.sort_by(f64::total_cmp);
    let p50 = sorted[sorted.len() / 2];
    Ok(
        serde_json::json!({"streams": streams, "bytes_per_stream": bytes, "trials": trials,
        "seconds": seconds, "p50_seconds": p50, "p50_payload_mib_s": (bytes * streams) as f64 / (1024.0 * 1024.0) / p50}),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual bulk proof; use --release and optionally FABRIC_BENCH_BIN for a released artifact"]
async fn owned_relay_throughput() -> Result<()> {
    let _guard = super::local_slice_guard().await;
    let binary = std::env::var_os("FABRIC_BENCH_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_fabric")));
    let bytes = std::env::var("FABRIC_BENCH_BYTES")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(16 * 1024 * 1024);
    let streams = 4;
    let trials = 3;
    assert!(bytes > 0 && bytes <= 128 * 1024 * 1024);
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_addr = backend.local_addr()?.to_string();
    let sink = tokio::spawn(async move {
        loop {
            let (mut stream, _) = backend.accept().await?;
            tokio::spawn(async move {
                let bytes = stream.read_u64().await?;
                let mut remaining = bytes;
                let mut block = vec![0; 64 * 1024];
                while remaining > 0 {
                    let n = (remaining as usize).min(block.len());
                    stream.read_exact(&mut block[..n]).await?;
                    if block[..n].iter().any(|byte| *byte != b'r') {
                        bail!("bulk sink payload differs");
                    }
                    remaining -= n as u64;
                }
                stream.write_u64(bytes).await?;
                stream.shutdown().await?;
                Ok::<_, anyhow::Error>(())
            });
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    });
    let mut relay_config = ServerConfig::default();
    relay_config.relay = Some(RelayConfig::new((std::net::Ipv4Addr::LOCALHOST, 0)));
    let relay = Server::spawn(relay_config).await?;
    let relay_url = format!("http://{}/", relay.http_addr().unwrap());
    let load_before = fs::read_to_string("/proc/loadavg").ok().map(|text| {
        text.split_whitespace()
            .take(3)
            .map(str::to_string)
            .collect::<Vec<_>>()
    });
    let plain = measure(&backend_addr, bytes, streams, trials).await?;
    let mut carriers = serde_json::json!({"plain_tcp": plain});
    let mut version = String::new();
    for (mode, expected) in [("direct-only", "direct"), ("relay-only", "relay")] {
        let server_dir = TempDir::new()?;
        let client_dir = TempDir::new()?;
        let server = FabricHome::new(server_dir.path());
        let client = FabricHome::new(client_dir.path());
        for home in [&server, &client] {
            home.prepare()?;
            fs::write(home.config_path(), format!("relays = [{relay_url:?}]\n"))?;
        }
        let server_daemon = start(&binary, &server, mode).await?;
        let client_daemon = start(&binary, &client, mode).await?;
        version = cli(&binary, &client, &["--version"])?;
        cli(
            &binary,
            &server,
            &[
                "add",
                &cli(&binary, &client, &["id"])?,
                "client",
                "--addr-json",
                &cli(&binary, &client, &["addr"])?,
                "--allow",
                "bench-sink",
                "--inbound-only",
            ],
        )?;
        cli(
            &binary,
            &client,
            &[
                "add",
                &cli(&binary, &server, &["id"])?,
                "server",
                "--addr-json",
                &cli(&binary, &server, &["addr"])?,
                "--dial-only",
            ],
        )?;
        cli(
            &binary,
            &server,
            &["expose", "bench-sink", "--tcp", &backend_addr],
        )?;
        let probe: serde_json::Value = serde_json::from_str(&cli(
            &binary,
            &client,
            &["probe", "server", "bench-sink", "--json"],
        )?)?;
        assert_eq!(probe["outcome"], "supported");
        assert_eq!(probe["transport"], expected);
        let addr = cli(
            &binary,
            &client,
            &["dial", "server", "bench-sink", "--tcp", "127.0.0.1:0"],
        )?;
        let mut measured = measure(&addr, bytes, streams, trials).await?;
        measured["probe_transport"] = expected.into();
        carriers[mode] = measured;
        cli(&binary, &client, &["down"])?;
        cli(&binary, &server, &["down"])?;
        drop(client_daemon);
        drop(server_daemon);
    }
    let lock: toml::Value = toml::from_str(include_str!("../../Cargo.lock"))?;
    let relay_version = lock["package"]
        .as_array()
        .context("lock has no packages")?
        .iter()
        .find(|package| package["name"].as_str() == Some("iroh-relay"))
        .and_then(|package| package["version"].as_str())
        .context("lock has no relay server version")?;
    let receipt = serde_json::json!({"version": version, "binary_sha256": Sha256::digest(fs::read(&binary)?).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "workload": "verified one-way payload to identical loopback TCP sink, application acknowledgment; fresh local sessions, warm peer transport",
        "relay": "owned local iroh-relay, HTTP without TLS; no pasta hop", "relay_version": relay_version, "carriers": carriers,
        "platform": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH},
        "load_before": load_before, "load_after": fs::read_to_string("/proc/loadavg").ok().map(|text| text.split_whitespace().take(3).map(str::to_string).collect::<Vec<_>>())});
    println!("MEASURE {}", receipt);
    if let Some(output) = std::env::var_os("FABRIC_BENCH_OUTPUT") {
        fs::write(output, serde_json::to_string_pretty(&receipt)? + "\n")?;
    }
    sink.abort();
    relay.shutdown().await?;
    Ok(())
}
