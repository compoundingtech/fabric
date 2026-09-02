//! Git's native smart protocol over an authenticated Fabric connection.

use std::{
    collections::HashMap,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use iroh::EndpointId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
    sync::{Semaphore, mpsc},
};

use crate::{
    config::{Denied, FabricHome, GitAccess, PeerBook, validate_git_remote_name},
    control::{ControlRequest, ControlResponse},
    daemon::send_control,
};

pub const GIT_ALPN: &[u8] = b"fabric/git/1";
pub const GIT_PROTOCOL: &str = "fabric/git/1";
const MAX_CONTROL_FRAME: usize = 16 * 1024;
const MAX_OUTPUT_FRAME: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_GIT_SESSIONS: usize = 8;
pub const MAX_GIT_SESSIONS_PER_PEER: usize = 4;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GitOperation {
    Read,
    Write,
}

impl GitOperation {
    fn access(self) -> GitAccess {
        match self {
            Self::Read => GitAccess::Read,
            Self::Write => GitAccess::Write,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }

    fn git_service(self) -> &'static str {
        match self {
            Self::Read => "upload-pack",
            Self::Write => "receive-pack",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionRequest {
    remote: String,
    operation: GitOperation,
    git_protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum SessionResponse {
    Ready,
    Denied {
        no_grants: bool,
        requester: String,
        required: String,
    },
    Unavailable { message: String },
    Busy,
}

#[derive(Debug)]
enum OutputFrame {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
}

#[derive(Debug, Clone)]
pub struct GitSessionLimits {
    total: Arc<Semaphore>,
    per_peer: Arc<Mutex<HashMap<EndpointId, usize>>>,
    max_per_peer: usize,
}

impl Default for GitSessionLimits {
    fn default() -> Self {
        Self::new(MAX_GIT_SESSIONS, MAX_GIT_SESSIONS_PER_PEER)
    }
}

impl GitSessionLimits {
    pub fn new(total: usize, max_per_peer: usize) -> Self {
        Self {
            total: Arc::new(Semaphore::new(total)),
            per_peer: Arc::new(Mutex::new(HashMap::new())),
            max_per_peer,
        }
    }

    fn try_acquire(&self, peer: EndpointId) -> Option<GitSessionPermit> {
        let total = self.total.clone().try_acquire_owned().ok()?;
        let mut counts = self.per_peer.lock().unwrap();
        let count = counts.entry(peer).or_default();
        if *count >= self.max_per_peer {
            return None;
        }
        *count += 1;
        drop(counts);
        Some(GitSessionPermit {
            _total: total,
            peer,
            per_peer: self.per_peer.clone(),
        })
    }
}

struct GitSessionPermit {
    _total: tokio::sync::OwnedSemaphorePermit,
    peer: EndpointId,
    per_peer: Arc<Mutex<HashMap<EndpointId, usize>>>,
}

impl Drop for GitSessionPermit {
    fn drop(&mut self) {
        let mut counts = self.per_peer.lock().unwrap();
        if let Some(count) = counts.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.peer);
            }
        }
    }
}

pub fn invoked_as_remote_helper() -> bool {
    std::env::args_os()
        .next()
        .as_deref()
        .and_then(OsStr::to_str)
        .and_then(|path| std::path::Path::new(path).file_name())
        == Some(OsStr::new("git-remote-fabric"))
}

pub fn helper_path_for(binary: &Path) -> Result<PathBuf> {
    let directory = binary
        .parent()
        .context("the Fabric binary has no parent directory")?;
    Ok(directory.join("git-remote-fabric"))
}

pub fn helper_is_installed_for(binary: &Path) -> Result<bool> {
    let helper = helper_path_for(binary)?;
    let metadata = match fs::symlink_metadata(&helper) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    Ok(fs::read_link(&helper)? == Path::new("fabric"))
}

pub fn validate_helper_install(binary: &Path) -> Result<()> {
    let helper = helper_path_for(binary)?;
    match fs::symlink_metadata(&helper) {
        Ok(_) if helper_is_installed_for(binary)? => Ok(()),
        Ok(_) => bail!(
            "refusing to replace unrelated Git helper at {}; move it first",
            helper.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn install_helper_for(binary: &Path) -> Result<PathBuf> {
    validate_helper_install(binary)?;
    let helper = helper_path_for(binary)?;
    if helper_is_installed_for(binary)? {
        return Ok(helper);
    }
    std::os::unix::fs::symlink("fabric", &helper)
        .with_context(|| format!("failed to install Git helper at {}", helper.display()))?;
    Ok(helper)
}

pub async fn run_remote_helper() -> Result<i32> {
    let args = std::env::args().collect::<Vec<_>>();
    let url = args
        .get(2)
        .context("Git did not supply a fabric:// remote URL")?;
    let (peer, remote) = parse_url(url)?;
    let home = FabricHome::resolve(None)?;

    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).await? == 0 {
            return Ok(0);
        }
        let command = line.trim_end_matches(['\r', '\n']);
        match command {
            "capabilities" => {
                output.write_all(b"connect\n\n").await?;
                output.flush().await?;
            }
            "connect git-upload-pack" => {
                let raw_input = input.into_inner();
                return run_connected_helper(
                    &home,
                    &peer,
                    &remote,
                    GitOperation::Read,
                    raw_input,
                    output,
                )
                .await;
            }
            "connect git-receive-pack" => {
                let raw_input = input.into_inner();
                return run_connected_helper(
                    &home,
                    &peer,
                    &remote,
                    GitOperation::Write,
                    raw_input,
                    output,
                )
                .await;
            }
            "" => return Ok(0),
            other => bail!("Git asked the Fabric helper for unsupported command {other:?}"),
        }
    }
}

async fn run_connected_helper<R, W>(
    home: &FabricHome,
    peer: &str,
    remote: &str,
    operation: GitOperation,
    input: R,
    mut output: W,
) -> Result<i32>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let request = SessionRequest {
        remote: remote.to_string(),
        operation,
        git_protocol: valid_git_protocol(std::env::var("GIT_PROTOCOL").ok())?,
    };
    let (stream, response) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let response = send_control(
            home,
            ControlRequest::Git {
                peer: peer.to_string(),
            },
        )
        .await
        .with_context(|| "the running Fabric daemon could not open a Git transport")?;
        let socket = match response {
            ControlResponse::Git { socket } => socket,
            other => bail!("the running Fabric daemon returned an unexpected reply: {other:?}"),
        };
        let mut stream = tokio::net::UnixStream::connect(&socket)
            .await
            .with_context(|| format!("failed to connect to Fabric at {}", socket.display()))?;
        write_json(&mut stream, &request).await?;
        let response = read_json::<_, SessionResponse>(&mut stream).await?;
        Ok::<_, anyhow::Error>((stream, response))
    })
    .await
    .with_context(|| format!("peer {peer:?} gave no Git answer within 10 seconds"))??;
    match response {
        SessionResponse::Ready => {}
        SessionResponse::Denied {
            no_grants,
            requester,
            required,
        } => {
            if no_grants {
                bail!(
                    "{peer} has no grants for this machine; required grant: {required}\n\
                     on {peer}, run: fabric git grant {remote} {requester} --{}",
                    operation.name()
                );
            }
            bail!(
                "{peer} did not grant {} access to Git remote {remote:?}\n\
                 on {peer}, run: fabric git grant {remote} {requester} --{}",
                operation.name(),
                operation.name()
            );
        }
        SessionResponse::Unavailable { message } => bail!("{message}"),
        SessionResponse::Busy => {
            bail!("the Git service on {peer} is busy; retry this command")
        }
    }

    output.write_all(b"\n").await?;
    output.flush().await?;
    let (mut remote_read, mut remote_write) = stream.into_split();
    let input_task = tokio::spawn(async move {
        let mut input = input;
        tokio::io::copy(&mut input, &mut remote_write).await?;
        remote_write.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    });
    tokio::pin!(input_task);
    let mut error = tokio::io::stderr();
    loop {
        let frame = read_output_frame(&mut remote_read).await?;
        match frame {
            OutputFrame::Stdout(bytes) => {
                output.write_all(&bytes).await?;
                output.flush().await?;
            }
            OutputFrame::Stderr(bytes) => {
                error.write_all(&bytes).await?;
                error.flush().await?;
            }
            OutputFrame::Exit(code) => {
                input_task.abort();
                return Ok(if code < 0 { 1 } else { code });
            }
        }
    }
}

pub async fn serve_session<R, W>(
    mut recv: R,
    mut send: W,
    book: PeerBook,
    peer: EndpointId,
    limits: GitSessionLimits,
) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let request = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        read_json::<_, SessionRequest>(&mut recv),
    )
    .await
    .context("the peer did not send a Git request within 10 seconds")??;
    validate_git_remote_name(&request.remote)?;
    let required = request.operation.access().permission(&request.remote);
    let requester = book
        .peers()
        .iter()
        .find(|entry| entry.id == peer)
        .and_then(|entry| entry.name.clone())
        .unwrap_or_else(|| peer.to_string());
    let permission = book.may(&peer, &required);
    let remote = book.git_remote(&request.remote);
    if permission.is_err() || remote.is_none() {
        let no_grants = matches!(permission, Err(Denied::NoGrants { .. }));
        write_json(
            &mut send,
            &SessionResponse::Denied {
                no_grants,
                requester,
                required,
            },
        )
        .await?;
        send.shutdown().await?;
        return Ok(());
    }
    let remote = remote.unwrap();
    if !remote.path.is_dir() {
        write_json(
            &mut send,
            &SessionResponse::Unavailable {
                message: format!(
                    "granted Git remote {:?} is unavailable on this peer",
                    request.remote
                ),
            },
        )
        .await?;
        send.shutdown().await?;
        return Ok(());
    }
    let Some(_permit) = limits.try_acquire(peer) else {
        write_json(&mut send, &SessionResponse::Busy).await?;
        send.shutdown().await?;
        return Ok(());
    };

    let mut command = Command::new("git");
    command
        .arg(request.operation.git_service())
        .arg(&remote.path)
        .env("FABRIC_PEER", peer.to_string())
        .env("FABRIC_GIT_REMOTE", &request.remote)
        .env("FABRIC_GIT_ACCESS", request.operation.name())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(protocol) = request.git_protocol {
        command.env("GIT_PROTOCOL", protocol);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            write_json(
                &mut send,
                &SessionResponse::Unavailable {
                    message: format!("failed to start host Git service: {error}"),
                },
            )
            .await?;
            send.shutdown().await?;
            return Ok(());
        }
    };
    let child_stdin = child.stdin.take().context("Git child has no stdin")?;
    let child_stdout = child.stdout.take().context("Git child has no stdout")?;
    let child_stderr = child.stderr.take().context("Git child has no stderr")?;
    write_json(&mut send, &SessionResponse::Ready).await?;

    let (frames_tx, frames_rx) = mpsc::channel(8);
    let stdout_task = tokio::spawn(pump_output(child_stdout, 1, frames_tx.clone()));
    let stderr_task = tokio::spawn(pump_output(child_stderr, 2, frames_tx.clone()));
    let mut writer_task = tokio::spawn(write_output_frames(send, frames_rx));
    let mut input_task = tokio::spawn(async move {
        let mut child_stdin = child_stdin;
        tokio::io::copy(&mut recv, &mut child_stdin).await?;
        child_stdin.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    });

    let status = tokio::select! {
        status = child.wait() => status?,
        input = &mut input_task => {
            input.context("Git input task failed")??;
            tokio::select! {
                status = child.wait() => status?,
                output = &mut writer_task => {
                    output.context("Git output task failed")??;
                    bail!("the Git output connection ended before the host Git process")
                }
            }
        }
        output = &mut writer_task => {
            output.context("Git output task failed")??;
            bail!("the Git output connection ended before the host Git process")
        }
    };
    input_task.abort();
    stdout_task.await.context("Git stdout task failed")??;
    stderr_task.await.context("Git stderr task failed")??;
    let code = status.code().unwrap_or(-1);
    frames_tx.send(OutputFrame::Exit(code)).await.ok();
    drop(frames_tx);
    writer_task.await.context("Git output task failed")??;
    Ok(())
}

async fn pump_output<R>(mut input: R, kind: u8, sender: mpsc::Sender<OutputFrame>) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = vec![0; MAX_OUTPUT_FRAME];
    loop {
        let read = input.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        let frame = match kind {
            1 => OutputFrame::Stdout(buffer[..read].to_vec()),
            _ => OutputFrame::Stderr(buffer[..read].to_vec()),
        };
        if sender.send(frame).await.is_err() {
            return Ok(());
        }
    }
}

async fn write_output_frames<W>(mut output: W, mut frames: mpsc::Receiver<OutputFrame>) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = frames.recv().await {
        write_output_frame(&mut output, &frame).await?;
    }
    output.shutdown().await?;
    Ok(())
}

fn parse_url(url: &str) -> Result<(String, String)> {
    let rest = url
        .strip_prefix("fabric://")
        .with_context(|| format!("Git remote URL must start with fabric://, got {url:?}"))?;
    let parts = rest.split('/').collect::<Vec<_>>();
    if parts.len() != 2 {
        bail!("Git remote URL must be fabric://<peer>/<remote>, got {url:?}");
    }
    validate_url_segment(parts[0], "peer")?;
    validate_git_remote_name(parts[1])?;
    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn validate_url_segment(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > 64 || matches!(value, "." | "..") {
        bail!("Git remote {label} segment is invalid: {value:?}");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("Git remote {label} segment contains an invalid byte: {value:?}");
    }
    Ok(())
}

fn valid_git_protocol(value: Option<String>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.len() > 256
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_graphic() || byte == b'\\')
    {
        bail!("GIT_PROTOCOL contains invalid or excessive data");
    }
    Ok(Some(value))
}

async fn write_json<W, T>(output: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() > MAX_CONTROL_FRAME {
        bail!("Git control frame is too large");
    }
    output.write_u32(encoded.len() as u32).await?;
    output.write_all(&encoded).await?;
    output.flush().await?;
    Ok(())
}

async fn read_json<R, T>(input: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = input.read_u32().await? as usize;
    if len > MAX_CONTROL_FRAME {
        bail!("Git control frame is too large: {len} bytes");
    }
    let mut encoded = vec![0; len];
    input.read_exact(&mut encoded).await?;
    serde_json::from_slice(&encoded).context("invalid Git control frame")
}

async fn write_output_frame<W>(output: &mut W, frame: &OutputFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let (kind, bytes) = match frame {
        OutputFrame::Stdout(bytes) => (1, bytes.as_slice()),
        OutputFrame::Stderr(bytes) => (2, bytes.as_slice()),
        OutputFrame::Exit(code) => {
            output.write_u8(3).await?;
            output.write_u32(4).await?;
            output.write_i32(*code).await?;
            output.flush().await?;
            return Ok(());
        }
    };
    output.write_u8(kind).await?;
    output.write_u32(bytes.len() as u32).await?;
    output.write_all(bytes).await?;
    output.flush().await?;
    Ok(())
}

async fn read_output_frame<R>(input: &mut R) -> Result<OutputFrame>
where
    R: AsyncRead + Unpin,
{
    let kind = input.read_u8().await?;
    let len = input.read_u32().await? as usize;
    if len > MAX_OUTPUT_FRAME {
        bail!("Git output frame is too large: {len} bytes");
    }
    if kind == 3 {
        if len != 4 {
            bail!("Git exit frame has invalid length {len}");
        }
        return Ok(OutputFrame::Exit(input.read_i32().await?));
    }
    let mut bytes = vec![0; len];
    input.read_exact(&mut bytes).await?;
    match kind {
        1 => Ok(OutputFrame::Stdout(bytes)),
        2 => Ok(OutputFrame::Stderr(bytes)),
        _ => bail!("unknown Git output frame kind {kind}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fabric_urls_are_strict_and_path_free() {
        assert_eq!(
            parse_url("fabric://hetz/mandat").unwrap(),
            ("hetz".to_string(), "mandat".to_string())
        );
        for invalid in [
            "https://hetz/mandat",
            "fabric://hetz/a/b",
            "fabric://user@hetz/mandat",
            "fabric://hetz:443/mandat",
            "fabric://hetz/../mandat",
            "fabric://hetz/mandat?x=1",
            "fabric://hetz/percent%20name",
        ] {
            assert!(parse_url(invalid).is_err(), "invalid URL passed: {invalid}");
        }
    }

    #[tokio::test]
    async fn output_frames_keep_stdout_stderr_and_exit_separate() {
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let task = tokio::spawn(async move {
            write_output_frame(&mut writer, &OutputFrame::Stdout(b"pack".to_vec()))
                .await
                .unwrap();
            write_output_frame(&mut writer, &OutputFrame::Stderr(b"hook".to_vec()))
                .await
                .unwrap();
            write_output_frame(&mut writer, &OutputFrame::Exit(7))
                .await
                .unwrap();
        });
        assert!(matches!(
            read_output_frame(&mut reader).await.unwrap(),
            OutputFrame::Stdout(bytes) if bytes == b"pack"
        ));
        assert!(matches!(
            read_output_frame(&mut reader).await.unwrap(),
            OutputFrame::Stderr(bytes) if bytes == b"hook"
        ));
        assert!(matches!(
            read_output_frame(&mut reader).await.unwrap(),
            OutputFrame::Exit(7)
        ));
        task.await.unwrap();
    }

    #[test]
    fn the_session_limits_are_immediate_and_per_peer() {
        let limits = GitSessionLimits::new(3, 2);
        let first = iroh::SecretKey::generate().public();
        let second = iroh::SecretKey::generate().public();
        let a = limits.try_acquire(first).unwrap();
        let b = limits.try_acquire(first).unwrap();
        assert!(limits.try_acquire(first).is_none());
        let c = limits.try_acquire(second).unwrap();
        assert!(limits.try_acquire(second).is_none());
        drop(a);
        assert!(limits.try_acquire(first).is_some());
        drop((b, c));
    }

    #[test]
    fn helper_install_is_relative_idempotent_and_refuses_an_unrelated_file() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("fabric");
        fs::write(&binary, b"binary").unwrap();

        let helper = install_helper_for(&binary).unwrap();
        assert_eq!(fs::read_link(&helper).unwrap(), Path::new("fabric"));
        assert_eq!(install_helper_for(&binary).unwrap(), helper);

        fs::remove_file(&helper).unwrap();
        fs::write(&helper, b"somebody else's helper").unwrap();
        let error = install_helper_for(&binary).unwrap_err().to_string();
        assert!(error.contains("refusing"), "wrong error: {error}");
        assert_eq!(fs::read(&helper).unwrap(), b"somebody else's helper");
    }
}
